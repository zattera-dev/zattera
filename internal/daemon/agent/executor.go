package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/logstore"
	"github.com/zattera-dev/zattera/internal/daemon/runtime"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// Executor-owned label keys (identity beyond the shared runtime.Label* set).
const (
	labelConfigHash = "dev.zattera/config-hash"
	labelReleaseID  = "dev.zattera/release-id"
)

// maxAttempts caps create/start retries for one config_hash before the
// assignment is parked (no further attempts until its config changes).
const maxAttempts = 3

// defaultStopGrace is the SIGTERM→SIGKILL window when the spec omits one.
const defaultStopGrace = 10 * time.Second

// StatusSink receives observed instance-status transitions. The agent wires it
// to enqueue a StatusBatch on the sync stream.
type StatusSink func(observed map[string]*zatterav1.AssignmentObserved)

// ExecutorConfig configures the reconciler.
type ExecutorConfig struct {
	NodeID string
	// HostIP is where published ports bind (the node's mesh IP, or 127.0.0.1 in
	// single-node/dev).
	HostIP  string
	Runtime runtime.ContainerRuntime
	Clock   clock.Clock
	Logger  *slog.Logger
	// Report emits status transitions. Required in production; tests capture it.
	Report StatusSink
	// RegistryAuth pulls private images (from the join credential, T-17). May be
	// nil for public images.
	RegistryAuth *runtime.RegistryAuth
	// PollInterval drives the liveness re-check + retry tick (default 5s).
	PollInterval time.Duration
}

// Executor converges the local container runtime to the assignment set the
// control plane pushes. It is the agent's reconcile loop: idempotent,
// crash-safe (adopts existing containers by label on restart), and never
// touches containers it does not own.
type Executor struct {
	cfg   ExecutorConfig
	rt    runtime.ContainerRuntime
	clock clock.Clock
	log   *slog.Logger

	// latest is the most recent set to converge to; the Run loop coalesces
	// bursts to it.
	sig    chan struct{}
	mu     sync.Mutex
	latest *clusterv1.AssignmentSet

	// failCount/failHash track create/start failures per assignment id for the
	// retry-then-park policy. Accessed only from the reconcile path (serial).
	failCount map[string]int
	failHash  map[string]string

	// health probes running instances and reports HEALTHY/UNHEALTHY. Built in
	// Run (bounded by its context); nil when reconcile is driven directly
	// (e.g. unit tests that assert raw RUNNING transitions).
	health *Manager
}

// NewExecutor builds an Executor, filling defaults.
func NewExecutor(cfg ExecutorConfig) *Executor {
	if cfg.Clock == nil {
		cfg.Clock = clock.Real{}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.HostIP == "" {
		cfg.HostIP = "127.0.0.1"
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 5 * time.Second
	}
	return &Executor{
		cfg:       cfg,
		rt:        cfg.Runtime,
		clock:     cfg.Clock,
		log:       cfg.Logger,
		sig:       make(chan struct{}, 1),
		failCount: map[string]int{},
		failHash:  map[string]string{},
	}
}

// Submit hands the executor the latest assignment set (coalescing: only the
// newest pending set is kept) and wakes the Run loop.
func (e *Executor) Submit(set *clusterv1.AssignmentSet) {
	e.mu.Lock()
	e.latest = set
	e.mu.Unlock()
	select {
	case e.sig <- struct{}{}:
	default:
	}
}

// Run drives reconciliation: on every submitted set and on a periodic tick
// (which retries failed assignments and re-checks liveness) until ctx ends.
func (e *Executor) Run(ctx context.Context) {
	// Health probes are bounded by this run's lifetime.
	e.health = NewManager(ctx, ManagerConfig{
		Clock:   e.clock,
		Report:  e.cfg.Report,
		Runtime: e.rt,
		HostIP:  e.cfg.HostIP,
		Logger:  e.log,
	})
	tick := e.clock.NewTicker(e.cfg.PollInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-e.sig:
			e.reconcile(ctx, e.current())
		case <-tick.C():
			e.reconcile(ctx, e.current())
			e.pollLiveness(ctx)
		}
	}
}

func (e *Executor) current() *clusterv1.AssignmentSet {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.latest
}

// MatchingStreams returns the assignment ids on this node whose metadata match
// the log selector (used by the agent log query server, T-54).
func (e *Executor) MatchingStreams(sel *zatterav1.LogSelector) []logstore.StreamID {
	var out []logstore.StreamID
	for _, a := range e.current().GetAssignments() {
		if sel.GetInstanceId() != "" && a.GetMeta().GetId() != sel.GetInstanceId() {
			continue
		}
		if sel.GetProjectId() != "" && a.GetProjectId() != sel.GetProjectId() {
			continue
		}
		if sel.GetAppId() != "" && a.GetAppId() != sel.GetAppId() {
			continue
		}
		if sel.GetEnvironmentId() != "" && a.GetEnvironmentId() != sel.GetEnvironmentId() {
			continue
		}
		if sel.GetJobId() != "" && a.GetJobId() != sel.GetJobId() {
			continue
		}
		out = append(out, logstore.StreamID(a.GetMeta().GetId()))
	}
	return out
}

// reconcile converges the runtime to set: it removes containers whose
// assignment is gone / stopped / config-changed, then creates+starts missing
// desired-RUN containers. STOP is applied before RUN so freed host ports are
// available to new containers.
func (e *Executor) reconcile(ctx context.Context, set *clusterv1.AssignmentSet) {
	if set == nil {
		return
	}
	desired := e.desiredByID(set)
	runtimes := set.GetRuntime()

	current, err := e.listOwned(ctx)
	if err != nil {
		e.log.Warn("executor: list containers failed", "err", err)
		return
	}

	observed := map[string]*zatterav1.AssignmentObserved{}

	// Classify current containers: stop (gone/STOP) vs replace (hash drift) vs
	// adopt (matching RUN). Everything not adopted needs a (re)create.
	adopted := map[string]bool{}
	var toRemove []ownedContainer
	for id, c := range current {
		a, ok := desired[id]
		switch {
		case !ok || a.GetDesired() != zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN:
			toRemove = append(toRemove, c)
			if ok {
				observed[id] = observe(zatterav1.InstanceState_INSTANCE_STATE_STOPPED, c.id, "", e.now())
			}
		case c.labels[labelConfigHash] != a.GetConfigHash():
			toRemove = append(toRemove, c) // config drift → replace
		default:
			adopted[id] = true
		}
	}

	// STOP pass (deterministic order).
	sort.Slice(toRemove, func(i, j int) bool { return toRemove[i].assignID < toRemove[j].assignID })
	for _, c := range toRemove {
		e.stopAndRemove(ctx, c)
		if e.health != nil {
			e.health.Remove(c.assignID)
		}
	}

	// live tracks assignments with a running container (adopted or newly
	// started) so the health manager can prune monitors for the rest.
	live := map[string]string{} // assignment id → container id
	for id := range adopted {
		live[id] = current[id].id
	}

	// RUN pass: create+start every desired-RUN assignment without a live match.
	for _, id := range sortedKeys(desired) {
		a := desired[id]
		if a.GetDesired() != zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN || adopted[id] {
			continue
		}
		obs := e.bringUp(ctx, a, runtimes[id])
		if obs == nil {
			continue
		}
		observed[id] = obs
		if obs.GetState() == zatterav1.InstanceState_INSTANCE_STATE_RUNNING {
			live[id] = obs.GetContainerId()
		}
	}

	// Emit the RUNNING/STOPPED batch before health so a subsequent HEALTHY
	// transition is not clobbered by the RUNNING report.
	if len(observed) > 0 {
		e.emit(observed)
	}

	// Health monitoring: prune monitors for gone instances, ensure one per live
	// instance (idempotent). Skipped when reconcile runs without a manager.
	if e.health != nil {
		liveSet := make(map[string]bool, len(live))
		for id := range live {
			liveSet[id] = true
		}
		e.health.Reconcile(liveSet)
		for id, cid := range live {
			e.health.Ensure(ctx, desired[id], runtimes[id].GetSpec(), cid)
		}
	}
}

// bringUp pulls the image, ensures volumes, creates and starts the container,
// and returns the observed transition (RUNNING with bound ports, or FAILED).
// nil means the assignment is parked (already failed maxAttempts for this hash).
func (e *Executor) bringUp(ctx context.Context, a *zatterav1.Assignment, rt *clusterv1.AssignmentRuntime) *zatterav1.AssignmentObserved {
	id := a.GetMeta().GetId()
	hash := a.GetConfigHash()

	// Reset the retry counter when the config changed; skip parked assignments.
	if e.failHash[id] != "" && e.failHash[id] != hash {
		delete(e.failCount, id)
		delete(e.failHash, id)
	}
	if e.failCount[id] >= maxAttempts {
		return nil // parked until config changes
	}
	if rt == nil {
		return e.fail(id, hash, "no runtime payload (image/spec) for assignment")
	}

	e.emit(map[string]*zatterav1.AssignmentObserved{
		id: observe(zatterav1.InstanceState_INSTANCE_STATE_PULLING, "", "", e.now()),
	})
	if err := e.rt.EnsureImage(ctx, rt.GetImageRef(), e.cfg.RegistryAuth, nil); err != nil {
		return e.fail(id, hash, fmt.Sprintf("pull %s: %v", rt.GetImageRef(), err))
	}

	spec, err := e.containerSpec(ctx, a, rt)
	if err != nil {
		return e.fail(id, hash, err.Error())
	}

	cid, err := e.rt.CreateContainer(ctx, spec)
	if err != nil {
		return e.fail(id, hash, fmt.Sprintf("create: %v", err))
	}
	e.emit(map[string]*zatterav1.AssignmentObserved{
		id: observe(zatterav1.InstanceState_INSTANCE_STATE_STARTING, cid, "", e.now()),
	})
	if err := e.rt.StartContainer(ctx, cid); err != nil {
		// Roll back the created-but-unstarted container so the next reconcile
		// doesn't adopt a dead one.
		_ = e.rt.RemoveContainer(ctx, cid, true)
		return e.fail(id, hash, fmt.Sprintf("start: %v", err))
	}

	delete(e.failCount, id)
	delete(e.failHash, id)

	obs := observe(zatterav1.InstanceState_INSTANCE_STATE_RUNNING, cid, "", e.now())
	if st, err := e.rt.InspectContainer(ctx, cid); err == nil {
		// Correlate the inspected host ports back to PortSpec names by container
		// port — Docker's inspect data has no notion of our port names, so the
		// spec is the only place the name→container_port mapping lives (routing
		// keys mesh_port_bindings by name).
		obs.MeshPortBindings = namedPortBindings(st.Ports, rt.GetSpec().GetPorts())
	}
	return obs
}

// namedPortBindings maps each inspected host binding to its PortSpec name via the
// container port.
func namedPortBindings(inspected []runtime.PortBinding, ports []*zatterav1.PortSpec) map[string]uint32 {
	nameByPort := make(map[uint32]string, len(ports))
	for _, p := range ports {
		nameByPort[p.GetContainerPort()] = p.GetName()
	}
	out := map[string]uint32{}
	for _, b := range inspected {
		if b.HostPort == 0 {
			continue
		}
		if name := nameByPort[b.ContainerPort]; name != "" {
			out[name] = b.HostPort
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// stopAndRemove stops (grace) then force-removes a container we own.
func (e *Executor) stopAndRemove(ctx context.Context, c ownedContainer) {
	if err := e.rt.StopContainer(ctx, c.id, 0); err != nil && !isNotFound(err) {
		e.log.Warn("executor: stop failed", "container", c.id, "err", err)
	}
	if err := e.rt.RemoveContainer(ctx, c.id, true); err != nil && !isNotFound(err) {
		e.log.Warn("executor: remove failed", "container", c.id, "err", err)
	}
}

// containerSpec builds the runtime spec from the assignment + its resolved
// runtime payload (image, frozen ServiceSpec, decrypted env).
func (e *Executor) containerSpec(ctx context.Context, a *zatterav1.Assignment, rt *clusterv1.AssignmentRuntime) (runtime.ContainerSpec, error) {
	spec := rt.GetSpec()

	var mounts []runtime.Mount
	for _, vm := range spec.GetVolumes() {
		volName := volumeName(a.GetEnvironmentId(), vm.GetVolumeName())
		if err := e.rt.EnsureVolume(ctx, volName, e.labels(a, "")); err != nil {
			return runtime.ContainerSpec{}, fmt.Errorf("ensure volume %s: %w", volName, err)
		}
		mounts = append(mounts, runtime.Mount{VolumeName: volName, Target: vm.GetMountPath()})
	}

	var ports []runtime.PortBinding
	for _, p := range spec.GetPorts() {
		ports = append(ports, runtime.PortBinding{
			Name:          p.GetName(),
			ContainerPort: p.GetContainerPort(),
			Protocol:      protocol(p.GetProtocol()),
			HostIP:        e.cfg.HostIP,
			HostPort:      0, // Docker allocates; reported back after Start.
		})
	}

	// Per-(project,env,node) bridge network (T-46): control sends the allocated
	// subnet; attach the container and point its DNS at the network gateway
	// (where the internal resolver binds, T-47).
	var network string
	var dns []string
	if subnet := rt.GetSubnetCidr(); subnet != "" {
		name := NetworkName(a.GetProjectId(), a.GetEnvironmentId())
		info, err := e.rt.EnsureNetwork(ctx, runtime.NetworkSpec{Name: name, Subnet: subnet, Labels: e.labels(a, "")})
		if err != nil {
			return runtime.ContainerSpec{}, fmt.Errorf("ensure network %s: %w", name, err)
		}
		network = info.Name
		gw := info.Gateway
		if gw == "" {
			gw, _ = GatewayIP(subnet)
		}
		if gw != "" {
			dns = []string{gw}
		}
	}

	var command, entrypoint []string
	if c := strings.TrimSpace(spec.GetCommand()); c != "" {
		// A one-shot job runs an arbitrary command that must replace the
		// container's process. Override the image ENTRYPOINT: leaving it in
		// place would pass the command as arguments to it (e.g. a web-server
		// entrypoint that ignores them and runs forever, so the job never
		// exits). Services keep the historical CMD-only behavior.
		if a.GetJobId() != "" {
			entrypoint = []string{"/bin/sh", "-c", c}
		} else {
			command = []string{"/bin/sh", "-c", c}
		}
	}

	stopGrace := defaultStopGrace
	if g := spec.GetStopGrace().AsDuration(); g > 0 {
		stopGrace = g
	}

	// Jobs (T-53) are one-shot: never let Docker restart a completed job
	// container. The scheduler observes the exit and reaps the assignment.
	restart := runtime.RestartUnlessStopped
	if a.GetJobId() != "" {
		restart = runtime.RestartNever
	}

	return runtime.ContainerSpec{
		Name:       containerName(a),
		Image:      rt.GetImageRef(),
		Command:    command,
		Entrypoint: entrypoint,
		Env:        envList(rt.GetEnv()),
		Labels:     e.labels(a, runtime.LabelRole),
		Ports:      ports,
		Mounts:     mounts,
		Resources: runtime.Resources{
			CPUMillis: spec.GetResources().GetCpuMillis(),
			MemoryMB:  spec.GetResources().GetMemoryMb(),
		},
		Restart:   restart,
		StopGrace: stopGrace,
		Network:   network,
		DNS:       dns,
	}, nil
}

// pollLiveness inspects owned containers and reports state changes so the
// control plane learns about crashes between assignment pushes. Health probes
// (T-16) refine this; here we report RUNNING vs a non-zero exit as FAILED.
func (e *Executor) pollLiveness(ctx context.Context) {
	current, err := e.listOwned(ctx)
	if err != nil {
		return
	}
	runtimes := e.current().GetRuntime()
	observed := map[string]*zatterav1.AssignmentObserved{}
	for id, c := range current {
		st, err := e.rt.InspectContainer(ctx, c.id)
		if err != nil {
			continue
		}
		switch {
		case st.Running:
			obs := observe(zatterav1.InstanceState_INSTANCE_STATE_RUNNING, c.id, "", e.now())
			// Re-report bound host ports (bringUp's inspect can race Docker's
			// port allocation and see none); keyed by PortSpec name for routing.
			obs.MeshPortBindings = namedPortBindings(st.Ports, runtimes[id].GetSpec().GetPorts())
			observed[id] = obs
		case st.ExitCode != 0:
			obs := observe(zatterav1.InstanceState_INSTANCE_STATE_FAILED, c.id, fmt.Sprintf("exited with code %d", st.ExitCode), e.now())
			obs.ExitCode = int32(st.ExitCode)
			observed[id] = obs
		default:
			observed[id] = observe(zatterav1.InstanceState_INSTANCE_STATE_STOPPED, c.id, "", e.now())
		}
	}
	if len(observed) > 0 {
		e.emit(observed)
	}
}

// --- helpers --------------------------------------------------------------

// ownedContainer is a managed service container the agent tracks by label.
type ownedContainer struct {
	id       string
	assignID string
	labels   map[string]string
}

// listOwned returns our managed service containers indexed by assignment id.
func (e *Executor) listOwned(ctx context.Context) (map[string]ownedContainer, error) {
	infos, err := e.rt.ListContainers(ctx, map[string]string{
		runtime.ManagedLabel: "true",
		runtime.LabelRole:    "service",
	})
	if err != nil {
		return nil, err
	}
	out := make(map[string]ownedContainer, len(infos))
	for _, in := range infos {
		aid := in.Labels[runtime.LabelAssignmentID]
		if aid == "" {
			continue // never touch a managed container without an assignment id
		}
		out[aid] = ownedContainer{id: in.ID, assignID: aid, labels: in.Labels}
	}
	return out, nil
}

func (e *Executor) desiredByID(set *clusterv1.AssignmentSet) map[string]*zatterav1.Assignment {
	out := map[string]*zatterav1.Assignment{}
	for _, a := range set.GetAssignments() {
		if a.GetNodeId() != "" && e.cfg.NodeID != "" && a.GetNodeId() != e.cfg.NodeID {
			continue // defensive: control filters per node
		}
		out[a.GetMeta().GetId()] = a
	}
	return out
}

// fail records a failure for id, reports FAILED, and returns the observation.
func (e *Executor) fail(id, hash, msg string) *zatterav1.AssignmentObserved {
	e.failCount[id]++
	e.failHash[id] = hash
	if e.failCount[id] >= maxAttempts {
		e.log.Warn("executor: parking assignment after repeated failures", "assignment", id, "attempts", e.failCount[id], "err", msg)
	}
	obs := observe(zatterav1.InstanceState_INSTANCE_STATE_FAILED, "", msg, e.now())
	return obs
}

func (e *Executor) emit(observed map[string]*zatterav1.AssignmentObserved) {
	if e.cfg.Report != nil {
		e.cfg.Report(observed)
	}
}

func (e *Executor) labels(a *zatterav1.Assignment, roleKey string) map[string]string {
	l := map[string]string{
		runtime.ManagedLabel:       "true",
		runtime.LabelAssignmentID:  a.GetMeta().GetId(),
		runtime.LabelEnvironmentID: a.GetEnvironmentId(),
		runtime.LabelAppID:         a.GetAppId(),
		runtime.LabelProjectID:     a.GetProjectId(),
		labelConfigHash:            a.GetConfigHash(),
		labelReleaseID:             a.GetReleaseId(),
	}
	if roleKey != "" {
		l[roleKey] = "service"
	}
	return l
}

func (e *Executor) now() *timestamppb.Timestamp { return timestamppb.New(e.clock.Now()) }

func observe(state zatterav1.InstanceState, containerID, msg string, now *timestamppb.Timestamp) *zatterav1.AssignmentObserved {
	return &zatterav1.AssignmentObserved{
		State:       state,
		ContainerId: containerID,
		Message:     msg,
		UpdatedAt:   now,
	}
}

func containerName(a *zatterav1.Assignment) string {
	id := a.GetMeta().GetId()
	if len(id) > 8 {
		id = id[:8]
	}
	return fmt.Sprintf("zt-%s-%s-%s", short(a.GetAppId()), short(a.GetEnvironmentId()), id)
}

func short(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

func volumeName(envID, vol string) string { return "zt-" + short(envID) + "-" + vol }

func envList(env map[string]string) []string {
	if len(env) == 0 {
		return nil
	}
	out := make([]string, 0, len(env))
	for k, v := range env {
		out = append(out, k+"="+v)
	}
	sort.Strings(out) // deterministic for reproducible container config
	return out
}

func protocol(p zatterav1.Protocol) string {
	if p == zatterav1.Protocol_PROTOCOL_UDP {
		return "udp"
	}
	return "tcp"
}

func sortedKeys(m map[string]*zatterav1.Assignment) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func isNotFound(err error) bool { return errors.Is(err, runtime.ErrNotFound) }
