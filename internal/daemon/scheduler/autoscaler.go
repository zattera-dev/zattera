package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/leaderrunner"
	"github.com/zattera-dev/zattera/internal/daemon/livestate"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

const (
	// autoscaleTick is the evaluation cadence (spec F7).
	autoscaleTick = 15 * time.Second
	// scaleDownHold is how long a signal must stay below the down band before a
	// scale-down fires.
	scaleDownHold = 5 * time.Minute
	// scaleCooldown gates the next change for this long after any change, in
	// BOTH directions — the scale-up branch checks it too. (This comment used
	// to claim scale-up was exempt, which its own code has never done.)
	scaleCooldown = 3 * time.Minute
	// downBand: a scale-down only starts once utilization sits below this
	// fraction of target (hysteresis against flapping at the boundary).
	downBand = 0.8
)

// LiveView is the slice of livestate the autoscaler reads: the current live
// sample of every connected node.
type LiveView interface {
	Snapshot() []livestate.NodeState
}

// Autoscaler runs on the leader and adjusts each environment's
// effective_replicas from observed CPU/memory/RPS against its Autoscale targets
// (spec F7). It only writes effective_replicas; the scheduler (T-23) converges
// the replica count into assignments.
type Autoscaler struct {
	store *raftstore.Store
	live  LiveView
	clock clock.Clock
	log   *slog.Logger

	// Per-env in-memory timers, reset on every leadership term (conservative:
	// a new leader restarts the down-hold window). lowSince marks when an env
	// first became a scale-down candidate; lastChange gates the cooldown.
	lowSince   map[string]time.Time
	lastChange map[string]time.Time
}

// NewAutoscaler builds the autoscaler.
func NewAutoscaler(store *raftstore.Store, live LiveView, clk clock.Clock, log *slog.Logger) *Autoscaler {
	if log == nil {
		log = slog.Default()
	}
	if clk == nil {
		clk = clock.Real{}
	}
	return &Autoscaler{
		store:      store,
		live:       live,
		clock:      clk,
		log:        log,
		lowSince:   map[string]time.Time{},
		lastChange: map[string]time.Time{},
	}
}

// Run evaluates while this node leads, resetting hold timers on each term.
func (a *Autoscaler) Run(ctx context.Context) {
	leaderrunner.Run(ctx, a.store, a.clock, a.leaderLoop)
}

func (a *Autoscaler) leaderLoop(ctx context.Context) {
	// Fresh timers per leadership term (spec gotcha: leadership change resets
	// the in-memory windows).
	a.lowSince = map[string]time.Time{}
	a.lastChange = map[string]time.Time{}

	tick := a.clock.NewTicker(autoscaleTick)
	defer tick.Stop()
	for {
		if err := a.evaluate(ctx); errors.Is(err, raftstore.ErrNotLeader) {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-a.store.LeaderCh():
			if !a.store.IsLeader() {
				return
			}
		case <-tick.C():
		}
	}
}

// evaluate scales every autoscale-enabled environment once.
func (a *Autoscaler) evaluate(ctx context.Context) error {
	if !a.store.IsLeader() {
		return raftstore.ErrNotLeader
	}
	st := a.store.State()
	for _, env := range st.ListEnvironments("", "") {
		if err := a.evaluateEnv(ctx, st, env); err != nil {
			return err
		}
	}
	return nil
}

// evaluateEnv decides and applies the effective replica count for one env.
func (a *Autoscaler) evaluateEnv(ctx context.Context, st *state.Store, env *zatterav1.Environment) error {
	envID := env.GetMeta().GetId()
	auto := env.GetService().GetAutoscale()
	if !autoscaleConfigured(auto) || env.GetActiveReleaseId() == "" {
		return nil
	}
	if env.GetService().GetMaxConcurrency() > 0 {
		return nil // the serverless loop (T-71) owns concurrency-scaled envs
	}
	// A running deployment (T-26) owns placement; don't fight it.
	if a.deploymentActive(st, envID) {
		return nil
	}

	minRep := int(env.GetService().GetReplicas().GetMin())
	if minRep < 1 {
		minRep = 1 // effective_replicas=0 is reserved for scale-to-zero (T-71)
	}
	maxRep := int(env.GetService().GetReplicas().GetMax())
	if maxRep <= minRep {
		return nil // no room to scale
	}

	obs, ok := a.observe(st, env)
	if !ok {
		// Missing data (agent gap / no running replicas): freeze, never scale on
		// absent metrics. Hold timers are left intact.
		return nil
	}

	// The formula's replica base is the observed running count; the decision
	// compares the computed target against the currently-set effective count.
	desired, ok := scaleTarget(auto, obs, 1.0, minRep, maxRep)
	if !ok {
		return nil // a configured signal had no data → freeze
	}
	effective := desiredReplicas(env)
	if effective < 1 {
		effective = minRep
	}
	now := a.clock.Now()

	switch {
	case desired > effective:
		// Scale up: no hold, but still respect the post-change cooldown.
		delete(a.lowSince, envID)
		if a.inCooldown(envID, now) {
			return nil
		}
		return a.applyScale(ctx, st, env, effective, desired, now)

	case desired < effective:
		// Only a genuinely low signal (below the down band) starts the hold.
		low, lowOK := scaleTarget(auto, obs, downBand, minRep, maxRep)
		if !lowOK || low >= effective {
			delete(a.lowSince, envID) // in the deadband; not a down candidate
			return nil
		}
		if a.lowSince[envID].IsZero() {
			a.lowSince[envID] = now
		}
		if now.Sub(a.lowSince[envID]) < scaleDownHold {
			return nil // not held long enough yet
		}
		if a.inCooldown(envID, now) {
			return nil
		}
		return a.applyScale(ctx, st, env, effective, desired, now)

	default:
		delete(a.lowSince, envID)
		return nil
	}
}

// inCooldown reports whether env changed within the cooldown window.
func (a *Autoscaler) inCooldown(envID string, now time.Time) bool {
	last, ok := a.lastChange[envID]
	return ok && now.Sub(last) < scaleCooldown
}

// applyScale writes effective_replicas and records the change. It re-reads the
// env so it never clobbers a concurrent field update.
func (a *Autoscaler) applyScale(ctx context.Context, st *state.Store, env *zatterav1.Environment, from, to int, now time.Time) error {
	cur, ok := st.Environment(env.GetMeta().GetId())
	if !ok {
		return nil
	}
	cur = proto.Clone(cur).(*zatterav1.Environment)
	cur.EffectiveReplicas = uint32(to)
	cur.GetMeta().UpdatedAt = timestamppb.New(now)
	if err := a.apply(ctx, &clusterv1.Command{
		Mutation: &clusterv1.Command_PutEnvironment{PutEnvironment: &clusterv1.PutEnvironment{Environment: cur}},
	}); err != nil {
		return err
	}
	a.lastChange[env.GetMeta().GetId()] = now
	delete(a.lowSince, env.GetMeta().GetId())
	a.emitEvent(ctx, env, "autoscale.scaled", "info", "autoscaled %s from %d to %d replicas", env.GetName(), from, to)
	a.log.Info("autoscaled env", "env", env.GetMeta().GetId(), "from", from, "to", to)
	return nil
}

// envObservation is the measured load for one environment.
type envObservation struct {
	replicas int
	avgCPU   float64 // percent; valid only if cpuOK
	avgMem   float64 // percent of the memory limit; valid only if memOK
	totalRPS float64
	cpuOK    bool
	memOK    bool
	rpsOK    bool
}

// observe gathers the env's live load from heartbeats. ok is false when there is
// no usable data at all (no running replicas observed) — the caller then freezes.
func (a *Autoscaler) observe(st *state.Store, env *zatterav1.Environment) (envObservation, bool) {
	envID := env.GetMeta().GetId()
	runIDs := map[string]bool{}
	for _, asg := range st.ListAssignments(envID) {
		if asg.GetJobId() == "" && asg.GetDesired() == zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN {
			runIDs[asg.GetMeta().GetId()] = true
		}
	}
	memLimit := float64(env.GetService().GetResources().GetMemoryMb()) * 1024 * 1024

	var obs envObservation
	var cpuSum, memSum float64
	var cpuN, memN int
	seen := map[string]bool{}
	for _, ns := range a.live.Snapshot() {
		hb := ns.Heartbeat
		if hb == nil {
			continue
		}
		obs.rpsOK = true // a live node reporting means proxy counters are current (0 = no traffic)
		if ps, ok := hb.GetProxy()[envID]; ok {
			obs.totalRPS += ps.GetRps()
		}
		for aid, s := range hb.GetInstances() {
			if !runIDs[aid] || seen[aid] {
				continue
			}
			seen[aid] = true
			obs.replicas++
			cpuSum += s.GetCpuPercent()
			cpuN++
			if memLimit > 0 {
				memSum += float64(s.GetMemoryBytes()) / memLimit * 100
				memN++
			}
		}
	}
	if obs.replicas == 0 {
		return envObservation{}, false // startup/agent gap → freeze
	}
	if cpuN > 0 {
		obs.avgCPU = cpuSum / float64(cpuN)
		obs.cpuOK = true
	}
	if memN > 0 {
		obs.avgMem = memSum / float64(memN)
		obs.memOK = true
	}
	return obs, true
}

// scaleTarget computes the desired replica count for the configured signals,
// scaling each target by band (1.0 for the up/steady decision, downBand for the
// scale-down hysteresis check). ok is false when a configured signal has no
// observed data (→ freeze). The result is clamped to [minRep, maxRep].
func scaleTarget(auto *zatterav1.Autoscale, obs envObservation, band float64, minRep, maxRep int) (int, bool) {
	desired := minRep
	consider := func(replicas int, observed float64, target uint32) {
		d := int(math.Ceil(float64(replicas) * observed / (float64(target) * band)))
		if d > desired {
			desired = d
		}
	}

	if t := auto.GetTargetCpuPercent(); t > 0 {
		if !obs.cpuOK {
			return 0, false
		}
		consider(obs.replicas, obs.avgCPU, t)
	}
	if t := auto.GetTargetMemoryPercent(); t > 0 {
		// Memory scaling needs a limit to form a percent; without one the signal
		// is simply skipped (not a data gap).
		if obs.memOK {
			consider(obs.replicas, obs.avgMem, t)
		}
	}
	if t := auto.GetTargetRpsPerReplica(); t > 0 {
		if !obs.rpsOK {
			return 0, false
		}
		// ceil(replicas × (totalRPS/replicas) / target) == ceil(totalRPS/target).
		if d := int(math.Ceil(obs.totalRPS / (float64(t) * band))); d > desired {
			desired = d
		}
	}

	if desired < minRep {
		desired = minRep
	}
	if desired > maxRep {
		desired = maxRep
	}
	return desired, true
}

// autoscaleConfigured reports whether any autoscale target is set.
func autoscaleConfigured(auto *zatterav1.Autoscale) bool {
	return auto.GetTargetCpuPercent() > 0 || auto.GetTargetMemoryPercent() > 0 || auto.GetTargetRpsPerReplica() > 0
}

// deploymentActive reports whether a non-terminal deployment owns this env.
func (a *Autoscaler) deploymentActive(st *state.Store, envID string) bool {
	for _, d := range st.ListDeployments(envID) {
		if !isTerminalPhase(d.GetPhase()) {
			return true
		}
	}
	return false
}

func (a *Autoscaler) apply(ctx context.Context, cmd *clusterv1.Command) error {
	cmd.RequestId = ids.New()
	cmd.Actor = "system:autoscaler"
	cmd.Time = timestamppb.Now()
	err := a.store.Apply(ctx, cmd)
	if errors.Is(err, raftstore.ErrNotLeader) {
		return err
	}
	if err != nil {
		a.log.Warn("autoscaler apply failed", "err", err)
	}
	return nil
}

func (a *Autoscaler) emitEvent(ctx context.Context, env *zatterav1.Environment, kind, severity, format string, args ...any) {
	ev := &zatterav1.Event{
		Meta:          &zatterav1.Meta{Id: ids.New(), CreatedAt: timestamppb.Now()},
		Kind:          kind,
		Severity:      severity,
		ProjectId:     env.GetProjectId(),
		AppId:         env.GetAppId(),
		EnvironmentId: env.GetMeta().GetId(),
		Message:       sprintf(format, args...),
	}
	_ = a.apply(ctx, &clusterv1.Command{
		Mutation: &clusterv1.Command_AppendEvents{AppendEvents: &clusterv1.AppendEvents{Events: []*zatterav1.Event{ev}}},
	})
}
