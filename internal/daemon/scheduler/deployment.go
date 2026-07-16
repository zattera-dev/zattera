package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/leaderrunner"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

const (
	// drainWindow keeps blue instances warm after promotion for fast rollback.
	drainWindow = 10 * time.Minute
	// defaultHealthGrace is the per-instance time-to-first-success default.
	defaultHealthGrace = 60 * time.Second
	// healthDeadlineExtra pads the overall HEALTHCHECKING deadline.
	healthDeadlineExtra = 60 * time.Second
)

// Orchestrator drives red/green Deployments through their phase machine on the
// leader. Every arm is idempotent and every transition is a single Apply, so a
// leader failover resumes from durable state (never in-memory progress).
type Orchestrator struct {
	store *raftstore.Store
	clock clock.Clock
	log   *slog.Logger
	// drainDur overrides the blue-drain window when > 0 (chaos/tests use a short
	// window; production keeps the 10m default).
	drainDur time.Duration
}

// SetDrainWindow overrides the blue-drain window (0 restores the default).
func (o *Orchestrator) SetDrainWindow(d time.Duration) { o.drainDur = d }

func (o *Orchestrator) drainWindowDur() time.Duration {
	if o.drainDur > 0 {
		return o.drainDur
	}
	return drainWindow
}

// NewOrchestrator builds the deployment orchestrator.
func NewOrchestrator(store *raftstore.Store, clk clock.Clock, log *slog.Logger) *Orchestrator {
	if log == nil {
		log = slog.Default()
	}
	if clk == nil {
		clk = clock.Real{}
	}
	return &Orchestrator{store: store, clock: clk, log: log}
}

// Run reconciles deployments while this node leads, resuming on re-election.
func (o *Orchestrator) Run(ctx context.Context) {
	leaderrunner.Run(ctx, o.store, o.clock, o.leaderLoop)
}

func (o *Orchestrator) leaderLoop(ctx context.Context) {
	sub := o.store.State().Watch(state.KindDeployment, state.KindAssignment)
	defer sub.Close()
	tick := o.clock.NewTicker(evalTick)
	defer tick.Stop()
	for {
		if err := o.reconcileAll(ctx); errors.Is(err, raftstore.ErrNotLeader) {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-o.store.LeaderCh():
			if !o.store.IsLeader() {
				return
			}
		case <-sub.Notify():
			sub.Drain()
		case <-tick.C():
		}
	}
}

func (o *Orchestrator) reconcileAll(ctx context.Context) error {
	if !o.store.IsLeader() {
		return raftstore.ErrNotLeader
	}
	for _, d := range o.store.State().ListDeployments("") {
		if isTerminalPhase(d.GetPhase()) {
			continue
		}
		if err := o.reconcile(ctx, d); err != nil {
			return err
		}
	}
	return nil
}

// reconcile advances one deployment by (at most) one phase.
func (o *Orchestrator) reconcile(ctx context.Context, d *zatterav1.Deployment) error {
	if isTerminalPhase(d.GetPhase()) {
		return nil
	}
	st := o.store.State()
	env, ok := st.Environment(d.GetEnvironmentId())
	if !ok {
		return o.abort(ctx, d, "environment not found")
	}
	rel, ok := st.Release(d.GetReleaseId())
	if !ok {
		return o.abort(ctx, d, "release not found")
	}

	// A newer non-terminal deployment for this env takes over.
	if o.superseded(st, d) {
		// If we already promoted (DRAINING_OLD), traffic has switched and this
		// deploy succeeded; leave every instance in place and just complete.
		// Reaping our green would kill the serving release, and stopping our
		// blue would yank the warm rollback target out from under the newer
		// deployment (e.g. a rollback promoting exactly that release). The
		// scheduler reaps any genuinely stale-release instances once no
		// deployment owns the env.
		if d.GetPhase() == zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_DRAINING_OLD {
			return o.setPhase(ctx, d, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_SUCCEEDED, "")
		}
		if err := o.reapGreen(ctx, st, d); err != nil {
			return err
		}
		return o.setPhase(ctx, d, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_SUPERSEDED, "superseded by a newer deployment")
	}

	switch d.GetPhase() {
	case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PENDING:
		if rel.GetService().GetStateful() {
			return o.abort(ctx, d, "stateful services use stop-then-start deploys (T-63), not red/green")
		}
		// Source deploys carry a build_id: build the image before placing.
		if d.GetBuildId() != "" && rel.GetImageRef() == "" {
			return o.setPhase(ctx, d, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_BUILDING, "")
		}
		if rel.GetImageRef() == "" {
			return o.abort(ctx, d, "release has no image ref")
		}
		if d.GetIsRollback() && o.warmEnough(st, env, rel, d) {
			return o.setPhase(ctx, d, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PROMOTING, "")
		}
		return o.setPhase(ctx, d, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PLACING, "")

	case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_BUILDING:
		return o.checkBuild(ctx, rel, d)

	case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PLACING:
		return o.place(ctx, st, env, rel, d)

	case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_STARTING:
		return o.checkStarting(ctx, st, d)

	case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_HEALTHCHECKING:
		return o.checkHealth(ctx, st, rel, d)

	case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PROMOTING:
		return o.promote(ctx, d)

	case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_DRAINING_OLD:
		return o.drain(ctx, st, d)
	}
	return nil
}

// checkBuild gates a source deployment on its build: it stays in BUILDING until
// the build succeeds (then it stamps the built image onto the release and
// advances to PLACING) or fails (then the deployment fails).
func (o *Orchestrator) checkBuild(ctx context.Context, rel *zatterav1.Release, d *zatterav1.Deployment) error {
	b, ok := o.store.State().Build(d.GetBuildId())
	if !ok {
		return o.abort(ctx, d, "build not found")
	}
	switch b.GetStatus() {
	case zatterav1.BuildStatus_BUILD_STATUS_SUCCEEDED:
		if b.GetImageRef() == "" {
			return o.abort(ctx, d, "build produced no image")
		}
		if rel.GetImageRef() != b.GetImageRef() {
			rel.ImageRef = b.GetImageRef()
			// The build knows what it produced (T-88): freeze its platforms
			// into the release so placement is arch-aware.
			rel.Platforms = b.GetPlatforms()
			if err := o.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_PutRelease{PutRelease: &clusterv1.PutRelease{Release: rel}}}); err != nil {
				return err
			}
		}
		return o.setPhase(ctx, d, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PLACING, "")
	case zatterav1.BuildStatus_BUILD_STATUS_FAILED, zatterav1.BuildStatus_BUILD_STATUS_CANCELED:
		return o.abort(ctx, d, "build failed: "+b.GetError())
	default:
		return nil // QUEUED / RUNNING: keep waiting
	}
}

// place ensures the green replica set exists, then advances to STARTING.
func (o *Orchestrator) place(ctx context.Context, st *state.Store, env *zatterav1.Environment, rel *zatterav1.Release, d *zatterav1.Deployment) error {
	green := greenAssignments(st, d)
	desired := deployReplicas(env, rel)

	if len(green) < desired {
		picks, _ := Place(st, rel, env.GetMeta().GetId(), desired-len(green), nodeSet(green))
		var puts []*zatterav1.Assignment
		for _, nodeID := range picks {
			puts = append(puts, greenAssignment(env, rel, nodeID, d.GetMeta().GetId()))
		}
		if len(puts) > 0 {
			if err := o.apply(ctx, putAssignments(puts)); err != nil {
				return err
			}
			green = greenAssignments(st, d)
		}
	}
	if len(green) >= desired {
		return o.setPhase(ctx, d, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_STARTING, "")
	}
	return nil // capacity short; retry on the next tick
}

// checkStarting waits for all green instances to be RUNNING/HEALTHY.
func (o *Orchestrator) checkStarting(ctx context.Context, st *state.Store, d *zatterav1.Deployment) error {
	green := greenAssignments(st, d)
	if anyFailed(green) {
		return o.abort(ctx, d, "a green instance failed to start")
	}
	for _, a := range green {
		s := a.GetObserved().GetState()
		if s != zatterav1.InstanceState_INSTANCE_STATE_RUNNING && s != zatterav1.InstanceState_INSTANCE_STATE_HEALTHY {
			return nil // still coming up
		}
	}
	return o.setPhase(ctx, d, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_HEALTHCHECKING, "")
}

// checkHealth waits for all green instances to be HEALTHY within the deadline.
func (o *Orchestrator) checkHealth(ctx context.Context, st *state.Store, rel *zatterav1.Release, d *zatterav1.Deployment) error {
	green := greenAssignments(st, d)
	if anyFailed(green) {
		return o.abort(ctx, d, "a green instance became unhealthy")
	}
	healthy := true
	for _, a := range green {
		if a.GetObserved().GetState() != zatterav1.InstanceState_INSTANCE_STATE_HEALTHY {
			healthy = false
			break
		}
	}
	if healthy {
		return o.setPhase(ctx, d, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PROMOTING, "")
	}
	// Overall deadline measured from phase entry (meta.updated_at).
	entry := d.GetMeta().GetUpdatedAt().AsTime()
	if o.clock.Now().After(entry.Add(healthDeadline(rel))) {
		return o.abort(ctx, d, "green instances did not become healthy in time")
	}
	return nil
}

// promote flips traffic to the new release and starts the drain window.
func (o *Orchestrator) promote(ctx context.Context, d *zatterav1.Deployment) error {
	if err := o.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_PromoteRelease{PromoteRelease: &clusterv1.PromoteRelease{
		EnvironmentId: d.GetEnvironmentId(),
		ReleaseId:     d.GetReleaseId(),
		DeploymentId:  d.GetMeta().GetId(),
	}}}); err != nil {
		return err
	}
	now := o.clock.Now()
	return o.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_SetDeploymentPhase{SetDeploymentPhase: &clusterv1.SetDeploymentPhase{
		DeploymentId:  d.GetMeta().GetId(),
		Phase:         zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_DRAINING_OLD,
		PromotedAt:    timestamppb.New(now),
		DrainDeadline: timestamppb.New(now.Add(o.drainWindowDur())),
	}}})
}

// drain stops the blue set once the drain window elapses, then succeeds.
func (o *Orchestrator) drain(ctx context.Context, st *state.Store, d *zatterav1.Deployment) error {
	if dl := d.GetDrainDeadline(); dl != nil && o.clock.Now().Before(dl.AsTime()) {
		return nil // keep blue warm for rollback
	}
	return o.completeDrain(ctx, st, d)
}

// completeDrain stops this deployment's outgoing blue set and marks it
// SUCCEEDED. It never touches the green set (the promoted, live release), so it
// is safe to call both when the drain window elapses and when a newer
// deployment takes over an already-promoted one.
func (o *Orchestrator) completeDrain(ctx context.Context, st *state.Store, d *zatterav1.Deployment) error {
	blue := blueAssignments(st, d)
	var puts []*zatterav1.Assignment
	for _, a := range blue {
		a.Desired = zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_STOP
		puts = append(puts, a)
	}
	if len(puts) > 0 {
		if err := o.apply(ctx, putAssignments(puts)); err != nil {
			return err
		}
	}
	return o.setPhase(ctx, d, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_SUCCEEDED, "")
}

// abort reaps this deployment's green set, records the failure, and leaves blue
// (and thus live traffic) untouched.
func (o *Orchestrator) abort(ctx context.Context, d *zatterav1.Deployment, reason string) error {
	if err := o.reapGreen(ctx, o.store.State(), d); err != nil {
		return err
	}
	o.emitEvent(ctx, d, "deploy.failed", reason)
	return o.setPhase(ctx, d, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_FAILED, reason)
}

func (o *Orchestrator) reapGreen(ctx context.Context, st *state.Store, d *zatterav1.Deployment) error {
	green := greenAssignments(st, d)
	if len(green) == 0 {
		return nil
	}
	ids := make([]string, 0, len(green))
	for _, a := range green {
		ids = append(ids, a.GetMeta().GetId())
	}
	return o.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_DeleteAssignments{DeleteAssignments: &clusterv1.DeleteAssignments{AssignmentIds: ids}}})
}

// superseded reports whether a newer non-terminal deployment exists for the env.
func (o *Orchestrator) superseded(st *state.Store, d *zatterav1.Deployment) bool {
	for _, other := range st.ListDeployments(d.GetEnvironmentId()) {
		if other.GetMeta().GetId() == d.GetMeta().GetId() || isTerminalPhase(other.GetPhase()) {
			continue
		}
		if deploymentNewer(other, d) {
			return true
		}
	}
	return false
}

// warmEnough reports whether the rollback target already has enough healthy
// running instances (still warm from the previous deployment's drain window).
func (o *Orchestrator) warmEnough(st *state.Store, env *zatterav1.Environment, rel *zatterav1.Release, d *zatterav1.Deployment) bool {
	want := deployReplicas(env, rel)
	if want == 0 {
		return false
	}
	healthy := 0
	for _, a := range st.ListAssignments(env.GetMeta().GetId()) {
		if a.GetReleaseId() == d.GetReleaseId() &&
			a.GetDesired() == zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN &&
			a.GetObserved().GetState() == zatterav1.InstanceState_INSTANCE_STATE_HEALTHY {
			healthy++
		}
	}
	return healthy >= want
}

// --- apply helpers --------------------------------------------------------

func (o *Orchestrator) setPhase(ctx context.Context, d *zatterav1.Deployment, phase zatterav1.DeploymentPhase, errMsg string) error {
	return o.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_SetDeploymentPhase{SetDeploymentPhase: &clusterv1.SetDeploymentPhase{
		DeploymentId: d.GetMeta().GetId(),
		Phase:        phase,
		Error:        errMsg,
	}}})
}

func (o *Orchestrator) emitEvent(ctx context.Context, d *zatterav1.Deployment, kind, msg string) {
	_ = o.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_AppendEvents{AppendEvents: &clusterv1.AppendEvents{Events: []*zatterav1.Event{{
		Meta:          &zatterav1.Meta{Id: ids.New(), CreatedAt: timestamppb.Now()},
		Kind:          kind,
		Severity:      "error",
		ProjectId:     d.GetProjectId(),
		AppId:         d.GetAppId(),
		EnvironmentId: d.GetEnvironmentId(),
		Message:       msg,
	}}}}})
}

func (o *Orchestrator) apply(ctx context.Context, cmd *clusterv1.Command) error {
	cmd.RequestId = ids.New()
	cmd.Actor = "system:deploy"
	cmd.Time = timestamppb.New(o.clock.Now())
	err := o.store.Apply(ctx, cmd)
	if errors.Is(err, raftstore.ErrNotLeader) {
		return err
	}
	if err != nil {
		o.log.Warn("deploy apply failed", "err", err)
	}
	return nil
}
