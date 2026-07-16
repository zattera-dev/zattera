// Package scheduler runs on the leader and reconciles desired replica counts
// into Assignments: it decides WHAT should run WHERE, writing only desired
// state (agents converge it, T-15). The red/green deployment orchestrator
// (T-26) drives green placement through the same helpers.
package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"sort"
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

// evalTick is the periodic re-evaluation interval (in addition to watch-driven
// triggers), catching time-based changes and missed notifications.
const evalTick = 15 * time.Second

// Scheduler is the leader-only placement loop.
type Scheduler struct {
	store *raftstore.Store
	clock clock.Clock
	log   *slog.Logger
}

// New builds the scheduler.
func New(store *raftstore.Store, clk clock.Clock, log *slog.Logger) *Scheduler {
	if log == nil {
		log = slog.Default()
	}
	if clk == nil {
		clk = clock.Real{}
	}
	return &Scheduler{store: store, clock: clk, log: log}
}

// Run evaluates while this node is the leader, stopping cleanly on leadership
// loss and resuming when re-elected. Blocks until ctx is canceled.
func (s *Scheduler) Run(ctx context.Context) {
	leaderrunner.Run(ctx, s.store, s.clock, s.leaderLoop)
}

// leaderLoop runs the watch+tick evaluation until leadership is lost or ctx ends.
func (s *Scheduler) leaderLoop(ctx context.Context) {
	sub := s.store.State().Watch(
		state.KindEnvironment, state.KindRelease, state.KindDeployment,
		state.KindNode, state.KindAssignment, state.KindVolume, state.KindJob,
	)
	defer sub.Close()

	tick := s.clock.NewTicker(evalTick)
	defer tick.Stop()

	for {
		if err := s.evaluate(ctx); errors.Is(err, raftstore.ErrNotLeader) {
			return // leadership lost mid-evaluation is normal
		}
		select {
		case <-ctx.Done():
			return
		case <-s.store.LeaderCh():
			if !s.store.IsLeader() {
				return
			}
		case <-sub.Notify():
			sub.Drain()
		case <-tick.C():
		}
	}
}

// evaluate reconciles every environment once. Returns ErrNotLeader if an apply
// reveals leadership was lost.
func (s *Scheduler) evaluate(ctx context.Context) error {
	if !s.store.IsLeader() {
		return raftstore.ErrNotLeader
	}
	st := s.store.State()
	for _, env := range st.ListEnvironments("", "") {
		if err := s.evaluateEnv(ctx, st, env); err != nil {
			return err // propagate ErrNotLeader; log others inside
		}
	}
	// Reap assignments whose env/release was deleted (T-27); these no longer
	// belong to any environment the loop above visits.
	if err := s.reconcileOrphans(ctx, st); err != nil {
		return err
	}
	// Allocate/free per-(project,env,node) bridge subnets (T-46).
	if err := s.reconcileNetworks(ctx, st); err != nil {
		return err
	}
	// Allocate/free a service VIP per environment that exposes a port (T-48).
	if err := s.reconcileVIPs(ctx, st); err != nil {
		return err
	}
	// Place and drive one-shot jobs (T-53).
	if err := s.reconcileJobs(ctx, st); err != nil {
		return err
	}
	// Migrate/stop instances off DRAINING nodes and mark them DRAINED (T-29).
	return s.reconcileDrains(ctx, st)
}

// evaluateEnv converges one environment's active-release replica count.
func (s *Scheduler) evaluateEnv(ctx context.Context, st *state.Store, env *zatterav1.Environment) error {
	// A deployment orchestrator (T-26) owns placement while it runs; don't fight
	// it. (No deployments exist until T-25/T-26, so this is dormant for now.)
	if s.deploymentOwnsEnv(st, env) {
		return nil
	}

	envID := env.GetMeta().GetId()
	relID := env.GetActiveReleaseId()
	desired := desiredReplicas(env)

	var rel *zatterav1.Release
	if relID != "" {
		if r, ok := st.Release(relID); ok {
			rel = r
		} else {
			return nil // release vanished; nothing coherent to place
		}
	}

	// Partition the env's RUN assignments.
	var good []*zatterav1.Assignment // active release, node ALIVE
	var stopIDs []string             // flip to STOP (stale release / excess)
	var deleteIDs []string           // already observed STOPPED / lost stateless
	for _, a := range st.ListAssignments(envID) {
		// Job assignments (T-53) are one-shot runs owned by reconcileJobs; they
		// must never count toward service replica math.
		if a.GetJobId() != "" {
			continue
		}
		switch a.GetDesired() {
		case zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_STOP:
			// Reap once the agent reports it stopped.
			if isStopped(a) {
				deleteIDs = append(deleteIDs, a.GetMeta().GetId())
			}
			continue
		case zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN:
		default:
			continue
		}

		if relID == "" || a.GetReleaseId() != relID {
			stopIDs = append(stopIDs, a.GetMeta().GetId()) // stale release
			continue
		}
		if nodeDown(st, a.GetNodeId()) {
			if nodeDraining(st, a.GetNodeId()) {
				// Draining node: the replica is not counted as "good" (so a
				// replacement is placed on a live node), but it is left running
				// — reconcileDrains stops it once the replacement is healthy.
				continue
			}
			if isStateful(rel) {
				// Hard down + stateful: leave in place; the volume is pinned to
				// the down node. TODO(T-62): mark the volume NODE_LOST.
				good = append(good, a)
				continue
			}
			// Hard down + stateless: drop it so placement replaces it elsewhere.
			deleteIDs = append(deleteIDs, a.GetMeta().GetId())
			continue
		}
		good = append(good, a)
	}

	var puts []*zatterav1.Assignment

	// Scale up: place the shortfall (T-24 placement). Spread is handled by the
	// scorer; no exclusions (a replacement may co-locate when nodes are scarce).
	if missing := desired - len(good); missing > 0 && rel != nil {
		nodes, err := Place(st, rel, envID, missing, nil)
		for _, nodeID := range nodes {
			puts = append(puts, newAssignment(env, rel, nodeID))
		}
		if err != nil {
			s.emitEvent(ctx, env, "schedule.no_capacity", "warning", "%v", err)
		}
	}

	// Scale down: flip the newest excess replicas to STOP (deterministic order).
	if excess := len(good) - desired; excess > 0 {
		sort.Slice(good, func(i, j int) bool { return good[i].GetMeta().GetId() < good[j].GetMeta().GetId() })
		for _, a := range good[len(good)-excess:] {
			stopIDs = append(stopIDs, a.GetMeta().GetId())
		}
	}

	if err := s.applyBatch(ctx, envID, puts, stopIDs, deleteIDs); err != nil {
		return err
	}
	return nil
}

// applyBatch writes the reconciliation: new/flipped assignments in one
// PutAssignments, reaped ids in one DeleteAssignments.
func (s *Scheduler) applyBatch(ctx context.Context, envID string, puts []*zatterav1.Assignment, stopIDs, deleteIDs []string) error {
	// Flip stopIDs to STOP (re-read to preserve fields).
	st := s.store.State()
	for _, id := range stopIDs {
		if a, ok := st.Assignment(id); ok && a.GetDesired() != zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_STOP {
			a.Desired = zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_STOP
			puts = append(puts, a)
		}
	}

	if len(puts) > 0 {
		if err := s.apply(ctx, &clusterv1.Command{
			Mutation: &clusterv1.Command_PutAssignments{PutAssignments: &clusterv1.PutAssignments{Assignments: puts}},
		}); err != nil {
			return err
		}
	}
	if len(deleteIDs) > 0 {
		if err := s.apply(ctx, &clusterv1.Command{
			Mutation: &clusterv1.Command_DeleteAssignments{DeleteAssignments: &clusterv1.DeleteAssignments{AssignmentIds: deleteIDs}},
		}); err != nil {
			return err
		}
	}
	if len(puts) > 0 || len(deleteIDs) > 0 {
		s.log.Info("scheduler reconciled env", "env", envID, "puts", len(puts), "deletes", len(deleteIDs))
	}
	return nil
}

func (s *Scheduler) apply(ctx context.Context, cmd *clusterv1.Command) error {
	cmd.RequestId = ids.New()
	cmd.Actor = "system:scheduler"
	cmd.Time = timestamppb.Now()
	err := s.store.Apply(ctx, cmd)
	if errors.Is(err, raftstore.ErrNotLeader) {
		return err
	}
	if err != nil {
		s.log.Warn("scheduler apply failed", "err", err)
	}
	return nil
}

func (s *Scheduler) emitEvent(ctx context.Context, env *zatterav1.Environment, kind, severity, format string, args ...any) {
	ev := &zatterav1.Event{
		Meta:          &zatterav1.Meta{Id: ids.New(), CreatedAt: timestamppb.Now()},
		Kind:          kind,
		Severity:      severity,
		ProjectId:     env.GetProjectId(),
		AppId:         env.GetAppId(),
		EnvironmentId: env.GetMeta().GetId(),
		Message:       sprintf(format, args...),
	}
	_ = s.apply(ctx, &clusterv1.Command{
		Mutation: &clusterv1.Command_AppendEvents{AppendEvents: &clusterv1.AppendEvents{Events: []*zatterav1.Event{ev}}},
	})
}

// deploymentOwnsEnv reports whether a non-terminal deployment is orchestrating
// this env (the scheduler must not also place for it).
func (s *Scheduler) deploymentOwnsEnv(st *state.Store, env *zatterav1.Environment) bool {
	for _, d := range st.ListDeployments(env.GetMeta().GetId()) {
		if !isTerminalPhase(d.GetPhase()) {
			return true
		}
	}
	return false
}
