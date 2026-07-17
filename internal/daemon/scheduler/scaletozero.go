package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/leaderrunner"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

// scaleToZeroTick is the idle re-check cadence.
const scaleToZeroTick = 15 * time.Second

// ScaleToZero cools an idle scale-to-zero environment down to zero replicas
// (T-69). It runs on the leader, reads per-env request activity from proxy
// heartbeats (livestate), and sets effective_replicas=0 once an env has been
// idle past its idle_timeout — the scheduler then stops the replicas. Waking a
// cold env back up is the activator's job (T-70); this loop only scales down.
type ScaleToZero struct {
	store *raftstore.Store
	live  LiveView
	clock clock.Clock
	log   *slog.Logger

	// lastActive is the last time each env showed traffic, reset per leadership
	// term. A fresh term seeds it to "now" on first sight so a just-elected leader
	// grants every env a full idle window before cooling it down.
	lastActive map[string]time.Time
}

// NewScaleToZero builds the loop.
func NewScaleToZero(store *raftstore.Store, live LiveView, clk clock.Clock, log *slog.Logger) *ScaleToZero {
	if log == nil {
		log = slog.Default()
	}
	if clk == nil {
		clk = clock.Real{}
	}
	return &ScaleToZero{store: store, live: live, clock: clk, log: log, lastActive: map[string]time.Time{}}
}

// Run evaluates while this node leads.
func (s *ScaleToZero) Run(ctx context.Context) {
	leaderrunner.Run(ctx, s.store, s.clock, s.leaderLoop)
}

func (s *ScaleToZero) leaderLoop(ctx context.Context) {
	s.lastActive = map[string]time.Time{}
	tick := s.clock.NewTicker(scaleToZeroTick)
	defer tick.Stop()
	for {
		if err := s.evaluate(ctx); errors.Is(err, raftstore.ErrNotLeader) {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-s.store.LeaderCh():
			if !s.store.IsLeader() {
				return
			}
		case <-tick.C():
		}
	}
}

func (s *ScaleToZero) evaluate(ctx context.Context) error {
	if !s.store.IsLeader() {
		return raftstore.ErrNotLeader
	}
	st := s.store.State()
	now := s.clock.Now()
	for _, env := range st.ListEnvironments("", "") {
		if err := s.evaluateEnv(ctx, st, env, now); err != nil {
			return err
		}
	}
	return nil
}

// evaluateEnv cools one env to zero when it has been idle past its timeout.
func (s *ScaleToZero) evaluateEnv(ctx context.Context, st *state.Store, env *zatterav1.Environment, now time.Time) error {
	spec := env.GetService()
	if !spec.GetScaleToZero() || spec.GetStateful() || env.GetActiveReleaseId() == "" {
		return nil
	}
	timeout := env.GetIdleTimeout().AsDuration()
	if timeout <= 0 {
		return nil // no idle window configured
	}
	envID := env.GetMeta().GetId()
	// Already cold, or a deployment owns the env: leave it alone.
	if env.GetEffectiveReplicas() == 0 || s.deploymentActive(st, envID) {
		delete(s.lastActive, envID)
		return nil
	}

	lastReq, inflight, haveData := s.activity(st, envID)
	if !haveData {
		// No live node reporting: never cool on a blackout (agent/proxy gap).
		return nil
	}

	// Advance the activity mark: in-flight traffic or a newer last-request time
	// counts as active; first sight this term seeds the full window.
	seen, ok := s.lastActive[envID]
	if !ok {
		seen = now
	}
	if inflight > 0 {
		seen = now
	} else if lastReq.After(seen) {
		seen = lastReq
	}
	s.lastActive[envID] = seen

	if now.Sub(seen) < timeout {
		return nil // still within the idle window
	}
	return s.cool(ctx, st, env, now)
}

// activity gathers the env's request activity from proxy heartbeats: the latest
// last_request_at seen across live nodes and the summed in-flight count. haveData
// is false when no live node is reporting at all.
func (s *ScaleToZero) activity(_ *state.Store, envID string) (lastReq time.Time, inflight uint32, haveData bool) {
	for _, ns := range s.live.Snapshot() {
		hb := ns.Heartbeat
		if hb == nil {
			continue
		}
		haveData = true
		ps, ok := hb.GetProxy()[envID]
		if !ok {
			continue
		}
		inflight += ps.GetInflight()
		if t := ps.GetLastRequestAt(); t != nil {
			if at := t.AsTime(); at.After(lastReq) {
				lastReq = at
			}
		}
	}
	return lastReq, inflight, haveData
}

// cool sets effective_replicas=0 (re-reading the env to avoid clobbering a
// concurrent update) and records the event.
func (s *ScaleToZero) cool(ctx context.Context, st *state.Store, env *zatterav1.Environment, now time.Time) error {
	cur, ok := st.Environment(env.GetMeta().GetId())
	if !ok {
		return nil
	}
	cur = proto.Clone(cur).(*zatterav1.Environment)
	if cur.GetEffectiveReplicas() == 0 { // raced with another writer
		return nil
	}
	cur.EffectiveReplicas = 0
	cur.GetMeta().UpdatedAt = timestamppb.New(now)
	if err := s.apply(ctx, &clusterv1.Command{
		Mutation: &clusterv1.Command_PutEnvironment{PutEnvironment: &clusterv1.PutEnvironment{Environment: cur}},
	}); err != nil {
		return err
	}
	delete(s.lastActive, env.GetMeta().GetId())
	s.emitEvent(ctx, env, "scaletozero.cooled", "info", "scaled %s to zero after idle", env.GetName())
	s.log.Info("scaled env to zero", "env", env.GetMeta().GetId())
	return nil
}

// deploymentActive reports whether a non-terminal deployment owns this env.
func (s *ScaleToZero) deploymentActive(st *state.Store, envID string) bool {
	for _, d := range st.ListDeployments(envID) {
		if !isTerminalPhase(d.GetPhase()) {
			return true
		}
	}
	return false
}

func (s *ScaleToZero) apply(ctx context.Context, cmd *clusterv1.Command) error {
	cmd.RequestId = ids.New()
	cmd.Actor = "system:scaletozero"
	cmd.Time = timestamppb.New(s.clock.Now())
	err := s.store.Apply(ctx, cmd)
	if errors.Is(err, raftstore.ErrNotLeader) {
		return err
	}
	if err != nil {
		s.log.Warn("scaletozero apply failed", "err", err)
	}
	return nil
}

func (s *ScaleToZero) emitEvent(ctx context.Context, env *zatterav1.Environment, kind, severity, format string, args ...any) {
	ev := &zatterav1.Event{
		Meta:          &zatterav1.Meta{Id: ids.New(), CreatedAt: timestamppb.New(s.clock.Now())},
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
