package scheduler

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/livestate"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

const idleTimeout = 15 * time.Minute

func newS2Z(t *testing.T) (*ScaleToZero, *raftstore.Store, *clock.Fake, *livestate.Registry) {
	t.Helper()
	rs := raftstore.NewTestStore(t)
	clk := clock.NewFake()
	live := livestate.New(clk)
	return NewScaleToZero(rs, live, clk, nil), rs, clk, live
}

// addS2ZEnv seeds a scale-to-zero env with idle_timeout, effective replicas, and
// `running` RUN assignments. stateful marks the (invalid but defensively tested)
// stateful combination.
func addS2ZEnv(st *state.Store, eff uint32, running int, stateful bool) {
	spec := &zatterav1.ServiceSpec{
		Replicas:    &zatterav1.ReplicaRange{Min: 1, Max: 5},
		ScaleToZero: true,
		Stateful:    stateful,
	}
	st.PutRelease(&zatterav1.Release{Meta: &zatterav1.Meta{Id: relID}, EnvironmentId: envID, ConfigHash: "h", Service: spec})
	st.PutEnvironment(&zatterav1.Environment{
		Meta:              &zatterav1.Meta{Id: envID},
		Name:              "production",
		ActiveReleaseId:   relID,
		Service:           spec,
		EffectiveReplicas: eff,
		IdleTimeout:       durationpb.New(idleTimeout),
	})
	for i := 0; i < running; i++ {
		st.PutAssignment(&zatterav1.Assignment{
			Meta:          &zatterav1.Meta{Id: ids.New()},
			EnvironmentId: envID,
			ReleaseId:     relID,
			NodeId:        "n1",
			Desired:       zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN,
		})
	}
}

// proxyBeat pushes a node heartbeat carrying the env's proxy activity.
func proxyBeat(live *livestate.Registry, clk *clock.Fake, lastReq time.Time, inflight uint32) {
	ps := &clusterv1.ProxySample{Inflight: inflight}
	if !lastReq.IsZero() {
		ps.LastRequestAt = timestamppb.New(lastReq)
	}
	live.Heartbeat("n1", &clusterv1.Heartbeat{
		Time:  timestamppb.New(clk.Now()),
		Proxy: map[string]*clusterv1.ProxySample{envID: ps},
	})
}

func evalS2Z(t *testing.T, s *ScaleToZero) {
	t.Helper()
	if err := s.evaluate(context.Background()); err != nil {
		t.Fatalf("evaluate: %v", err)
	}
}

func TestScaleToZero(t *testing.T) {
	t.Run("cools_after_idle_timeout", testCoolsAfterIdle)
	t.Run("recent_traffic_stays_warm", testRecentTrafficWarm)
	t.Run("inflight_stays_warm", testInflightWarm)
	t.Run("stateful_never_cools", testStatefulNeverCools)
	t.Run("no_timeout_never_cools", testNoTimeoutNeverCools)
	t.Run("blackout_freezes", testBlackoutFreezes)
	t.Run("already_zero_noop", testAlreadyZeroNoop)
}

func testCoolsAfterIdle(t *testing.T) {
	s, rs, clk, live := newS2Z(t)
	st := rs.State()
	addS2ZEnv(st, 1, 1, false)
	start := clk.Now()

	// First pass: last request just now → env is warm.
	proxyBeat(live, clk, start, 0)
	evalS2Z(t, s)
	if got := effectiveOf(t, st); got != 1 {
		t.Fatalf("cooled while active: effective=%d", got)
	}

	// No further traffic; cross the idle window → cool to zero.
	clk.Advance(idleTimeout + time.Minute)
	proxyBeat(live, clk, start, 0) // last_request_at still the old time
	evalS2Z(t, s)
	if got := effectiveOf(t, st); got != 0 {
		t.Fatalf("did not cool to zero after idle: effective=%d", got)
	}
}

func testRecentTrafficWarm(t *testing.T) {
	s, rs, clk, live := newS2Z(t)
	st := rs.State()
	addS2ZEnv(st, 1, 1, false)

	proxyBeat(live, clk, clk.Now(), 0)
	evalS2Z(t, s)

	// Time passes past the window, but a request arrived 1m ago → stays warm.
	clk.Advance(idleTimeout + time.Minute)
	proxyBeat(live, clk, clk.Now().Add(-time.Minute), 0)
	evalS2Z(t, s)
	if got := effectiveOf(t, st); got != 1 {
		t.Fatalf("cooled despite recent traffic: effective=%d", got)
	}
}

func testInflightWarm(t *testing.T) {
	s, rs, clk, live := newS2Z(t)
	st := rs.State()
	addS2ZEnv(st, 1, 1, false)

	proxyBeat(live, clk, clk.Now(), 0)
	evalS2Z(t, s)

	// Window elapses with a stale last_request_at, but a request is in flight.
	clk.Advance(idleTimeout + time.Minute)
	proxyBeat(live, clk, clk.Now().Add(-2*idleTimeout), 3)
	evalS2Z(t, s)
	if got := effectiveOf(t, st); got != 1 {
		t.Fatalf("cooled with in-flight requests: effective=%d", got)
	}
}

func testStatefulNeverCools(t *testing.T) {
	s, rs, clk, live := newS2Z(t)
	st := rs.State()
	addS2ZEnv(st, 1, 1, true) // stateful: must never be cooled

	clk.Advance(idleTimeout + time.Minute)
	proxyBeat(live, clk, time.Time{}, 0)
	evalS2Z(t, s)
	if got := effectiveOf(t, st); got != 1 {
		t.Fatalf("stateful env cooled: effective=%d", got)
	}
}

func testNoTimeoutNeverCools(t *testing.T) {
	s, rs, clk, live := newS2Z(t)
	st := rs.State()
	addS2ZEnv(st, 1, 1, false)
	// Clear the idle timeout: no window → never cools.
	env, _ := st.Environment(envID)
	env.IdleTimeout = nil
	st.PutEnvironment(env)

	clk.Advance(2 * idleTimeout)
	proxyBeat(live, clk, time.Time{}, 0)
	evalS2Z(t, s)
	if got := effectiveOf(t, st); got != 1 {
		t.Fatalf("cooled without an idle_timeout: effective=%d", got)
	}
}

func testBlackoutFreezes(t *testing.T) {
	s, rs, clk, _ := newS2Z(t)
	st := rs.State()
	addS2ZEnv(st, 1, 1, false)

	// No heartbeats at all (agent/proxy blackout): must not cool.
	clk.Advance(2 * idleTimeout)
	evalS2Z(t, s)
	if got := effectiveOf(t, st); got != 1 {
		t.Fatalf("cooled during a heartbeat blackout: effective=%d", got)
	}
}

func testAlreadyZeroNoop(t *testing.T) {
	s, rs, clk, live := newS2Z(t)
	st := rs.State()
	addS2ZEnv(st, 0, 0, false) // already cold

	clk.Advance(idleTimeout + time.Minute)
	proxyBeat(live, clk, time.Time{}, 0)
	evalS2Z(t, s)
	if got := effectiveOf(t, st); got != 0 {
		t.Fatalf("effective changed while already zero: %d", got)
	}
}
