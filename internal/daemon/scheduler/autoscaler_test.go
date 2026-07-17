package scheduler

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/livestate"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

func newAuto(t *testing.T) (*Autoscaler, *raftstore.Store, *clock.Fake, *livestate.Registry) {
	t.Helper()
	rs := raftstore.NewTestStore(t)
	clk := clock.NewFake()
	live := livestate.New(clk)
	return NewAutoscaler(rs, live, clk, nil), rs, clk, live
}

// addAutoEnv creates an env with an Autoscale target and `running` RUN
// assignments, and returns their ids.
func addAutoEnv(st *state.Store, min, max uint32, auto *zatterav1.Autoscale, running int) []string {
	spec := &zatterav1.ServiceSpec{
		Replicas:  &zatterav1.ReplicaRange{Min: min, Max: max},
		Autoscale: auto,
		Resources: &zatterav1.ResourceLimits{MemoryMb: 100},
	}
	st.PutRelease(&zatterav1.Release{Meta: &zatterav1.Meta{Id: relID}, EnvironmentId: envID, ConfigHash: "h", Service: spec})
	st.PutEnvironment(&zatterav1.Environment{
		Meta: &zatterav1.Meta{Id: envID}, Name: "production", ActiveReleaseId: relID, Service: spec,
	})
	var aids []string
	for i := 0; i < running; i++ {
		id := ids.New()
		aids = append(aids, id)
		st.PutAssignment(&zatterav1.Assignment{
			Meta:          &zatterav1.Meta{Id: id},
			EnvironmentId: envID,
			ReleaseId:     relID,
			NodeId:        "n1",
			Desired:       zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN,
		})
	}
	return aids
}

// heartbeat pushes one node heartbeat carrying per-instance CPU and per-env RPS.
func heartbeat(live *livestate.Registry, clk *clock.Fake, aids []string, cpu float64, rps float64) {
	inst := map[string]*clusterv1.InstanceSample{}
	for _, id := range aids {
		inst[id] = &clusterv1.InstanceSample{CpuPercent: cpu, MemoryBytes: 10 << 20}
	}
	live.Heartbeat("n1", &clusterv1.Heartbeat{
		Time:      timestamppb.New(clk.Now()),
		Instances: inst,
		Proxy:     map[string]*clusterv1.ProxySample{envID: {Rps: rps}},
	})
}

func effectiveOf(t *testing.T, st *state.Store) int {
	t.Helper()
	env, ok := st.Environment(envID)
	if !ok {
		t.Fatal("env missing")
	}
	return int(env.GetEffectiveReplicas())
}

func mustEvalAuto(t *testing.T, a *Autoscaler) {
	t.Helper()
	if err := a.evaluate(context.Background()); err != nil {
		t.Fatalf("evaluate: %v", err)
	}
}

func TestAutoscaler(t *testing.T) {
	t.Run("scales up on cpu spike", func(t *testing.T) {
		a, rs, clk, live := newAuto(t)
		st := rs.State()
		aids := addAutoEnv(st, 1, 5, &zatterav1.Autoscale{TargetCpuPercent: 50}, 2)
		heartbeat(live, clk, aids, 90, 0) // ceil(2*90/50)=4

		mustEvalAuto(t, a)

		if got := effectiveOf(t, st); got != 4 {
			t.Fatalf("effective_replicas = %d, want 4", got)
		}
	})

	t.Run("clamps to max", func(t *testing.T) {
		a, rs, clk, live := newAuto(t)
		st := rs.State()
		aids := addAutoEnv(st, 1, 3, &zatterav1.Autoscale{TargetCpuPercent: 50}, 2)
		heartbeat(live, clk, aids, 100, 0) // ceil(2*100/50)=4, clamp to 3

		mustEvalAuto(t, a)

		if got := effectiveOf(t, st); got != 3 {
			t.Fatalf("effective_replicas = %d, want clamped 3", got)
		}
	})

	t.Run("scales on rps", func(t *testing.T) {
		a, rs, clk, live := newAuto(t)
		st := rs.State()
		aids := addAutoEnv(st, 1, 10, &zatterav1.Autoscale{TargetRpsPerReplica: 100}, 2)
		heartbeat(live, clk, aids, 5, 500) // ceil(500/100)=5

		mustEvalAuto(t, a)

		if got := effectiveOf(t, st); got != 5 {
			t.Fatalf("effective_replicas = %d, want 5", got)
		}
	})

	t.Run("scales down only after sustained low + hold", func(t *testing.T) {
		a, rs, clk, live := newAuto(t)
		st := rs.State()
		aids := addAutoEnv(st, 1, 5, &zatterav1.Autoscale{TargetCpuPercent: 50}, 4)
		// Pre-set effective to 4 so the decision sees a scale-down.
		env, _ := st.Environment(envID)
		env.EffectiveReplicas = 4
		st.PutEnvironment(env)
		heartbeat(live, clk, aids, 10, 0) // well below 0.8*50 → down candidate

		// First tick: starts the hold, no change yet.
		mustEvalAuto(t, a)
		if got := effectiveOf(t, st); got != 4 {
			t.Fatalf("scaled down before hold elapsed: effective=%d", got)
		}

		// Not enough time.
		clk.Advance(2 * time.Minute)
		heartbeat(live, clk, aids, 10, 0)
		mustEvalAuto(t, a)
		if got := effectiveOf(t, st); got != 4 {
			t.Fatalf("scaled down before 5m hold: effective=%d", got)
		}

		// Past the 5m hold → scale down to ceil(4*10/50)=1.
		clk.Advance(4 * time.Minute)
		heartbeat(live, clk, aids, 10, 0)
		mustEvalAuto(t, a)
		if got := effectiveOf(t, st); got != 1 {
			t.Fatalf("effective_replicas = %d, want 1 after hold", got)
		}
	})

	t.Run("freezes on missing data", func(t *testing.T) {
		a, rs, _, _ := newAuto(t)
		st := rs.State()
		addAutoEnv(st, 1, 5, &zatterav1.Autoscale{TargetCpuPercent: 50}, 2)
		// No heartbeat pushed → no instance samples → freeze.
		env, _ := st.Environment(envID)
		env.EffectiveReplicas = 3
		st.PutEnvironment(env)

		mustEvalAuto(t, a)

		if got := effectiveOf(t, st); got != 3 {
			t.Fatalf("effective_replicas changed on missing data: %d, want 3", got)
		}
	})

	t.Run("cooldown blocks a second change", func(t *testing.T) {
		a, rs, clk, live := newAuto(t)
		st := rs.State()
		aids := addAutoEnv(st, 1, 10, &zatterav1.Autoscale{TargetCpuPercent: 50}, 2)
		heartbeat(live, clk, aids, 90, 0)
		mustEvalAuto(t, a) // → 4, lastChange = now
		if got := effectiveOf(t, st); got != 4 {
			t.Fatalf("first scale = %d, want 4", got)
		}

		// A higher spike within the cooldown must not scale again.
		clk.Advance(1 * time.Minute)
		heartbeat(live, clk, aids, 100, 0)
		mustEvalAuto(t, a)
		if got := effectiveOf(t, st); got != 4 {
			t.Fatalf("scaled during cooldown: effective=%d, want 4", got)
		}

		// After cooldown, it may scale again: ceil(2*100/50)=4 (unchanged here,
		// so bump replicas to force a higher target).
		clk.Advance(3 * time.Minute)
		aids = append(aids, ids.New())
		st.PutAssignment(&zatterav1.Assignment{
			Meta: &zatterav1.Meta{Id: aids[len(aids)-1]}, EnvironmentId: envID, ReleaseId: relID,
			NodeId: "n1", Desired: zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN,
		})
		heartbeat(live, clk, aids, 100, 0) // ceil(3*100/50)=6
		mustEvalAuto(t, a)
		if got := effectiveOf(t, st); got != 6 {
			t.Fatalf("did not scale after cooldown: effective=%d, want 6", got)
		}
	})

	t.Run("no autoscale config is a no-op", func(t *testing.T) {
		a, rs, clk, live := newAuto(t)
		st := rs.State()
		aids := addAutoEnv(st, 2, 5, &zatterav1.Autoscale{}, 2)
		heartbeat(live, clk, aids, 100, 0)

		mustEvalAuto(t, a)

		if got := effectiveOf(t, st); got != 0 {
			t.Fatalf("effective_replicas set without autoscale config: %d", got)
		}
	})
}
