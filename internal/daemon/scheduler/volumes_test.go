package scheduler

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/state"
)

// addStatefulEnv creates a stateful env+release declaring one volume mount.
func addStatefulEnv(st *state.Store, volName string) {
	spec := &zatterav1.ServiceSpec{
		Replicas:  &zatterav1.ReplicaRange{Min: 1, Max: 1},
		Stateful:  true,
		Volumes:   []*zatterav1.VolumeMount{{VolumeName: volName, MountPath: "/data"}},
		Resources: &zatterav1.ResourceLimits{MemoryMb: 128},
	}
	st.PutRelease(&zatterav1.Release{Meta: &zatterav1.Meta{Id: relID}, EnvironmentId: envID, ConfigHash: "h", Service: spec})
	st.PutEnvironment(&zatterav1.Environment{
		Meta: &zatterav1.Meta{Id: envID}, Name: "production", ProjectId: "p1",
		ActiveReleaseId: relID, Service: spec,
	})
}

func theVolume(t *testing.T, st *state.Store) *zatterav1.Volume {
	t.Helper()
	vols := st.ListVolumes("")
	if len(vols) != 1 {
		t.Fatalf("want exactly 1 volume, got %d", len(vols))
	}
	return vols[0]
}

func runAssignments(st *state.Store) []*zatterav1.Assignment {
	var out []*zatterav1.Assignment
	for _, a := range st.ListAssignments(envID) {
		if a.GetDesired() == zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN {
			out = append(out, a)
		}
	}
	return out
}

func TestVolumeLease(t *testing.T) {
	t.Run("auto-create pins the volume and places one replica", func(t *testing.T) {
		s, rs := newSched(t)
		st := rs.State()
		addNodes(st, "n1", "n2")
		addStatefulEnv(st, "data")

		mustEval(t, s)

		v := theVolume(t, st)
		if v.GetName() != "data" || v.GetStatus() != zatterav1.VolumeStatus_VOLUME_STATUS_ACTIVE {
			t.Fatalf("volume not ACTIVE/named: %+v", v)
		}
		if v.GetNodeId() != "n1" && v.GetNodeId() != "n2" {
			t.Fatalf("volume pinned to unknown node %q", v.GetNodeId())
		}
		run := runAssignments(st)
		if len(run) != 1 {
			t.Fatalf("want 1 RUN assignment, got %d", len(run))
		}
		if run[0].GetNodeId() != v.GetNodeId() {
			t.Fatalf("assignment on %s but volume pinned to %s", run[0].GetNodeId(), v.GetNodeId())
		}
	})

	t.Run("lease acquired then renewed", func(t *testing.T) {
		s, rs := newSched(t)
		clk := s.clock.(*clock.Fake)
		st := rs.State()
		addNodes(st, "n1", "n2")
		addStatefulEnv(st, "data")

		mustEval(t, s)
		v := theVolume(t, st)
		run := runAssignments(st)[0]
		lease := v.GetLease()
		if lease == nil {
			t.Fatal("no lease acquired")
		}
		if lease.GetNodeId() != v.GetNodeId() || lease.GetAssignmentId() != run.GetMeta().GetId() {
			t.Fatalf("lease names %s/%s, want %s/%s", lease.GetNodeId(), lease.GetAssignmentId(), v.GetNodeId(), run.GetMeta().GetId())
		}
		want := clk.Now().Add(leaseTTL)
		if !lease.GetExpiresAt().AsTime().Equal(want) {
			t.Fatalf("lease expiry %v, want %v", lease.GetExpiresAt().AsTime(), want)
		}

		// Renew: advance the clock, re-evaluate, expiry must move forward.
		clk.Advance(evalTick)
		mustEval(t, s)
		v2 := theVolume(t, st)
		newWant := clk.Now().Add(leaseTTL)
		if !v2.GetLease().GetExpiresAt().AsTime().Equal(newWant) {
			t.Fatalf("lease not renewed: expiry %v, want %v", v2.GetLease().GetExpiresAt().AsTime(), newWant)
		}
	})

	t.Run("node down → NODE_LOST, lease lapses, no reschedule", func(t *testing.T) {
		s, rs := newSched(t)
		clk := s.clock.(*clock.Fake)
		st := rs.State()
		addNodes(st, "n1", "n2")
		addStatefulEnv(st, "data")
		mustEval(t, s)

		v := theVolume(t, st)
		pinned := v.GetNodeId()
		leaseAtDown := v.GetLease().GetExpiresAt().AsTime()

		// The pinned node goes DOWN.
		down, _ := st.Node(pinned)
		down.Status = zatterav1.NodeStatus_NODE_STATUS_DOWN
		st.PutNode(down)

		clk.Advance(evalTick)
		mustEval(t, s)

		v = theVolume(t, st)
		if v.GetStatus() != zatterav1.VolumeStatus_VOLUME_STATUS_NODE_LOST {
			t.Fatalf("volume status = %v, want NODE_LOST", v.GetStatus())
		}
		// Lease was NOT renewed (a dead node keeps no fresh lease).
		if !v.GetLease().GetExpiresAt().AsTime().Equal(leaseAtDown) {
			t.Fatalf("lease renewed for a down node: %v", v.GetLease().GetExpiresAt().AsTime())
		}
		// No second replica placed anywhere — the stateful service is not
		// rescheduled off its pinned node (no double-run).
		if run := runAssignments(st); len(run) != 1 || run[0].GetNodeId() != pinned {
			t.Fatalf("stateful replica rescheduled: %d RUN assignments", len(run))
		}

		// Past the TTL the lease is expired.
		clk.Advance(leaseTTL)
		if !leaseExpired(v.GetLease(), clk.Now()) {
			t.Fatal("lease should be expired past its TTL")
		}
	})

	t.Run("recovery clears NODE_LOST", func(t *testing.T) {
		s, rs := newSched(t)
		st := rs.State()
		addNodes(st, "n1", "n2")
		addStatefulEnv(st, "data")
		mustEval(t, s)
		v := theVolume(t, st)
		pinned := v.GetNodeId()

		down, _ := st.Node(pinned)
		down.Status = zatterav1.NodeStatus_NODE_STATUS_DOWN
		st.PutNode(down)
		mustEval(t, s)
		if theVolume(t, st).GetStatus() != zatterav1.VolumeStatus_VOLUME_STATUS_NODE_LOST {
			t.Fatal("expected NODE_LOST after node down")
		}

		up, _ := st.Node(pinned)
		up.Status = zatterav1.NodeStatus_NODE_STATUS_ALIVE
		st.PutNode(up)
		mustEval(t, s)
		if theVolume(t, st).GetStatus() != zatterav1.VolumeStatus_VOLUME_STATUS_ACTIVE {
			t.Fatal("expected ACTIVE after node recovery")
		}
	})
}

func TestLeaseHelpers(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	fresh := &zatterav1.VolumeLease{NodeId: "n1", ExpiresAt: timestamppb.New(now.Add(30 * time.Second))}
	stale := &zatterav1.VolumeLease{NodeId: "n1", ExpiresAt: timestamppb.New(now.Add(-time.Second))}

	if leaseExpired(fresh, now) {
		t.Error("fresh lease reported expired")
	}
	if !leaseExpired(stale, now) {
		t.Error("stale lease reported valid")
	}
	if !leaseExpired(nil, now) {
		t.Error("nil lease should read as expired")
	}
	// A valid lease naming another node blocks; same node or expired does not.
	if !leaseHeldByOther(fresh, "n2", now) {
		t.Error("fresh lease on n1 should block n2")
	}
	if leaseHeldByOther(fresh, "n1", now) {
		t.Error("lease should not block its own node")
	}
	if leaseHeldByOther(stale, "n2", now) {
		t.Error("expired lease should not block")
	}
}
