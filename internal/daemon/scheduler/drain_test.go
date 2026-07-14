package scheduler

import (
	"strings"
	"testing"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/state"
)

func TestDrain(t *testing.T) {
	s, rs := newSched(t)
	st := rs.State()

	// Two nodes; n1 will be drained.
	addNodes(st, "n1", "n2")

	// A stateless service with its single replica on n1.
	spec := &zatterav1.ServiceSpec{Replicas: &zatterav1.ReplicaRange{Min: 1}}
	st.PutRelease(&zatterav1.Release{Meta: &zatterav1.Meta{Id: "relS"}, EnvironmentId: "envS", ConfigHash: "h", Service: spec})
	st.PutEnvironment(&zatterav1.Environment{Meta: &zatterav1.Meta{Id: "envS"}, ActiveReleaseId: "relS", Service: spec})
	st.PutAssignment(&zatterav1.Assignment{
		Meta: &zatterav1.Meta{Id: "sless"}, NodeId: "n1",
		EnvironmentId: "envS", ReleaseId: "relS",
		Desired:  zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN,
		Observed: &zatterav1.AssignmentObserved{State: zatterav1.InstanceState_INSTANCE_STATE_HEALTHY},
	})

	// A stateful service pinned to n1 (a volume lives there).
	statefulSpec := &zatterav1.ServiceSpec{
		Replicas: &zatterav1.ReplicaRange{Min: 1}, Stateful: true,
		Volumes: []*zatterav1.VolumeMount{{VolumeName: "data", MountPath: "/data"}},
	}
	st.PutRelease(&zatterav1.Release{Meta: &zatterav1.Meta{Id: "relF"}, EnvironmentId: "envF", ConfigHash: "h", Service: statefulSpec})
	st.PutEnvironment(&zatterav1.Environment{Meta: &zatterav1.Meta{Id: "envF"}, ProjectId: "p1", ActiveReleaseId: "relF", Service: statefulSpec})
	st.PutVolume(&zatterav1.Volume{Meta: &zatterav1.Meta{Id: "vol"}, ProjectId: "p1", EnvironmentId: "envF", Name: "data", NodeId: "n1"})
	st.PutAssignment(&zatterav1.Assignment{
		Meta: &zatterav1.Meta{Id: "sfull"}, NodeId: "n1",
		EnvironmentId: "envF", ReleaseId: "relF",
		Desired:  zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN,
		Observed: &zatterav1.AssignmentObserved{State: zatterav1.InstanceState_INSTANCE_STATE_HEALTHY},
	})

	// Start draining n1.
	n1, _ := st.Node("n1")
	n1.Status = zatterav1.NodeStatus_NODE_STATUS_DRAINING
	n1.Schedulable = false
	st.PutNode(n1)

	// Pass 1: a stateless replacement is placed on n2; the stateful replica is
	// stopped by design; the stateless old keeps running (replacement not
	// healthy yet).
	mustEval(t, s)
	if !onNode(st, "n2", "envS", zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN) {
		t.Fatalf("a stateless replacement should be placed on n2")
	}
	if sfull, _ := st.Assignment("sfull"); sfull.GetDesired() != zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_STOP {
		t.Fatalf("stateful replica should be stopped on drain, got %v", sfull.GetDesired())
	}
	if !hasEvent(st, "node.drain.stateful_stopped") {
		t.Fatal("expected a stateful-stopped drain event")
	}

	// The replacement becomes healthy; the stateful stop is observed.
	markHealthy(st, "envS", "n2")
	markStopped(st, "sfull")

	// Pass 2: the stateless old is now safe to stop.
	mustEval(t, s)
	if sless, _ := st.Assignment("sless"); sless.GetDesired() != zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_STOP {
		t.Fatalf("stateless old should stop once the replacement is healthy, got %v", sless.GetDesired())
	}
	// The stopped stateful assignment is reaped.
	if _, ok := st.Assignment("sfull"); ok {
		t.Fatal("stopped stateful assignment should be deleted")
	}

	// The stateless old reports stopped.
	markStopped(st, "sless")

	// Pass 3: n1 has no instances left → DRAINED.
	mustEval(t, s)
	n, _ := st.Node("n1")
	if n.GetStatus() != zatterav1.NodeStatus_NODE_STATUS_DRAINED {
		t.Fatalf("node should be DRAINED after all instances migrate/stop, got %v", n.GetStatus())
	}
}

func onNode(st *state.Store, node, envID string, desired zatterav1.AssignmentDesired) bool {
	for _, a := range st.ListAssignments(envID) {
		if a.GetNodeId() == node && a.GetDesired() == desired {
			return true
		}
	}
	return false
}

func markHealthy(st *state.Store, envID, node string) {
	for _, a := range st.ListAssignments(envID) {
		if a.GetNodeId() == node {
			st.SetAssignmentObserved(node, map[string]*zatterav1.AssignmentObserved{
				a.GetMeta().GetId(): {State: zatterav1.InstanceState_INSTANCE_STATE_HEALTHY},
			})
		}
	}
}

func markStopped(st *state.Store, id string) {
	a, _ := st.Assignment(id)
	st.SetAssignmentObserved(a.GetNodeId(), map[string]*zatterav1.AssignmentObserved{
		id: {State: zatterav1.InstanceState_INSTANCE_STATE_STOPPED},
	})
}

func hasEvent(st *state.Store, kind string) bool {
	for _, e := range st.ListEvents(100) {
		if strings.Contains(e.GetKind(), kind) {
			return true
		}
	}
	return false
}
