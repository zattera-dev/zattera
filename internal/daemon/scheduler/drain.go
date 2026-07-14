package scheduler

import (
	"context"

	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

// reconcileDrains empties DRAINING nodes: it stops each of their instances and,
// once none remain, marks the node DRAINED. Stateless replicas are stopped only
// after a healthy replacement exists elsewhere (the normal evaluator places it
// because a draining node's replicas don't count as "good") — a zero-downtime
// migration. Stateful/pinned replicas are stopped by design (spec F25, they
// cannot migrate) with an event.
func (s *Scheduler) reconcileDrains(ctx context.Context, st *state.Store) error {
	for _, node := range st.ListNodes() {
		if node.GetStatus() != zatterav1.NodeStatus_NODE_STATUS_DRAINING {
			continue
		}
		nodeID := node.GetMeta().GetId()

		var stopIDs, deleteIDs, statefulStopped []string
		occupied := 0
		for _, a := range st.ListAssignmentsByNode(nodeID) {
			switch a.GetDesired() {
			case zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_STOP:
				if isStopped(a) {
					deleteIDs = append(deleteIDs, a.GetMeta().GetId())
				} else {
					occupied++ // still stopping
				}
			case zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN:
				occupied++
				rel, _ := st.Release(a.GetReleaseId())
				switch {
				case isStateful(rel):
					stopIDs = append(stopIDs, a.GetMeta().GetId())
					statefulStopped = append(statefulStopped, a.GetMeta().GetId())
				case s.replacementReady(st, a):
					stopIDs = append(stopIDs, a.GetMeta().GetId())
				}
				// else stateless without a healthy replacement yet: keep serving.
			}
		}

		if err := s.applyBatch(ctx, "drain:"+nodeID, nil, stopIDs, deleteIDs); err != nil {
			return err
		}
		if len(statefulStopped) > 0 {
			s.emitNodeEvent(ctx, nodeID, "node.drain.stateful_stopped",
				"stopped %d stateful service(s) on the draining node (volumes are pinned here)", len(statefulStopped))
		}
		if occupied == 0 {
			if err := s.setNodeStatus(ctx, nodeID, zatterav1.NodeStatus_NODE_STATUS_DRAINED); err != nil {
				return err
			}
		}
	}
	return nil
}

// replacementReady reports whether enough healthy replicas of the assignment's
// release run on OTHER nodes to safely stop this one.
func (s *Scheduler) replacementReady(st *state.Store, a *zatterav1.Assignment) bool {
	env, ok := st.Environment(a.GetEnvironmentId())
	if !ok {
		return true // env gone; nothing to protect
	}
	want := desiredReplicas(env)
	if want == 0 {
		return true
	}
	healthy := 0
	for _, o := range st.ListAssignments(a.GetEnvironmentId()) {
		if o.GetNodeId() == a.GetNodeId() || o.GetReleaseId() != a.GetReleaseId() {
			continue
		}
		if o.GetDesired() != zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN {
			continue
		}
		switch o.GetObserved().GetState() {
		case zatterav1.InstanceState_INSTANCE_STATE_RUNNING, zatterav1.InstanceState_INSTANCE_STATE_HEALTHY:
			healthy++
		}
	}
	return healthy >= want
}

func (s *Scheduler) setNodeStatus(ctx context.Context, nodeID string, status zatterav1.NodeStatus) error {
	return s.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_SetNodeStatus{SetNodeStatus: &clusterv1.SetNodeStatus{
		NodeId: nodeID,
		Status: status,
	}}})
}

func (s *Scheduler) emitNodeEvent(ctx context.Context, nodeID, kind, format string, args ...any) {
	ev := &zatterav1.Event{
		Meta:     &zatterav1.Meta{Id: ids.New(), CreatedAt: timestamppb.Now()},
		Kind:     kind,
		Severity: "warning",
		NodeId:   nodeID,
		Message:  sprintf(format, args...),
	}
	_ = s.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_AppendEvents{AppendEvents: &clusterv1.AppendEvents{Events: []*zatterav1.Event{ev}}}})
}
