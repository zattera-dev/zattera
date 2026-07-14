package scheduler

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

// desiredReplicas is the target replica count for an env: the autoscaler's
// effective count when set, else replicas.min. 0 when there is no active
// release (nothing to run).
func desiredReplicas(env *zatterav1.Environment) int {
	if env.GetActiveReleaseId() == "" {
		return 0
	}
	if r := env.GetEffectiveReplicas(); r > 0 {
		return int(r)
	}
	return int(env.GetService().GetReplicas().GetMin())
}

// newAssignment builds a desired-RUN assignment for a replica of rel on nodeID.
func newAssignment(env *zatterav1.Environment, rel *zatterav1.Release, nodeID string) *zatterav1.Assignment {
	now := timestamppb.Now()
	return &zatterav1.Assignment{
		Meta:          &zatterav1.Meta{Id: ids.New(), CreatedAt: now, UpdatedAt: now},
		NodeId:        nodeID,
		ProjectId:     env.GetProjectId(),
		AppId:         env.GetAppId(),
		EnvironmentId: env.GetMeta().GetId(),
		ReleaseId:     rel.GetMeta().GetId(),
		Desired:       zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN,
		ConfigHash:    rel.GetConfigHash(),
	}
}

// nodeDown reports whether a node is unknown or not ALIVE.
func nodeDown(st *state.Store, nodeID string) bool {
	n, ok := st.Node(nodeID)
	return !ok || n.GetStatus() != zatterav1.NodeStatus_NODE_STATUS_ALIVE
}

// nodeDraining reports whether a node is being drained.
func nodeDraining(st *state.Store, nodeID string) bool {
	n, ok := st.Node(nodeID)
	return ok && n.GetStatus() == zatterav1.NodeStatus_NODE_STATUS_DRAINING
}

func isStopped(a *zatterav1.Assignment) bool {
	return a.GetObserved().GetState() == zatterav1.InstanceState_INSTANCE_STATE_STOPPED
}

func isStateful(rel *zatterav1.Release) bool {
	return rel.GetService().GetStateful()
}

// labelsMatch reports whether node labels satisfy all placement constraints.
func labelsMatch(nodeLabels, constraints map[string]string) bool {
	for k, v := range constraints {
		if nodeLabels[k] != v {
			return false
		}
	}
	return true
}

// isTerminalPhase reports whether a deployment phase is finished, so the
// scheduler may resume ownership of the env. T-26 refines which live phases own
// placement.
func isTerminalPhase(p zatterav1.DeploymentPhase) bool {
	switch p {
	case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_SUCCEEDED,
		zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_FAILED,
		zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_ROLLED_BACK,
		zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_SUPERSEDED,
		zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_UNSPECIFIED:
		return true
	default:
		return false
	}
}

func sprintf(format string, args ...any) string { return fmt.Sprintf(format, args...) }

// --- deployment helpers ---------------------------------------------------

// greenAssignments returns the RUN assignments tagged with this deployment id.
func greenAssignments(st *state.Store, d *zatterav1.Deployment) []*zatterav1.Assignment {
	var out []*zatterav1.Assignment
	for _, a := range st.ListAssignments(d.GetEnvironmentId()) {
		if a.GetDeploymentId() == d.GetMeta().GetId() && a.GetDesired() == zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN {
			out = append(out, a)
		}
	}
	return out
}

// blueAssignments returns the RUN assignments of the deployment's previous
// (outgoing) release that are not part of this deployment's green set.
func blueAssignments(st *state.Store, d *zatterav1.Deployment) []*zatterav1.Assignment {
	prev := d.GetPreviousReleaseId()
	if prev == "" {
		return nil
	}
	var out []*zatterav1.Assignment
	for _, a := range st.ListAssignments(d.GetEnvironmentId()) {
		if a.GetReleaseId() == prev &&
			a.GetDeploymentId() != d.GetMeta().GetId() &&
			a.GetDesired() == zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN {
			out = append(out, a)
		}
	}
	return out
}

// greenAssignment builds a desired-RUN replica of rel tagged with deploymentID.
func greenAssignment(env *zatterav1.Environment, rel *zatterav1.Release, nodeID, deploymentID string) *zatterav1.Assignment {
	a := newAssignment(env, rel, nodeID)
	a.DeploymentId = deploymentID
	return a
}

// deployReplicas is the target green count for a deployment.
func deployReplicas(env *zatterav1.Environment, rel *zatterav1.Release) int {
	if r := env.GetEffectiveReplicas(); r > 0 {
		return int(r)
	}
	return int(rel.GetService().GetReplicas().GetMin())
}

// healthDeadline is the overall HEALTHCHECKING window: grace × 2 + a pad.
func healthDeadline(rel *zatterav1.Release) time.Duration {
	grace := defaultHealthGrace
	if g := rel.GetService().GetHealthcheck().GetGracePeriod().AsDuration(); g > 0 {
		grace = g
	}
	return 2*grace + healthDeadlineExtra
}

func anyFailed(as []*zatterav1.Assignment) bool {
	for _, a := range as {
		if a.GetObserved().GetState() == zatterav1.InstanceState_INSTANCE_STATE_FAILED {
			return true
		}
	}
	return false
}

func nodeSet(as []*zatterav1.Assignment) map[string]bool {
	m := map[string]bool{}
	for _, a := range as {
		m[a.GetNodeId()] = true
	}
	return m
}

// deploymentNewer reports whether a is newer than b (by creation time, id tie).
func deploymentNewer(a, b *zatterav1.Deployment) bool {
	at, bt := a.GetMeta().GetCreatedAt().AsTime(), b.GetMeta().GetCreatedAt().AsTime()
	if !at.Equal(bt) {
		return at.After(bt)
	}
	return a.GetMeta().GetId() > b.GetMeta().GetId()
}

// putAssignments wraps a batch in a PutAssignments command (request metadata is
// filled by the caller's apply()).
func putAssignments(as []*zatterav1.Assignment) *clusterv1.Command {
	return &clusterv1.Command{Mutation: &clusterv1.Command_PutAssignments{PutAssignments: &clusterv1.PutAssignments{Assignments: as}}}
}
