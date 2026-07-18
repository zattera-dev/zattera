package scheduler

import (
	"context"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

// leaseTTL is the fencing-lease validity window; generous vs clock skew.
// Renewal is not on its own timer — reconcileLeases runs from the scheduler's
// eval loop, so the real cadence is evalTick (15s), comfortably under the TTL
// (spec §9.1). A separate leaseRenew constant used to sit here documenting a
// 20s intent that nothing read; it was deleted rather than left to be mistaken
// for the actual cadence.
const leaseTTL = 60 * time.Second

// ensureVolumes auto-creates the Volume objects a stateful service declares and
// tracks NODE_LOST when a volume's pinned node goes down (spec §3.8). Runs
// before placement so pinnedNodeID can resolve.
func (s *Scheduler) ensureVolumes(ctx context.Context, st *state.Store) error {
	for _, env := range st.ListEnvironments("", "") {
		rel := s.activeStatefulRelease(st, env)
		if rel == nil {
			continue
		}
		for _, vm := range rel.GetService().GetVolumes() {
			v, ok := st.VolumeByName(env.GetProjectId(), env.GetMeta().GetId(), vm.GetVolumeName())
			if !ok {
				if err := s.createVolume(ctx, st, env, vm.GetVolumeName()); err != nil {
					return err
				}
				continue
			}
			if err := s.trackVolumeNode(ctx, st, env, v); err != nil {
				return err
			}
		}
	}
	return nil
}

// createVolume pins a new volume to the least-used ALIVE worker and stores it.
func (s *Scheduler) createVolume(ctx context.Context, st *state.Store, env *zatterav1.Environment, name string) error {
	node := leastUsedVolumeNode(st)
	if node == "" {
		s.emitEvent(ctx, env, "volume.no_capacity", "warning", "no schedulable node for volume %q", name)
		return nil
	}
	now := timestamppb.New(s.clock.Now())
	v := &zatterav1.Volume{
		Meta:          &zatterav1.Meta{Id: ids.New(), CreatedAt: now, UpdatedAt: now},
		ProjectId:     env.GetProjectId(),
		EnvironmentId: env.GetMeta().GetId(),
		Name:          name,
		NodeId:        node,
		Status:        zatterav1.VolumeStatus_VOLUME_STATUS_ACTIVE,
	}
	if err := s.apply(ctx, &clusterv1.Command{
		Mutation: &clusterv1.Command_PutVolume{PutVolume: &clusterv1.PutVolume{Volume: v}},
	}); err != nil {
		return err
	}
	s.log.Info("volume created", "volume", v.GetMeta().GetId(), "name", name, "node", node, "env", env.GetMeta().GetId())
	return nil
}

// trackVolumeNode flips a volume to NODE_LOST when its pinned node is down and
// back to ACTIVE when the node returns (spec §3.8: stateful data is pinned, so a
// down node stops the service rather than rescheduling it).
func (s *Scheduler) trackVolumeNode(ctx context.Context, st *state.Store, env *zatterav1.Environment, v *zatterav1.Volume) error {
	down := nodeDown(st, v.GetNodeId())
	switch {
	case down && v.GetStatus() != zatterav1.VolumeStatus_VOLUME_STATUS_NODE_LOST:
		if err := s.setVolumeStatus(ctx, v, zatterav1.VolumeStatus_VOLUME_STATUS_NODE_LOST); err != nil {
			return err
		}
		s.emitEvent(ctx, env, "volume.node_lost", "warning",
			"volume %q node %s is down; stateful service stopped until it returns", v.GetName(), v.GetNodeId())
	case !down && v.GetStatus() == zatterav1.VolumeStatus_VOLUME_STATUS_NODE_LOST:
		if err := s.setVolumeStatus(ctx, v, zatterav1.VolumeStatus_VOLUME_STATUS_ACTIVE); err != nil {
			return err
		}
		s.emitEvent(ctx, env, "volume.recovered", "info", "volume %q node %s recovered", v.GetName(), v.GetNodeId())
	}
	return nil
}

func (s *Scheduler) setVolumeStatus(ctx context.Context, v *zatterav1.Volume, status zatterav1.VolumeStatus) error {
	nv := proto.Clone(v).(*zatterav1.Volume)
	nv.Status = status
	nv.GetMeta().UpdatedAt = timestamppb.New(s.clock.Now())
	return s.apply(ctx, &clusterv1.Command{
		Mutation: &clusterv1.Command_PutVolume{PutVolume: &clusterv1.PutVolume{Volume: nv}},
	})
}

// reconcileLeases renews the fencing lease for every volume whose stateful
// assignment is RUN on an ALIVE pinned node. Runs after placement so a freshly
// placed assignment is leased in the same evaluation pass. A lease is never
// stolen from another node while it is still valid (the double-run guard).
func (s *Scheduler) reconcileLeases(ctx context.Context, st *state.Store) error {
	now := s.clock.Now()
	for _, v := range st.ListVolumes("") {
		if nodeDown(st, v.GetNodeId()) {
			continue // node down: let the lease lapse; never renew for a dead node
		}
		holder := statefulHolder(st, v)
		if holder == nil {
			continue // nothing running to fence
		}
		if lease := v.GetLease(); leaseHeldByOther(lease, v.GetNodeId(), now) {
			continue // a still-valid lease names another node: do not steal
		}
		lease := &zatterav1.VolumeLease{
			NodeId:       v.GetNodeId(),
			AssignmentId: holder.GetMeta().GetId(),
			ExpiresAt:    timestamppb.New(now.Add(leaseTTL)),
		}
		if err := s.apply(ctx, &clusterv1.Command{
			Mutation: &clusterv1.Command_PutVolumeLease{PutVolumeLease: &clusterv1.PutVolumeLease{VolumeId: v.GetMeta().GetId(), Lease: lease}},
		}); err != nil {
			return err
		}
	}
	return nil
}

// activeStatefulRelease returns the env's active release iff it is a stateful
// service that declares at least one volume.
func (s *Scheduler) activeStatefulRelease(st *state.Store, env *zatterav1.Environment) *zatterav1.Release {
	if env.GetActiveReleaseId() == "" {
		return nil
	}
	rel, ok := st.Release(env.GetActiveReleaseId())
	if !ok || !isStateful(rel) || len(rel.GetService().GetVolumes()) == 0 {
		return nil
	}
	return rel
}

// statefulHolder returns the RUN assignment fencing volume v: the env's active
// stateful assignment on v's pinned node. When several qualify (a misconfigured
// replicas>1), the lowest id is chosen so the lease holder is stable — the agent
// only starts the leased assignment, so the others never run (exactly-one).
func statefulHolder(st *state.Store, v *zatterav1.Volume) *zatterav1.Assignment {
	var holder *zatterav1.Assignment
	for _, a := range st.ListAssignments(v.GetEnvironmentId()) {
		if a.GetJobId() != "" || a.GetDesired() != zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN {
			continue
		}
		if a.GetNodeId() != v.GetNodeId() {
			continue
		}
		if holder == nil || a.GetMeta().GetId() < holder.GetMeta().GetId() {
			holder = a
		}
	}
	return holder
}

// leaseHeldByOther reports whether lease is still valid and names a node other
// than want.
func leaseHeldByOther(lease *zatterav1.VolumeLease, want string, now time.Time) bool {
	if lease == nil || lease.GetNodeId() == want {
		return false
	}
	return !leaseExpired(lease, now)
}

// leaseExpired reports whether the lease is absent or past its TTL.
func leaseExpired(lease *zatterav1.VolumeLease, now time.Time) bool {
	if lease == nil || lease.GetExpiresAt() == nil {
		return true
	}
	return !now.Before(lease.GetExpiresAt().AsTime())
}

// leastUsedVolumeNode picks the ALIVE schedulable worker hosting the fewest
// volumes (ties broken by node id).
func leastUsedVolumeNode(st *state.Store) string {
	counts := map[string]int{}
	for _, v := range st.ListVolumes("") {
		counts[v.GetNodeId()]++
	}
	best, bestCount := "", 0
	for _, n := range st.ListNodes() {
		id := n.GetMeta().GetId()
		if n.GetStatus() != zatterav1.NodeStatus_NODE_STATUS_ALIVE || !n.GetSchedulable() {
			continue
		}
		c := counts[id]
		if best == "" || c < bestCount || (c == bestCount && id < best) {
			best, bestCount = id, c
		}
	}
	return best
}
