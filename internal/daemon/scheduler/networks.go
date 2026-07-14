package scheduler

import (
	"context"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/agent"
	"github.com/zattera-dev/zattera/internal/state"
)

// reconcileNetworks keeps the per-(project,env,node) bridge subnet allocations
// in sync with running instances (T-46): it allocates a cluster-unique
// 10.201.X.0/24 for each tuple that runs a replica and frees allocations whose
// tuple no longer runs anything. The agent attaches containers to the network
// named for the env with the allocated subnet.
func (s *Scheduler) reconcileNetworks(ctx context.Context, st *state.Store) error {
	// Tuples that currently need a network (a RUN assignment on a node).
	needed := map[string]netTuple{}
	for _, a := range st.ListAssignments("") {
		if a.GetDesired() != zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN || a.GetNodeId() == "" {
			continue
		}
		t := netTuple{a.GetProjectId(), a.GetEnvironmentId(), a.GetNodeId()}
		needed[t.key()] = t
	}

	// Free stale allocations; collect the subnets still in use.
	have := map[string]bool{}
	var used []string
	for _, na := range st.ListNetworkAllocations() {
		t := netTuple{na.GetProjectId(), na.GetEnvironmentId(), na.GetNodeId()}
		if _, ok := needed[t.key()]; ok {
			have[t.key()] = true
			if na.GetSubnetCidr() != "" {
				used = append(used, na.GetSubnetCidr())
			}
			continue
		}
		if err := s.setAllocation(ctx, t, ""); err != nil {
			return err
		}
	}

	// Allocate for needed tuples that don't have one yet.
	for key, t := range needed {
		if have[key] {
			continue
		}
		subnet, err := agent.NextFreeSubnet(used)
		if err != nil {
			s.log.Warn("network subnet pool exhausted", "err", err)
			continue
		}
		if err := s.setAllocation(ctx, t, subnet); err != nil {
			return err
		}
		used = append(used, subnet)
	}
	return nil
}

type netTuple struct{ project, env, node string }

func (t netTuple) key() string { return t.project + "/" + t.env + "/" + t.node }

func (s *Scheduler) setAllocation(ctx context.Context, t netTuple, subnet string) error {
	return s.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_PutNetworkAllocation{
		PutNetworkAllocation: &clusterv1.PutNetworkAllocation{
			ProjectId: t.project, EnvironmentId: t.env, NodeId: t.node, SubnetCidr: subnet,
		},
	}})
}
