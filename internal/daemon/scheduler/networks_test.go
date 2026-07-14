package scheduler

import (
	"context"
	"testing"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/state"
)

func runAssignment(st *state.Store, id, node string) {
	st.PutAssignment(&zatterav1.Assignment{
		Meta: &zatterav1.Meta{Id: id}, ProjectId: "proj", EnvironmentId: envID, NodeId: node,
		Desired: zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN,
	})
}

func TestNetworksReconcileAllocatesAndFrees(t *testing.T) {
	s, rs := newSched(t)
	st := rs.State()
	ctx := context.Background()

	// Two nodes running the env → two distinct cluster-unique subnets.
	runAssignment(st, "a1", "n1")
	runAssignment(st, "a2", "n2")
	if err := s.reconcileNetworks(ctx, st); err != nil {
		t.Fatal(err)
	}
	sub1, ok1 := st.NetworkAllocation("proj", envID, "n1")
	sub2, ok2 := st.NetworkAllocation("proj", envID, "n2")
	if !ok1 || !ok2 || sub1 == "" || sub2 == "" {
		t.Fatalf("allocations missing: %q/%v %q/%v", sub1, ok1, sub2, ok2)
	}
	if sub1 == sub2 {
		t.Fatalf("subnets must be cluster-unique, both %q", sub1)
	}

	// Idempotent: a second pass changes nothing.
	if err := s.reconcileNetworks(ctx, st); err != nil {
		t.Fatal(err)
	}
	if sub, _ := st.NetworkAllocation("proj", envID, "n1"); sub != sub1 {
		t.Fatalf("allocation changed on re-run: %q → %q", sub1, sub)
	}

	// Drop n2's instance → its allocation is freed on the next pass.
	st.DeleteAssignments([]string{"a2"})
	if err := s.reconcileNetworks(ctx, st); err != nil {
		t.Fatal(err)
	}
	if _, ok := st.NetworkAllocation("proj", envID, "n2"); ok {
		t.Fatal("n2 allocation should have been freed")
	}
	if _, ok := st.NetworkAllocation("proj", envID, "n1"); !ok {
		t.Fatal("n1 allocation must survive")
	}
}
