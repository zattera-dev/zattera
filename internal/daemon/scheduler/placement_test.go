package scheduler

import (
	"reflect"
	"sort"
	"strings"
	"testing"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/state"
)

func TestPlacement(t *testing.T) {
	t.Run("placement_constraints filter to matching nodes", func(t *testing.T) {
		st := state.New()
		st.PutNode(pnode("n1", "eu", 1000, 1024))
		st.PutNode(pnode("n2", "us", 1000, 1024))
		spec := &zatterav1.ServiceSpec{PlacementConstraints: map[string]string{"region": "eu"}}

		got, err := Place(st, specRel(spec), "env1", 1, nil)
		if err != nil {
			t.Fatalf("place: %v", err)
		}
		if !reflect.DeepEqual(got, []string{"n1"}) {
			t.Fatalf("constraint should select only n1, got %v", got)
		}
	})

	t.Run("down or unschedulable nodes are excluded", func(t *testing.T) {
		st := state.New()
		down := pnode("n1", "", 1000, 1024)
		down.Status = zatterav1.NodeStatus_NODE_STATUS_DOWN
		st.PutNode(down)
		cordoned := pnode("n2", "", 1000, 1024)
		cordoned.Schedulable = false
		st.PutNode(cordoned)
		st.PutNode(pnode("n3", "", 1000, 1024))

		got, err := Place(st, specRel(&zatterav1.ServiceSpec{}), "env1", 1, nil)
		if err != nil || !reflect.DeepEqual(got, []string{"n3"}) {
			t.Fatalf("only n3 is schedulable, got %v err=%v", got, err)
		}
	})

	t.Run("exclude skips a candidate", func(t *testing.T) {
		st := state.New()
		st.PutNode(pnode("n1", "", 1000, 1024))
		st.PutNode(pnode("n2", "", 1000, 1024))
		got, _ := Place(st, specRel(&zatterav1.ServiceSpec{}), "env1", 1, map[string]bool{"n1": true})
		if !reflect.DeepEqual(got, []string{"n2"}) {
			t.Fatalf("excluded n1 should not be picked, got %v", got)
		}
	})

	t.Run("spreads 3 replicas across 3 nodes", func(t *testing.T) {
		st := state.New()
		st.PutNode(pnode("n1", "", 1000, 4096))
		st.PutNode(pnode("n2", "", 1000, 4096))
		st.PutNode(pnode("n3", "", 1000, 4096))

		got, err := Place(st, specRel(&zatterav1.ServiceSpec{}), "env1", 3, nil)
		if err != nil {
			t.Fatalf("place: %v", err)
		}
		sorted := append([]string(nil), got...)
		sort.Strings(sorted)
		if !reflect.DeepEqual(sorted, []string{"n1", "n2", "n3"}) {
			t.Fatalf("3 replicas should land one-per-node, got %v", got)
		}
	})

	t.Run("capacity exhaustion places fewer than requested with an error", func(t *testing.T) {
		st := state.New()
		st.PutNode(pnode("n1", "", 1000, 512)) // fits two 256MB replicas
		spec := &zatterav1.ServiceSpec{Resources: &zatterav1.ResourceLimits{MemoryMb: 256}}

		got, err := Place(st, specRel(spec), "env1", 3, nil)
		if err == nil {
			t.Fatal("expected a capacity error")
		}
		if len(got) != 2 {
			t.Fatalf("only two 256MB replicas fit in 512MB, got %d: %v", len(got), got)
		}
	})

	t.Run("reservations from existing RUN assignments count against capacity", func(t *testing.T) {
		st := state.New()
		st.PutNode(pnode("n1", "", 1000, 512))
		// A release whose replica already reserves 256MB on n1.
		st.PutRelease(&zatterav1.Release{
			Meta:    &zatterav1.Meta{Id: "relX"},
			Service: &zatterav1.ServiceSpec{Resources: &zatterav1.ResourceLimits{MemoryMb: 256}},
		})
		st.PutAssignment(&zatterav1.Assignment{
			Meta: &zatterav1.Meta{Id: "aX"}, NodeId: "n1", ReleaseId: "relX",
			Desired: zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN,
		})
		spec := &zatterav1.ServiceSpec{Resources: &zatterav1.ResourceLimits{MemoryMb: 256}}

		got, err := Place(st, specRel(spec), "env1", 2, nil)
		if err == nil || len(got) != 1 {
			t.Fatalf("only one more 256MB replica fits beside the reserved one, got %v err=%v", got, err)
		}
	})

	t.Run("stateful service is pinned to its volume's node", func(t *testing.T) {
		st := state.New()
		st.PutNode(pnode("n1", "", 1000, 4096))
		st.PutNode(pnode("n2", "", 1000, 4096))
		st.PutNode(pnode("n3", "", 1000, 4096))
		st.PutEnvironment(&zatterav1.Environment{Meta: &zatterav1.Meta{Id: "env1"}, ProjectId: "p1"})
		st.PutVolume(&zatterav1.Volume{
			Meta: &zatterav1.Meta{Id: "vol1"}, ProjectId: "p1", EnvironmentId: "env1",
			Name: "data", NodeId: "n2",
		})
		spec := &zatterav1.ServiceSpec{
			Stateful: true,
			Volumes:  []*zatterav1.VolumeMount{{VolumeName: "data", MountPath: "/data"}},
		}

		got, err := Place(st, specRel(spec), "env1", 1, nil)
		if err != nil || !reflect.DeepEqual(got, []string{"n2"}) {
			t.Fatalf("stateful replica must pin to the volume node n2, got %v err=%v", got, err)
		}
	})

	t.Run("output is deterministic", func(t *testing.T) {
		st := state.New()
		st.PutNode(pnode("n3", "", 1000, 4096))
		st.PutNode(pnode("n1", "", 1000, 4096))
		st.PutNode(pnode("n2", "", 1000, 4096))
		a, _ := Place(st, specRel(&zatterav1.ServiceSpec{}), "env1", 2, nil)
		b, _ := Place(st, specRel(&zatterav1.ServiceSpec{}), "env1", 2, nil)
		if !reflect.DeepEqual(a, b) {
			t.Fatalf("placement must be deterministic: %v vs %v", a, b)
		}
		if !reflect.DeepEqual(a, []string{"n1", "n2"}) {
			t.Fatalf("expected id-ordered spread [n1 n2], got %v", a)
		}
	})
}

// specRel wraps a bare spec in a release, as Place consumes releases (T-88).
func specRel(spec *zatterav1.ServiceSpec) *zatterav1.Release {
	return &zatterav1.Release{Service: spec}
}

func pnode(id, region string, cpu, mem uint32) *zatterav1.Node {
	labels := map[string]string{}
	if region != "" {
		labels["region"] = region
	}
	return &zatterav1.Node{
		Meta:        &zatterav1.Meta{Id: id},
		Status:      zatterav1.NodeStatus_NODE_STATUS_ALIVE,
		Schedulable: true,
		Labels:      labels,
		Capacity:    &zatterav1.ResourceLimits{CpuMillis: cpu, MemoryMb: mem},
	}
}

// TestUnsatisfiablePlacementConstraintErrors: a constraint no node matches used
// to return zero candidates and no error, which parks the replicas silently and
// looks identical to "still deploying". It must fail like an arch mismatch does.
func TestUnsatisfiablePlacementConstraintErrors(t *testing.T) {
	st := state.New()
	st.PutNode(pnode("n1", "eu", 1000, 1024))
	st.PutNode(pnode("n2", "us", 1000, 1024))
	spec := &zatterav1.ServiceSpec{PlacementConstraints: map[string]string{"region": "moon"}}

	got, err := Place(st, specRel(spec), "env1", 1, nil)
	if err == nil {
		t.Fatalf("want an error for an unsatisfiable constraint, got placements %v", got)
	}
	if !strings.Contains(err.Error(), "region=moon") {
		t.Errorf("error should name the constraint, got %q", err)
	}
}

// TestPlacementConstraintSatisfiedByFullNode: a constraint that DOES match a
// node still fails when nothing fits, but must not be reported as a constraint
// mismatch — "no node matches region=eu" would send an operator hunting for a
// typo when the real answer is that the cluster is out of room.
func TestPlacementConstraintSatisfiedByFullNode(t *testing.T) {
	st := state.New()
	st.PutNode(pnode("n1", "eu", 1, 1))
	spec := &zatterav1.ServiceSpec{
		PlacementConstraints: map[string]string{"region": "eu"},
		Resources:            &zatterav1.ResourceLimits{CpuMillis: 4000, MemoryMb: 4096},
	}

	_, err := Place(st, specRel(spec), "env1", 1, nil)
	if err == nil {
		t.Fatal("want an error when nothing fits")
	}
	if strings.Contains(err.Error(), "no node matches") {
		t.Errorf("capacity shortfall must not read as a constraint mismatch: %q", err)
	}
}
