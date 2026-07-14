package agent

import (
	"testing"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/testutil/fakeruntime"
)

func TestNetworksSubnetAllocation(t *testing.T) {
	// First allocation from an empty pool.
	s0, err := NextFreeSubnet(nil)
	if err != nil || s0 != "10.201.0.0/24" {
		t.Fatalf("first subnet = %q err=%v", s0, err)
	}
	// Skips taken slots and picks the lowest free one (reuse-after-free).
	used := []string{"10.201.0.0/24", "10.201.1.0/24", "10.201.3.0/24"}
	got, err := NextFreeSubnet(used)
	if err != nil || got != "10.201.2.0/24" {
		t.Fatalf("next free = %q err=%v, want 10.201.2.0/24", got, err)
	}
	// Freeing slot 0 makes it the next pick again.
	got, _ = NextFreeSubnet([]string{"10.201.1.0/24", "10.201.2.0/24"})
	if got != "10.201.0.0/24" {
		t.Fatalf("reuse-after-free = %q, want 10.201.0.0/24", got)
	}
	// Foreign CIDRs are ignored when scanning.
	got, _ = NextFreeSubnet([]string{"10.90.0.0/24", "10.201.0.0/24"})
	if got != "10.201.1.0/24" {
		t.Fatalf("with foreign cidr = %q, want 10.201.1.0/24", got)
	}
}

func TestNetworksPoolExhaustion(t *testing.T) {
	used := make([]string, 256)
	for i := 0; i < 256; i++ {
		used[i], _ = NextFreeSubnet(used[:i])
	}
	if _, err := NextFreeSubnet(used); err == nil {
		t.Fatal("expected pool exhaustion error")
	}
}

func TestNetworksGatewayAndName(t *testing.T) {
	gw, err := GatewayIP("10.201.7.0/24")
	if err != nil || gw != "10.201.7.1" {
		t.Fatalf("gateway = %q err=%v", gw, err)
	}
	name := NetworkName("project-abcdefghXYZ", "01HXENVID000000000000")
	if name != "zt-project--01hxenvid000" {
		t.Fatalf("network name = %q", name)
	}
}

// TestNetworksExecutorWiring asserts the executor ensures the bridge network and
// wires the container's Network + DNS from the runtime frame's subnet.
func TestNetworksExecutorWiring(t *testing.T) {
	rt := fakeruntime.New()
	e := newExec(rt, discardRec())

	rp := &clusterv1.AssignmentRuntime{
		ImageRef:   "nginx:alpine",
		Spec:       &zatterav1.ServiceSpec{},
		SubnetCidr: "10.201.5.0/24",
	}
	e.reconcile(ctx(), buildSet(1, pair(assign("a1", "h1", run), rp)))

	snap := rt.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("want 1 container, got %d", len(snap))
	}
	spec := snap[0].Spec
	wantName := NetworkName("proj1", "env1")
	if spec.Network != wantName {
		t.Fatalf("container network = %q, want %q", spec.Network, wantName)
	}
	if len(spec.DNS) != 1 || spec.DNS[0] != "10.201.5.1" {
		t.Fatalf("container DNS = %v, want [10.201.5.1]", spec.DNS)
	}
}

// TestNetworksNoSubnetNoNetwork: without a subnet the container joins the
// default bridge (no explicit network/DNS).
func TestNetworksNoSubnetNoNetwork(t *testing.T) {
	rt := fakeruntime.New()
	e := newExec(rt, discardRec())
	rp := &clusterv1.AssignmentRuntime{ImageRef: "nginx:alpine", Spec: &zatterav1.ServiceSpec{}}
	e.reconcile(ctx(), buildSet(1, pair(assign("a1", "h1", run), rp)))

	spec := rt.Snapshot()[0].Spec
	if spec.Network != "" || len(spec.DNS) != 0 {
		t.Fatalf("no subnet should mean no explicit network/DNS: net=%q dns=%v", spec.Network, spec.DNS)
	}
}
