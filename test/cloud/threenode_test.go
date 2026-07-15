//go:build cloud

package cloud

import (
	"testing"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

// TestThreeNodeCluster spins up a REAL 3-node cluster on Hetzner — one control
// node and two workers (mixed arch) — and asserts all three register, come up
// ALIVE, and run Docker. Everything is destroyed on exit.
//
//	HCLOUD_TOKEN=... go test -tags cloud ./test/cloud/ -run TestThreeNodeCluster -v
//
// On failure it writes a per-node debug bundle; add ZT_CLOUD_KEEP=1 to keep the
// cluster up and print an attach kit for live debugging.
func TestThreeNodeCluster(t *testing.T) {
	c := NewCluster(t)

	// 1 control + 2 workers = 3 nodes. Mixed arch exercises real cross-arch
	// join + reporting; swap to all-amd64 for a slightly faster/cheaper run.
	c.StartControl("amd64", "cloud-3node.zattera.invalid")
	c.JoinWorker("amd64")
	c.JoinWorker("arm64")

	// Barrier: every node registered and ALIVE.
	c.WaitNodesReady(3)

	nodes := c.Nodes()
	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes, cluster reports %d: %v", len(nodes), nodeNames(nodes))
	}
	for _, n := range nodes {
		if n.GetStatus() != zatterav1.NodeStatus_NODE_STATUS_ALIVE {
			t.Errorf("node %s not ALIVE: %v", n.GetName(), n.GetStatus())
		}
		if !n.GetSchedulable() {
			t.Errorf("node %s not schedulable", n.GetName())
		}
	}
	t.Logf("cloud: 3-node cluster up — %v", c.nodeArchStrings())
}

func nodeNames(nodes []*zatterav1.Node) []string {
	out := make([]string, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, n.GetName())
	}
	return out
}
