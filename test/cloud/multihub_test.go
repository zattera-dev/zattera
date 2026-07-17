//go:build cloud

package cloud

import (
	"testing"
	"time"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

// TestMultiHubFailover proves T-55c end to end on real infra: a joined control
// node is a real WireGuard HUB (not a spoke), and a worker's whole-mesh route
// fails over from one hub to another when the active hub dies — while the worker
// itself rolls its control-plane connection onto a surviving control node.
//
// Topology: 3 control nodes (all hubs) + 2 hub-routed workers. The workers
// advertise no public endpoint, so they have no direct worker↔worker path — all
// cross-worker traffic traverses the active control hub (the lowest-id ALIVE
// control node = the bootstrap node). We prove reachability, then STOP the
// bootstrap node (which is simultaneously the raft leader AND the active hub AND
// the workers' join-control node — the worst case) and assert:
//
//  1. the quorum survives and keeps serving writes (T-55);
//  2. the dead node is marked DOWN (T-56); and
//  3. worker↔worker traffic RECOVERS — the /16 route failed over to a surviving
//     hub and the workers reconnected to a surviving control node (T-55c).
//
// Everything is destroyed on exit. ZT_CLOUD_KEEP=1 keeps it up for debugging.
//
//	HCLOUD_TOKEN=... go test -tags cloud ./test/cloud/ -run TestMultiHubFailover -v -timeout 60m
func TestMultiHubFailover(t *testing.T) {
	c := NewCluster(t)

	// 3-control quorum (every control node is a hub) + 2 hub-routed workers.
	leader := c.StartControl("amd64", "")
	f1 := c.JoinControl("amd64")
	c.JoinControl("amd64")
	wA := c.JoinHubWorker("amd64")
	wB := c.JoinHubWorker("amd64")
	c.WaitNodesReady(5)

	aMesh := c.meshIP(wA.Name())
	bMesh := c.meshIP(wB.Name())
	if aMesh == "" || bMesh == "" {
		t.Fatalf("hub workers missing mesh IPs: a=%q b=%q", aMesh, bMesh)
	}

	// Baseline: cross-worker traffic flows — through the active hub (the
	// bootstrap node), since the workers have no direct path.
	assertMeshReachable(t, wA, bMesh, 120*time.Second)
	assertMeshReachable(t, wB, aMesh, 120*time.Second)
	t.Logf("cloud: T-55c — worker↔worker reachable through the active hub %s", leader.Name())

	// Kill the bootstrap node: raft leader + active hub + both workers'
	// join-control node, all at once. Keep it down (no systemd restart).
	survivor := c.APIFor(f1)
	leader.StopDaemon()
	killedAt := time.Now()

	// 1) The quorum keeps accepting writes through a survivor.
	requireClusterServesWrites(t, survivor, 120*time.Second)
	t.Logf("cloud: T-55 — quorum survived loss of the leader+hub")

	// 2) The dead node is demoted.
	waitNodeStatus(t, survivor, leader.Name(), zatterav1.NodeStatus_NODE_STATUS_DOWN, 90*time.Second)
	t.Logf("cloud: T-56 — dead hub marked DOWN in %v", time.Since(killedAt).Round(time.Second))

	// 3) The core proof: cross-worker traffic RECOVERS. This can only succeed if
	// the workers reconnected to a surviving control node and were re-pushed a
	// peer set with the /16 hub route re-pointed to a live control node.
	assertMeshReachable(t, wA, bMesh, 150*time.Second)
	assertMeshReachable(t, wB, aMesh, 150*time.Second)
	t.Logf("cloud: T-55c — worker↔worker connectivity recovered via a surviving hub after the active hub died")
}
