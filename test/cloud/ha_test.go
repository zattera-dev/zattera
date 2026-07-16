//go:build cloud

package cloud

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/emptypb"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/pkg/apiclient"
)

// TestControlHAAndGossip spins up a REAL 3-node control-plane quorum on Hetzner
// and exercises both T-55 (multi-control HA) and T-56 (gossip failure
// detection) end to end:
//
//  1. two extra control nodes JOIN the bootstrap node's raft quorum (T-55b:
//     handover + runJoinedControl over the real mesh);
//  2. killing a FOLLOWER is detected DOWN within the gossip window — far faster
//     than the 30s heartbeat deadline alone (T-56);
//  3. killing the LEADER, the surviving two control nodes re-elect and keep
//     serving writes (T-55: quorum survives leader loss).
//
// Everything is destroyed on exit. ZT_CLOUD_KEEP=1 keeps it up for debugging.
//
//	HCLOUD_TOKEN=... go test -tags cloud ./test/cloud/ -run TestControlHAAndGossip -v -timeout 40m
func TestControlHAAndGossip(t *testing.T) {
	c := NewCluster(t)

	// 1) Bootstrap control node + two joining control nodes → a quorum of 3.
	leader := c.StartControl("amd64", "")
	f1 := c.JoinControl("amd64")
	f2 := c.JoinControl("amd64")
	c.WaitNodesReady(3)

	// T-55: all three registered as control-role and ALIVE.
	if got := controlNodeCount(c.Nodes()); got != 3 {
		t.Fatalf("expected 3 control nodes in the quorum, got %d", got)
	}
	t.Logf("cloud: 3-control quorum formed (leader=%s, followers=%s,%s)", leader.Name(), f1.Name(), f2.Name())

	// 2) T-56: kill a follower's daemon and time how long until the leader marks
	// it DOWN. The bootstrap node stays leader across joins, so f1/f2 are
	// followers — killing one triggers no election, isolating the detector.
	leaderAPI := c.APIFor(leader)
	f2.KillDaemon()
	killedAt := time.Now()
	waitNodeStatus(t, leaderAPI, f2.Name(), zatterav1.NodeStatus_NODE_STATUS_DOWN, gossipDetectBound)
	detect := time.Since(killedAt)
	if detect >= heartbeatOnlyBound {
		t.Errorf("follower marked DOWN in %v — no faster than heartbeat-only; gossip may not be working", detect)
	}
	t.Logf("cloud: T-56 — follower %s detected DOWN in %v (heartbeat-only would need >=%v)", f2.Name(), detect.Round(time.Second), heartbeatOnlyBound)

	// Recover the follower — it must rejoin and go ALIVE again.
	f2.StartDaemon()
	waitNodeStatus(t, leaderAPI, f2.Name(), zatterav1.NodeStatus_NODE_STATUS_ALIVE, recoverBound)
	t.Logf("cloud: follower %s recovered ALIVE", f2.Name())

	// 3) T-55: kill the LEADER. The surviving two control nodes (majority of 3)
	// must re-elect and keep serving writes, reached via a survivor.
	leader.KillDaemon()
	survivorAPI := c.APIFor(f1)
	requireClusterServesWrites(t, survivorAPI, failoverBound)
	waitNodeStatus(t, survivorAPI, leader.Name(), zatterav1.NodeStatus_NODE_STATUS_DOWN, failoverBound)
	t.Logf("cloud: T-55 — quorum survived leader loss; %s serves and the old leader is DOWN", f1.Name())
}

const (
	// gossipDetectBound is the ceiling for gossip-accelerated DOWN detection.
	gossipDetectBound = 25 * time.Second
	// heartbeatOnlyBound is the floor a heartbeat-only detector could achieve
	// (the 30s deadline). Detecting DOWN faster than this proves gossip helped.
	heartbeatOnlyBound = 30 * time.Second
	recoverBound       = 90 * time.Second
	failoverBound      = 90 * time.Second
)

// controlNodeCount counts registered nodes carrying the control role.
func controlNodeCount(nodes []*zatterav1.Node) int {
	n := 0
	for _, node := range nodes {
		for _, r := range node.GetRoles() {
			if r == zatterav1.NodeRole_NODE_ROLE_CONTROL {
				n++
				break
			}
		}
	}
	return n
}

// waitNodeStatus polls the given API until node `name` reaches `want`, or fails.
func waitNodeStatus(t *testing.T, api *apiclient.Client, name string, want zatterav1.NodeStatus, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last zatterav1.NodeStatus
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		resp, err := api.Nodes.ListNodes(ctx, &emptypb.Empty{})
		cancel()
		if err == nil {
			for _, n := range resp.GetNodes() {
				if n.GetName() == name {
					last = n.GetStatus()
					if last == want {
						return
					}
				}
			}
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("cloud: node %q never reached %v within %s (last=%v)", name, want, timeout, last)
}

// requireClusterServesWrites asserts a MUTATING call succeeds through `api`
// (which forwards to whoever now leads), proving the quorum still commits.
func requireClusterServesWrites(t *testing.T, api *apiclient.Client, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, lastErr = api.Nodes.CreateJoinToken(ctx, &zatterav1.CreateJoinTokenRequest{
			SingleUse: true,
			Roles:     []zatterav1.NodeRole{zatterav1.NodeRole_NODE_ROLE_WORKER},
		})
		cancel()
		if lastErr == nil {
			return
		}
		time.Sleep(3 * time.Second)
	}
	t.Fatalf("cloud: cluster did not accept writes within %s after leader loss: %v", timeout, lastErr)
}
