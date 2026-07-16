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
// and verifies both T-55 (multi-control HA) and T-56 (gossip failure detection)
// end to end:
//
//  1. two extra control nodes JOIN the bootstrap node's raft quorum over the
//     real mesh, and all three reach ALIVE (T-55b: handover + runJoinedControl,
//     and gossip is what makes a follower — whose heartbeats go to its own
//     livestate — visible to the leader);
//  2. killing the LEADER, the two survivors re-elect and keep serving writes
//     (T-55: the quorum survives leader loss); and
//  3. the dead leader is marked DOWN within the gossip window — during the NEW
//     leader's post-election grace period, which ONLY gossip can bypass
//     (heartbeat-only detection would wait the grace out) (T-56).
//
// Everything is destroyed on exit. ZT_CLOUD_KEEP=1 keeps it up for debugging.
//
//	HCLOUD_TOKEN=... go test -tags cloud ./test/cloud/ -run TestControlHAAndGossip -v -timeout 45m
func TestControlHAAndGossip(t *testing.T) {
	c := NewCluster(t)

	// 1) Bootstrap control node + two joining control nodes → a quorum of 3.
	leader := c.StartControl("amd64", "")
	f1 := c.JoinControl("amd64")
	f2 := c.JoinControl("amd64")
	c.WaitNodesReady(3)

	if got := controlNodeCount(c.Nodes()); got != 3 {
		t.Fatalf("expected 3 control nodes in the quorum, got %d", got)
	}
	t.Logf("cloud: T-55 — 3-control quorum formed and ALIVE (leader=%s, followers=%s,%s)", leader.Name(), f1.Name(), f2.Name())

	// 2) + 3) Kill the bootstrap leader. A survivor's API (which forwards to
	// whoever now leads) must keep accepting writes, and the dead leader must be
	// marked DOWN fast.
	survivor := c.APIFor(f1)
	leader.KillDaemon()
	killedAt := time.Now()

	requireClusterServesWrites(t, survivor, failoverBound)
	t.Logf("cloud: T-55 — quorum survived leader loss; %s still serves writes", f1.Name())

	waitNodeStatus(t, survivor, leader.Name(), zatterav1.NodeStatus_NODE_STATUS_DOWN, detectPollBound)
	detect := time.Since(killedAt)
	if detect >= gossipProof {
		t.Errorf("dead leader marked DOWN in %v — not within the gossip window (<%v); heartbeat-only + the new leader's grace would take this long, so gossip may not be working", detect, gossipProof)
	}
	t.Logf("cloud: T-56 — dead leader detected DOWN in %v (heartbeat-only, through the new leader's grace, would need much longer)", detect.Round(time.Second))
}

const (
	// failoverBound is how long the survivors have to re-elect and accept a write.
	failoverBound = 90 * time.Second
	// detectPollBound is how long we poll for the DOWN transition.
	detectPollBound = 60 * time.Second
	// gossipProof: detecting the dead leader DOWN faster than this proves gossip
	// did it. A brand-new leader is inside its 45s post-election grace, so a
	// heartbeat-only detector could not demote the old leader until the grace
	// expires (~45s+); only gossip (which bypasses grace) does it in ~15-20s.
	gossipProof = 30 * time.Second
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
// (which forwards to whoever now leads), proving the quorum still commits. It
// retries the transient no-leader window while the survivors re-elect.
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
