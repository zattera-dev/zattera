package daemon

import (
	"testing"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
)

func TestControlEndpointsRotateAndRefresh(t *testing.T) {
	ce := &controlEndpoints{port: "8443"}
	ce.set([]string{"10.90.0.1:8443", "10.90.0.2:8443", "10.90.0.1:8443"}) // dup dropped

	// pick rotates deterministically through the sorted, deduped set.
	seen := map[string]int{}
	var order []string
	for i := 0; i < 6; i++ {
		addr, creds := ce.pick()
		if creds == nil {
			t.Fatalf("pick %d returned nil creds", i)
		}
		seen[addr]++
		order = append(order, addr)
	}
	if len(seen) != 2 {
		t.Fatalf("expected 2 distinct endpoints across picks, got %v", seen)
	}
	if order[0] == order[1] {
		t.Fatalf("consecutive picks should rotate, got %q twice", order[0])
	}

	// A peer set with control peers refreshes the failover set; a worker peer is
	// ignored; a new control node becomes reachable.
	ce.updateFromPeers(&clusterv1.PeerSet{Peers: []*clusterv1.Peer{
		{NodeId: "c1", MeshIp: "10.90.0.1", IsControl: true},
		{NodeId: "c3", MeshIp: "10.90.0.3", IsControl: true},
		{NodeId: "w1", MeshIp: "10.90.1.1", IsControl: false},
	}})
	got := map[string]bool{}
	for i := 0; i < 4; i++ {
		addr, _ := ce.pick()
		got[addr] = true
	}
	if got["10.90.1.1:8443"] {
		t.Fatal("worker peer must not become a control endpoint")
	}
	if !got["10.90.0.1:8443"] || !got["10.90.0.3:8443"] {
		t.Fatalf("refreshed set should contain c1 and c3, got %v", got)
	}
	if got["10.90.0.2:8443"] {
		t.Fatal("c2 dropped from the peer set should no longer be dialed")
	}
}

func TestControlEndpointsIgnoresEmptyRefresh(t *testing.T) {
	ce := &controlEndpoints{port: "8443"}
	ce.set([]string{"10.90.0.1:8443"})
	ce.updateFromPeers(&clusterv1.PeerSet{}) // no control peers → keep current set
	addr, _ := ce.pick()
	if addr != "10.90.0.1:8443" {
		t.Fatalf("empty refresh should not strand the worker, got %q", addr)
	}
}

func TestControlEndpointsEmptyPick(t *testing.T) {
	ce := &controlEndpoints{port: "8443"}
	if addr, creds := ce.pick(); addr != "" || creds != nil {
		t.Fatalf("empty holder should return no endpoint, got %q", addr)
	}
}

func TestControlEndpointsLeaderTracking(t *testing.T) {
	ce := &controlEndpoints{port: "8443", leader: "10.90.0.1:8443"}
	ce.set([]string{"10.90.0.1:8443", "10.90.0.2:8443", "10.90.0.3:8443"})

	// pickLeader is pinned to the leader, unlike the rotating pick.
	for i := 0; i < 3; i++ {
		if addr, _ := ce.pickLeader(); addr != "10.90.0.1:8443" {
			t.Fatalf("pickLeader should stay on the leader, got %q", addr)
		}
	}

	// A peer set naming a new leader re-points pickLeader (post-election).
	ce.updateFromPeers(&clusterv1.PeerSet{LeaderNodeId: "c2", Peers: []*clusterv1.Peer{
		{NodeId: "c1", MeshIp: "10.90.0.1", IsControl: true},
		{NodeId: "c2", MeshIp: "10.90.0.2", IsControl: true},
	}})
	if addr, _ := ce.pickLeader(); addr != "10.90.0.2:8443" {
		t.Fatalf("pickLeader should follow the new leader c2, got %q", addr)
	}

	// An election (no leader named) keeps the last-known leader.
	ce.updateFromPeers(&clusterv1.PeerSet{LeaderNodeId: "", Peers: []*clusterv1.Peer{
		{NodeId: "c2", MeshIp: "10.90.0.2", IsControl: true},
	}})
	if addr, _ := ce.pickLeader(); addr != "10.90.0.2:8443" {
		t.Fatalf("pickLeader should keep the last-known leader through an election, got %q", addr)
	}
}

// TestControlEndpointsLeaderFallback: with no leader known, pickLeader rotates
// so the agent still finds a control node to connect to.
func TestControlEndpointsLeaderFallback(t *testing.T) {
	ce := &controlEndpoints{port: "8443"}
	ce.set([]string{"10.90.0.1:8443", "10.90.0.2:8443"})
	addr, creds := ce.pickLeader()
	if addr == "" || creds == nil {
		t.Fatal("pickLeader with no leader should fall back to rotation, got empty")
	}
}
