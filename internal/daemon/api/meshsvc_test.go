package api

import (
	"testing"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/state"
)

func TestPeerSets(t *testing.T) {
	st := state.New()
	// Two control nodes with public endpoints, one NAT'd worker, one worker
	// with a public endpoint, and one node without mesh material (ignored).
	st.PutNode(meshNode("c1", "10.90.0.1", "keyc1", true, "203.0.113.1:51820"))
	st.PutNode(meshNode("c2", "10.90.0.2", "keyc2", true, "203.0.113.2:51820"))
	st.PutNode(meshNode("w1", "10.90.1.1", "keyw1", false)) // NAT'd (no endpoint)
	st.PutNode(meshNode("w2", "10.90.1.2", "keyw2", false, "198.51.100.9:51820"))
	st.PutNode(meshNode("nomesh", "", "", false)) // no mesh_ip/pubkey → skipped

	s := NewMeshServer(st, clock.NewFake(), nil)

	t.Run("worker sees only controls with the whole-mesh hub route", func(t *testing.T) {
		ps := s.buildPeerSet("w1")
		if !ps.GetHubAndSpoke() {
			t.Fatal("worker peer set should be hub-and-spoke")
		}
		if len(ps.GetPeers()) != 2 {
			t.Fatalf("worker should see 2 control peers, got %d", len(ps.GetPeers()))
		}
		for _, p := range ps.GetPeers() {
			if !p.GetIsControl() {
				t.Fatalf("worker peer %s should be a control", p.GetNodeId())
			}
			if len(p.GetAllowedIps()) != 1 || p.GetAllowedIps()[0] != meshCIDR {
				t.Fatalf("control peer allowed_ips = %v, want %s", p.GetAllowedIps(), meshCIDR)
			}
			// w1 is NAT'd → keepalive set on its hub peers.
			if p.GetPersistentKeepaliveSeconds() != natKeepaliveSeconds {
				t.Fatalf("NAT'd worker should set keepalive %d, got %d", natKeepaliveSeconds, p.GetPersistentKeepaliveSeconds())
			}
		}
	})

	t.Run("non-NAT worker sets no keepalive", func(t *testing.T) {
		ps := s.buildPeerSet("w2")
		for _, p := range ps.GetPeers() {
			if p.GetPersistentKeepaliveSeconds() != 0 {
				t.Fatalf("worker with a public endpoint should not keepalive, got %d", p.GetPersistentKeepaliveSeconds())
			}
		}
	})

	t.Run("control sees every node with a /32 and no keepalive", func(t *testing.T) {
		ps := s.buildPeerSet("c1")
		if ps.GetHubAndSpoke() {
			t.Fatal("control peer set is not hub-and-spoke")
		}
		// c2, w1, w2 (nomesh excluded, self excluded).
		if len(ps.GetPeers()) != 3 {
			t.Fatalf("control should see 3 peers, got %d", len(ps.GetPeers()))
		}
		byID := map[string]bool{}
		for _, p := range ps.GetPeers() {
			byID[p.GetNodeId()] = true
			if len(p.GetAllowedIps()) != 1 || p.GetAllowedIps()[0] != p.GetMeshIp()+"/32" {
				t.Fatalf("peer %s allowed_ips = %v, want %s/32", p.GetNodeId(), p.GetAllowedIps(), p.GetMeshIp())
			}
			if p.GetPersistentKeepaliveSeconds() != 0 {
				t.Fatalf("control should not set keepalive on %s", p.GetNodeId())
			}
		}
		// The NAT'd worker w1 appears with an empty endpoint (hub waits).
		if !byID["w1"] || !byID["w2"] || !byID["c2"] {
			t.Fatalf("control missing expected peers: %v", byID)
		}
	})

	t.Run("unknown node yields an empty set", func(t *testing.T) {
		if ps := s.buildPeerSet("ghost"); len(ps.GetPeers()) != 0 {
			t.Fatalf("unknown node should have no peers, got %d", len(ps.GetPeers()))
		}
	})
}

func meshNode(id, meshIP, wgKey string, control bool, endpoints ...string) *zatterav1.Node {
	role := zatterav1.NodeRole_NODE_ROLE_WORKER
	if control {
		role = zatterav1.NodeRole_NODE_ROLE_CONTROL
	}
	return &zatterav1.Node{
		Meta:               &zatterav1.Meta{Id: id},
		Roles:              []zatterav1.NodeRole{role},
		MeshIp:             meshIP,
		WireguardPublicKey: wgKey,
		PublicEndpoints:    endpoints,
		Status:             zatterav1.NodeStatus_NODE_STATUS_ALIVE,
	}
}
