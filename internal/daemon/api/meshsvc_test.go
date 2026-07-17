package api

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
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

	s := NewMeshServer(st, nil, clock.NewFake(), nil)

	t.Run("worker routes the /16 through one active hub, /32 to the standby", func(t *testing.T) {
		ps := s.buildPeerSet("w1")
		if !ps.GetHubAndSpoke() {
			t.Fatal("worker peer set should be hub-and-spoke")
		}
		if len(ps.GetPeers()) != 2 {
			t.Fatalf("worker should see 2 control peers, got %d", len(ps.GetPeers()))
		}
		byID := map[string]*clusterv1.Peer{}
		for _, p := range ps.GetPeers() {
			if !p.GetIsControl() {
				t.Fatalf("worker peer %s should be a control", p.GetNodeId())
			}
			// w1 is NAT'd → keepalive set on EVERY hub (active + warm standby).
			if p.GetPersistentKeepaliveSeconds() != natKeepaliveSeconds {
				t.Fatalf("NAT'd worker should keepalive hub %s, got %d", p.GetNodeId(), p.GetPersistentKeepaliveSeconds())
			}
			byID[p.GetNodeId()] = p
		}
		// c1 has the lowest mesh IP among ALIVE controls → the active hub carries
		// the /16; c2 is a warm standby with a direct /32.
		if got := byID["c1"].GetAllowedIps(); len(got) != 1 || got[0] != meshCIDR {
			t.Fatalf("active hub c1 allowed_ips = %v, want %s", got, meshCIDR)
		}
		if got := byID["c2"].GetAllowedIps(); len(got) != 1 || got[0] != "10.90.0.2/32" {
			t.Fatalf("standby hub c2 allowed_ips = %v, want 10.90.0.2/32", got)
		}
	})

	t.Run("hub failover: the /16 moves to the next live control when the active hub is DOWN", func(t *testing.T) {
		down := meshNode("c1", "10.90.0.1", "keyc1", true, "203.0.113.1:51820")
		down.Status = zatterav1.NodeStatus_NODE_STATUS_DOWN
		st.PutNode(down)
		defer st.PutNode(meshNode("c1", "10.90.0.1", "keyc1", true, "203.0.113.1:51820")) // restore ALIVE

		ps := s.buildPeerSet("w1")
		byID := map[string]*clusterv1.Peer{}
		for _, p := range ps.GetPeers() {
			byID[p.GetNodeId()] = p
		}
		// c1 is DOWN → c2 (next-lowest mesh IP among ALIVE controls) takes the /16;
		// the dead c1 drops to a /32 so it no longer owns the whole-mesh route.
		if got := byID["c2"].GetAllowedIps(); len(got) != 1 || got[0] != meshCIDR {
			t.Fatalf("after failover c2 should own the /16, got %v", got)
		}
		if got := byID["c1"].GetAllowedIps(); len(got) != 1 || got[0] != "10.90.0.1/32" {
			t.Fatalf("dead hub c1 should drop to /32, got %v", got)
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

	t.Run("two workers with endpoints get a direct /32 and keep the hub route", func(t *testing.T) {
		st2 := state.New()
		st2.PutNode(meshNode("c1", "10.90.0.1", "keyc1", true, "203.0.113.1:51820"))
		st2.PutNode(meshNode("wa", "10.90.1.1", "keywa", false, "198.51.100.1:51820"))
		st2.PutNode(meshNode("wb", "10.90.1.2", "keywb", false, "198.51.100.2:51820"))
		ps := NewMeshServer(st2, nil, clock.NewFake(), nil).buildPeerSet("wa")

		var hub, direct *clusterv1.Peer
		for _, p := range ps.GetPeers() {
			switch p.GetNodeId() {
			case "c1":
				hub = p
			case "wb":
				direct = p
			}
		}
		if hub == nil || direct == nil {
			t.Fatalf("expected both a hub (c1) and a direct (wb) peer, got %+v", ps.GetPeers())
		}
		if len(hub.GetAllowedIps()) != 1 || hub.GetAllowedIps()[0] != meshCIDR {
			t.Fatalf("hub peer must keep the /16 route, got %v", hub.GetAllowedIps())
		}
		if len(direct.GetAllowedIps()) != 1 || direct.GetAllowedIps()[0] != "10.90.1.2/32" {
			t.Fatalf("direct worker peer must be /32, got %v", direct.GetAllowedIps())
		}
		if direct.GetPersistentKeepaliveSeconds() != natKeepaliveSeconds {
			t.Fatalf("direct worker peer should keepalive, got %d", direct.GetPersistentKeepaliveSeconds())
		}
	})
}

// watchPeersStream is a minimal MeshService_WatchPeersServer that pushes every
// Send onto a channel so a test can observe re-pushes.
type watchPeersStream struct {
	grpc.ServerStream
	ctx  context.Context
	sent chan *clusterv1.PeerSet
}

func (w *watchPeersStream) Context() context.Context { return w.ctx }
func (w *watchPeersStream) Send(ps *clusterv1.PeerSet) error {
	w.sent <- ps
	return nil
}

// TestWatchPeersHubFailover proves the failover wiring end to end at the service
// level: when the active hub is marked DOWN, WatchPeers re-pushes the worker a
// peer set with the /16 re-pointed to the next live control node (T-55c).
func TestWatchPeersHubFailover(t *testing.T) {
	st := state.New()
	st.PutNode(meshNode("c1", "10.90.0.1", "keyc1", true, "203.0.113.1:51820"))
	st.PutNode(meshNode("c2", "10.90.0.2", "keyc2", true, "203.0.113.2:51820"))
	st.PutNode(meshNode("w1", "10.90.1.1", "keyw1", false, "198.51.100.9:51820"))

	s := NewMeshServer(st, nil, clock.Real{}, nil)
	s.debounce = time.Millisecond // re-push promptly on change

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := &watchPeersStream{ctx: ctx, sent: make(chan *clusterv1.PeerSet, 8)}
	done := make(chan error, 1)
	go func() { done <- s.WatchPeers(&clusterv1.WatchPeersRequest{NodeId: "w1"}, stream) }()

	hubOf := func(ps *clusterv1.PeerSet) string {
		for _, p := range ps.GetPeers() {
			if len(p.GetAllowedIps()) == 1 && p.GetAllowedIps()[0] == meshCIDR {
				return p.GetNodeId()
			}
		}
		return ""
	}
	recv := func(want string) {
		t.Helper()
		deadline := time.After(3 * time.Second)
		for {
			select {
			case ps := <-stream.sent:
				if hubOf(ps) == want {
					return
				}
			case <-deadline:
				t.Fatalf("timed out waiting for the /16 to land on %s", want)
			}
		}
	}

	// Initial: c1 (lowest-id ALIVE) is the active hub.
	recv("c1")

	// Kill c1 → the worker must be re-pushed with c2 owning the /16.
	down := meshNode("c1", "10.90.0.1", "keyc1", true, "203.0.113.1:51820")
	down.Status = zatterav1.NodeStatus_NODE_STATUS_DOWN
	st.PutNode(down)
	recv("c2")

	cancel()
	<-done
}

// fakeLeaderApplier is an Applier that also exposes LeaderAddr so buildPeerSet
// can advertise the leader.
type fakeLeaderApplier struct {
	leaderID string
}

func (f fakeLeaderApplier) Apply(context.Context, *clusterv1.Command) error { return nil }
func (f fakeLeaderApplier) IsLeader() bool                                  { return true }
func (f fakeLeaderApplier) LeaderAddr() (string, string)                    { return "", f.leaderID }

func TestBuildPeerSetAdvertisesLeader(t *testing.T) {
	st := state.New()
	st.PutNode(meshNode("c1", "10.90.0.1", "keyc1", true, "203.0.113.1:51820"))
	st.PutNode(meshNode("c2", "10.90.0.2", "keyc2", true, "203.0.113.2:51820"))
	st.PutNode(meshNode("w1", "10.90.1.1", "keyw1", false))

	s := NewMeshServer(st, fakeLeaderApplier{leaderID: "c2"}, clock.NewFake(), nil)
	if got := s.buildPeerSet("w1").GetLeaderNodeId(); got != "c2" {
		t.Fatalf("peer set leader_node_id = %q, want c2", got)
	}
	// A bare Applier (no LeaderAddr) yields "" rather than panicking.
	if got := NewMeshServer(st, nil, clock.NewFake(), nil).buildPeerSet("w1").GetLeaderNodeId(); got != "" {
		t.Fatalf("nil raft should yield empty leader, got %q", got)
	}
}

func TestActiveHubID(t *testing.T) {
	alive := func(id, ip string) *zatterav1.Node { return meshNode(id, ip, "k"+id, true) }
	withStatus := func(n *zatterav1.Node, s zatterav1.NodeStatus) *zatterav1.Node { n.Status = s; return n }

	t.Run("lowest mesh IP wins regardless of node id", func(t *testing.T) {
		// zzz has the lowest mesh IP (.1) though the highest id → it is the hub,
		// proving selection is by mesh IP (numeric), not id.
		nodes := []*zatterav1.Node{alive("aaa", "10.90.0.10"), alive("zzz", "10.90.0.1"), alive("mmm", "10.90.0.2")}
		if got := activeHubID(nodes); got != "zzz" {
			t.Fatalf("activeHubID = %q, want zzz (mesh IP 10.90.0.1)", got)
		}
	})
	t.Run("mesh IPs compare numerically not lexically", func(t *testing.T) {
		// .2 must beat .10 (lexically "10.90.0.10" < "10.90.0.2").
		nodes := []*zatterav1.Node{alive("c10", "10.90.0.10"), alive("c2", "10.90.0.2")}
		if got := activeHubID(nodes); got != "c2" {
			t.Fatalf("activeHubID = %q, want c2 (.2 < .10 numerically)", got)
		}
	})
	t.Run("DOWN control is skipped for the next live one", func(t *testing.T) {
		nodes := []*zatterav1.Node{withStatus(alive("c1", "10.90.0.1"), zatterav1.NodeStatus_NODE_STATUS_DOWN), alive("c2", "10.90.0.2")}
		if got := activeHubID(nodes); got != "c2" {
			t.Fatalf("activeHubID = %q, want c2 (c1 is DOWN)", got)
		}
	})
	t.Run("no ALIVE control falls back to the lowest mesh IP", func(t *testing.T) {
		nodes := []*zatterav1.Node{
			withStatus(alive("c2", "10.90.0.2"), zatterav1.NodeStatus_NODE_STATUS_DOWN),
			withStatus(alive("c1", "10.90.0.1"), zatterav1.NodeStatus_NODE_STATUS_DRAINING),
		}
		if got := activeHubID(nodes); got != "c1" {
			t.Fatalf("activeHubID = %q, want c1 fallback (lowest mesh IP)", got)
		}
	})
	t.Run("a control without mesh material is not eligible", func(t *testing.T) {
		notReady := &zatterav1.Node{Meta: &zatterav1.Meta{Id: "c0"}, Roles: []zatterav1.NodeRole{zatterav1.NodeRole_NODE_ROLE_CONTROL}, Status: zatterav1.NodeStatus_NODE_STATUS_ALIVE}
		nodes := []*zatterav1.Node{notReady, alive("c1", "10.90.0.2")}
		if got := activeHubID(nodes); got != "c1" {
			t.Fatalf("activeHubID = %q, want c1 (c0 has no mesh material)", got)
		}
	})
	t.Run("no control nodes yields empty", func(t *testing.T) {
		nodes := []*zatterav1.Node{meshNode("w1", "10.90.1.1", "kw1", false)}
		if got := activeHubID(nodes); got != "" {
			t.Fatalf("activeHubID = %q, want empty", got)
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
