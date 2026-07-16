package meshsock

import (
	"net/netip"
	"sync"
	"testing"
	"time"

	"golang.zx2c4.com/wireguard/conn"
)

// fastTiming keeps path tests snappy and deterministic-enough.
func fastTiming() *Timing {
	return &Timing{
		ProbeInterval:     20 * time.Millisecond,
		KeepaliveInterval: 40 * time.Millisecond,
		PathMisses:        2,
		PunchAfter:        60 * time.Millisecond,
		PunchCooldown:     500 * time.Millisecond,
		RelayAfter:        150 * time.Millisecond,
		BurstProbes:       3,
		BurstSpacing:      10 * time.Millisecond,
	}
}

// pairConfig assembles two binds on the fabric with each other in their peer
// tables and returns them plus their conns.
type testNode struct {
	id   string
	bind *Bind
	conn *fakeSock
	addr netip.AddrPort // the address peers should try (public or advertised)
}

func newTestNode(t *testing.T, fabric *fakeNet, id string, seed byte, c *fakeSock, addr netip.AddrPort, punch PunchRequester, relay RelaySender) *testNode {
	t.Helper()
	b := New(Config{
		NodeID: id, WGPublicKey: testKey(seed), CAHash: []byte("ca"),
		Listen: listenOn(c), Punch: punch, Relay: relay,
		Timing: fastTiming(), Logger: discard(),
	})
	fns, _, err := b.Open(c.local.Port())
	if err != nil {
		t.Fatalf("open %s: %v", id, err)
	}
	// Stand in for wireguard-go: run the UDP receive func in a loop so probe
	// frames get processed (there is no real WG traffic in path tests).
	go func() {
		packets := [][]byte{make([]byte, 2048)}
		sizes, eps := []int{0}, []conn.Endpoint{nil}
		for {
			if _, err := fns[0](packets, sizes, eps); err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() { _ = b.Close() })
	return &testNode{id: id, bind: b, conn: c, addr: addr}
}

// peerEachOther exchanges peer tables between two nodes with the given
// candidate addresses.
func peerEachOther(a, b *testNode, aCands, bCands []netip.AddrPort) {
	a.bind.SetPeers([]PeerInfo{{NodeID: b.id, WGPublicKey: testKey(b.seed()), Candidates: bCands}})
	b.bind.SetPeers([]PeerInfo{{NodeID: a.id, WGPublicKey: testKey(a.seed()), Candidates: aCands}})
}

// seed recovers the key seed from the node id convention ("node-<seed>").
func (n *testNode) seed() byte {
	return n.id[len(n.id)-1]
}

// waitPath polls until the bind reports `want` for peer, or fails.
func waitPath(t *testing.T, b *Bind, peer, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		last = b.PeerPaths()[peer]
		if last == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("peer %s path = %q, want %q", peer, last, want)
}

// TestPathDirectVerify: two public nodes with each other's real addresses
// verify a direct path in both directions.
func TestPathDirectVerify(t *testing.T) {
	fabric := newFakeNet()
	ca := fabric.newConn("192.0.2.1:51820")
	cb := fabric.newConn("192.0.2.2:51820")
	a := newTestNode(t, fabric, "node-a", 'a', ca, ca.local, nil, nil)
	b := newTestNode(t, fabric, "node-b", 'b', cb, cb.local, nil, nil)

	peerEachOther(a, b, []netip.AddrPort{ca.local}, []netip.AddrPort{cb.local})

	waitPath(t, a.bind, "node-b", "direct", 3*time.Second)
	waitPath(t, b.bind, "node-a", "direct", 3*time.Second)
}

// coordinator is an in-memory punch coordinator: RequestPunch returns the
// target's advertised endpoints and a punch time, and pushes the command to
// the target's path manager (as control's PunchStream would).
type coordinator struct {
	mu    sync.Mutex
	nodes map[string]*testNode // id → node
	adv   map[string][]netip.AddrPort
}

func newCoordinator() *coordinator {
	return &coordinator{nodes: map[string]*testNode{}, adv: map[string][]netip.AddrPort{}}
}

func (c *coordinator) register(n *testNode, advertised ...netip.AddrPort) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nodes[n.id] = n
	c.adv[n.id] = advertised
}

// requesterFor returns node `id`'s PunchRequester.
func (c *coordinator) requesterFor(id string) PunchRequester {
	return punchFunc(func(target string) ([]netip.AddrPort, time.Time, bool) {
		c.mu.Lock()
		tn, ok := c.nodes[target]
		targetEps := append([]netip.AddrPort(nil), c.adv[target]...)
		requesterEps := append([]netip.AddrPort(nil), c.adv[id]...)
		c.mu.Unlock()
		if !ok {
			return nil, time.Time{}, false
		}
		at := time.Now().Add(30 * time.Millisecond)
		// Push the command to the target (control → PunchStream → PunchNow).
		tn.bind.PunchNow(id, requesterEps, at)
		return targetEps, at, true
	})
}

// punchFunc adapts a func to PunchRequester.
type punchFunc func(target string) ([]netip.AddrPort, time.Time, bool)

func (f punchFunc) RequestPunch(target string) ([]netip.AddrPort, time.Time, bool) { return f(target) }

// TestPathPunchFullCone: both nodes behind full-cone NATs, no working
// configured candidates — the coordinated simultaneous-open verifies a punched
// path on both sides.
func TestPathPunchFullCone(t *testing.T) {
	fabric := newFakeNet()
	disco := netip.MustParseAddrPort("203.0.113.1:7900") // pre-opens mappings

	ca := fabric.newNATConn("10.0.0.1:51820", "198.51.100.1", natFullCone)
	cb := fabric.newNATConn("10.0.0.2:51820", "198.51.100.2", natFullCone)
	aPub, bPub := ca.advertise(disco), cb.advertise(disco)

	coord := newCoordinator()
	a := newTestNode(t, fabric, "node-a", 'a', ca, aPub, coord.requesterFor("node-a"), nil)
	b := newTestNode(t, fabric, "node-b", 'b', cb, bPub, coord.requesterFor("node-b"), nil)
	coord.register(a, aPub)
	coord.register(b, bPub)

	// No candidates in the peer set (private addrs are unroutable): every
	// direct probe goes nowhere, forcing punch escalation.
	peerEachOther(a, b, nil, nil)

	waitPath(t, a.bind, "node-b", "punched", 5*time.Second)
	waitPath(t, b.bind, "node-a", "punched", 5*time.Second)
}

// TestPathSymmetricFallsBackToRelay: symmetric NATs on both sides defeat
// punching (destination-locked mappings), so with a relay sender wired both
// nodes settle on the relay path.
func TestPathSymmetricFallsBackToRelay(t *testing.T) {
	fabric := newFakeNet()
	disco := netip.MustParseAddrPort("203.0.113.1:7900")

	ca := fabric.newNATConn("10.0.0.1:51820", "198.51.100.1", natSymmetric)
	cb := fabric.newNATConn("10.0.0.2:51820", "198.51.100.2", natSymmetric)
	aPub, bPub := ca.advertise(disco), cb.advertise(disco)

	coord := newCoordinator()
	relay := func(string, []byte) error { return nil }
	a := newTestNode(t, fabric, "node-a", 'a', ca, aPub, coord.requesterFor("node-a"), relay)
	b := newTestNode(t, fabric, "node-b", 'b', cb, bPub, coord.requesterFor("node-b"), relay)
	coord.register(a, aPub)
	coord.register(b, bPub)

	peerEachOther(a, b, nil, nil)

	waitPath(t, a.bind, "node-b", "relay", 5*time.Second)
	waitPath(t, b.bind, "node-a", "relay", 5*time.Second)
}

// TestPathLossFallsBackHome: a verified direct path whose peer goes dark drops
// back to home after the keepalive miss budget.
func TestPathLossFallsBackHome(t *testing.T) {
	fabric := newFakeNet()
	ca := fabric.newConn("192.0.2.1:51820")
	cb := fabric.newConn("192.0.2.2:51820")
	a := newTestNode(t, fabric, "node-a", 'a', ca, ca.local, nil, nil)
	b := newTestNode(t, fabric, "node-b", 'b', cb, cb.local, nil, nil)

	peerEachOther(a, b, []netip.AddrPort{ca.local}, []netip.AddrPort{cb.local})
	waitPath(t, a.bind, "node-b", "direct", 3*time.Second)

	// Cut b's traffic entirely: a's keepalives go unanswered.
	fabric.setDrop(func(src, dst netip.AddrPort) bool {
		return dst == cb.local || src == cb.local
	})
	waitPath(t, a.bind, "node-b", "home", 5*time.Second)
}
