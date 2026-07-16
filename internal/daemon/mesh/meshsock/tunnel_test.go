package meshsock

import (
	"fmt"
	"net/netip"
	"testing"
	"time"

	wgdevice "golang.zx2c4.com/wireguard/device"
	"golang.zx2c4.com/wireguard/tun/tuntest"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
)

// tunnelNode is a real wireguard-go device driven by a meshsock Bind over the
// NAT simulator, with a channel TUN for injecting/reading tunnelled IP packets.
type tunnelNode struct {
	id      string
	dev     *wgdevice.Device
	tun     *tuntest.ChannelTUN
	bind    *Bind
	priv    wgtypes.Key
	meshIP  netip.Addr
	advAddr netip.AddrPort
}

func newTunnelNode(t *testing.T, id string, c *fakeSock, meshIP string, advAddr netip.AddrPort, punch PunchRequester, relay RelaySender) *tunnelNode {
	t.Helper()
	priv, err := wgtypes.GeneratePrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	bind := New(Config{
		NodeID: id, WGPublicKey: priv.PublicKey(), CAHash: []byte("ca"),
		Listen: listenOn(c), Punch: punch, Relay: relay, Timing: fastTiming(), Logger: discard(),
	})
	tunDev := tuntest.NewChannelTUN()
	dev := wgdevice.NewDevice(tunDev.TUN(), bind, wgdevice.NewLogger(wgdevice.LogLevelError, id+" "))
	n := &tunnelNode{id: id, dev: dev, tun: tunDev, bind: bind, priv: priv, meshIP: netip.MustParseAddr(meshIP), advAddr: advAddr}
	t.Cleanup(func() { dev.Close() })
	return n
}

// configure programs this node's private key + listen port and the peer's
// public key, endpoint (meshsock-managed) and allowed IP.
func (n *tunnelNode) configure(t *testing.T, peer *tunnelNode) {
	t.Helper()
	// meshsock-managed endpoint: "peerID@" (punch-only) or "peerID@host:port".
	endpoint := peer.id + "@"
	cfg := fmt.Sprintf("private_key=%s\nlisten_port=%d\npublic_key=%s\nendpoint=%s\nallowed_ip=%s/32\npersistent_keepalive_interval=1\n",
		hexKeyOf(n.priv), n.bind.Port(),
		hexKeyOf(peer.priv.PublicKey()), endpoint, peer.meshIP)
	if err := n.dev.IpcSet(cfg); err != nil {
		t.Fatalf("%s IpcSet: %v", n.id, err)
	}
	if err := n.dev.Up(); err != nil {
		t.Fatalf("%s up: %v", n.id, err)
	}
	// No configured candidates: the peer's endpoint is only learned through
	// control-coordinated punching, so the verified path is labelled "punched".
	n.bind.SetPeers([]PeerInfo{{NodeID: peer.id, WGPublicKey: peer.priv.PublicKey()}})
}

func hexKeyOf(k wgtypes.Key) string {
	const hexd = "0123456789abcdef"
	b := make([]byte, 64)
	for i, c := range k[:] {
		b[i*2] = hexd[c>>4]
		b[i*2+1] = hexd[c&0xf]
	}
	return string(b)
}

// pingThrough sends an ICMP echo from src's mesh IP to dst's mesh IP and waits
// for it to arrive decrypted on dst's TUN — proving the encrypted tunnel works
// over whatever meshsock path is active.
func pingThrough(t *testing.T, src, dst *tunnelNode, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	tick := time.NewTicker(150 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case src.tun.Outbound <- tuntest.Ping(dst.meshIP, src.meshIP):
		default:
		}
		select {
		case <-dst.tun.Inbound:
			return // a packet tunnelled through
		case <-tick.C:
		case <-deadline:
			t.Fatalf("no packet tunnelled %s → %s (paths: %v / %v)",
				src.id, dst.id, src.bind.PeerPaths(), dst.bind.PeerPaths())
		}
	}
}

// TestTunnelPunchedPath: two full-cone-NAT'd wireguard-go devices establish a
// tunnel over a coordinated hole-punched UDP path — no direct reachability, no
// relay.
func TestTunnelPunchedPath(t *testing.T) {
	fabric := newFakeNet()
	disco := netip.MustParseAddrPort("203.0.113.1:7900")
	ca := fabric.newNATConn("10.0.0.1:51820", "198.51.100.1", natFullCone)
	cb := fabric.newNATConn("10.0.0.2:51820", "198.51.100.2", natFullCone)
	aPub, bPub := ca.advertise(disco), cb.advertise(disco)

	coord := newTunnelCoordinator()
	a := newTunnelNode(t, id26("node-a"), ca, "10.90.1.1", aPub, coord.requester(id26("node-a")), nil)
	b := newTunnelNode(t, id26("node-b"), cb, "10.90.1.2", bPub, coord.requester(id26("node-b")), nil)
	coord.register(a, aPub)
	coord.register(b, bPub)

	a.configure(t, b)
	b.configure(t, a)

	waitPath(t, a.bind, b.id, "punched", 6*time.Second)
	pingThrough(t, a, b, 8*time.Second)
	pingThrough(t, b, a, 8*time.Second)
}

// TestTunnelRelayFallback: symmetric NATs defeat punching, so the tunnel runs
// over the TCP-relay-equivalent path (wired here as an in-process relay hub
// that mirrors the real relay server's routing).
func TestTunnelRelayFallback(t *testing.T) {
	fabric := newFakeNet()
	disco := netip.MustParseAddrPort("203.0.113.1:7900")
	ca := fabric.newNATConn("10.0.0.1:51820", "198.51.100.1", natSymmetric)
	cb := fabric.newNATConn("10.0.0.2:51820", "198.51.100.2", natSymmetric)
	aPub, bPub := ca.advertise(disco), cb.advertise(disco)

	hub := newRelayHub()
	coord := newTunnelCoordinator()
	a := newTunnelNode(t, id26("node-a"), ca, "10.90.1.1", aPub, coord.requester(id26("node-a")), hub.senderFor(id26("node-a")))
	b := newTunnelNode(t, id26("node-b"), cb, "10.90.1.2", bPub, coord.requester(id26("node-b")), hub.senderFor(id26("node-b")))
	coord.register(a, aPub)
	coord.register(b, bPub)
	hub.attach(id26("node-a"), a.bind)
	hub.attach(id26("node-b"), b.bind)

	a.configure(t, b)
	b.configure(t, a)

	waitPath(t, a.bind, b.id, "relay", 10*time.Second)
	pingThrough(t, a, b, 10*time.Second)
	pingThrough(t, b, a, 10*time.Second)
}

func id26(s string) string {
	const w = 26
	if len(s) >= w {
		return s[:w]
	}
	out := s
	for len(out) < w {
		out += "0"
	}
	return out
}
