package meshsock

import (
	"bytes"
	"io"
	"log/slog"
	"net/netip"
	"testing"
	"time"

	"golang.zx2c4.com/wireguard/conn"
)

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func testKey(seed byte) [32]byte {
	var k [32]byte
	for i := range k {
		k[i] = seed
	}
	return k
}

// TestFrameDiscrimination pins the first-byte multiplexing contract: WG
// message types 1..4 are never probe frames; 0xff-prefixed frames are, and
// they round-trip encode/decode/verify with tamper detection.
func TestFrameDiscrimination(t *testing.T) {
	for _, wgType := range []byte{1, 2, 3, 4} {
		if IsProbeFrame([]byte{wgType, 0, 0, 0}) {
			t.Fatalf("WG message type %d misread as probe frame", wgType)
		}
	}
	key := probeKey(testKey(1), []byte("ca-hash"))
	frame := encodeFrame(probePing, 42, pingPayload("node-a"), key)
	if !IsProbeFrame(frame) {
		t.Fatal("encoded probe frame not recognized")
	}
	typ, tx, payload, signed, mac, err := decodeFrame(frame)
	if err != nil || typ != probePing || tx != 42 || string(payload) != "node-a" {
		t.Fatalf("decode = %v %d %q err=%v", typ, tx, payload, err)
	}
	if !verifyFrame(signed, mac, key) {
		t.Fatal("valid frame failed verification")
	}
	if verifyFrame(signed, mac, probeKey(testKey(2), []byte("ca-hash"))) {
		t.Fatal("frame verified under the wrong key")
	}
	tampered := append([]byte(nil), frame...)
	tampered[frameHeaderLen] ^= 0x01
	_, _, _, signed2, mac2, err := decodeFrame(tampered)
	if err != nil {
		t.Fatalf("tampered frame should still parse: %v", err)
	}
	if verifyFrame(signed2, mac2, key) {
		t.Fatal("tampered frame passed verification")
	}
	// Pong payload round-trip.
	ponder, observed, ok := splitPong(pongPayload("node-b", "198.51.100.7:4242"))
	if !ok || ponder != "node-b" || observed != "198.51.100.7:4242" {
		t.Fatalf("pong payload round-trip: %q %q %v", ponder, observed, ok)
	}
}

// TestParseEndpoint covers plain, managed, and punch-only endpoint forms, and
// the per-peer singleton property.
func TestParseEndpoint(t *testing.T) {
	b := New(Config{NodeID: "self", WGPublicKey: testKey(9), CAHash: []byte("ca"), Logger: discard()})

	plain, err := b.ParseEndpoint("192.0.2.1:51820")
	if err != nil {
		t.Fatalf("plain: %v", err)
	}
	if plain.(*Endpoint).NodeID() != "" || plain.DstToString() != "192.0.2.1:51820" {
		t.Fatalf("plain endpoint mangled: %q %q", plain.(*Endpoint).NodeID(), plain.DstToString())
	}

	m1, err := b.ParseEndpoint("nodeb@192.0.2.2:51820")
	if err != nil {
		t.Fatalf("managed: %v", err)
	}
	m2, err := b.ParseEndpoint("nodeb@192.0.2.2:51820")
	if err != nil {
		t.Fatalf("managed re-parse: %v", err)
	}
	if m1 != m2 {
		t.Fatal("managed endpoint is not a per-peer singleton")
	}
	if m1.(*Endpoint).NodeID() != "nodeb" {
		t.Fatalf("node id = %q", m1.(*Endpoint).NodeID())
	}

	punchOnly, err := b.ParseEndpoint("nodec@")
	if err != nil {
		t.Fatalf("punch-only: %v", err)
	}
	if punchOnly.(*Endpoint).NodeID() != "nodec" || punchOnly.(*Endpoint).home.IsValid() {
		t.Fatal("punch-only endpoint should have no home address")
	}

	if _, err := b.ParseEndpoint("@1.2.3.4:1"); err == nil {
		t.Fatal("empty node id should fail")
	}
	if _, err := b.ParseEndpoint("not-an-addr"); err == nil {
		t.Fatal("garbage should fail")
	}
}

// TestSendRoutesByPath verifies Send follows the endpoint's current path:
// home → direct swap → relay.
func TestSendRoutesByPath(t *testing.T) {
	fabric := newFakeNet()
	self := fabric.newConn("192.0.2.1:51820")
	home := fabric.newConn("192.0.2.2:51820")
	direct := fabric.newConn("192.0.2.3:51820")

	var relayed [][]byte
	b := New(Config{
		NodeID: "self", WGPublicKey: testKey(1), CAHash: []byte("ca"),
		Listen: listenOn(self),
		Relay: func(dst string, payload []byte) error {
			relayed = append(relayed, append([]byte(nil), payload...))
			return nil
		},
		Logger: discard(),
	})
	if _, _, err := b.Open(51820); err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = b.Close() }()

	ep, err := b.ParseEndpoint("peer@192.0.2.2:51820")
	if err != nil {
		t.Fatal(err)
	}
	wgPkt := []byte{4, 0, 0, 0, 9, 9} // WG transport-looking packet

	// Home path.
	if err := b.Send([][]byte{wgPkt}, ep); err != nil {
		t.Fatalf("send home: %v", err)
	}
	assertDelivered(t, home, wgPkt)

	// Direct path swap redirects subsequent sends without re-parsing.
	ep.(*Endpoint).setPath(PathDirect, netip.MustParseAddrPort("192.0.2.3:51820"))
	if err := b.Send([][]byte{wgPkt}, ep); err != nil {
		t.Fatalf("send direct: %v", err)
	}
	assertDelivered(t, direct, wgPkt)

	// Relay path hands packets to the relay sender.
	ep.(*Endpoint).setPath(PathRelay, netip.AddrPort{})
	if err := b.Send([][]byte{wgPkt}, ep); err != nil {
		t.Fatalf("send relay: %v", err)
	}
	if len(relayed) != 1 || !bytes.Equal(relayed[0], wgPkt) {
		t.Fatalf("relay sender not invoked correctly: %v", relayed)
	}
}

// TestReceiveFiltersProbesAndAttributes verifies the receive path: probe
// frames never reach WireGuard, WG packets from a known source are attributed
// to the peer's singleton endpoint, and injected (relay) packets arrive via
// the second ReceiveFunc.
func TestReceiveFiltersProbesAndAttributes(t *testing.T) {
	fabric := newFakeNet()
	selfConn := fabric.newConn("192.0.2.1:51820")
	peerConn := fabric.newConn("192.0.2.2:51820")

	b := New(Config{NodeID: "self", WGPublicKey: testKey(1), CAHash: []byte("ca"), Listen: listenOn(selfConn), Logger: discard()})
	fns, _, err := b.Open(51820)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()
	recvUDP, recvInjected := fns[0], fns[1]

	ep, _ := b.ParseEndpoint("peer@192.0.2.2:51820")

	// A probe frame followed by a WG packet: the ReceiveFunc must skip the
	// probe and return only the WG packet, attributed to the singleton.
	probe := encodeFrame(probePing, 7, pingPayload("unknown"), probeKey(testKey(3), []byte("ca")))
	if _, err := peerConn.WriteToUDPAddrPort(probe, netip.MustParseAddrPort("192.0.2.1:51820")); err != nil {
		t.Fatal(err)
	}
	wgPkt := []byte{1, 2, 3, 4}
	if _, err := peerConn.WriteToUDPAddrPort(wgPkt, netip.MustParseAddrPort("192.0.2.1:51820")); err != nil {
		t.Fatal(err)
	}

	packets := [][]byte{make([]byte, 2048)}
	sizes := []int{0}
	eps := []conn.Endpoint{nil}
	n, err := recvUDP(packets, sizes, eps)
	if err != nil || n != 1 {
		t.Fatalf("recv: n=%d err=%v", n, err)
	}
	if !bytes.Equal(packets[0][:sizes[0]], wgPkt) {
		t.Fatalf("got %v, want WG packet %v (probe leaked through?)", packets[0][:sizes[0]], wgPkt)
	}
	if eps[0] != ep {
		t.Fatal("known source not attributed to the peer's singleton endpoint")
	}

	// Injected relay packet arrives via the second ReceiveFunc with the
	// sender's managed endpoint.
	b.InjectRelayed("peer", []byte{9, 9, 9})
	n, err = recvInjected(packets, sizes, eps)
	if err != nil || n != 1 || !bytes.Equal(packets[0][:sizes[0]], []byte{9, 9, 9}) {
		t.Fatalf("injected recv: n=%d err=%v pkt=%v", n, err, packets[0][:sizes[0]])
	}
	if eps[0].(*Endpoint).NodeID() != "peer" {
		t.Fatalf("injected packet attributed to %q", eps[0].(*Endpoint).NodeID())
	}

	// Close unblocks both receive funcs with net.ErrClosed.
	_ = b.Close()
	if _, err := recvInjected(packets, sizes, eps); err == nil {
		t.Fatal("recvInjected should fail after Close")
	}
	deadline := time.After(2 * time.Second)
	done := make(chan error, 1)
	go func() { _, err := recvUDP(packets, sizes, eps); done <- err }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("recvUDP should fail after Close")
		}
	case <-deadline:
		t.Fatal("recvUDP did not unblock on Close")
	}
}

func assertDelivered(t *testing.T, s *fakeSock, want []byte) {
	t.Helper()
	select {
	case pkt := <-s.rx:
		if !bytes.Equal(pkt.payload, want) {
			t.Fatalf("delivered %v, want %v", pkt.payload, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("packet not delivered")
	}
}
