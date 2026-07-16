package meshsock

// natsim_test.go is an in-memory packet fabric with programmable NAT behavior
// (full-cone and symmetric) — the "network" under every meshsock test. No real
// sockets, no root, deterministic addresses.
//
// A fabric address is a persistent *fakeSock (receive mailbox + NAT state);
// each Open of a bind gets a fresh *fakeConn view over it, so wireguard-go's
// open→close→reopen bind cycle revives cleanly (Close closes only the transient
// conn, never the mailbox).

import (
	"errors"
	"net"
	"net/netip"
	"sync"
)

type natKind int

const (
	natNone natKind = iota
	// natFullCone allocates ONE public mapping per socket on first outbound
	// send; once open, inbound from ANY remote is delivered. Hole punching
	// succeeds.
	natFullCone
	// natSymmetric allocates a DISTINCT public mapping per destination and only
	// accepts inbound on a mapping from that exact destination. Hole punching
	// fails; the relay is the only escape.
	natSymmetric
)

type fakePacket struct {
	payload []byte
	src     netip.AddrPort
}

// fakeNet routes packets between fakeSocks by their public/visible address.
type fakeNet struct {
	mu      sync.Mutex
	entries map[netip.AddrPort]*netEntry
	nextPub uint16
	drop    func(src, dst netip.AddrPort) bool
}

// netEntry is a deliverable address: the sock behind it and, for symmetric NAT
// mappings, the only remote allowed to use it.
type netEntry struct {
	sock     *fakeSock
	onlyFrom netip.AddrPort // zero = accept any source
}

func newFakeNet() *fakeNet {
	return &fakeNet{entries: map[netip.AddrPort]*netEntry{}, nextPub: 40000}
}

func (f *fakeNet) setDrop(fn func(src, dst netip.AddrPort) bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.drop = fn
}

func (f *fakeNet) deliver(payload []byte, src, dst netip.AddrPort) {
	f.mu.Lock()
	if f.drop != nil && f.drop(src, dst) {
		f.mu.Unlock()
		return
	}
	e, ok := f.entries[dst]
	f.mu.Unlock()
	if !ok || (e.onlyFrom.IsValid() && e.onlyFrom != src) {
		return
	}
	select {
	case e.sock.rx <- fakePacket{payload: append([]byte(nil), payload...), src: src}:
	default: // queue full: drop, UDP semantics
	}
}

// fakeSock is a persistent fabric endpoint: its receive mailbox and NAT state
// survive across bind reopens.
type fakeSock struct {
	net   *fakeNet
	local netip.AddrPort
	rx    chan fakePacket

	nat      natKind
	pubIP    netip.Addr
	fullCone netip.AddrPort
	symMap   map[netip.AddrPort]netip.AddrPort
}

// newConn registers a public socket at ip:port.
func (f *fakeNet) newConn(ipPort string) *fakeSock {
	addr := netip.MustParseAddrPort(ipPort)
	s := &fakeSock{net: f, local: addr, rx: make(chan fakePacket, 256)}
	f.mu.Lock()
	f.entries[addr] = &netEntry{sock: s}
	f.mu.Unlock()
	return s
}

// newNATConn creates a socket at private ipPort behind a NAT with public pubIP.
func (f *fakeNet) newNATConn(ipPort, pubIP string, kind natKind) *fakeSock {
	return &fakeSock{
		net: f, local: netip.MustParseAddrPort(ipPort), rx: make(chan fakePacket, 256),
		nat: kind, pubIP: netip.MustParseAddr(pubIP), symMap: map[netip.AddrPort]netip.AddrPort{},
	}
}

// mappingFor returns (allocating on demand) the public address this sock's
// packets appear from when sent to dst.
func (s *fakeSock) mappingFor(dst netip.AddrPort) netip.AddrPort {
	s.net.mu.Lock()
	defer s.net.mu.Unlock()
	switch s.nat {
	case natFullCone:
		if !s.fullCone.IsValid() {
			s.net.nextPub++
			s.fullCone = netip.AddrPortFrom(s.pubIP, s.net.nextPub)
			s.net.entries[s.fullCone] = &netEntry{sock: s}
		}
		return s.fullCone
	case natSymmetric:
		if m, ok := s.symMap[dst]; ok {
			return m
		}
		s.net.nextPub++
		m := netip.AddrPortFrom(s.pubIP, s.net.nextPub)
		s.symMap[dst] = m
		s.net.entries[m] = &netEntry{sock: s, onlyFrom: dst}
		return m
	default:
		return s.local
	}
}

// WriteToUDPAddrPort sends directly from this sock (used by tests to inject
// traffic without a bind). Applies NAT translation like a real send.
func (s *fakeSock) WriteToUDPAddrPort(b []byte, dst netip.AddrPort) (int, error) {
	src := s.local
	if s.nat != natNone {
		src = s.mappingFor(dst)
	}
	s.net.deliver(b, src, dst)
	return len(b), nil
}

// advertise pre-opens a NAT mapping toward `via` (the disco hop) and returns the
// resulting public address — this node's advertised reflexive endpoint.
func (s *fakeSock) advertise(via netip.AddrPort) netip.AddrPort {
	if s.nat == natNone {
		return s.local
	}
	return s.mappingFor(via)
}

// fakeConn is a transient Open view over a fakeSock. Close closes only this
// view; the sock's mailbox and NAT state persist for the next Open.
type fakeConn struct {
	sock   *fakeSock
	closed chan struct{}
	once   sync.Once
}

func (c *fakeConn) WriteToUDPAddrPort(b []byte, dst netip.AddrPort) (int, error) {
	select {
	case <-c.closed:
		return 0, net.ErrClosed
	default:
	}
	src := c.sock.local
	if c.sock.nat != natNone {
		src = c.sock.mappingFor(dst)
	}
	c.sock.net.deliver(b, src, dst)
	return len(b), nil
}

func (c *fakeConn) ReadFromUDPAddrPort(b []byte) (int, netip.AddrPort, error) {
	select {
	case pkt := <-c.sock.rx:
		return copy(b, pkt.payload), pkt.src, nil
	case <-c.closed:
		return 0, netip.AddrPort{}, net.ErrClosed
	}
}

func (c *fakeConn) LocalAddr() net.Addr {
	return &net.UDPAddr{IP: c.sock.local.Addr().AsSlice(), Port: int(c.sock.local.Port())}
}

func (c *fakeConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

// listenOn adapts a fabric sock to the bind's ListenFunc: each Open yields a
// fresh conn over the same sock.
func listenOn(s *fakeSock) ListenFunc {
	return func(port uint16) (packetConn, uint16, error) {
		if port != 0 && port != s.local.Port() {
			return nil, 0, errors.New("natsim: port mismatch")
		}
		return &fakeConn{sock: s, closed: make(chan struct{})}, s.local.Port(), nil
	}
}
