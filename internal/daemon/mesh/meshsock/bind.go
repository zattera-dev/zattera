package meshsock

import (
	"errors"
	"log/slog"
	"net"
	"net/netip"
	"sync"
	"time"

	"golang.zx2c4.com/wireguard/conn"
)

// packetConn is the socket surface the bind needs — *net.UDPConn satisfies it;
// the NAT-simulator tests inject an in-memory implementation.
type packetConn interface {
	ReadFromUDPAddrPort(b []byte) (int, netip.AddrPort, error)
	WriteToUDPAddrPort(b []byte, addr netip.AddrPort) (int, error)
	LocalAddr() net.Addr
	Close() error
}

// ListenFunc opens the UDP socket for a port (0 = ephemeral) and reports the
// actual port. Production uses netListen; tests inject the NAT simulator.
type ListenFunc func(port uint16) (packetConn, uint16, error)

// netListen is the production ListenFunc: one dual-stack UDP socket.
func netListen(port uint16) (packetConn, uint16, error) {
	pc, err := net.ListenUDP("udp", &net.UDPAddr{Port: int(port)})
	if err != nil {
		return nil, 0, err
	}
	actual := uint16(pc.LocalAddr().(*net.UDPAddr).Port)
	return pc, actual, nil
}

// RelaySender is the Phase D escape hatch: deliver an opaque WG packet to a
// peer via the control TCP relay. Wired by the daemon when a relay client is
// available; nil disables the relay path.
type RelaySender func(dstNodeID string, payload []byte) error

// Config parameterizes a Bind.
type Config struct {
	// NodeID is this node's cluster id (probe frames carry it).
	NodeID string
	// WGPublicKey is this node's WireGuard public key (probe key derivation).
	WGPublicKey [32]byte
	// CAHash is the cluster CA certificate hash (probe key derivation).
	CAHash []byte
	// Listen is the socket factory; nil = real UDP.
	Listen ListenFunc
	// Punch requests control-coordinated hole punching; nil disables punching.
	Punch PunchRequester
	// Relay sends packets via the control TCP relay; nil disables relaying.
	Relay RelaySender
	// Timing overrides the path manager's probe/punch/relay timing (tests).
	Timing *Timing
	Logger *slog.Logger
}

// injected is a packet handed to WireGuard from a non-UDP source (the relay).
type injected struct {
	payload []byte
	from    *Endpoint
}

// Bind implements conn.Bind over one UDP socket, multiplexing WireGuard
// transport packets with probe frames, plus an injection queue for
// relay-received packets. See the package comment for the model.
type Bind struct {
	cfg    Config
	ownKey []byte
	log    *slog.Logger

	mu     sync.Mutex
	pc     packetConn
	port   uint16
	open   bool
	closed chan struct{}

	// peers: nodeID → *peerState (endpoint singleton + path machinery).
	peers sync.Map
	// addrToPeer: netip.AddrPort → *peerState. Attributing an inbound packet's
	// source to a peer returns that peer's singleton Endpoint, so WireGuard's
	// roaming never replaces our managed endpoints (the magicsock trick).
	addrToPeer sync.Map

	inject chan injected

	paths *pathManager
}

// New builds a Bind. It is passed to device.NewDevice in place of
// conn.NewDefaultBind(); Open runs when the device starts.
func New(cfg Config) *Bind {
	if cfg.Listen == nil {
		cfg.Listen = netListen
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	b := &Bind{
		cfg:    cfg,
		ownKey: probeKey(cfg.WGPublicKey, cfg.CAHash),
		log:    cfg.Logger,
		inject: make(chan injected, 64),
		closed: make(chan struct{}),
	}
	b.paths = newPathManager(b, cfg.Punch, cfg.Timing, cfg.Logger)
	return b
}

// --- conn.Bind ---------------------------------------------------------------

// Open binds the UDP socket and returns two receive paths: the socket reader
// (which filters probe frames out of the WG stream) and the injection queue
// (relay-received packets). wireguard-go runs one receive goroutine per func.
func (b *Bind) Open(port uint16) ([]conn.ReceiveFunc, uint16, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.open {
		return nil, 0, conn.ErrBindAlreadyOpen
	}
	pc, actual, err := b.cfg.Listen(port)
	if err != nil {
		return nil, 0, err
	}
	b.pc = pc
	b.port = actual
	b.open = true
	b.closed = make(chan struct{})
	b.paths.start()
	return []conn.ReceiveFunc{b.receiveUDP, b.receiveInjected}, actual, nil
}

// Close stops the socket and the injection queue; both ReceiveFuncs unblock
// with net.ErrClosed.
func (b *Bind) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.open {
		return nil
	}
	b.open = false
	b.paths.stop()
	close(b.closed)
	return b.pc.Close()
}

// SetMark is a no-op (no policy routing over the mesh socket).
func (b *Bind) SetMark(uint32) error { return nil }

// BatchSize is 1: reads and writes are unbatched. Simple and portable; GSO
// batching is a later optimization if profiles ever demand it.
func (b *Bind) BatchSize() int { return 1 }

// ParseEndpoint resolves a uapi endpoint string. "nodeID@host:port" (or
// "nodeID@" for punch-only peers) returns the peer's singleton managed
// endpoint; plain "host:port" returns a fixed plain endpoint.
func (b *Bind) ParseEndpoint(s string) (conn.Endpoint, error) {
	nodeID, addr, err := parseEndpointString(s)
	if err != nil {
		return nil, err
	}
	if nodeID == "" {
		return &Endpoint{home: addr}, nil
	}
	ps := b.peerByID(nodeID)
	ps.setHome(addr)
	if addr.IsValid() {
		b.addrToPeer.Store(addr, ps)
	}
	return ps.ep, nil
}

// Send writes packets to a peer, resolving the current path: relay → the
// relay sender; anything else → the path's UDP address (home as fallback).
func (b *Bind) Send(bufs [][]byte, ep conn.Endpoint) error {
	me, ok := ep.(*Endpoint)
	if !ok {
		return conn.ErrWrongEndpointType
	}
	p := me.current()
	if p.kind == PathRelay {
		if b.cfg.Relay == nil {
			return errors.New("meshsock: relay path active but no relay sender")
		}
		for _, buf := range bufs {
			if err := b.cfg.Relay(me.nodeID, buf); err != nil {
				return err
			}
		}
		return nil
	}
	if !p.addr.IsValid() {
		// No usable route yet (punch-only peer before any path verified):
		// drop like an unroutable UDP send. WireGuard retries handshakes.
		return nil
	}
	pc := b.socket()
	if pc == nil {
		return net.ErrClosed
	}
	for _, buf := range bufs {
		if _, err := pc.WriteToUDPAddrPort(buf, p.addr); err != nil {
			return err
		}
	}
	return nil
}

// --- receive paths -----------------------------------------------------------

// receiveUDP reads the socket, diverting probe frames to the path manager and
// returning only WireGuard packets. Known sources map to the peer's singleton
// endpoint; unknown sources get a plain endpoint (stdlib behavior).
func (b *Bind) receiveUDP(packets [][]byte, sizes []int, eps []conn.Endpoint) (int, error) {
	pc := b.socket()
	if pc == nil {
		return 0, net.ErrClosed
	}
	for {
		n, src, err := pc.ReadFromUDPAddrPort(packets[0])
		if err != nil {
			return 0, err
		}
		if n > 0 && IsProbeFrame(packets[0][:n]) {
			b.paths.handleProbe(append([]byte(nil), packets[0][:n]...), src)
			continue
		}
		sizes[0] = n
		if ps, ok := b.addrToPeer.Load(src); ok {
			eps[0] = ps.(*peerState).ep
		} else {
			eps[0] = &Endpoint{home: src}
		}
		return 1, nil
	}
}

// receiveInjected hands relay-received packets to WireGuard.
func (b *Bind) receiveInjected(packets [][]byte, sizes []int, eps []conn.Endpoint) (int, error) {
	b.mu.Lock()
	closed := b.closed
	b.mu.Unlock()
	select {
	case in := <-b.inject:
		n := copy(packets[0], in.payload)
		sizes[0] = n
		eps[0] = in.from
		return 1, nil
	case <-closed:
		return 0, net.ErrClosed
	}
}

// InjectRelayed queues a WG packet received via the control relay, attributed
// to srcNodeID's managed endpoint. Called by the relay client's read loop.
func (b *Bind) InjectRelayed(srcNodeID string, payload []byte) {
	ps := b.peerByID(srcNodeID)
	select {
	case b.inject <- injected{payload: payload, from: ps.ep}:
	default:
		// Queue full: drop, UDP semantics.
	}
}

// --- peer registry -----------------------------------------------------------

// peerByID returns (creating if needed) the peer's state + singleton endpoint.
func (b *Bind) peerByID(nodeID string) *peerState {
	if v, ok := b.peers.Load(nodeID); ok {
		return v.(*peerState)
	}
	ps := newPeerState(nodeID)
	if actual, loaded := b.peers.LoadOrStore(nodeID, ps); loaded {
		return actual.(*peerState)
	}
	return ps
}

// SetPeers updates the probing view of the peer set: WG public keys and
// candidate addresses per node. Peers absent from the update are forgotten
// (their endpoints revert to home on the next ParseEndpoint).
func (b *Bind) SetPeers(peers []PeerInfo) {
	seen := map[string]bool{}
	for _, p := range peers {
		seen[p.NodeID] = true
		ps := b.peerByID(p.NodeID)
		ps.setIdentity(p.WGPublicKey, probeKey(p.WGPublicKey, b.cfg.CAHash))
		ps.setCandidates(p.Candidates)
	}
	b.peers.Range(func(k, v any) bool {
		if !seen[k.(string)] {
			b.peers.Delete(k)
			v.(*peerState).ep.clearPath()
		}
		return true
	})
	b.paths.kick()
}

// PeerInfo is one peer as the path manager sees it.
type PeerInfo struct {
	NodeID      string
	WGPublicKey [32]byte
	// Candidates are addresses worth probing for a direct path (configured
	// endpoints + disco-observed reflexive addresses).
	Candidates []netip.AddrPort
}

// PunchNow schedules a probe burst toward peerID's endpoints at `at`. The
// daemon calls this when control pushes a PunchCommand (the other node
// requested a punch with us); the path manager also uses it for our own
// punch requests.
func (b *Bind) PunchNow(peerID string, endpoints []netip.AddrPort, at time.Time) {
	b.paths.PunchNow(peerID, endpoints, at)
}

// PeerPaths reports the current path kind per peer (Status/observability).
func (b *Bind) PeerPaths() map[string]string {
	out := map[string]string{}
	b.peers.Range(func(k, v any) bool {
		out[k.(string)] = v.(*peerState).ep.current().kind.String()
		return true
	})
	return out
}

// Port returns the bound UDP port (0 before Open).
func (b *Bind) Port() uint16 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.port
}

func (b *Bind) socket() packetConn {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.open {
		return nil
	}
	return b.pc
}

// markVerified records a verified UDP path for a peer and updates source
// attribution so inbound packets from addr map to the peer's endpoint.
func (b *Bind) markVerified(ps *peerState, kind PathKind, addr netip.AddrPort) {
	ps.ep.setPath(kind, addr)
	b.addrToPeer.Store(addr, ps)
	b.log.Info("meshsock path verified", "peer", ps.nodeID, "path", kind.String(), "addr", addr.String())
}

// markRelay switches a peer to the relay path.
func (b *Bind) markRelay(ps *peerState) {
	ps.ep.setPath(PathRelay, netip.AddrPort{})
	b.log.Info("meshsock path fallback to relay", "peer", ps.nodeID)
}

var _ conn.Bind = (*Bind)(nil)
