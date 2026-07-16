package meshsock

import (
	"fmt"
	"net/netip"
	"strings"
	"sync/atomic"

	"golang.zx2c4.com/wireguard/conn"
)

// PathKind labels how packets currently reach a peer, in preference order.
type PathKind uint8

const (
	// PathHome sends to the configured (uapi) endpoint — for hub-and-spoke
	// peers that is the control hub, the always-working fallback.
	PathHome PathKind = iota
	// PathDirect is a verified configured/observed candidate address.
	PathDirect
	// PathPunched is a verified address opened by coordinated hole punching.
	PathPunched
	// PathRelay sends through the control TCP relay (Phase D).
	PathRelay
)

func (k PathKind) String() string {
	switch k {
	case PathDirect:
		return "direct"
	case PathPunched:
		return "punched"
	case PathRelay:
		return "relay"
	default:
		return "home"
	}
}

// path is the atomically-swapped current route to a peer. addr is meaningless
// for PathRelay.
type path struct {
	kind PathKind
	addr netip.AddrPort
}

// Endpoint implements conn.Endpoint. For meshsock-managed peers it is a
// per-peer singleton whose destination is an atomic pointer the path manager
// swaps — WireGuard holds one Endpoint per peer, so a swap redirects all
// subsequent sends without touching the device. Plain (nodeID == "") endpoints
// behave like the stdlib bind's: a fixed address.
type Endpoint struct {
	// nodeID is the peer's cluster node id ("" = plain address endpoint).
	nodeID string
	// home is the configured endpoint from uapi (hub or known-good address).
	// Zero when the peer has no configured endpoint (NAT'd peer: punch only).
	home netip.AddrPort
	// cur holds *path — the currently preferred route. nil means home.
	cur atomic.Pointer[path]
}

// NodeID returns the peer's node id ("" for plain endpoints).
func (e *Endpoint) NodeID() string { return e.nodeID }

// current resolves the route to use for the next send.
func (e *Endpoint) current() path {
	if p := e.cur.Load(); p != nil {
		return *p
	}
	return path{kind: PathHome, addr: e.home}
}

// setPath atomically swaps the active route.
func (e *Endpoint) setPath(kind PathKind, addr netip.AddrPort) {
	e.cur.Store(&path{kind: kind, addr: addr})
}

// clearPath drops back to the home route.
func (e *Endpoint) clearPath() { e.cur.Store(nil) }

// --- conn.Endpoint ---------------------------------------------------------

func (e *Endpoint) ClearSrc()           {}
func (e *Endpoint) SrcToString() string { return "" }
func (e *Endpoint) SrcIP() netip.Addr   { return netip.Addr{} }

func (e *Endpoint) DstIP() netip.Addr { return e.current().addr.Addr() }

// DstToString renders the current destination; used by wireguard-go for uapi
// dumps and logs.
func (e *Endpoint) DstToString() string {
	p := e.current()
	if e.nodeID != "" {
		return e.nodeID + "@" + p.addr.String()
	}
	return p.addr.String()
}

// DstToBytes feeds WireGuard's mac2 cookie computation. It must be stable per
// destination; for relay paths there is no UDP address, so the node id serves
// as the cookie identity.
func (e *Endpoint) DstToBytes() []byte {
	p := e.current()
	if p.kind == PathRelay || !p.addr.IsValid() {
		return []byte(e.nodeID)
	}
	b, _ := p.addr.MarshalBinary()
	return b
}

var _ conn.Endpoint = (*Endpoint)(nil)

// parseEndpointString splits the uapi endpoint form. meshsock-managed peers are
// configured as "nodeID@host:port" (the home address may be empty for NAT'd
// peers: "nodeID@"); plain "host:port" stays stdlib-compatible.
func parseEndpointString(s string) (nodeID string, addr netip.AddrPort, err error) {
	host := s
	if i := strings.IndexByte(s, '@'); i >= 0 {
		nodeID, host = s[:i], s[i+1:]
		if nodeID == "" {
			return "", netip.AddrPort{}, fmt.Errorf("meshsock: empty node id in endpoint %q", s)
		}
		if host == "" {
			return nodeID, netip.AddrPort{}, nil // punch-only peer: no home addr
		}
	}
	addr, perr := netip.ParseAddrPort(host)
	if perr != nil {
		return "", netip.AddrPort{}, fmt.Errorf("meshsock: endpoint %q: %w", s, perr)
	}
	return nodeID, addr, nil
}
