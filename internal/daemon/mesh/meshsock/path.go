package meshsock

import (
	"log/slog"
	"net/netip"
	"sync"
	"time"
)

// Timing groups the path manager's knobs. Zero values take the defaults;
// tests shrink them to milliseconds.
type Timing struct {
	// ProbeInterval is the cadence for probing unverified peers.
	ProbeInterval time.Duration
	// KeepaliveInterval re-verifies an established UDP path.
	KeepaliveInterval time.Duration
	// PathMisses is how many consecutive unanswered keepalives drop a path.
	PathMisses int
	// PunchAfter is how long a peer stays unverified before requesting a
	// control-coordinated punch.
	PunchAfter time.Duration
	// PunchCooldown is the minimum gap between punch requests per peer.
	PunchCooldown time.Duration
	// RelayAfter is how long a peer stays unverified before falling back to
	// the TCP relay (only with a relay sender wired).
	RelayAfter time.Duration
	// BurstProbes/BurstSpacing shape the punch-time probe burst.
	BurstProbes  int
	BurstSpacing time.Duration
}

func (t *Timing) withDefaults() Timing {
	out := Timing{
		ProbeInterval:     2 * time.Second,
		KeepaliveInterval: 15 * time.Second,
		PathMisses:        3,
		PunchAfter:        3 * time.Second,
		PunchCooldown:     30 * time.Second,
		RelayAfter:        10 * time.Second,
		BurstProbes:       5,
		BurstSpacing:      100 * time.Millisecond,
	}
	if t == nil {
		return out
	}
	if t.ProbeInterval > 0 {
		out.ProbeInterval = t.ProbeInterval
	}
	if t.KeepaliveInterval > 0 {
		out.KeepaliveInterval = t.KeepaliveInterval
	}
	if t.PathMisses > 0 {
		out.PathMisses = t.PathMisses
	}
	if t.PunchAfter > 0 {
		out.PunchAfter = t.PunchAfter
	}
	if t.PunchCooldown > 0 {
		out.PunchCooldown = t.PunchCooldown
	}
	if t.RelayAfter > 0 {
		out.RelayAfter = t.RelayAfter
	}
	if t.BurstProbes > 0 {
		out.BurstProbes = t.BurstProbes
	}
	if t.BurstSpacing > 0 {
		out.BurstSpacing = t.BurstSpacing
	}
	return out
}

// PunchRequester asks the control plane to coordinate a simultaneous-open with
// a target node. ok=false means the target cannot punch (no punch stream —
// kernel-WG or offline); the caller falls back to hub/relay.
type PunchRequester interface {
	RequestPunch(targetNodeID string) (peerEndpoints []netip.AddrPort, at time.Time, ok bool)
}

// peerState is the path manager's view of one peer.
type peerState struct {
	nodeID string
	ep     *Endpoint

	mu         sync.Mutex
	key        []byte // probe key derived from the peer's WG public key
	candidates []netip.AddrPort

	// probing bookkeeping
	pending       map[uint64]pendingProbe // txID → in-flight probe
	verifiedAddr  netip.AddrPort
	verifiedKind  PathKind
	lastPong      time.Time
	misses        int
	unverifiedAt  time.Time // when this peer last became unverified
	lastPunchReq  time.Time
	punchAttempts int
}

type pendingProbe struct {
	addr netip.AddrPort
	sent time.Time
}

func newPeerState(nodeID string) *peerState {
	return &peerState{
		nodeID:       nodeID,
		ep:           &Endpoint{nodeID: nodeID},
		pending:      map[uint64]pendingProbe{},
		unverifiedAt: time.Now(),
	}
}

func (ps *peerState) setHome(addr netip.AddrPort) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.ep.home = addr
}

func (ps *peerState) setIdentity(_ [32]byte, key []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.key = key
}

func (ps *peerState) setCandidates(cands []netip.AddrPort) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.candidates = append([]netip.AddrPort(nil), cands...)
}

// addCandidate appends a discovered candidate (e.g. the source of a ping we
// received — strong evidence of the peer's reflexive address).
func (ps *peerState) addCandidate(addr netip.AddrPort) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	for _, c := range ps.candidates {
		if c == addr {
			return
		}
	}
	ps.candidates = append(ps.candidates, addr)
}

// pathManager drives probing, punching, and relay fallback for every peer.
type pathManager struct {
	bind   *Bind
	punch  PunchRequester
	timing Timing
	log    *slog.Logger

	mu     sync.Mutex
	txSeq  uint64
	stopCh chan struct{}
	kickCh chan struct{}
}

func newPathManager(b *Bind, punch PunchRequester, timing *Timing, log *slog.Logger) *pathManager {
	return &pathManager{
		bind:   b,
		punch:  punch,
		timing: timing.withDefaults(),
		log:    log,
		kickCh: make(chan struct{}, 1),
	}
}

func (pm *pathManager) start() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if pm.stopCh != nil {
		return
	}
	pm.stopCh = make(chan struct{})
	go pm.run(pm.stopCh)
}

func (pm *pathManager) stop() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if pm.stopCh != nil {
		close(pm.stopCh)
		pm.stopCh = nil
	}
}

// kick wakes the loop immediately (peer set changed).
func (pm *pathManager) kick() {
	select {
	case pm.kickCh <- struct{}{}:
	default:
	}
}

func (pm *pathManager) run(stop <-chan struct{}) {
	tick := time.NewTicker(pm.timing.ProbeInterval)
	defer tick.Stop()
	for {
		select {
		case <-stop:
			return
		case <-tick.C:
		case <-pm.kickCh:
		}
		pm.evaluate()
	}
}

// evaluate advances every peer's state machine one step.
func (pm *pathManager) evaluate() {
	now := time.Now()
	pm.bind.peers.Range(func(_, v any) bool {
		ps := v.(*peerState)
		pm.evaluatePeer(ps, now)
		return true
	})
}

func (pm *pathManager) evaluatePeer(ps *peerState, now time.Time) {
	ps.mu.Lock()
	verified := ps.verifiedAddr.IsValid()
	sinceUnverified := now.Sub(ps.unverifiedAt)
	lastPong := ps.lastPong
	misses := ps.misses
	candidates := append([]netip.AddrPort(nil), ps.candidates...)
	hasKey := len(ps.key) != 0
	lastPunch := ps.lastPunchReq
	verifiedAddr := ps.verifiedAddr
	relayActive := ps.ep.current().kind == PathRelay
	ps.mu.Unlock()

	if !hasKey {
		return // identity not yet known (no SetPeers for this peer)
	}

	if verified {
		// Established path: keepalive-probe it; drop after too many misses.
		if now.Sub(lastPong) >= pm.timing.KeepaliveInterval {
			if misses >= pm.timing.PathMisses {
				pm.log.Info("meshsock path lost", "peer", ps.nodeID, "addr", verifiedAddr.String())
				ps.mu.Lock()
				ps.verifiedAddr = netip.AddrPort{}
				ps.misses = 0
				ps.unverifiedAt = now
				ps.mu.Unlock()
				ps.ep.clearPath()
				pm.bind.addrToPeer.Delete(verifiedAddr)
				return
			}
			ps.mu.Lock()
			ps.misses++
			ps.mu.Unlock()
			pm.sendProbe(ps, verifiedAddr)
		}
		return
	}

	// Unverified: probe every candidate this round.
	for _, c := range candidates {
		pm.sendProbe(ps, c)
	}

	// Escalate: punch, then relay. Both are one-way doors per round — the
	// relay path stays active while probing continues in the background, so a
	// later verified pong upgrades the peer back to a UDP path.
	if pm.punch != nil && sinceUnverified >= pm.timing.PunchAfter && now.Sub(lastPunch) >= pm.timing.PunchCooldown {
		ps.mu.Lock()
		ps.lastPunchReq = now
		ps.punchAttempts++
		ps.mu.Unlock()
		go pm.requestPunch(ps)
	}
	if pm.bind.cfg.Relay != nil && !relayActive && sinceUnverified >= pm.timing.RelayAfter {
		pm.bind.markRelay(ps)
	}
}

// requestPunch asks control to coordinate a punch and schedules our burst.
func (pm *pathManager) requestPunch(ps *peerState) {
	eps, at, ok := pm.punch.RequestPunch(ps.nodeID)
	if !ok {
		pm.log.Debug("meshsock punch unavailable", "peer", ps.nodeID)
		return
	}
	pm.log.Info("meshsock punch coordinated", "peer", ps.nodeID, "endpoints", len(eps), "at", at)
	pm.PunchNow(ps.nodeID, eps, at)
}

// PunchNow schedules a probe burst toward the peer's endpoints at `at` — both
// sides bursting inside the same window is what opens the NAT holes. Called
// for our own punch requests and for control-pushed PunchCommands.
func (pm *pathManager) PunchNow(peerID string, endpoints []netip.AddrPort, at time.Time) {
	ps := pm.bind.peerByID(peerID)
	for _, ep := range endpoints {
		ps.addCandidate(ep)
	}
	delay := time.Until(at)
	if delay < 0 {
		delay = 0
	}
	time.AfterFunc(delay, func() {
		for i := 0; i < pm.timing.BurstProbes; i++ {
			for _, ep := range endpoints {
				pm.sendProbe(ps, ep)
			}
			select {
			case <-time.After(pm.timing.BurstSpacing):
			case <-pm.stopped():
				return
			}
		}
	})
}

func (pm *pathManager) stopped() <-chan struct{} {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if pm.stopCh == nil {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return pm.stopCh
}

// sendProbe fires one signed ping at addr through the bind socket (same source
// port as WireGuard — that shared mapping is what makes punching work).
func (pm *pathManager) sendProbe(ps *peerState, addr netip.AddrPort) {
	pc := pm.bind.socket()
	if pc == nil || !addr.IsValid() {
		return
	}
	pm.mu.Lock()
	pm.txSeq++
	tx := pm.txSeq
	pm.mu.Unlock()

	ps.mu.Lock()
	ps.pending[tx] = pendingProbe{addr: addr, sent: time.Now()}
	// Bound the pending map: drop entries older than a minute.
	for id, p := range ps.pending {
		if time.Since(p.sent) > time.Minute {
			delete(ps.pending, id)
		}
	}
	ps.mu.Unlock()

	frame := encodeFrame(probePing, tx, pingPayload(pm.bind.cfg.NodeID), pm.bind.ownKey)
	_, _ = pc.WriteToUDPAddrPort(frame, addr)
}

// handleProbe processes an inbound probe frame (called from receiveUDP).
func (pm *pathManager) handleProbe(pkt []byte, src netip.AddrPort) {
	typ, tx, payload, signed, mac, err := decodeFrame(pkt)
	if err != nil {
		return
	}
	switch typ {
	case probePing:
		senderID := string(payload)
		v, ok := pm.bind.peers.Load(senderID)
		if !ok {
			return // unknown peer
		}
		ps := v.(*peerState)
		ps.mu.Lock()
		key := ps.key
		ps.mu.Unlock()
		if len(key) == 0 || !verifyFrame(signed, mac, key) {
			return
		}
		// An authenticated ping is strong evidence of the peer's reflexive
		// address: candidate it so our own periodic prober targets it (which is
		// what verifies the reverse direction). Reply with a pong only — never
		// probe back here, or ping↔probe amplifies into a storm.
		ps.addCandidate(src)
		if pc := pm.bind.socket(); pc != nil {
			pong := encodeFrame(probePong, tx, pongPayload(pm.bind.cfg.NodeID, src.String()), key)
			_, _ = pc.WriteToUDPAddrPort(pong, src)
		}
	case probePong:
		ponderID, _, ok := splitPong(payload)
		if !ok || !verifyFrame(signed, mac, pm.bind.ownKey) {
			return
		}
		v, okp := pm.bind.peers.Load(ponderID)
		if !okp {
			return
		}
		ps := v.(*peerState)
		ps.mu.Lock()
		probe, okTx := ps.pending[tx]
		if okTx {
			delete(ps.pending, tx)
		}
		wasVerified := ps.verifiedAddr.IsValid()
		var kind PathKind
		if okTx && probe.addr == src {
			ps.verifiedAddr = src
			ps.lastPong = time.Now()
			ps.misses = 0
			// Heuristic provenance: a path that verified only after punch
			// coordination is labelled punched; anything else is direct.
			kind = PathDirect
			if ps.punchAttempts > 0 {
				kind = PathPunched
			}
			ps.verifiedKind = kind
		}
		ps.mu.Unlock()
		if okTx && probe.addr == src && !wasVerified {
			pm.bind.markVerified(ps, kind, src)
		}
	}
}
