package meshsock

import (
	"net/netip"
	"sync"
	"time"
)

// tunnelCoordinator is the punch coordinator for the tunnel tests, working with
// *tunnelNode (which carries the bind).
type tunnelCoordinator struct {
	mu    sync.Mutex
	nodes map[string]*tunnelNode
	adv   map[string][]netip.AddrPort
}

func newTunnelCoordinator() *tunnelCoordinator {
	return &tunnelCoordinator{nodes: map[string]*tunnelNode{}, adv: map[string][]netip.AddrPort{}}
}

func (c *tunnelCoordinator) register(n *tunnelNode, adv ...netip.AddrPort) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nodes[n.id] = n
	c.adv[n.id] = adv
}

func (c *tunnelCoordinator) requester(id string) PunchRequester {
	return punchFunc(func(target string) ([]netip.AddrPort, time.Time, bool) {
		c.mu.Lock()
		tn, ok := c.nodes[target]
		targetEps := append([]netip.AddrPort(nil), c.adv[target]...)
		reqEps := append([]netip.AddrPort(nil), c.adv[id]...)
		c.mu.Unlock()
		if !ok {
			return nil, time.Time{}, false
		}
		at := time.Now().Add(30 * time.Millisecond)
		tn.bind.PunchNow(id, reqEps, at)
		return targetEps, at, true
	})
}

// relayHub is an in-process stand-in for the control TCP relay: it routes a
// framed WG packet from a sender to the destination bind's InjectRelayed,
// mirroring relay.Server routing without TLS/TCP (the real server is unit
// tested in the relay package).
type relayHub struct {
	mu    sync.Mutex
	binds map[string]*Bind
}

func newRelayHub() *relayHub { return &relayHub{binds: map[string]*Bind{}} }

func (h *relayHub) attach(nodeID string, b *Bind) {
	h.mu.Lock()
	h.binds[nodeID] = b
	h.mu.Unlock()
}

// senderFor returns a RelaySender that delivers to dst via the hub, tagging the
// source so the receiver attributes it correctly.
func (h *relayHub) senderFor(srcNodeID string) RelaySender {
	return func(dstNodeID string, payload []byte) error {
		h.mu.Lock()
		dst := h.binds[dstNodeID]
		h.mu.Unlock()
		if dst != nil {
			dst.InjectRelayed(srcNodeID, append([]byte(nil), payload...))
		}
		return nil
	}
}
