package mesh

import (
	"crypto/sha256"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"

	"github.com/zattera-dev/zattera/internal/daemon/nodehealth"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// Gossip is a memberlist-based failure detector (T-56). Control nodes run it
// over the mesh: SWIM-style probing spots a dead node within a few seconds —
// far faster than the 30s heartbeat deadline — and feeds the same durable
// SetNodeStatus path as the heartbeat monitor (T-21). The two detectors are
// combined by Decide so neither can flap a node's status on its own.
const (
	// GossipPort is the memberlist bind port. It is bound to the mesh IP only —
	// never a public interface.
	GossipPort = 7946

	// WAN-friendly tuning: tolerate slower/looser links than a LAN default.
	gossipSuspicionMult = 6
	gossipProbeInterval = 2 * time.Second
)

// tracker maintains per-node gossip liveness from memberlist events. Safe for
// concurrent use (memberlist delivers events from its own goroutines).
type tracker struct {
	clock clock.Clock
	mu    sync.Mutex
	nodes map[string]nodehealth.GossipLiveness
}

func newTracker(clk clock.Clock) *tracker {
	if clk == nil {
		clk = clock.Real{}
	}
	return &tracker{clock: clk, nodes: map[string]nodehealth.GossipLiveness{}}
}

// set records a node's alive/dead state, stamping the transition time only when
// the state actually changes (so Since reflects the last transition).
func (t *tracker) set(id string, alive bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if cur, ok := t.nodes[id]; ok && cur.Alive == alive {
		return
	}
	t.nodes[id] = nodehealth.GossipLiveness{Alive: alive, Since: t.clock.Now()}
}

func (t *tracker) snapshot() map[string]nodehealth.GossipLiveness {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[string]nodehealth.GossipLiveness, len(t.nodes))
	for k, v := range t.nodes {
		out[k] = v
	}
	return out
}

// eventDelegate bridges memberlist events into the tracker. A memberlist node's
// Name is the zattera node id (see NewGossip). Events about ourselves are
// ignored — a node never judges its own liveness.
type eventDelegate struct {
	self    string
	tracker *tracker
}

func (d *eventDelegate) NotifyJoin(n *memberlist.Node) {
	if n.Name != d.self {
		d.tracker.set(n.Name, true)
	}
}

func (d *eventDelegate) NotifyLeave(n *memberlist.Node) {
	if n.Name != d.self {
		d.tracker.set(n.Name, false)
	}
}

func (d *eventDelegate) NotifyUpdate(*memberlist.Node) {}

// gossipJoinRetry is how often a node re-attempts to join its peers until it
// reaches at least one — the mesh tunnel may not be up the instant gossip starts.
const gossipJoinRetry = 3 * time.Second

// Gossip owns the running memberlist and its liveness tracker.
type Gossip struct {
	ml      *memberlist.Memberlist
	tracker *tracker
	log     *slog.Logger
	stop    chan struct{}
}

// GossipConfig configures the gossip failure detector.
type GossipConfig struct {
	// NodeID is this node's zattera id — used as the memberlist node name so
	// events map straight back to nodes.
	NodeID string
	// BindAddr is the mesh IP to bind (e.g. 10.90.0.1). Never a public IP.
	BindAddr string
	// Peers are other control nodes' mesh IPs to join (host or host:port).
	Peers []string
	// CAHash is the cluster CA hash; the gossip encryption key derives from it,
	// so only cluster members can join or read the gossip.
	CAHash []byte
	Clock  clock.Clock
	Logger *slog.Logger
}

// NewGossip starts memberlist bound to the mesh IP and joins the given peers
// (best-effort — a node that cannot reach any peer yet still runs and will be
// discovered when a peer probes it).
func NewGossip(cfg GossipConfig) (*Gossip, error) {
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	if cfg.NodeID == "" || cfg.BindAddr == "" {
		return nil, fmt.Errorf("mesh: gossip needs a node id and bind address")
	}
	tr := newTracker(cfg.Clock)

	mlCfg := memberlist.DefaultLANConfig()
	mlCfg.Name = cfg.NodeID
	mlCfg.BindAddr = cfg.BindAddr
	mlCfg.BindPort = GossipPort
	mlCfg.AdvertiseAddr = cfg.BindAddr
	mlCfg.AdvertisePort = GossipPort
	mlCfg.SecretKey = gossipSecret(cfg.CAHash)
	mlCfg.SuspicionMult = gossipSuspicionMult
	mlCfg.ProbeInterval = gossipProbeInterval
	mlCfg.Events = &eventDelegate{self: cfg.NodeID, tracker: tr}
	mlCfg.LogOutput = io.Discard // memberlist is chatty; surface state via slog instead

	ml, err := memberlist.Create(mlCfg)
	if err != nil {
		return nil, fmt.Errorf("mesh: gossip create: %w", err)
	}
	g := &Gossip{ml: ml, tracker: tr, log: log, stop: make(chan struct{})}

	// Join in the background, retrying until at least one peer is reached: the
	// WireGuard tunnel to a peer is often not up the instant gossip starts, and
	// memberlist's Join is one-shot — a single early failure would otherwise
	// isolate this node from the cluster's gossip forever.
	if peers := gossipJoinAddrs(cfg.Peers, cfg.NodeID, cfg.BindAddr); len(peers) > 0 {
		go g.retryJoin(peers)
	}
	log.Info("mesh gossip up", "bind", cfg.BindAddr, "port", GossipPort)
	return g, nil
}

// retryJoin re-attempts to join the peer set until it reaches at least one, or
// Shutdown is called.
func (g *Gossip) retryJoin(peers []string) {
	for {
		if n, err := g.ml.Join(peers); err == nil && n > 0 {
			g.log.Info("mesh gossip joined", "peers", n)
			return
		} else {
			g.log.Warn("mesh gossip join incomplete; retrying", "reached", n, "of", len(peers), "err", err)
		}
		select {
		case <-g.stop:
			return
		case <-time.After(gossipJoinRetry):
		}
	}
}

// Snapshot returns the current gossip liveness view keyed by node id.
func (g *Gossip) Snapshot() map[string]nodehealth.GossipLiveness { return g.tracker.snapshot() }

// Leave broadcasts a graceful departure, then Shutdown stops the listener.
func (g *Gossip) Leave(timeout time.Duration) error { return g.ml.Leave(timeout) }

// Shutdown stops the retry-join loop and the memberlist listener.
func (g *Gossip) Shutdown() error {
	select {
	case <-g.stop:
	default:
		close(g.stop)
	}
	return g.ml.Shutdown()
}

// gossipSecret derives memberlist's 32-byte AES key from the cluster CA hash, so
// only nodes that hold the cluster identity can participate.
func gossipSecret(caHash []byte) []byte {
	sum := sha256.Sum256(append([]byte("zattera-gossip-v1:"), caHash...))
	return sum[:]
}

// gossipJoinAddrs normalizes peer mesh IPs to host:port and drops self / empties.
func gossipJoinAddrs(peers []string, selfID, selfBind string) []string {
	var out []string
	for _, p := range peers {
		if p == "" || p == selfBind {
			continue
		}
		if _, _, err := net.SplitHostPort(p); err != nil {
			p = net.JoinHostPort(p, fmt.Sprintf("%d", GossipPort))
		}
		out = append(out, p)
	}
	return out
}
