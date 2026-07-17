package daemon

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
)

// controlDialOpts are the dial options for a worker's long-lived control-plane
// streams (agent, peer sync, routes). The keepalive is what makes failover
// actually work: when a control node dies, its WireGuard tunnel drops and the
// stream would otherwise hang for the OS TCP timeout (minutes) — a server-stream
// like peer sync receives but never sends, so it never notices on its own. A
// 15s client ping with a 5s timeout detects the dead connection in ~20s, which
// trips the reconnect loop and rolls the worker onto a surviving control node
// (T-55c). 15s stays above the server's 10s MinTime enforcement policy.
func controlDialOpts(creds credentials.TransportCredentials) []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                15 * time.Second,
			Timeout:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	}
}

// controlEndpoints is a worker's rotating view of the reachable control-node API
// addresses. The agent stream, peer sync and route client each call pick() on
// every (re)connect, so when the control node a worker is talking to dies, the
// next reconnect lands on a surviving control node. That is what lets a worker
// ride out the loss of its join-control node — which in a mesh cluster is also
// its active WireGuard hub — instead of retrying one dead address forever
// (T-55c). It is seeded from the join response and refreshed from every peer set
// as control nodes come and go.
//
// The holder carries this node's own mTLS material and builds per-target creds:
// each control node's API cert is SAN'd for its own mesh IP, so the SNI must
// match the address being dialed.
type controlEndpoints struct {
	cert tls.Certificate
	pool *x509.CertPool
	port string

	mu     sync.Mutex
	eps    []string
	idx    int
	leader string // "host:port" of the current raft leader, "" if unknown
}

// newControlEndpoints builds the holder from the join result: the join-control
// address plus every control node in the initial peer set, all on the API port.
func newControlEndpoints(jr *joinResult) (*controlEndpoints, error) {
	cert, err := tls.X509KeyPair(jr.certPEM, jr.keyPEM)
	if err != nil {
		return nil, fmt.Errorf("daemon: load node cert: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(jr.caPEM) {
		return nil, fmt.Errorf("daemon: parse cluster CA")
	}
	_, port, err := net.SplitHostPort(jr.ControlGRPCAddr)
	if err != nil || port == "" {
		port = "8443"
	}
	// Seed the leader as the join-control node: a worker joins through a control
	// node's API, which in the common case is the leader.
	ce := &controlEndpoints{cert: cert, pool: pool, port: port, leader: jr.ControlGRPCAddr}
	seed := []string{jr.ControlGRPCAddr}
	for _, p := range jr.initialPeers.GetPeers() {
		if p.GetIsControl() && p.GetMeshIp() != "" {
			seed = append(seed, net.JoinHostPort(p.GetMeshIp(), port))
		}
	}
	ce.set(seed)
	ce.updateFromPeers(jr.initialPeers)
	return ce, nil
}

// set replaces the endpoint set (deduped + sorted for determinism). A zero-length
// update is ignored so a transient empty peer set never strands the worker.
func (c *controlEndpoints) set(eps []string) {
	seen := map[string]bool{}
	out := make([]string, 0, len(eps))
	for _, e := range eps {
		if e == "" || seen[e] {
			continue
		}
		seen[e] = true
		out = append(out, e)
	}
	if len(out) == 0 {
		return
	}
	sort.Strings(out)
	c.mu.Lock()
	c.eps = out
	c.mu.Unlock()
}

// updateFromPeers refreshes the control set from a peer set's control peers and
// records the current leader (leader_node_id → that control peer's mesh IP). Wire
// it to peer sync so a newly-joined control node becomes a failover target, a
// removed one drops out, and the agent re-targets the leader after an election.
func (c *controlEndpoints) updateFromPeers(ps *clusterv1.PeerSet) {
	var eps []string
	leaderAddr := ""
	for _, p := range ps.GetPeers() {
		if !p.GetIsControl() || p.GetMeshIp() == "" {
			continue
		}
		addr := net.JoinHostPort(p.GetMeshIp(), c.port)
		eps = append(eps, addr)
		if p.GetNodeId() == ps.GetLeaderNodeId() {
			leaderAddr = addr
		}
	}
	c.set(eps)
	// Only overwrite the leader when the peer set names one — keep the last-known
	// leader through an election (leader_node_id == "").
	if leaderAddr != "" {
		c.mu.Lock()
		c.leader = leaderAddr
		c.mu.Unlock()
	}
}

// pick returns the next control address to try and TLS creds whose SNI matches
// it, rotating so successive reconnects fail over to a different control node.
// Use for leaderless-safe streams (peer sync, routes) that any control node can
// serve from replicated state.
func (c *controlEndpoints) pick() (string, credentials.TransportCredentials) {
	c.mu.Lock()
	if len(c.eps) == 0 {
		c.mu.Unlock()
		return "", nil
	}
	addr := c.eps[c.idx%len(c.eps)]
	c.idx++
	c.mu.Unlock()
	return addr, c.credsFor(addr)
}

// pickLeader returns the current leader's address + creds, falling back to pick()
// (rotation) when no leader is known yet. Use for the AgentSync stream: livestate
// is leader-memory, so heartbeats must reach the leader or the node never goes
// ALIVE. On the next reconnect after an election the refreshed leader is used.
func (c *controlEndpoints) pickLeader() (string, credentials.TransportCredentials) {
	c.mu.Lock()
	leader := c.leader
	c.mu.Unlock()
	if leader == "" {
		return c.pick()
	}
	return leader, c.credsFor(leader)
}

// credsFor builds node-mTLS creds whose SNI matches addr's host (each control
// node's API cert is SAN'd for its own mesh IP).
func (c *controlEndpoints) credsFor(addr string) credentials.TransportCredentials {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	return credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{c.cert},
		RootCAs:      c.pool,
		ServerName:   host,
	})
}
