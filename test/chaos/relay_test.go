//go:build chaos

package chaos

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/zattera-dev/zattera/internal/daemon/ca"
	"github.com/zattera-dev/zattera/internal/daemon/mesh/relay"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
)

// relayFailoverBudget is the T-68 requirement: after the relay a client uses
// dies, traffic must move to another control relay well within 15s.
const relayFailoverBudget = 15 * time.Second

// TestRelayFailover verifies the DERP-lite relay client (T-58) survives the loss
// of the relay it is using: when relay A dies, the client reconnects to relay B
// and packets to a peer registered on both relays keep flowing — within the 15s
// budget the chaos suite asserts.
func TestRelayFailover(t *testing.T) {
	authority, err := ca.LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatalf("ca: %v", err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	relayA := startRelay(t, authority, log)
	relayB := startRelay(t, authority, log)

	// Receiver R is present on BOTH relays (one client per relay), so a frame
	// reaches it whichever relay the sender is currently using.
	rID := ids.New()
	recv := make(chan string, 8)
	onRecv := func(_ string, p []byte) { recv <- string(p) }
	startRelayClient(t, rID, log, relay.DialTLS(relayA.addr, clientTLS(t, authority, rID)), onRecv)
	startRelayClient(t, rID, log, relay.DialTLS(relayB.addr, clientTLS(t, authority, rID)), onRecv)

	// Sender S prefers relay A, falling back to relay B — the RTT-ordered dial
	// the production client performs, reduced to a two-relay preference list.
	sID := ids.New()
	sender := relay.NewClient(relay.Config{
		NodeID: sID,
		Dial:   failoverDial(clientTLS(t, authority, sID), relayA.addr, relayB.addr),
		Logger: log,
	})
	sctx, scancel := context.WithCancel(context.Background())
	t.Cleanup(scancel)
	go sender.Run(sctx)

	// Baseline: S→R is delivered over relay A.
	if !deliver(t, sender, rID, "ping-A", recv, 5*time.Second) {
		t.Fatal("baseline delivery over relay A failed")
	}

	// Kill relay A (drop its listener AND its live connections, as a crash would).
	relayA.kill()

	// Within the budget, S must reconnect to relay B and delivery must resume.
	start := time.Now()
	if !deliver(t, sender, rID, "ping-B", recv, relayFailoverBudget) {
		t.Fatalf("delivery did not recover via relay B within %s", relayFailoverBudget)
	}
	if elapsed := time.Since(start); elapsed > relayFailoverBudget {
		t.Fatalf("relay failover took %s, over the %s budget", elapsed, relayFailoverBudget)
	}
}

// --- relay test harness ---------------------------------------------------

type testRelay struct {
	addr   string
	cancel context.CancelFunc
	lis    *trackingListener
}

func (r *testRelay) kill() {
	r.cancel()       // Serve closes the listener
	r.lis.closeAll() // and drop live connections so clients see the crash
}

// startRelay boots a relay server on a fresh loopback port with the cluster CA,
// requiring node client certs.
func startRelay(t *testing.T, authority *ca.CA, log *slog.Logger) *testRelay {
	t.Helper()
	srvCfg, err := authority.ServerTLSConfig([]string{"localhost"}, []net.IP{net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("server tls: %v", err)
	}
	srvCfg.ClientAuth = tls.RequireAndVerifyClientCert

	tcp, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	lis := &trackingListener{Listener: tls.NewListener(tcp, srvCfg)}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	srv := relay.NewServer(log)
	go func() { _ = srv.Serve(ctx, lis, relay.NodeIDFromURISANs) }()
	return &testRelay{addr: tcp.Addr().String(), cancel: cancel, lis: lis}
}

// startRelayClient runs a relay client for nodeID until the test ends.
func startRelayClient(t *testing.T, nodeID string, log *slog.Logger, dial func(context.Context) (net.Conn, string, error), onRecv func(string, []byte)) {
	t.Helper()
	c := relay.NewClient(relay.Config{NodeID: nodeID, Dial: dial, OnReceive: onRecv, Logger: log})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go c.Run(ctx)
}

// clientTLS builds an mTLS config presenting nodeID's identity cert.
func clientTLS(t *testing.T, authority *ca.CA, nodeID string) *tls.Config {
	t.Helper()
	leaf, err := authority.IssueNode(nodeID, nil, time.Hour)
	if err != nil {
		t.Fatalf("issue node cert: %v", err)
	}
	cert, err := leaf.TLSCertificate(authority.CABundlePEM())
	if err != nil {
		t.Fatalf("tls cert: %v", err)
	}
	return authority.ClientTLSConfig(cert)
}

// failoverDial tries the relays in preference order, returning the first that
// connects — the reduced form of the production RTT-ordered relay selection.
func failoverDial(cfg *tls.Config, addrs ...string) func(context.Context) (net.Conn, string, error) {
	return func(ctx context.Context) (net.Conn, string, error) {
		for _, addr := range addrs {
			dctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			conn, err := (&tls.Dialer{Config: cfg}).DialContext(dctx, "tcp", addr)
			cancel()
			if err == nil {
				return conn, addr, nil
			}
		}
		return nil, "", errors.New("no relay reachable")
	}
}

// deliver retries Send until the payload is received or the budget elapses
// (Send fails with net.ErrClosed while the client is between relays).
func deliver(t *testing.T, sender *relay.Client, dst, payload string, recv <-chan string, budget time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(budget)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for time.Now().Before(deadline) {
		_ = sender.Send(dst, []byte(payload))
		select {
		case got := <-recv:
			if got == payload {
				return true
			}
		case <-tick.C:
		}
	}
	return false
}

// trackingListener records accepted connections so a simulated relay crash can
// close them (canceling Serve's context only closes the listener, not the
// already-accepted sockets a client is still using).
type trackingListener struct {
	net.Listener
	mu    sync.Mutex
	conns []net.Conn
}

func (l *trackingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		l.mu.Lock()
		l.conns = append(l.conns, c)
		l.mu.Unlock()
	}
	return c, err
}

func (l *trackingListener) closeAll() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, c := range l.conns {
		_ = c.Close()
	}
}
