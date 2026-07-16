package relay

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"
)

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// id26 pads/truncates s to the 26-byte node id width for tests.
func id26(s string) string {
	if len(s) >= nodeIDLen {
		return s[:nodeIDLen]
	}
	return s + strings.Repeat("0", nodeIDLen-len(s))
}

// TestFrameRoundTrip covers the wire codec and its caps.
func TestFrameRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	if err := writeFrame(&buf, id26("node-a"), []byte("hello")); err != nil {
		t.Fatal(err)
	}
	id, payload, err := readFrame(&buf)
	if err != nil || id != id26("node-a") || string(payload) != "hello" {
		t.Fatalf("round trip: %q %q %v", id, payload, err)
	}
	if err := writeFrame(&buf, "short", nil); err == nil {
		t.Fatal("short node id should be rejected")
	}
	if err := writeFrame(&buf, id26("n"), make([]byte, MaxPayload+1)); err == nil {
		t.Fatal("oversized payload should be rejected")
	}
}

// TestServerRoutesBetweenClients: two mTLS clients connect; a frame from one
// addressed to the other is delivered with the SOURCE id.
func TestServerRoutesBetweenClients(t *testing.T) {
	ca := newTestCA(t)
	srvAddr, stop := startServer(t, ca)
	defer stop()

	a := id26("node-a")
	b := id26("node-b")
	recvB := make(chan framed, 4)
	ca.dialClient(t, srvAddr, b, func(src string, p []byte) { recvB <- framed{src: src, payload: p} })
	connA := ca.dialClient(t, srvAddr, a, nil)

	// a → b.
	if err := writeFrame(connA, b, []byte("ping")); err != nil {
		t.Fatal(err)
	}
	select {
	case f := <-recvB:
		if f.src != a || string(f.payload) != "ping" {
			t.Fatalf("delivered wrong: src=%q payload=%q", f.src, f.payload)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("frame not relayed to b")
	}

	// a → absent node c: silently dropped (no error, nothing delivered).
	if err := writeFrame(connA, id26("node-c"), []byte("void")); err != nil {
		t.Fatal(err)
	}
	select {
	case f := <-recvB:
		t.Fatalf("b received a frame addressed to c: %v", f)
	case <-time.After(300 * time.Millisecond):
	}
}

// TestBackpressureDropsOldest: a stuck receiver's queue overflows and drops the
// OLDEST frames without blocking the relay or the sender.
func TestBackpressureDropsOldest(t *testing.T) {
	s := NewServer(discard())
	// A relayConn whose socket write always blocks (never drained).
	blocked := &relayConn{nodeID: id26("slow"), conn: blockingConn{}, out: make(chan framed, writeQueueDepth), closed: make(chan struct{})}
	s.mu.Lock()
	s.conns[blocked.nodeID] = blocked
	s.mu.Unlock()

	// Enqueue more than the queue depth; enqueue must never block.
	done := make(chan struct{})
	go func() {
		for i := 0; i < writeQueueDepth*3; i++ {
			blocked.enqueue(framed{src: id26("src"), payload: []byte{byte(i)}})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("enqueue blocked under backpressure")
	}
	if len(blocked.out) > writeQueueDepth {
		t.Fatalf("queue exceeded its bound: %d", len(blocked.out))
	}
}

// --- helpers ---------------------------------------------------------------

type blockingConn struct{ net.Conn }

func (blockingConn) Write([]byte) (int, error) { select {} }
func (blockingConn) Close() error              { return nil }

type testCA struct {
	cert   *x509.Certificate
	key    *ecdsa.PrivateKey
	pool   *x509.CertPool
	caPEM  []byte
	caCert tls.Certificate
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, BasicConstraintsValid: true, IsCA: true,
	}
	der, _ := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	cert, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &testCA{cert: cert, key: key, pool: pool}
}

// leaf issues a node identity cert with URI SAN zattera://node/<id>.
func (ca *testCA) leaf(t *testing.T, nodeID string) tls.Certificate {
	t.Helper()
	lk, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	uri, _ := url.Parse("zattera://node/" + nodeID)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: "node"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		DNSNames:    []string{"node"},
		URIs:        []*url.URL{uri},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &lk.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: lk, Leaf: mustParse(der)}
}

func mustParse(der []byte) *x509.Certificate { c, _ := x509.ParseCertificate(der); return c }

func startServer(t *testing.T, ca *testCA) (string, func()) {
	t.Helper()
	srvLeaf := ca.leaf(t, id26("relay-srv"))
	lis, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{srvLeaf},
		ClientCAs:    ca.pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := NewServer(discard())
	go func() { _ = s.Serve(ctx, lis, NodeIDFromURISANs) }()
	return lis.Addr().String(), func() { cancel(); _ = lis.Close() }
}

// dialClient connects a relay client and, if onRecv is set, runs its read loop.
func (ca *testCA) dialClient(t *testing.T, addr, nodeID string, onRecv func(string, []byte)) net.Conn {
	t.Helper()
	leaf := ca.leaf(t, nodeID)
	conn, err := tls.Dial("tcp", addr, &tls.Config{
		Certificates: []tls.Certificate{leaf}, RootCAs: ca.pool, ServerName: "node", MinVersion: tls.VersionTLS12,
	})
	if err != nil {
		t.Fatalf("dial %s: %v", nodeID, err)
	}
	if err := conn.Handshake(); err != nil {
		t.Fatalf("handshake %s: %v", nodeID, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if onRecv != nil {
		go func() {
			for {
				src, payload, err := readFrame(conn)
				if err != nil {
					return
				}
				onRecv(src, payload)
			}
		}()
	}
	return conn
}
