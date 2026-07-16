package raftstore

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	"github.com/zattera-dev/zattera/internal/daemon/ca"
	"github.com/zattera-dev/zattera/internal/state"
)

// nodeTLSCert issues a node identity leaf from authority and returns a usable
// tls.Certificate.
func nodeTLSCert(t *testing.T, authority *ca.CA, nodeID string, ip net.IP) tls.Certificate {
	t.Helper()
	leaf, err := authority.IssueNode(nodeID, ip, ca.NodeCertTTL)
	if err != nil {
		t.Fatalf("issue node cert: %v", err)
	}
	cert, err := leaf.TLSCertificate(authority.CABundlePEM())
	if err != nil {
		t.Fatalf("build tls cert: %v", err)
	}
	return cert
}

// loopbackAddr grabs a free loopback TCP port and returns "127.0.0.1:port".
func loopbackAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// newTLSTestStore boots one in-memory raft node on the given TLS transport,
// bootstrapping the shared server set.
func newTLSTestStore(t *testing.T, id string, tr raft.Transport, servers []raft.Server) *Store {
	t.Helper()
	st, err := New(Config{
		NodeID:           id,
		Inmem:            true,
		Bootstrap:        true,
		BootstrapServers: servers,
		Transport:        tr,
	}, state.New())
	if err != nil {
		t.Fatalf("raftstore.New %s: %v", id, err)
	}
	t.Cleanup(func() { _ = st.Shutdown() })
	return st
}

// TestTLSTransportReplication brings up two raft nodes over the mTLS transport
// and verifies a command applied on the leader replicates to the follower.
func TestTLSTransportReplication(t *testing.T) {
	authority, err := ca.LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatalf("ca: %v", err)
	}
	pool := authority.Pool()

	addr1, addr2 := loopbackAddr(t), loopbackAddr(t)
	tr1, err := NewTLSTransport(addr1, addr1, nodeTLSCert(t, authority, "n1", net.ParseIP("127.0.0.1")), pool, io.Discard)
	if err != nil {
		t.Fatalf("transport 1: %v", err)
	}
	tr2, err := NewTLSTransport(addr2, addr2, nodeTLSCert(t, authority, "n2", net.ParseIP("127.0.0.1")), pool, io.Discard)
	if err != nil {
		t.Fatalf("transport 2: %v", err)
	}

	servers := []raft.Server{
		{ID: "n1", Address: raft.ServerAddress(addr1)},
		{ID: "n2", Address: raft.ServerAddress(addr2)},
	}
	s1 := newTLSTestStore(t, "n1", tr1, servers)
	s2 := newTLSTestStore(t, "n2", tr2, servers)

	leader := waitTLSLeader(t, s1, s2)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := leader.Apply(ctx, &clusterv1.Command{
		RequestId: "r1", Actor: "test", Time: timestamppb.Now(),
		Mutation: &clusterv1.Command_PutKv{PutKv: &clusterv1.PutKV{Key: "k", Value: []byte("v"), ExpectedVersion: -1}},
	}); err != nil {
		t.Fatalf("apply on leader: %v", err)
	}

	follower := s1
	if leader == s1 {
		follower = s2
	}
	// The follower must observe the replicated write over the TLS transport.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if got, _, _, ok := follower.State().KV("k"); ok && string(got) == "v" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("write did not replicate to follower over TLS transport")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestTLSTransportRejectsForeignCA verifies a peer holding a cert from a
// different CA cannot complete the mTLS handshake with the transport listener.
func TestTLSTransportRejectsForeignCA(t *testing.T) {
	authority, _ := ca.LoadOrCreate(t.TempDir())
	foreign, _ := ca.LoadOrCreate(t.TempDir())

	addr := loopbackAddr(t)
	tr, err := NewTLSTransport(addr, addr, nodeTLSCert(t, authority, "n1", net.ParseIP("127.0.0.1")), authority.Pool(), io.Discard)
	if err != nil {
		t.Fatalf("transport: %v", err)
	}
	defer func() { _ = tr.(io.Closer).Close() }()

	// Dial with a foreign-CA node cert: the server requires+verifies the client
	// cert against its own CA pool, so the handshake must fail.
	clientCfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		Certificates:       []tls.Certificate{nodeTLSCert(t, foreign, "evil", net.ParseIP("127.0.0.1"))},
		InsecureSkipVerify: true, //nolint:gosec // this test only checks the server rejects us
	}
	d := &net.Dialer{Timeout: 2 * time.Second}
	conn, derr := tls.DialWithDialer(d, "tcp", addr, clientCfg)
	if derr == nil {
		// Under TLS 1.3 the client's handshake completes before the server
		// rejects its cert (the alert arrives post-handshake), so force a read
		// to observe the rejection.
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		buf := make([]byte, 1)
		_, derr = conn.Read(buf)
		_ = conn.Close()
	}
	if derr == nil {
		t.Fatalf("dial with foreign CA cert should have failed")
	}
}

// waitTLSLeader waits until one of the stores becomes leader and returns it.
func waitTLSLeader(t *testing.T, stores ...*Store) *Store {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, s := range stores {
			if s.IsLeader() {
				return s
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no leader elected over TLS transport")
	return nil
}
