package api

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/ca"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
)

// recordingVoter records AddVoter calls for the enrollment test.
type recordingVoter struct {
	mu    sync.Mutex
	calls []string // "nodeID@addr"
}

func (r *recordingVoter) AddVoter(nodeID, addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, nodeID+"@"+addr)
	return nil
}

func (r *recordingVoter) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

// TestEnrollControlVoter verifies the leader waits until the joining control
// node's raft transport is reachable before adding it as a voter — never
// stranding a voter it cannot reach.
func TestEnrollControlVoter(t *testing.T) {
	// Reserve a port but do NOT listen yet: the node's transport is "down".
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()

	mem := &recordingVoter{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	done := make(chan struct{})
	go func() {
		enrollControlVoter(mem, clock.Real{}, "n2", addr, log)
		close(done)
	}()

	// While the address is unreachable, no voter is added.
	time.Sleep(300 * time.Millisecond)
	if mem.count() != 0 {
		t.Fatalf("AddVoter called before the node was reachable (%d calls)", mem.count())
	}

	// Bring the transport up: the probe now succeeds and the node is enrolled.
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("enrollment did not complete after the node became reachable")
	}
	if mem.count() != 1 || mem.calls[0] != "n2@"+addr {
		t.Fatalf("expected one AddVoter(n2@%s), got %v", addr, mem.calls)
	}
}

func TestJoin(t *testing.T) {
	t.Run("happy join: token verified, cert signed, mesh IP allocated", func(t *testing.T) {
		h := newJoinHarness(t, true)
		secret := h.mintToken(t, false, 0, zatterav1.NodeRole_NODE_ROLE_WORKER)
		csr, _ := genCSR(t)

		resp, err := h.srv.Join(context.Background(), &clusterv1.JoinRequest{
			TokenSecret: secret,
			NodeName:    "worker-1",
			CsrPem:      csr,
			OsArch:      "linux/amd64",
		})
		if err != nil {
			t.Fatalf("join: %v", err)
		}
		if resp.GetNodeId() == "" {
			t.Fatal("expected a node id")
		}
		if resp.GetMeshIp() != "10.90.1.1" {
			t.Fatalf("first worker mesh IP = %q, want 10.90.1.1", resp.GetMeshIp())
		}
		if !resp.GetMeshEnabled() || resp.GetControlGrpcAddr() == "" || resp.GetRegistryUsername() == "" || resp.GetRegistryPassword() == "" {
			t.Fatalf("response missing control/registry fields: %+v", resp)
		}
		// The signed node cert chains to the cluster CA.
		verifyChain(t, h.ca, resp.GetNodeCertPem())
		// The node is registered ALIVE and schedulable.
		n, ok := h.store().Node(resp.GetNodeId())
		if !ok || n.GetStatus() != zatterav1.NodeStatus_NODE_STATUS_ALIVE || !n.GetSchedulable() {
			t.Fatalf("node not registered correctly: %+v ok=%v", n, ok)
		}
		if n.GetLabels()["zattera.dev/os-arch"] != "linux/amd64" {
			t.Fatalf("os-arch label missing: %+v", n.GetLabels())
		}
	})

	t.Run("control join: handover carries data key, CA key, raft addr", func(t *testing.T) {
		h := newJoinHarness(t, true)
		secret := h.mintToken(t, true, 0, zatterav1.NodeRole_NODE_ROLE_CONTROL)
		csr, _ := genCSR(t)

		resp, err := h.srv.Join(context.Background(), &clusterv1.JoinRequest{
			TokenSecret: secret, NodeName: "control-2", CsrPem: csr, OsArch: "linux/amd64",
		})
		if err != nil {
			t.Fatalf("control join: %v", err)
		}
		// The token's CONTROL role is echoed back so the node runs the control stack.
		if len(resp.GetRoles()) != 1 || resp.GetRoles()[0] != zatterav1.NodeRole_NODE_ROLE_CONTROL {
			t.Fatalf("roles = %v, want [CONTROL]", resp.GetRoles())
		}
		// First control node beyond the seed takes 10.90.0.2; raft binds it.
		if want := resp.GetMeshIp() + ":7480"; resp.GetRaftBindAddr() != want {
			t.Fatalf("raft_bind_addr = %q, want %q", resp.GetRaftBindAddr(), want)
		}
		// Cluster secrets travel over the (mTLS) join hop.
		if len(resp.GetDataKey()) != 32 {
			t.Fatalf("data key length = %d, want 32", len(resp.GetDataKey()))
		}
		if resp.GetDataKeyVersion() == 0 {
			t.Fatal("data key version not set")
		}
		blk, _ := pem.Decode(resp.GetCaKeyPem())
		if blk == nil || blk.Type != "EC PRIVATE KEY" {
			t.Fatalf("ca_key_pem is not an EC private key: %v", blk)
		}
		if _, err := x509.ParseECPrivateKey(blk.Bytes); err != nil {
			t.Fatalf("ca_key_pem does not parse: %v", err)
		}
	})

	t.Run("control join without mesh: no handover, worker-only fallback", func(t *testing.T) {
		h := newJoinHarness(t, false) // mesh disabled
		secret := h.mintToken(t, true, 0, zatterav1.NodeRole_NODE_ROLE_CONTROL)
		csr, _ := genCSR(t)

		resp, err := h.srv.Join(context.Background(), &clusterv1.JoinRequest{
			TokenSecret: secret, NodeName: "control-x", CsrPem: csr, OsArch: "linux/amd64",
		})
		if err != nil {
			t.Fatalf("control join (no mesh): %v", err)
		}
		if resp.GetRaftBindAddr() != "" || len(resp.GetDataKey()) != 0 || len(resp.GetCaKeyPem()) != 0 {
			t.Fatalf("mesh-disabled control join must not hand over raft/secret material: %+v", resp)
		}
	})

	t.Run("re-join with existing id resumes the node, no duplicate", func(t *testing.T) {
		h := newJoinHarness(t, true)
		secret := h.mintToken(t, false, 0, zatterav1.NodeRole_NODE_ROLE_WORKER)

		csr1, _ := genCSR(t)
		first, err := h.srv.Join(context.Background(), &clusterv1.JoinRequest{
			TokenSecret: secret, NodeName: "luigi", CsrPem: csr1, OsArch: "linux/amd64",
		})
		if err != nil {
			t.Fatalf("first join: %v", err)
		}
		id, meshIP := first.GetNodeId(), first.GetMeshIp()
		before := len(h.rs.State().ListNodes())

		// Restart: re-enroll under the same id with a fresh CSR.
		csr2, _ := genCSR(t)
		second, err := h.srv.Join(context.Background(), &clusterv1.JoinRequest{
			TokenSecret: secret, NodeName: "luigi", ExistingNodeId: id, CsrPem: csr2, OsArch: "linux/amd64",
		})
		if err != nil {
			t.Fatalf("re-join: %v", err)
		}
		if second.GetNodeId() != id {
			t.Fatalf("re-join minted a new id %q, want %q", second.GetNodeId(), id)
		}
		if second.GetMeshIp() != meshIP {
			t.Fatalf("re-join changed mesh IP to %q, want %q", second.GetMeshIp(), meshIP)
		}
		if after := len(h.rs.State().ListNodes()); after != before {
			t.Fatalf("re-join added a node record (before=%d after=%d) — duplicate", before, after)
		}
		verifyChain(t, h.ca, second.GetNodeCertPem()) // fresh cert re-issued for the same id
	})

	t.Run("unknown existing id falls back to fresh enrollment", func(t *testing.T) {
		h := newJoinHarness(t, true)
		secret := h.mintToken(t, false, 0, zatterav1.NodeRole_NODE_ROLE_WORKER)
		csr, _ := genCSR(t)
		resp, err := h.srv.Join(context.Background(), &clusterv1.JoinRequest{
			TokenSecret: secret, ExistingNodeId: "01BOGUSNODEID", CsrPem: csr,
		})
		if err != nil {
			t.Fatalf("join: %v", err)
		}
		if resp.GetNodeId() == "01BOGUSNODEID" || resp.GetNodeId() == "" {
			t.Fatalf("unknown id should get a fresh node id, got %q", resp.GetNodeId())
		}
	})

	t.Run("control token gets a low mesh IP", func(t *testing.T) {
		h := newJoinHarness(t, true)
		secret := h.mintToken(t, false, 0, zatterav1.NodeRole_NODE_ROLE_CONTROL)
		csr, _ := genCSR(t)
		resp, err := h.srv.Join(context.Background(), &clusterv1.JoinRequest{TokenSecret: secret, CsrPem: csr})
		if err != nil {
			t.Fatalf("join: %v", err)
		}
		if resp.GetMeshIp() != "10.90.0.2" {
			t.Fatalf("control mesh IP = %q, want 10.90.0.2", resp.GetMeshIp())
		}
	})

	t.Run("expired token is rejected", func(t *testing.T) {
		h := newJoinHarness(t, true)
		secret := h.mintToken(t, false, -time.Hour, zatterav1.NodeRole_NODE_ROLE_WORKER)
		csr, _ := genCSR(t)
		_, err := h.srv.Join(context.Background(), &clusterv1.JoinRequest{TokenSecret: secret, CsrPem: csr})
		if status.Code(err) != codes.PermissionDenied {
			t.Fatalf("expected PermissionDenied, got %v", err)
		}
	})

	t.Run("single-use token cannot be reused", func(t *testing.T) {
		h := newJoinHarness(t, true)
		secret := h.mintToken(t, true, 0, zatterav1.NodeRole_NODE_ROLE_WORKER)
		csr, _ := genCSR(t)
		if _, err := h.srv.Join(context.Background(), &clusterv1.JoinRequest{TokenSecret: secret, CsrPem: csr}); err != nil {
			t.Fatalf("first join: %v", err)
		}
		_, err := h.srv.Join(context.Background(), &clusterv1.JoinRequest{TokenSecret: secret, CsrPem: csr})
		if status.Code(err) != codes.PermissionDenied {
			t.Fatalf("reuse should be PermissionDenied, got %v", err)
		}
	})

	t.Run("unknown token and missing CSR are rejected", func(t *testing.T) {
		h := newJoinHarness(t, true)
		csr, _ := genCSR(t)
		if _, err := h.srv.Join(context.Background(), &clusterv1.JoinRequest{TokenSecret: "nope", CsrPem: csr}); status.Code(err) != codes.PermissionDenied {
			t.Fatalf("unknown token → PermissionDenied, got %v", err)
		}
		secret := h.mintToken(t, false, 0, zatterav1.NodeRole_NODE_ROLE_WORKER)
		if _, err := h.srv.Join(context.Background(), &clusterv1.JoinRequest{TokenSecret: secret}); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("missing CSR → InvalidArgument, got %v", err)
		}
	})

	t.Run("mesh disabled yields no mesh IP and no peers", func(t *testing.T) {
		h := newJoinHarness(t, false)
		secret := h.mintToken(t, false, 0, zatterav1.NodeRole_NODE_ROLE_WORKER)
		csr, _ := genCSR(t)
		resp, err := h.srv.Join(context.Background(), &clusterv1.JoinRequest{TokenSecret: secret, CsrPem: csr})
		if err != nil {
			t.Fatalf("join: %v", err)
		}
		if resp.GetMeshIp() != "" || resp.GetMeshEnabled() || resp.GetInitialPeers() != nil {
			t.Fatalf("mesh-disabled response should omit mesh fields: %+v", resp)
		}
	})
}

// --- harness --------------------------------------------------------------

type joinHarness struct {
	srv *JoinServer
	rs  *raftstore.Store
	ca  *ca.CA
	clk *clock.Fake
}

func newJoinHarness(t *testing.T, mesh bool) *joinHarness {
	t.Helper()
	authority, err := ca.LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatalf("ca: %v", err)
	}
	rs := raftstore.NewTestStore(t)
	clk := clock.NewFake()
	cfg := JoinConfig{MeshEnabled: mesh, ControlGRPCAddr: "10.90.0.1:8443", RegistryAddr: "10.90.0.1:5000"}
	if mesh {
		cfg.RaftPort = "7480"
	}
	dataKey, err := secrets.GenerateDataKey()
	if err != nil {
		t.Fatalf("data key: %v", err)
	}
	keyring, err := secrets.NewKeyring(dataKey, 1)
	if err != nil {
		t.Fatalf("keyring: %v", err)
	}
	return &joinHarness{
		srv: NewJoinServer(rs.State(), rs, clk, authority, mustVault(keyring), cfg, nil),
		rs:  rs,
		ca:  authority,
		clk: clk,
	}
}

func (h *joinHarness) store() interface {
	Node(string) (*zatterav1.Node, bool)
} {
	return h.rs.State()
}

// mintToken applies a join token and returns its plaintext secret.
func (h *joinHarness) mintToken(t *testing.T, singleUse bool, ttl time.Duration, roles ...zatterav1.NodeRole) string {
	t.Helper()
	secret, err := randomBase62(32)
	if err != nil {
		t.Fatal(err)
	}
	jt := &zatterav1.JoinToken{
		Meta:       &zatterav1.Meta{Id: ids.New()},
		SecretHash: HashToken(secret),
		SingleUse:  singleUse,
		Roles:      roles,
	}
	if ttl != 0 {
		jt.ExpiresAt = timestamppb.New(h.clk.Now().Add(ttl))
	}
	cmd := &clusterv1.Command{
		RequestId: ids.New(),
		Actor:     "test",
		Time:      timestamppb.Now(),
		Mutation:  &clusterv1.Command_PutJoinToken{PutJoinToken: &clusterv1.PutJoinToken{Token: jt}},
	}
	if err := h.rs.Apply(context.Background(), cmd); err != nil {
		t.Fatalf("put join token: %v", err)
	}
	return secret
}

func genCSR(t *testing.T) ([]byte, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{Subject: pkix.Name{CommonName: "node"}}, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der}), key
}

func verifyChain(t *testing.T, authority *ca.CA, certPEM []byte) {
	t.Helper()
	block, _ := pem.Decode(certPEM)
	if block == nil {
		t.Fatal("node cert is not valid PEM")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse node cert: %v", err)
	}
	if _, err := cert.Verify(x509.VerifyOptions{Roots: authority.Pool(), KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny}}); err != nil {
		t.Fatalf("node cert does not chain to the cluster CA: %v", err)
	}
}
