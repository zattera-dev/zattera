package api

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/ca"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
)

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
	return &joinHarness{
		srv: NewJoinServer(rs.State(), rs, clk, authority, cfg, nil),
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
