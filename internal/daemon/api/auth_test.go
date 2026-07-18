package api

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/ca"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
)

// authHarness wires a live API server with auth for end-to-end tests.
type authHarness struct {
	rs     *raftstore.Store
	auth   *Authenticator
	srv    *Server
	caPool *x509.CertPool
	orgID  string
	clk    clock.Clock
}

func newAuthHarness(t *testing.T) *authHarness {
	t.Helper()
	rs := raftstore.NewTestStore(t)
	clk := clock.Real{}
	auth := NewAuthenticator(rs.State(), rs, clk)
	authSrv := NewAuthServer(rs.State(), rs, clk, "", secrets.NewVault())

	authority, err := ca.LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Options{
		CA:                 authority,
		Listen:             "127.0.0.1:0",
		DNSNames:           []string{"localhost"},
		IPs:                []net.IP{net.ParseIP("127.0.0.1")},
		AuthService:        authSrv,
		UnaryInterceptors:  []grpc.UnaryServerInterceptor{auth.UnaryInterceptor},
		StreamInterceptors: []grpc.StreamServerInterceptor{auth.StreamInterceptor},
	})
	if err != nil {
		t.Fatalf("api.New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = srv.Serve(ctx) }()
	t.Cleanup(cancel)

	h := &authHarness{rs: rs, auth: auth, srv: srv, caPool: authority.Pool(), clk: clk}
	// Seed an org so WhoAmI/CreateUser have somewhere to hang.
	h.orgID = ids.New()
	mustApply(t, rs, &clusterv1.Command{Mutation: &clusterv1.Command_PutOrg{PutOrg: &clusterv1.PutOrg{
		Org: &zatterav1.Org{Meta: newMeta(ids.New(), clk.Now()), Name: "default"},
	}}})
	waitReady(t, srv.Addr().String(), authority.Pool())
	return h
}

// seedUser inserts a user (known password) and a personal token; returns the
// user id and the plaintext token.
func (h *authHarness) seedUser(t *testing.T, email string, role zatterav1.Role, password string) (string, string) {
	t.Helper()
	hash, err := hashPassword(password)
	if err != nil {
		t.Fatal(err)
	}
	uid := ids.New()
	mustApply(t, h.rs, &clusterv1.Command{Mutation: &clusterv1.Command_PutUser{PutUser: &clusterv1.PutUser{
		User: &zatterav1.User{
			Meta: newMeta(uid, h.clk.Now()), Email: email, PasswordHash: hash,
			OrgId: h.orgID, OrgRole: role,
		},
	}}})
	token, hashTok, err := MintToken()
	if err != nil {
		t.Fatal(err)
	}
	mustApply(t, h.rs, &clusterv1.Command{Mutation: &clusterv1.Command_PutToken{PutToken: &clusterv1.PutToken{
		Token: &zatterav1.Token{
			Meta: newMeta(ids.New(), h.clk.Now()), UserId: uid, Name: "pat",
			SecretHash: hashTok, Kind: zatterav1.TokenKind_TOKEN_KIND_PERSONAL,
		},
	}}})
	return uid, token
}

func (h *authHarness) client(t *testing.T) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(h.srv.Addr().String(), grpc.WithTransportCredentials(
		credentials.NewTLS(&tls.Config{RootCAs: h.caPool, ServerName: "127.0.0.1"}),
	))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func bearerCtx(token string) context.Context {
	return metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+token)
}

func mustApply(t *testing.T, rs *raftstore.Store, cmd *clusterv1.Command) {
	t.Helper()
	cmd.RequestId = ids.New()
	cmd.Time = timestamppb.Now()
	if err := rs.Apply(context.Background(), cmd); err != nil {
		t.Fatalf("apply: %v", err)
	}
}

func TestAuthLoginWhoAmI(t *testing.T) {
	h := newAuthHarness(t)
	h.seedUser(t, "dev@local", zatterav1.Role_ROLE_DEVELOPER, "hunter2")
	ac := zatterav1.NewAuthServiceClient(h.client(t))

	// Login with correct password → session token.
	resp, err := ac.Login(context.Background(), &zatterav1.LoginRequest{Email: "dev@local", Password: "hunter2"})
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	if !LooksLikeToken(resp.GetToken()) {
		t.Fatalf("login token = %q", resp.GetToken())
	}
	if resp.GetUser().GetPasswordHash() != "" {
		t.Error("login response leaked password hash")
	}

	// WhoAmI with the session token.
	who, err := ac.WhoAmI(bearerCtx(resp.GetToken()), &emptypb.Empty{})
	if err != nil {
		t.Fatalf("whoami: %v", err)
	}
	if who.GetUser().GetEmail() != "dev@local" {
		t.Errorf("whoami email = %q", who.GetUser().GetEmail())
	}

	// Wrong password rejected.
	if _, err := ac.Login(context.Background(), &zatterav1.LoginRequest{Email: "dev@local", Password: "wrong"}); status.Code(err) != codes.Unauthenticated {
		t.Errorf("bad login code = %v, want Unauthenticated", status.Code(err))
	}
}

func TestAuthMissingAndBadToken(t *testing.T) {
	h := newAuthHarness(t)
	ac := zatterav1.NewAuthServiceClient(h.client(t))

	// No token → Unauthenticated (WhoAmI is reqUser).
	if _, err := ac.WhoAmI(context.Background(), &emptypb.Empty{}); status.Code(err) != codes.Unauthenticated {
		t.Errorf("no-token code = %v, want Unauthenticated", status.Code(err))
	}
	// Garbage token → Unauthenticated.
	if _, err := ac.WhoAmI(bearerCtx("zpat_notreal"), &emptypb.Empty{}); status.Code(err) != codes.Unauthenticated {
		t.Errorf("bad-token code = %v, want Unauthenticated", status.Code(err))
	}
}

func TestAuthExpiredToken(t *testing.T) {
	h := newAuthHarness(t)
	uid, _ := h.seedUser(t, "dev@local", zatterav1.Role_ROLE_DEVELOPER, "pw")
	// Insert an already-expired token.
	token, hashTok, _ := MintToken()
	mustApply(t, h.rs, &clusterv1.Command{Mutation: &clusterv1.Command_PutToken{PutToken: &clusterv1.PutToken{
		Token: &zatterav1.Token{
			Meta: newMeta(ids.New(), h.clk.Now()), UserId: uid, Name: "old",
			SecretHash: hashTok, Kind: zatterav1.TokenKind_TOKEN_KIND_SESSION,
			ExpiresAt: timestamppb.New(h.clk.Now().Add(-time.Hour)),
		},
	}}})
	ac := zatterav1.NewAuthServiceClient(h.client(t))
	if _, err := ac.WhoAmI(bearerCtx(token), &emptypb.Empty{}); status.Code(err) != codes.Unauthenticated {
		t.Errorf("expired-token code = %v, want Unauthenticated", status.Code(err))
	}
}

func TestAuthAdminTierEnforced(t *testing.T) {
	h := newAuthHarness(t)
	_, devTok := h.seedUser(t, "dev@local", zatterav1.Role_ROLE_DEVELOPER, "pw")
	_, adminTok := h.seedUser(t, "admin@local", zatterav1.Role_ROLE_OWNER, "pw")
	ac := zatterav1.NewAuthServiceClient(h.client(t))

	// Developer cannot CreateUser (admin tier).
	_, err := ac.CreateUser(bearerCtx(devTok), &zatterav1.CreateUserRequest{Email: "x@local", Password: "pw"})
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("dev CreateUser code = %v, want PermissionDenied", status.Code(err))
	}
	// Admin can.
	u, err := ac.CreateUser(bearerCtx(adminTok), &zatterav1.CreateUserRequest{Email: "x@local", Password: "pw", OrgRole: zatterav1.Role_ROLE_DEVELOPER})
	if err != nil {
		t.Fatalf("admin CreateUser: %v", err)
	}
	if u.GetPasswordHash() != "" {
		t.Error("CreateUser leaked password hash")
	}
	// The created user can now log in.
	lr, err := ac.Login(context.Background(), &zatterav1.LoginRequest{Email: "x@local", Password: "pw"})
	if err != nil || lr.GetToken() == "" {
		t.Fatalf("created user login: %v", err)
	}
}

// TestAuthNodeTier exercises the interceptor's identity resolution directly,
// since node-tier services aren't registered until later phases.
func TestAuthNodeTier(t *testing.T) {
	rs := raftstore.NewTestStore(t)
	auth := NewAuthenticator(rs.State(), rs, clock.Real{})

	authority, err := ca.LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	nodeLeaf, err := authority.IssueNode("01NODEX", net.ParseIP("10.90.0.1"), ca.NodeCertTTL)
	if err != nil {
		t.Fatal(err)
	}
	// Parse the issued node cert into a verified-chain peer context.
	nodeCtx := peerCtxWithCert(t, nodeLeaf.CertPEM)

	// Node cert reaches a Node-tier method.
	got, err := auth.authorize(nodeCtx, "/zattera.cluster.v1.AgentSyncService/Sync")
	if err != nil {
		t.Fatalf("node authorize: %v", err)
	}
	if id, _ := IdentityFrom(got); id.NodeID != "01NODEX" {
		t.Errorf("node id = %q, want 01NODEX", id.NodeID)
	}

	// A user token cannot reach a Node-tier method.
	seedUserToken(t, rs, "dev@local", zatterav1.Role_ROLE_DEVELOPER)
	_, userTok := seedUserToken(t, rs, "dev2@local", zatterav1.Role_ROLE_DEVELOPER)
	userCtx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("authorization", "Bearer "+userTok))
	if _, err := auth.authorize(userCtx, "/zattera.cluster.v1.AgentSyncService/Sync"); status.Code(err) != codes.PermissionDenied {
		t.Errorf("user→node code = %v, want PermissionDenied", status.Code(err))
	}
}

func TestAuthUnlistedMethodDenied(t *testing.T) {
	rs := raftstore.NewTestStore(t)
	auth := NewAuthenticator(rs.State(), rs, clock.Real{})
	if _, err := auth.authorize(context.Background(), "/zattera.v1.SomeService/Unknown"); status.Code(err) != codes.PermissionDenied {
		t.Errorf("unlisted method code = %v, want PermissionDenied", status.Code(err))
	}
}

func TestValidateMethodTable(t *testing.T) {
	// A registered method missing from the table must be rejected.
	info := map[string]grpc.ServiceInfo{
		"zattera.v1.GhostService": {Methods: []grpc.MethodInfo{{Name: "Vanish"}}},
	}
	if err := ValidateMethodTable(info); err == nil {
		t.Fatal("expected missing-entry error")
	}
	// Health is exempt.
	health := map[string]grpc.ServiceInfo{
		"grpc.health.v1.Health": {Methods: []grpc.MethodInfo{{Name: "Check"}}},
	}
	if err := ValidateMethodTable(health); err != nil {
		t.Errorf("health should be exempt: %v", err)
	}
}

// --- helpers ---

func seedUserToken(t *testing.T, rs *raftstore.Store, email string, role zatterav1.Role) (string, string) {
	t.Helper()
	uid := ids.New()
	mustApply(t, rs, &clusterv1.Command{Mutation: &clusterv1.Command_PutUser{PutUser: &clusterv1.PutUser{
		User: &zatterav1.User{Meta: newMeta(uid, time.Now()), Email: email, OrgRole: role},
	}}})
	token, hashTok, _ := MintToken()
	mustApply(t, rs, &clusterv1.Command{Mutation: &clusterv1.Command_PutToken{PutToken: &clusterv1.PutToken{
		Token: &zatterav1.Token{Meta: newMeta(ids.New(), time.Now()), UserId: uid, SecretHash: hashTok},
	}}})
	return uid, token
}

func peerCtxWithCert(t *testing.T, certPEM []byte) context.Context {
	t.Helper()
	block, _ := pem.Decode(certPEM)
	if block == nil {
		t.Fatal("bad cert PEM")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	st := tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{cert}}}
	return peer.NewContext(context.Background(), &peer.Peer{
		AuthInfo: credentials.TLSInfo{State: st},
	})
}
