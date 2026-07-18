package api

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/ca"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

type projHarness struct {
	rs     *raftstore.Store
	store  *state.Store
	addr   string
	caPool *x509.CertPool
}

func newProjHarness(t *testing.T) *projHarness {
	t.Helper()
	rs := raftstore.NewTestStore(t)
	clk := clock.Real{}
	st := rs.State()
	auth := NewAuthenticator(st, rs, clk)
	rbac := NewRBAC(st)

	authority, err := ca.LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Options{
		CA:             authority,
		Listen:         "127.0.0.1:0",
		DNSNames:       []string{"localhost"},
		IPs:            []net.IP{net.ParseIP("127.0.0.1")},
		AuthService:    NewAuthServer(st, rs, clk, "", secrets.NewVault()),
		ProjectService: NewProjectServer(st, rs, clk, rbac),
		UnaryInterceptors: []grpc.UnaryServerInterceptor{
			auth.UnaryInterceptor, rbac.UnaryInterceptor,
		},
		StreamInterceptors: []grpc.StreamServerInterceptor{auth.StreamInterceptor},
	})
	if err != nil {
		t.Fatalf("api.New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = srv.Serve(ctx) }()
	t.Cleanup(cancel)
	waitReady(t, srv.Addr().String(), authority.Pool())

	// Seed an org so CreateProject can attach to it.
	mustApply(t, rs, &clusterv1.Command{Mutation: &clusterv1.Command_PutOrg{PutOrg: &clusterv1.PutOrg{
		Org: &zatterav1.Org{Meta: newMeta(ids.New(), clk.Now()), Name: "default"},
	}}})
	return &projHarness{rs: rs, store: st, addr: srv.Addr().String(), caPool: authority.Pool()}
}

func (h *projHarness) projectClient(t *testing.T) zatterav1.ProjectServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(h.addr, grpc.WithTransportCredentials(
		credentials.NewTLS(&tls.Config{RootCAs: h.caPool, ServerName: "127.0.0.1"}),
	))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return zatterav1.NewProjectServiceClient(conn)
}

// seedRoleUser inserts a user with an org role and returns (id, personal token).
func seedRoleUser(t *testing.T, rs *raftstore.Store, email string, orgRole zatterav1.Role) (string, string) {
	t.Helper()
	uid := ids.New()
	mustApply(t, rs, &clusterv1.Command{Mutation: &clusterv1.Command_PutUser{PutUser: &clusterv1.PutUser{
		User: &zatterav1.User{Meta: newMeta(uid, clock.Real{}.Now()), Email: email, OrgRole: orgRole},
	}}})
	token, hashTok, _ := MintToken()
	mustApply(t, rs, &clusterv1.Command{Mutation: &clusterv1.Command_PutToken{PutToken: &clusterv1.PutToken{
		Token: &zatterav1.Token{Meta: newMeta(ids.New(), clock.Real{}.Now()), UserId: uid, SecretHash: hashTok},
	}}})
	return uid, token
}

func TestRBACAndProjects(t *testing.T) {
	h := newProjHarness(t)
	client := h.projectClient(t)

	// owner@local is an org ADMIN (bypasses project membership).
	_, ownerTok := seedRoleUser(t, h.rs, "owner@local", zatterav1.Role_ROLE_ADMIN)
	// The rest are plain org members (no org-level bypass).
	_, devTok := seedRoleUser(t, h.rs, "dev@local", zatterav1.Role_ROLE_DEVELOPER)
	_, viewerTok := seedRoleUser(t, h.rs, "viewer@local", zatterav1.Role_ROLE_DEVELOPER)
	_, outsiderTok := seedRoleUser(t, h.rs, "out@local", zatterav1.Role_ROLE_DEVELOPER)

	// Create a project (org admin) → becomes OWNER member.
	proj, err := client.CreateProject(bearerCtx(ownerTok), &zatterav1.CreateProjectRequest{Name: "demo"})
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	if proj.GetName() != "demo" {
		t.Fatalf("name = %q", proj.GetName())
	}

	// Invalid + duplicate name.
	if _, err := client.CreateProject(bearerCtx(ownerTok), &zatterav1.CreateProjectRequest{Name: "Bad_Name"}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("invalid name code = %v", status.Code(err))
	}
	if _, err := client.CreateProject(bearerCtx(ownerTok), &zatterav1.CreateProjectRequest{Name: "demo"}); status.Code(err) != codes.AlreadyExists {
		t.Errorf("dup name code = %v", status.Code(err))
	}

	// Add members by NAME — RBAC resolves the project name to its id.
	if _, err := client.AddMember(bearerCtx(ownerTok), &zatterav1.AddMemberRequest{ProjectId: "demo", UserEmail: "dev@local", Role: zatterav1.Role_ROLE_DEVELOPER}); err != nil {
		t.Fatalf("add dev: %v", err)
	}
	if _, err := client.AddMember(bearerCtx(ownerTok), &zatterav1.AddMemberRequest{ProjectId: "demo", UserEmail: "viewer@local", Role: zatterav1.Role_ROLE_VIEWER}); err != nil {
		t.Fatalf("add viewer: %v", err)
	}

	// Viewer/developer cannot AddMember (needs project ADMIN).
	if _, err := client.AddMember(bearerCtx(viewerTok), &zatterav1.AddMemberRequest{ProjectId: "demo", UserEmail: "out@local"}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("viewer AddMember code = %v, want PermissionDenied", status.Code(err))
	}
	if _, err := client.AddMember(bearerCtx(devTok), &zatterav1.AddMemberRequest{ProjectId: "demo", UserEmail: "out@local"}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("dev AddMember code = %v, want PermissionDenied", status.Code(err))
	}

	// Non-member gets NotFound (must not learn the project exists).
	if _, err := client.GetProject(bearerCtx(outsiderTok), &zatterav1.GetProjectRequest{ProjectId: "demo"}); status.Code(err) != codes.NotFound {
		t.Errorf("outsider GetProject code = %v, want NotFound", status.Code(err))
	}
	// Viewer can read.
	if _, err := client.GetProject(bearerCtx(viewerTok), &zatterav1.GetProjectRequest{ProjectId: "demo"}); err != nil {
		t.Errorf("viewer GetProject: %v", err)
	}

	// ListProjects filters to memberships.
	if lp, err := client.ListProjects(bearerCtx(outsiderTok), &emptypb.Empty{}); err != nil || len(lp.GetProjects()) != 0 {
		t.Errorf("outsider ListProjects = %d, err=%v", len(lp.GetProjects()), err)
	}
	if lp, err := client.ListProjects(bearerCtx(devTok), &emptypb.Empty{}); err != nil || len(lp.GetProjects()) != 1 {
		t.Errorf("dev ListProjects = %d, err=%v", len(lp.GetProjects()), err)
	}

	// Only OWNER (or org admin) can DeleteProject.
	if _, err := client.DeleteProject(bearerCtx(devTok), &zatterav1.DeleteProjectRequest{ProjectId: "demo"}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("dev DeleteProject code = %v, want PermissionDenied", status.Code(err))
	}
	if _, err := client.DeleteProject(bearerCtx(ownerTok), &zatterav1.DeleteProjectRequest{ProjectId: "demo"}); err != nil {
		t.Errorf("owner DeleteProject: %v", err)
	}
	if _, ok := h.store.ProjectByName("demo"); ok {
		t.Error("project not deleted")
	}
}

func TestRoleRank(t *testing.T) {
	if roleRank(zatterav1.Role_ROLE_OWNER) <= roleRank(zatterav1.Role_ROLE_ADMIN) {
		t.Error("OWNER should outrank ADMIN")
	}
	if roleRank(zatterav1.Role_ROLE_DEVELOPER) <= roleRank(zatterav1.Role_ROLE_VIEWER) {
		t.Error("DEVELOPER should outrank VIEWER")
	}
}
