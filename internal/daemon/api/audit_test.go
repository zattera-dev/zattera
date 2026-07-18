package api

import (
	"context"
	"crypto/tls"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/emptypb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/ca"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
)

func TestIsMutatingMethod(t *testing.T) {
	reads := []string{
		"/zattera.v1.ProjectService/GetProject",
		"/zattera.v1.ProjectService/ListProjects",
		"/zattera.v1.DeployService/WatchDeployment",
		"/zattera.v1.AuditService/QueryAudit",
		"/zattera.v1.AppService/GetEnvVars",
	}
	for _, m := range reads {
		if isMutatingMethod(m) {
			t.Errorf("%s classified as mutating", m)
		}
	}
	writes := []string{
		"/zattera.v1.ProjectService/CreateProject",
		"/zattera.v1.ProjectService/DeleteProject",
		"/zattera.v1.AppService/SetEnvVars",
		"/zattera.v1.AuthService/Login",
	}
	for _, m := range writes {
		if !isMutatingMethod(m) {
			t.Errorf("%s classified as read", m)
		}
	}
}

func TestAudit(t *testing.T) {
	rs := raftstore.NewTestStore(t)
	clk := clock.Real{}
	st := rs.State()
	auth := NewAuthenticator(st, rs, clk)
	rbac := NewRBAC(st)
	audit := NewAuditor(st, rs, nil, 0)

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
		AuditService:   audit,
		UnaryInterceptors: []grpc.UnaryServerInterceptor{
			auth.UnaryInterceptor, rbac.UnaryInterceptor, audit.UnaryInterceptor,
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

	mustApply(t, rs, &clusterv1.Command{Mutation: &clusterv1.Command_PutOrg{PutOrg: &clusterv1.PutOrg{
		Org: &zatterav1.Org{Meta: newMeta(ids.New(), clk.Now()), Name: "default"},
	}}})
	_, adminTok := seedRoleUser(t, rs, "admin@local", zatterav1.Role_ROLE_ADMIN)

	conn, err := grpc.NewClient(srv.Addr().String(), grpc.WithTransportCredentials(
		credentials.NewTLS(&tls.Config{RootCAs: authority.Pool(), ServerName: "127.0.0.1"})))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	pc := zatterav1.NewProjectServiceClient(conn)
	tctx := bearerCtx(adminTok)

	// One mutating call (CreateProject) and one read (ListProjects).
	if _, err := pc.CreateProject(tctx, &zatterav1.CreateProjectRequest{Name: "demo"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := pc.ListProjects(tctx, &emptypb.Empty{}); err != nil {
		t.Fatalf("list: %v", err)
	}
	// A mutating call that errors (duplicate) must still be recorded.
	_, _ = pc.CreateProject(tctx, &zatterav1.CreateProjectRequest{Name: "demo"})

	// Flush synchronously.
	audit.drainOnce(context.Background())

	entries := audit.QueryAuditAll(t)
	if len(entries) != 2 {
		t.Fatalf("want 2 audit entries (ok + error), got %d: %+v", len(entries), entries)
	}
	// Both are CreateProject; none is ListProjects.
	var okCount, errCount int
	for _, e := range entries {
		if e.GetMethod() != "/zattera.v1.ProjectService/CreateProject" {
			t.Errorf("unexpected method audited: %s", e.GetMethod())
		}
		if e.GetActorUserId() == "" {
			t.Error("audit entry missing actor")
		}
		if e.GetRequestSummary() != "name=demo" {
			t.Errorf("summary = %q, want name=demo", e.GetRequestSummary())
		}
		switch e.GetOutcome() {
		case "ok":
			okCount++
		case "AlreadyExists":
			errCount++
		}
	}
	if okCount != 1 || errCount != 1 {
		t.Errorf("outcomes: ok=%d err=%d, want 1/1", okCount, errCount)
	}

	// QueryAudit method-prefix filter.
	resp, err := audit.QueryAudit(context.Background(), &zatterav1.QueryAuditRequest{MethodPrefix: "/zattera.v1.AuthService/"})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetEntries()) != 0 {
		t.Errorf("auth-prefix filter returned %d, want 0", len(resp.GetEntries()))
	}
}

// QueryAuditAll is a test helper returning all entries.
func (a *Auditor) QueryAuditAll(t *testing.T) []*zatterav1.AuditEntry {
	t.Helper()
	resp, err := a.QueryAudit(context.Background(), &zatterav1.QueryAuditRequest{})
	if err != nil {
		t.Fatal(err)
	}
	return resp.GetEntries()
}
