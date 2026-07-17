package api

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/ca"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

type appHarness struct {
	rs    *raftstore.Store
	store *state.Store
	addr  string
	pool  *tls.Config
}

func newAppHarness(t *testing.T) (*appHarness, string) {
	t.Helper()
	rs := raftstore.NewTestStore(t)
	clk := clock.Real{}
	st := rs.State()
	auth := NewAuthenticator(st, rs, clk)
	rbac := NewRBAC(st)

	dataKey, _ := secrets.GenerateDataKey()
	sealer, _ := secrets.NewSealer(dataKey, 1)

	authority, err := ca.LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Options{
		CA:             authority,
		Listen:         "127.0.0.1:0",
		DNSNames:       []string{"localhost"},
		IPs:            []net.IP{net.ParseIP("127.0.0.1")},
		AuthService:    NewAuthServer(st, rs, clk, ""),
		ProjectService: NewProjectServer(st, rs, clk, rbac),
		AppService:     NewAppServer(st, rs, clk, sealer),
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

	mustApply(t, rs, &clusterv1.Command{Mutation: &clusterv1.Command_PutOrg{PutOrg: &clusterv1.PutOrg{
		Org: &zatterav1.Org{Meta: newMeta(ids.New(), clk.Now()), Name: "default"},
	}}})

	h := &appHarness{rs: rs, store: st, addr: srv.Addr().String(),
		pool: &tls.Config{RootCAs: authority.Pool(), ServerName: "127.0.0.1"}}
	return h, ""
}

func (h *appHarness) conn(t *testing.T) *grpc.ClientConn {
	t.Helper()
	c, err := grpc.NewClient(h.addr, grpc.WithTransportCredentials(credentials.NewTLS(h.pool)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestApps(t *testing.T) {
	h, _ := newAppHarness(t)
	_, adminTok := seedRoleUser(t, h.rs, "admin@local", zatterav1.Role_ROLE_ADMIN)

	conn := h.conn(t)
	pc := zatterav1.NewProjectServiceClient(conn)
	ac := zatterav1.NewAppServiceClient(conn)
	ctx := bearerCtx(adminTok)

	if _, err := pc.CreateProject(ctx, &zatterav1.CreateProjectRequest{Name: "demo"}); err != nil {
		t.Fatalf("create project: %v", err)
	}

	// CreateApp auto-creates production + staging.
	app, err := ac.CreateApp(ctx, &zatterav1.CreateAppRequest{ProjectId: "demo", Name: "web"})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	got, err := ac.GetApp(ctx, &zatterav1.GetAppRequest{ProjectId: "demo", AppId: "web"})
	if err != nil {
		t.Fatalf("get app: %v", err)
	}
	if len(got.GetEnvironments()) != 2 {
		t.Fatalf("want 2 envs, got %d", len(got.GetEnvironments()))
	}
	var prod *zatterav1.Environment
	for _, e := range got.GetEnvironments() {
		if e.GetName() == "production" {
			prod = e
		}
	}
	if prod == nil {
		t.Fatal("no production env")
	}
	if prod.GetService().GetReplicas().GetMin() != 1 {
		t.Errorf("default replicas.min = %d, want 1", prod.GetService().GetReplicas().GetMin())
	}
	if p := prod.GetService().GetPorts(); len(p) != 1 || p[0].GetContainerPort() != 8080 {
		t.Errorf("default port = %+v, want http/8080", p)
	}

	// Env var round trip: set, list redacted, reveal.
	if _, err := ac.SetEnvVars(ctx, &zatterav1.SetEnvVarsRequest{
		ProjectId: "demo", EnvironmentId: prod.GetMeta().GetId(),
		Set: map[string]string{"API_KEY": "s3cr3t", "DEBUG": "1"},
	}); err != nil {
		t.Fatalf("set env vars: %v", err)
	}
	// Default (no reveal): keys present, values blank, secrets not leaked.
	red, err := ac.GetEnvVars(ctx, &zatterav1.GetEnvVarsRequest{ProjectId: "demo", EnvironmentId: prod.GetMeta().GetId()})
	if err != nil {
		t.Fatalf("get env vars: %v", err)
	}
	if len(red.GetVars()) != 2 || red.GetVars()["API_KEY"] != "" {
		t.Errorf("redacted vars leaked or wrong: %+v", red.GetVars())
	}
	// Reveal.
	rev, err := ac.GetEnvVars(ctx, &zatterav1.GetEnvVarsRequest{ProjectId: "demo", EnvironmentId: prod.GetMeta().GetId(), Reveal: true})
	if err != nil {
		t.Fatalf("reveal: %v", err)
	}
	if rev.GetVars()["API_KEY"] != "s3cr3t" || rev.GetVars()["DEBUG"] != "1" {
		t.Errorf("revealed vars wrong: %+v", rev.GetVars())
	}
	// Unset one.
	if _, err := ac.SetEnvVars(ctx, &zatterav1.SetEnvVarsRequest{ProjectId: "demo", EnvironmentId: prod.GetMeta().GetId(), Unset: []string{"DEBUG"}}); err != nil {
		t.Fatalf("unset: %v", err)
	}
	rev2, _ := ac.GetEnvVars(ctx, &zatterav1.GetEnvVarsRequest{ProjectId: "demo", EnvironmentId: prod.GetMeta().GetId(), Reveal: true})
	if _, ok := rev2.GetVars()["DEBUG"]; ok {
		t.Error("DEBUG not unset")
	}

	// Sealed values are stored, never plaintext.
	sealed := h.store.EnvVars(prod.GetMeta().GetId())
	if v := sealed["API_KEY"]; v == nil || string(v.GetCiphertext()) == "s3cr3t" {
		t.Error("env var not sealed at rest")
	}

	// SetReplicas.
	env2, err := ac.SetReplicas(ctx, &zatterav1.SetReplicasRequest{ProjectId: "demo", EnvironmentId: prod.GetMeta().GetId(), Min: 2, Max: 5})
	if err != nil {
		t.Fatalf("set replicas: %v", err)
	}
	if env2.GetService().GetReplicas().GetMin() != 2 || env2.GetService().GetReplicas().GetMax() != 5 {
		t.Errorf("replicas = %+v, want 2/5", env2.GetService().GetReplicas())
	}

	// ApplyAppConfig creates a new preview environment, leaves others untouched.
	preview := defaultServiceSpec()
	_, err = ac.ApplyAppConfig(ctx, &zatterav1.ApplyAppConfigRequest{
		ProjectId: "demo", AppId: "web",
		Environments: map[string]*zatterav1.ServiceSpec{"preview-42": preview},
	})
	if err != nil {
		t.Fatalf("apply config: %v", err)
	}
	after, _ := ac.GetApp(ctx, &zatterav1.GetAppRequest{ProjectId: "demo", AppId: "web"})
	if len(after.GetEnvironments()) != 3 {
		t.Fatalf("want 3 envs after preview add, got %d", len(after.GetEnvironments()))
	}
	if e, ok := h.store.EnvironmentByName(app.GetMeta().GetId(), "preview-42"); !ok || e.GetType() != zatterav1.EnvironmentType_ENVIRONMENT_TYPE_PREVIEW {
		t.Error("preview env not created with PREVIEW type")
	}

	// Cross-project env id is rejected.
	if _, err := ac.GetEnvVars(ctx, &zatterav1.GetEnvVarsRequest{ProjectId: "demo", EnvironmentId: "01NOPE"}); status.Code(err) != codes.NotFound {
		t.Errorf("foreign env code = %v, want NotFound", status.Code(err))
	}
}

func TestAppInvalidName(t *testing.T) {
	h, _ := newAppHarness(t)
	_, tok := seedRoleUser(t, h.rs, "admin@local", zatterav1.Role_ROLE_ADMIN)
	conn := h.conn(t)
	pc := zatterav1.NewProjectServiceClient(conn)
	ac := zatterav1.NewAppServiceClient(conn)
	ctx := bearerCtx(tok)
	_, _ = pc.CreateProject(ctx, &zatterav1.CreateProjectRequest{Name: "demo"})
	if _, err := ac.CreateApp(ctx, &zatterav1.CreateAppRequest{ProjectId: "demo", Name: "Bad_Name"}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("bad app name code = %v, want InvalidArgument", status.Code(err))
	}
}

// TestApplyAppConfigScaleToZero covers the T-69 wiring: idle_timeout lands on the
// Environment, a scale-to-zero env is seeded to run (effective=min), and
// scale_to_zero + stateful is rejected.
func TestApplyAppConfigScaleToZero(t *testing.T) {
	rs := raftstore.NewTestStore(t)
	st := rs.State()
	st.PutApp(&zatterav1.App{Meta: &zatterav1.Meta{Id: "app1"}, Name: "web", ProjectId: "p1"})
	srv := NewAppServer(st, rs, clock.NewFake(), nil)
	ctx := withIdentity(context.Background(), Identity{UserID: "u1"})

	// Valid scale-to-zero env with an idle window.
	if _, err := srv.ApplyAppConfig(ctx, &zatterav1.ApplyAppConfigRequest{
		ProjectId: "p1", AppId: "app1",
		Environments: map[string]*zatterav1.ServiceSpec{
			"production": {ScaleToZero: true, Replicas: &zatterav1.ReplicaRange{Min: 2, Max: 5}},
		},
		IdleTimeouts: map[string]*durationpb.Duration{"production": durationpb.New(20 * time.Minute)},
	}); err != nil {
		t.Fatalf("apply scale-to-zero: %v", err)
	}
	env, ok := st.EnvironmentByName("app1", "production")
	if !ok {
		t.Fatal("production env not created")
	}
	if env.GetIdleTimeout().AsDuration() != 20*time.Minute {
		t.Fatalf("idle_timeout not applied: %v", env.GetIdleTimeout().AsDuration())
	}
	if env.GetEffectiveReplicas() != 2 {
		t.Fatalf("scale-to-zero env not seeded to min: effective=%d", env.GetEffectiveReplicas())
	}

	// scale_to_zero + stateful is rejected.
	_, err := srv.ApplyAppConfig(ctx, &zatterav1.ApplyAppConfigRequest{
		ProjectId: "p1", AppId: "app1",
		Environments: map[string]*zatterav1.ServiceSpec{
			"staging": {ScaleToZero: true, Stateful: true},
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("scale_to_zero+stateful code = %v, want InvalidArgument", status.Code(err))
	}
}
