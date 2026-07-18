package cli

import (
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/api"
	"github.com/zattera-dev/zattera/internal/daemon/ca"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/daemon/scheduler"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
)

// deployHarness runs a full in-process control plane — API (auth/project/app/
// deploy/node), the scheduler + red/green orchestrator, and a fake agent that
// marks placed instances HEALTHY — so a `deploy` walks to a terminal phase.
// It seeds an org/admin, project "demo", app "web" (production env) and two
// schedulable nodes, returning the address, CA and an admin token.
func deployHarness(t *testing.T) (addr string, caPEM []byte, adminToken string) {
	t.Helper()
	rs := raftstore.NewTestStore(t)
	st := rs.State()
	clk := clock.Real{}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	authn := api.NewAuthenticator(st, rs, clk)
	rbac := api.NewRBAC(st)
	dataKey, _ := secrets.GenerateDataKey()
	kr, _ := secrets.NewKeyring(dataKey, 1)
	vault, _ := secrets.NewUnsealedVault(kr)
	authority, err := ca.LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	srv, err := api.New(api.Options{
		CA:             authority,
		Listen:         "127.0.0.1:0",
		DNSNames:       []string{"localhost"},
		IPs:            []net.IP{net.ParseIP("127.0.0.1")},
		Logger:         log,
		AuthService:    api.NewAuthServer(st, rs, clk, "", vault),
		ProjectService: api.NewProjectServer(st, rs, clk, rbac),
		AppService:     api.NewAppServer(st, rs, clk, vault),
		DeployService:  api.NewDeployServer(st, rs, clk, t.TempDir()),
		NodeService:    api.NewNodeServer(st, rs, clk, authority),
		UnaryInterceptors: []grpc.UnaryServerInterceptor{
			authn.UnaryInterceptor, rbac.UnaryInterceptor,
		},
		StreamInterceptors: []grpc.StreamServerInterceptor{authn.StreamInterceptor},
	})
	if err != nil {
		t.Fatalf("api.New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = srv.Serve(ctx) }()
	t.Cleanup(cancel)

	// Control loops.
	go scheduler.New(rs, clk, log).Run(ctx)
	go scheduler.NewOrchestrator(rs, clk, log).Run(ctx)
	go fakeAgent(ctx, st)

	// Seed identity + project/app/env + nodes.
	uid := ids.New()
	tok, hash, _ := api.MintToken()
	spec := &zatterav1.ServiceSpec{Replicas: &zatterav1.ReplicaRange{Min: 1}}
	seed := []*clusterv1.Command{
		{Mutation: &clusterv1.Command_PutOrg{PutOrg: &clusterv1.PutOrg{Org: &zatterav1.Org{Meta: meta(), Name: "default"}}}},
		{Mutation: &clusterv1.Command_PutUser{PutUser: &clusterv1.PutUser{User: &zatterav1.User{Meta: metaID(uid), Email: "admin@local", OrgRole: zatterav1.Role_ROLE_OWNER}}}},
		{Mutation: &clusterv1.Command_PutToken{PutToken: &clusterv1.PutToken{Token: &zatterav1.Token{Meta: meta(), UserId: uid, SecretHash: hash}}}},
		{Mutation: &clusterv1.Command_PutProject{PutProject: &clusterv1.PutProject{Project: &zatterav1.Project{Meta: metaID("proj1"), Name: "demo"}}}},
		{Mutation: &clusterv1.Command_PutApp{PutApp: &clusterv1.PutApp{App: &zatterav1.App{Meta: metaID("app1"), ProjectId: "proj1", Name: "web"}}}},
		{Mutation: &clusterv1.Command_PutEnvironment{PutEnvironment: &clusterv1.PutEnvironment{Environment: &zatterav1.Environment{Meta: metaID("envprod"), ProjectId: "proj1", AppId: "app1", Name: "production", Service: spec}}}},
		{Mutation: &clusterv1.Command_PutNode{PutNode: &clusterv1.PutNode{Node: aliveSchedNode("n1")}}},
		{Mutation: &clusterv1.Command_PutNode{PutNode: &clusterv1.PutNode{Node: aliveSchedNode("n2")}}},
	}
	for _, cmd := range seed {
		cmd.RequestId = ids.New()
		cmd.Time = timestamppb.Now()
		if err := rs.Apply(context.Background(), cmd); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	return srv.Addr().String(), authority.CABundlePEM(), tok
}

// fakeAgent stands in for the node agents: it marks every desired-RUN assignment
// HEALTHY so the orchestrator's green set passes health checks.
func fakeAgent(ctx context.Context, st interface {
	ListAssignments(string) []*zatterav1.Assignment
	SetAssignmentObserved(string, map[string]*zatterav1.AssignmentObserved)
}) {
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			for _, a := range st.ListAssignments("") {
				if a.GetDesired() == zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN &&
					a.GetObserved().GetState() != zatterav1.InstanceState_INSTANCE_STATE_HEALTHY {
					st.SetAssignmentObserved(a.GetNodeId(), map[string]*zatterav1.AssignmentObserved{
						a.GetMeta().GetId(): {State: zatterav1.InstanceState_INSTANCE_STATE_HEALTHY, ContainerId: "fake"},
					})
				}
			}
		}
	}
}

func aliveSchedNode(id string) *zatterav1.Node {
	return &zatterav1.Node{
		Meta:        metaID(id),
		Status:      zatterav1.NodeStatus_NODE_STATUS_ALIVE,
		Schedulable: true,
		Roles:       []zatterav1.NodeRole{zatterav1.NodeRole_NODE_ROLE_WORKER},
		Capacity:    &zatterav1.ResourceLimits{CpuMillis: 4000, MemoryMb: 8192},
	}
}

func TestDeployCLI(t *testing.T) {
	addr, caPEM, token := deployHarness(t)
	t.Setenv("ZATTERA_CONFIG", filepath.Join(t.TempDir(), "config.toml"))
	caPath := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caPath, caPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := run(t, "login", "--server", "https://"+addr, "--token", token, "--ca-cert", caPath); err != nil {
		t.Fatalf("login: %v", err)
	}

	t.Run("deploy image walks to a released state", func(t *testing.T) {
		out, errOut, err := run(t, "deploy", "--image", "nginx:alpine", "--app", "web", "--prod", "--project", "demo")
		if err != nil {
			t.Fatalf("deploy: %v\nstderr:\n%s", err, errOut)
		}
		if !strings.Contains(out, "Released") {
			t.Fatalf("expected a Released success line, got stdout=%q stderr=%q", out, errOut)
		}
		// The watch resends only on phase change, so a fast deploy coalesces
		// rapid transitions — in the extreme, the only phase the stream ever
		// delivers is the terminal "released (draining old)". Assert that the
		// watch printed at least one phase line, terminal included.
		phases := []string{"pending", "building", "placing replicas", "starting", "health checking", "promoting", "released (draining old)"}
		progressed := false
		for _, ph := range phases {
			if strings.Contains(errOut, ph) {
				progressed = true
				break
			}
		}
		if !progressed {
			t.Fatalf("expected a deploy phase line on stderr, got %q", errOut)
		}
	})

	t.Run("ps lists the running instance", func(t *testing.T) {
		out, _, err := run(t, "ps", "--app", "web", "--project", "demo")
		if err != nil {
			t.Fatalf("ps: %v", err)
		}
		if !strings.Contains(out, "STATE") {
			t.Fatalf("ps should print a table header, got %q", out)
		}
	})

	t.Run("releases lists the deployed version", func(t *testing.T) {
		out, _, err := run(t, "releases", "--app", "web", "--prod", "--project", "demo")
		if err != nil {
			t.Fatalf("releases: %v", err)
		}
		if !strings.Contains(out, "v1") || !strings.Contains(out, "nginx:alpine") {
			t.Fatalf("releases should list v1 nginx:alpine, got %q", out)
		}
	})
}
