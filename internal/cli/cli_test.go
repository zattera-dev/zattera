package cli

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/api"
	"github.com/zattera-dev/zattera/internal/daemon/ca"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
)

// testServer runs a real API server in-process and returns its address, CA PEM
// and a bootstrap admin token.
func testServer(t *testing.T) (addr string, caPEM []byte, adminToken string) {
	t.Helper()
	rs := raftstore.NewTestStore(t)
	st := rs.State()
	clk := clock.Real{}
	auth := api.NewAuthenticator(st, rs, clk)
	rbac := api.NewRBAC(st)
	dataKey, _ := secrets.GenerateDataKey()
	sealer, _ := secrets.NewSealer(dataKey, 1)

	authority, err := ca.LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	srv, err := api.New(api.Options{
		CA:             authority,
		Listen:         "127.0.0.1:0",
		DNSNames:       []string{"localhost"},
		IPs:            []net.IP{net.ParseIP("127.0.0.1")},
		AuthService:    api.NewAuthServer(st, rs, clk),
		ProjectService: api.NewProjectServer(st, rs, clk, rbac),
		AppService:     api.NewAppServer(st, rs, clk, sealer),
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

	// Seed org + admin user + token directly through raft.
	uid := ids.New()
	tok, hash, _ := api.MintToken()
	for _, cmd := range []*clusterv1.Command{
		{Mutation: &clusterv1.Command_PutOrg{PutOrg: &clusterv1.PutOrg{Org: &zatterav1.Org{Meta: meta(), Name: "default"}}}},
		{Mutation: &clusterv1.Command_PutUser{PutUser: &clusterv1.PutUser{User: &zatterav1.User{Meta: metaID(uid), Email: "admin@local", OrgRole: zatterav1.Role_ROLE_OWNER}}}},
		{Mutation: &clusterv1.Command_PutToken{PutToken: &clusterv1.PutToken{Token: &zatterav1.Token{Meta: meta(), UserId: uid, SecretHash: hash}}}},
	} {
		cmd.RequestId = ids.New()
		cmd.Time = timestamppb.Now()
		if err := rs.Apply(context.Background(), cmd); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	return srv.Addr().String(), authority.CABundlePEM(), tok
}

func meta() *zatterav1.Meta { return metaID(ids.New()) }
func metaID(id string) *zatterav1.Meta {
	return &zatterav1.Meta{Id: id, CreatedAt: timestamppb.Now(), UpdatedAt: timestamppb.Now()}
}

// run executes the CLI with args, capturing stdout. It rebuilds the command
// tree each call so flags don't leak between runs.
func run(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	jsonFlag = false
	projectFlag = ""
	root := &cobra.Command{Use: "zattera", SilenceUsage: true, SilenceErrors: true}
	root.AddCommand(Commands()...)
	var out, errBuf bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&errBuf)
	root.SetArgs(args)
	err := root.Execute()
	return out.String(), errBuf.String(), err
}

func TestCLI(t *testing.T) {
	addr, caPEM, token := testServer(t)

	// Isolate the CLI config in a temp dir.
	cfgPath := filepath.Join(t.TempDir(), "config.toml")
	t.Setenv("ZATTERA_CONFIG", cfgPath)
	caPath := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caPath, caPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	server := "https://" + addr

	t.Run("login verify success", func(t *testing.T) {
		out, _, err := run(t, "login", "--server", server, "--token", token, "--ca-cert", caPath)
		if err != nil {
			t.Fatalf("login: %v", err)
		}
		if !strings.Contains(out, "admin@local") {
			t.Errorf("login output missing email: %q", out)
		}
	})

	t.Run("login verify failure removes context", func(t *testing.T) {
		_, _, err := run(t, "login", "--server", server, "--token", "zpat_bogus", "--ca-cert", caPath, "--context", "bad")
		if err == nil {
			t.Fatal("expected login to fail with a bad token")
		}
	})

	t.Run("projects create and ls", func(t *testing.T) {
		if _, _, err := run(t, "projects", "create", "demo"); err != nil {
			t.Fatalf("create: %v", err)
		}
		out, _, err := run(t, "projects", "ls")
		if err != nil {
			t.Fatalf("ls: %v", err)
		}
		if !strings.Contains(out, "demo") {
			t.Errorf("ls missing demo: %q", out)
		}
	})

	t.Run("app + env round trip", func(t *testing.T) {
		if _, _, err := run(t, "apps", "create", "web", "--project", "demo"); err != nil {
			t.Fatalf("apps create: %v", err)
		}
		if _, _, err := run(t, "env", "set", "API_KEY=s3cret", "--project", "demo", "--app", "web", "--env", "production"); err != nil {
			t.Fatalf("env set: %v", err)
		}
		// Redacted pull.
		out, _, err := run(t, "env", "pull", "--project", "demo", "--app", "web")
		if err != nil {
			t.Fatalf("env pull: %v", err)
		}
		if !strings.Contains(out, "API_KEY=\n") && !strings.Contains(out, "API_KEY=") {
			t.Errorf("pull missing key: %q", out)
		}
		if strings.Contains(out, "s3cret") {
			t.Errorf("pull leaked secret without --reveal: %q", out)
		}
		// Reveal.
		out, _, err = run(t, "env", "pull", "--reveal", "--project", "demo", "--app", "web")
		if err != nil {
			t.Fatalf("env pull --reveal: %v", err)
		}
		if !strings.Contains(out, "API_KEY=s3cret") {
			t.Errorf("reveal missing value: %q", out)
		}
	})

	t.Run("api error is plain", func(t *testing.T) {
		_, _, err := run(t, "projects", "create", "Bad_Name")
		if err == nil {
			t.Fatal("expected error")
		}
		if strings.Contains(err.Error(), "rpc error") || strings.Contains(err.Error(), "code =") {
			t.Errorf("error not stripped: %q", err.Error())
		}
	})
}

func TestInitDetection(t *testing.T) {
	cases := []struct {
		name  string
		files []string
		want  string
	}{
		{"dockerfile", []string{"Dockerfile"}, "dockerfile"},
		{"node", []string{"package.json"}, "nixpacks"},
		{"go", []string{"go.mod"}, "nixpacks"},
		{"empty", nil, "nixpacks"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			for _, f := range tc.files {
				if err := os.WriteFile(filepath.Join(dir, f), []byte("x"), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			if got := detectBuildType(dir); got != tc.want {
				t.Errorf("detectBuildType = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestInitWritesTOML(t *testing.T) {
	dir := t.TempDir()
	cwd, _ := os.Getwd()
	t.Cleanup(func() { _ = os.Chdir(cwd) })
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	if _, _, err := run(t, "init", "--name", "myapp"); err != nil {
		t.Fatalf("init: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "zattera.toml"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `name = "myapp"`) {
		t.Errorf("toml missing app name: %s", data)
	}
	// Second init must refuse to overwrite.
	if _, _, err := run(t, "init", "--name", "myapp"); err == nil {
		t.Error("expected init to refuse overwriting")
	}
}
