package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/config"
	"github.com/zattera-dev/zattera/internal/daemon/api"
	"github.com/zattera-dev/zattera/internal/daemon/ca"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
)

// TestClientJoin drives the client against a real JoinService over TLS: the
// happy path enrolls and persists identity; a wrong CA pin fails the handshake.
func TestClientJoin(t *testing.T) {
	authority, err := ca.LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatalf("ca: %v", err)
	}
	rs := raftstore.NewTestStore(t)
	joinSrv := api.NewJoinServer(rs.State(), rs, clock.NewFake(), authority, secrets.NewVault(), api.JoinConfig{
		MeshEnabled:     true,
		ControlGRPCAddr: "10.90.0.1:8443",
		RegistryAddr:    "10.90.0.1:5000",
	}, discardLog())

	addr := startJoinServer(t, authority, joinSrv)
	secret := mintJoinToken(t, rs)
	caHash := caHashHex(authority)

	t.Run("happy join persists identity and registers the node", func(t *testing.T) {
		dataDir := t.TempDir()
		cfg := config.Config{DataDir: dataDir, NodeName: "worker-1"}
		cfg.Join.Addr = addr
		cfg.Join.Token = "K10" + caHash + "::" + secret

		jr, err := runJoin(context.Background(), cfg, discardLog())
		if err != nil {
			t.Fatalf("runJoin: %v", err)
		}
		if jr.NodeID == "" || jr.MeshIP == "" || !jr.MeshEnabled {
			t.Fatalf("unexpected join result: %+v", jr)
		}
		// Identity persisted under <data-dir>/node/.
		for _, f := range []string{"node.crt", "node.key", "ca.crt", "id", "mesh.json"} {
			if _, err := os.Stat(filepath.Join(dataDir, "node", f)); err != nil {
				t.Fatalf("expected %s persisted: %v", f, err)
			}
		}
		if perm := fileMode(t, filepath.Join(dataDir, "node", "node.key")); perm != 0o600 {
			t.Fatalf("node.key mode = %o, want 600", perm)
		}
		if _, ok := rs.State().Node(jr.NodeID); !ok {
			t.Fatal("node should be registered in state")
		}
	})

	t.Run("wrong CA pin fails the handshake", func(t *testing.T) {
		other, _ := ca.LoadOrCreate(t.TempDir())
		cfg := config.Config{DataDir: t.TempDir(), NodeName: "worker-x"}
		cfg.Join.Addr = addr
		cfg.Join.Token = "K10" + caHashHex(other) + "::" + secret

		if _, err := runJoin(context.Background(), cfg, discardLog()); err == nil {
			t.Fatal("join should fail when the CA pin does not match")
		}
	})

	t.Run("malformed token is rejected before dialing", func(t *testing.T) {
		cfg := config.Config{DataDir: t.TempDir()}
		cfg.Join.Addr = addr
		cfg.Join.Token = "not-a-token"
		if _, err := runJoin(context.Background(), cfg, discardLog()); err == nil {
			t.Fatal("malformed token should error")
		}
	})
}

func startJoinServer(t *testing.T, authority *ca.CA, joinSrv clusterv1.JoinServiceServer) string {
	t.Helper()
	tlsCfg, err := authority.ServerTLSConfig([]string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatalf("server tls: %v", err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsCfg)))
	clusterv1.RegisterJoinServiceServer(srv, joinSrv)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

func mintJoinToken(t *testing.T, rs *raftstore.Store) string {
	t.Helper()
	secret := "join-secret-" + ids.New()
	jt := &zatterav1.JoinToken{
		Meta:       &zatterav1.Meta{Id: ids.New()},
		SecretHash: api.HashToken(secret),
		Roles:      []zatterav1.NodeRole{zatterav1.NodeRole_NODE_ROLE_WORKER},
	}
	err := rs.Apply(context.Background(), &clusterv1.Command{
		RequestId: ids.New(),
		Actor:     "test",
		Mutation:  &clusterv1.Command_PutJoinToken{PutJoinToken: &clusterv1.PutJoinToken{Token: jt}},
	})
	if err != nil {
		t.Fatalf("mint token: %v", err)
	}
	return secret
}

func caHashHex(authority *ca.CA) string {
	sum := sha256.Sum256(authority.Certificate().Raw)
	return hex.EncodeToString(sum[:])
}

func fileMode(t *testing.T, path string) os.FileMode {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fi.Mode().Perm()
}

func discardLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
