package api

import (
	"context"
	"testing"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/livestate"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

// leaderToggleApplier is an Applier whose leadership is configurable.
type leaderToggleApplier struct{ leader bool }

func (a leaderToggleApplier) Apply(context.Context, *clusterv1.Command) error { return nil }
func (a leaderToggleApplier) IsLeader() bool                                  { return a.leader }

// TestSyncServerNotLeader covers the T-55d guard: an agent stream is only served
// by the leader (livestate is leader-memory), and a nil applier (tests/single
// node) is treated as the leader.
func TestSyncServerNotLeader(t *testing.T) {
	mk := func(a Applier) *SyncServer {
		return NewSyncServer(state.New(), a, livestate.New(clock.NewFake()), clock.NewFake(), nil, secrets.NewVault())
	}
	if mk(nil).notLeader() {
		t.Fatal("nil applier must be treated as the leader")
	}
	if !mk(leaderToggleApplier{leader: false}).notLeader() {
		t.Fatal("a follower must reject agent streams")
	}
	if mk(leaderToggleApplier{leader: true}).notLeader() {
		t.Fatal("the leader must serve agent streams")
	}
}

// TestSyncServerRuntimePayload verifies that the control side resolves an
// assignment's release into an image + frozen spec and decrypts the
// environment's env vars into the per-assignment runtime payload (T-15 step 3).
func TestSyncServerRuntimePayload(t *testing.T) {
	dataKey, _ := secrets.GenerateDataKey()
	kr, err := secrets.NewKeyring(dataKey, 1)
	vault := mustVault(kr)
	if err != nil {
		t.Fatalf("new sealer: %v", err)
	}

	st := state.New()
	st.PutRelease(&zatterav1.Release{
		Meta:          &zatterav1.Meta{Id: "rel1"},
		EnvironmentId: "env1",
		ImageRef:      "registry.example/app@sha256:abc",
		Service: &zatterav1.ServiceSpec{
			Ports: []*zatterav1.PortSpec{{Name: "http", ContainerPort: 8080}},
		},
	})
	sealed, err := vault.Seal([]byte("s3cr3t"))
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	st.SetEnvVars("env1", map[string]*zatterav1.EncryptedValue{"TOKEN": sealed}, nil)

	s := NewSyncServer(st, nil, livestate.New(clock.NewFake()), clock.NewFake(), nil, vault)

	rt := s.buildRuntime(&zatterav1.Assignment{
		Meta:          &zatterav1.Meta{Id: "a1"},
		ReleaseId:     "rel1",
		EnvironmentId: "env1",
	})
	if rt == nil {
		t.Fatal("expected a runtime payload")
	}
	if rt.GetImageRef() != "registry.example/app@sha256:abc" {
		t.Fatalf("image ref = %q", rt.GetImageRef())
	}
	if got := rt.GetSpec().GetPorts(); len(got) != 1 || got[0].GetName() != "http" {
		t.Fatalf("spec ports not carried: %+v", got)
	}
	if rt.GetEnv()["TOKEN"] != "s3cr3t" {
		t.Fatalf("env not decrypted: %+v", rt.GetEnv())
	}
	// Platform-injected PORT comes from the first container port (T-50).
	if rt.GetEnv()["PORT"] != "8080" {
		t.Fatalf("PORT not injected: %+v", rt.GetEnv())
	}

	// Unknown release → nil payload (agent will report FAILED).
	if s.buildRuntime(&zatterav1.Assignment{Meta: &zatterav1.Meta{Id: "a2"}, ReleaseId: "missing"}) != nil {
		t.Fatal("unknown release should yield nil runtime payload")
	}

	// Without a sealer, sealed user vars are omitted but image/spec resolve and
	// platform vars (PORT) are still injected.
	noSeal := NewSyncServer(st, nil, livestate.New(clock.NewFake()), clock.NewFake(), nil, secrets.NewVault())
	rt2 := noSeal.buildRuntime(&zatterav1.Assignment{Meta: &zatterav1.Meta{Id: "a1"}, ReleaseId: "rel1", EnvironmentId: "env1"})
	if rt2 == nil || rt2.GetImageRef() == "" {
		t.Fatal("image/spec should resolve without a sealer")
	}
	if _, leaked := rt2.GetEnv()["TOKEN"]; leaked {
		t.Fatalf("sealed var must not appear without a sealer, got %+v", rt2.GetEnv())
	}
	if rt2.GetEnv()["PORT"] != "8080" {
		t.Fatalf("PORT should still be injected without a sealer, got %+v", rt2.GetEnv())
	}
}

// TestNodeVersionRecording covers T-93: an agent's reported version lands in
// its Node record, but only when it changed — an unconditional write would put
// a raft entry on every agent reconnect.
func TestNodeVersionRecording(t *testing.T) {
	rs := raftstore.NewTestStore(t)
	st := rs.State()
	counter := &countingApplier{inner: rs}
	nodeID := ids.New()
	st.PutNode(&zatterav1.Node{Meta: &zatterav1.Meta{Id: nodeID}, Name: "n1", BinaryVersion: "v0.3.0"})

	s := NewSyncServer(st, counter, livestate.New(clock.NewFake()), clock.NewFake(), nil, nil)
	ctx := context.Background()

	// Unchanged version: no write.
	s.recordNodeVersion(ctx, nodeID, "v0.3.0")
	if counter.n != 0 {
		t.Errorf("unchanged version caused %d raft writes", counter.n)
	}
	// Changed version: recorded.
	s.recordNodeVersion(ctx, nodeID, "v0.4.0")
	if counter.n != 1 {
		t.Fatalf("changed version caused %d writes, want 1", counter.n)
	}
	n, _ := st.Node(nodeID)
	if n.GetBinaryVersion() != "v0.4.0" {
		t.Errorf("stored version = %q", n.GetBinaryVersion())
	}
	// Re-reporting the new version is again a no-op.
	s.recordNodeVersion(ctx, nodeID, "v0.4.0")
	if counter.n != 1 {
		t.Errorf("re-reporting wrote again: %d", counter.n)
	}
	// Empty version and unknown node are ignored.
	s.recordNodeVersion(ctx, nodeID, "")
	s.recordNodeVersion(ctx, "no-such-node", "v0.5.0")
	if counter.n != 1 {
		t.Errorf("empty/unknown input caused writes: %d", counter.n)
	}
}

// countingApplier counts raft writes.
type countingApplier struct {
	inner Applier
	n     int
}

func (c *countingApplier) Apply(ctx context.Context, cmd *clusterv1.Command) error {
	c.n++
	return c.inner.Apply(ctx, cmd)
}
func (c *countingApplier) IsLeader() bool { return c.inner.IsLeader() }

// mustVault wraps a keyring in an unsealed Vault for tests.
func mustVault(kr *secrets.Keyring) *secrets.Vault {
	v, err := secrets.NewUnsealedVault(kr)
	if err != nil {
		panic(err)
	}
	return v
}

// mustKeyring builds a keyring for tests.
func mustKeyring(dataKey []byte, version uint32) *secrets.Keyring {
	kr, err := secrets.NewKeyring(dataKey, version)
	if err != nil {
		panic(err)
	}
	return kr
}
