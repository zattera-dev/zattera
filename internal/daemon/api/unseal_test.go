package api

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/emptypb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/state"
)

const testPassphrase = "AAAA-BBBB-CCCC-DDDD"

// sealedAuthHarness returns an AuthServer whose vault is sealed but whose
// cluster key material is present — the state every node is in after a restart.
func sealedAuthHarness(t *testing.T) (*AuthServer, *secrets.Vault, *raftstore.Store) {
	t.Helper()
	rs := raftstore.NewTestStore(t)
	dataKey, err := secrets.GenerateDataKey()
	if err != nil {
		t.Fatalf("data key: %v", err)
	}
	km, err := secrets.SealDataKey(dataKey, testPassphrase, 1)
	if err != nil {
		t.Fatalf("seal data key: %v", err)
	}
	rs.State().SetClusterKeyMaterial(km)
	vault := secrets.NewVault()
	return NewAuthServer(rs.State(), rs, clock.NewFake(), "", vault), vault, rs
}

func TestUnsealInstallsTheKey(t *testing.T) {
	srv, vault, _ := sealedAuthHarness(t)
	if vault.Unsealed() {
		t.Fatal("precondition: vault should start sealed")
	}
	resp, err := srv.Unseal(context.Background(), &zatterav1.UnsealRequest{Passphrase: testPassphrase})
	if err != nil {
		t.Fatalf("unseal: %v", err)
	}
	if resp.GetAlreadyUnsealed() {
		t.Error("already_unsealed should be false for a sealed node")
	}
	if !vault.Unsealed() {
		t.Fatal("vault still sealed after a successful Unseal")
	}
	if _, err := vault.Seal([]byte("now works")); err != nil {
		t.Fatalf("sealing after unseal: %v", err)
	}
}

func TestUnsealWrongPassphrase(t *testing.T) {
	srv, vault, _ := sealedAuthHarness(t)
	_, err := srv.Unseal(context.Background(), &zatterav1.UnsealRequest{Passphrase: "not-the-passphrase"})
	if statusCode(err) != codes.PermissionDenied {
		t.Fatalf("wrong passphrase = %v, want PermissionDenied", err)
	}
	if vault.Unsealed() {
		t.Fatal("a failed unseal must not unseal the vault")
	}
}

func TestUnsealRequiresPassphraseAndMaterial(t *testing.T) {
	srv, _, _ := sealedAuthHarness(t)
	if _, err := srv.Unseal(context.Background(), &zatterav1.UnsealRequest{}); statusCode(err) != codes.InvalidArgument {
		t.Fatalf("empty passphrase = %v, want InvalidArgument", err)
	}

	// A cluster with no key material at all (never bootstrapped).
	bare := NewAuthServer(state.New(), nil, clock.NewFake(), "", secrets.NewVault())
	if _, err := bare.Unseal(context.Background(), &zatterav1.UnsealRequest{Passphrase: "x"}); statusCode(err) != codes.FailedPrecondition {
		t.Fatalf("missing key material = %v, want FailedPrecondition", err)
	}
}

func TestUnsealIsIdempotent(t *testing.T) {
	srv, _, _ := sealedAuthHarness(t)
	if _, err := srv.Unseal(context.Background(), &zatterav1.UnsealRequest{Passphrase: testPassphrase}); err != nil {
		t.Fatalf("first unseal: %v", err)
	}
	resp, err := srv.Unseal(context.Background(), &zatterav1.UnsealRequest{Passphrase: "wrong on purpose"})
	if err != nil {
		t.Fatalf("second unseal on an unsealed node: %v", err)
	}
	if !resp.GetAlreadyUnsealed() {
		t.Error("already_unsealed should be true the second time")
	}
}

// TestUnsealHookRuns covers the caching callback that makes the NEXT restart
// automatic — without it an operator would have to unseal after every reboot.
func TestUnsealHookRuns(t *testing.T) {
	srv, _, _ := sealedAuthHarness(t)
	var called int
	srv.SetUnsealHook(func() { called++ })

	if _, err := srv.Unseal(context.Background(), &zatterav1.UnsealRequest{Passphrase: testPassphrase}); err != nil {
		t.Fatalf("unseal: %v", err)
	}
	if called != 1 {
		t.Fatalf("unseal hook ran %d times, want 1", called)
	}
	// A no-op unseal must not re-persist.
	_, _ = srv.Unseal(context.Background(), &zatterav1.UnsealRequest{Passphrase: testPassphrase})
	if called != 1 {
		t.Fatalf("hook ran again on an already-unsealed node: %d", called)
	}
}

// TestWhoAmIReportsSealed is what makes the degraded state visible: without it
// a sealed node looks healthy right up until a secret operation fails.
func TestWhoAmIReportsSealed(t *testing.T) {
	srv, vault, rs := sealedAuthHarness(t)
	rs.State().PutUser(&zatterav1.User{
		Meta: &zatterav1.Meta{Id: "u1"}, Email: "a@b.c", OrgRole: zatterav1.Role_ROLE_OWNER,
	})
	ctx := withIdentity(context.Background(), Identity{UserID: "u1"})

	resp, err := srv.WhoAmI(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("whoami: %v", err)
	}
	if !resp.GetSealed() {
		t.Fatal("WhoAmI should report sealed on a sealed node")
	}

	if err := vault.Install(mustKeyring(mustDataKey(t), 1)); err != nil {
		t.Fatalf("install: %v", err)
	}
	resp, err = srv.WhoAmI(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("whoami: %v", err)
	}
	if resp.GetSealed() {
		t.Fatal("WhoAmI should report unsealed once the key is installed")
	}
}

func mustDataKey(t *testing.T) []byte {
	t.Helper()
	k, err := secrets.GenerateDataKey()
	if err != nil {
		t.Fatalf("data key: %v", err)
	}
	return k
}

// --- KeyService (T-112) ---

// TestFetchDataKeyRequiresNodeIdentity: without a verified node certificate
// there is no caller identity, so the key must not move.
func TestFetchDataKeyRequiresNodeIdentity(t *testing.T) {
	st := state.New()
	vault, _ := secrets.NewUnsealedVault(mustKeyring(mustDataKey(t), 1))
	srv := NewKeyServer(st, vault, nil)

	_, err := srv.FetchDataKey(context.Background(), &clusterv1.FetchDataKeyRequest{})
	if statusCode(err) != codes.PermissionDenied {
		t.Fatalf("no node identity = %v, want PermissionDenied", err)
	}
}

// TestFetchDataKeySealedNode: a node that has no key cannot invent one.
func TestFetchDataKeySealedNode(t *testing.T) {
	st := state.New()
	srv := NewKeyServer(st, secrets.NewVault(), nil)
	_, err := srv.FetchDataKey(context.Background(), &clusterv1.FetchDataKeyRequest{})
	// Identity is checked first, so this is still PermissionDenied — the point
	// is that it never returns a key.
	if err == nil {
		t.Fatal("a sealed key server must not return a key")
	}
}
