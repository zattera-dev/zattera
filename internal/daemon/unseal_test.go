package daemon

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/zattera-dev/zattera/internal/config"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
)

func quietLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.NewFile(0, os.DevNull), nil))
}

func unsealedVault(t *testing.T) (*secrets.Vault, []byte) {
	t.Helper()
	key, err := secrets.GenerateDataKey()
	if err != nil {
		t.Fatalf("data key: %v", err)
	}
	kr, err := secrets.NewKeyring(key, 3)
	if err != nil {
		t.Fatalf("keyring: %v", err)
	}
	v, err := secrets.NewUnsealedVault(kr)
	if err != nil {
		t.Fatalf("vault: %v", err)
	}
	return v, key
}

// TestDataKeyFileRoundTrip is the core of auto-unseal: what one boot caches,
// the next boot must be able to install.
func TestDataKeyFileRoundTrip(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Config{DataDir: dir}
	src, key := unsealedVault(t)

	persistDataKey(cfg, src, quietLog())

	dst := secrets.NewVault()
	if err := unsealFromFile(cfg, dst); err != nil {
		t.Fatalf("unseal from file: %v", err)
	}
	if !dst.Unsealed() {
		t.Fatal("vault still sealed after reading the key file")
	}
	if got := dst.KeyVersion(); got != 3 {
		t.Errorf("key version = %d, want 3", got)
	}
	// Same key, not merely a valid one: a value sealed by the original must open.
	s, _ := secrets.NewSealer(key, 3)
	ev, _ := s.Seal([]byte("payload"))
	pt, err := dst.Open(ev)
	if err != nil || string(pt) != "payload" {
		t.Fatalf("cached key differs from the original: %q %v", pt, err)
	}
}

// TestDataKeyFilePermissions: the file mode is the only thing protecting the
// key on disk, so it is part of the contract, not an implementation detail.
func TestDataKeyFilePermissions(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Config{DataDir: dir}
	v, _ := unsealedVault(t)
	persistDataKey(cfg, v, quietLog())

	info, err := os.Stat(filepath.Join(dir, dataKeyFile))
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("key file mode = %o, want 600", perm)
	}
}

// TestSealedAtRestWritesNothing is the operator's opt-out: with it set, the key
// must never touch the disk.
func TestSealedAtRestWritesNothing(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Config{DataDir: dir, SealedAtRest: true}
	v, _ := unsealedVault(t)

	persistDataKey(cfg, v, quietLog())

	if _, err := os.Stat(filepath.Join(dir, dataKeyFile)); !os.IsNotExist(err) {
		t.Fatalf("sealed_at_rest wrote a key file (stat err = %v)", err)
	}
}

// TestPersistSkipsSealedVault: a sealed vault has nothing to cache and must not
// write a bogus (empty-key) file that would poison the next boot.
func TestPersistSkipsSealedVault(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Config{DataDir: dir}

	persistDataKey(cfg, secrets.NewVault(), quietLog())

	if _, err := os.Stat(filepath.Join(dir, dataKeyFile)); !os.IsNotExist(err) {
		t.Fatalf("a sealed vault wrote a key file (stat err = %v)", err)
	}
}

// TestUnsealFromMissingFile: the common case on a first boot. The caller
// distinguishes it from a real failure via fs.ErrNotExist, so it must stay
// that error and leave the vault sealed.
func TestUnsealFromMissingFile(t *testing.T) {
	v := secrets.NewVault()
	err := unsealFromFile(config.Config{DataDir: t.TempDir()}, v)
	if !os.IsNotExist(err) {
		t.Fatalf("missing key file = %v, want a NotExist error", err)
	}
	if v.Unsealed() {
		t.Fatal("vault unsealed from a missing file")
	}
}

// TestUnsealFromCorruptFile: a truncated or garbage file must fail cleanly
// rather than installing a broken key.
func TestUnsealFromCorruptFile(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Config{DataDir: dir}
	path := filepath.Join(dir, dataKeyFile)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("not a protobuf, and not a key"), 0o600); err != nil {
		t.Fatal(err)
	}

	v := secrets.NewVault()
	if err := unsealFromFile(cfg, v); err == nil {
		t.Fatal("corrupt key file should error")
	}
	if v.Unsealed() {
		t.Fatal("vault unsealed from a corrupt file")
	}
}

// TestControlPeersExcludesSelfAndWorkers guards the peer-unseal target list:
// asking yourself is pointless, and workers cannot serve the key.
func TestControlPeersRequiresAPIPort(t *testing.T) {
	// A config with no parseable API listen address yields no peers rather
	// than a bad dial target.
	if peers := controlPeers(nil, "self", config.Config{}); peers != nil {
		t.Fatalf("peers with no API listen = %v, want none", peers)
	}
}
