package secrets

import (
	"errors"
	"sync"
	"testing"
)

func testKeyring(t *testing.T) *Keyring {
	t.Helper()
	key, err := GenerateDataKey()
	if err != nil {
		t.Fatalf("data key: %v", err)
	}
	kr, err := NewKeyring(key, 1)
	if err != nil {
		t.Fatalf("keyring: %v", err)
	}
	return kr
}

func TestVaultSealedRefusesInsteadOfPanicking(t *testing.T) {
	v := NewVault()
	if v.Unsealed() {
		t.Fatal("a fresh vault must be sealed")
	}
	if _, err := v.Seal([]byte("x")); !errors.Is(err, ErrSealed) {
		t.Fatalf("Seal on a sealed vault = %v, want ErrSealed", err)
	}
	if _, err := v.Open(nil); !errors.Is(err, ErrSealed) {
		t.Fatalf("Open on a sealed vault = %v, want ErrSealed", err)
	}
	if v.DataKey() != nil {
		t.Error("sealed vault leaked a data key")
	}
	if v.KeyVersion() != 0 {
		t.Error("sealed vault reported a key version")
	}
}

func TestVaultInstallEnablesRoundTrip(t *testing.T) {
	v := NewVault()
	if err := v.Install(testKeyring(t)); err != nil {
		t.Fatalf("install: %v", err)
	}
	if !v.Unsealed() {
		t.Fatal("vault should be unsealed after Install")
	}
	ev, err := v.Seal([]byte("s3cr3t"))
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	pt, err := v.Open(ev)
	if err != nil || string(pt) != "s3cr3t" {
		t.Fatalf("open = %q, %v", pt, err)
	}
}

// TestVaultInstallIsIdempotent pins the concurrency contract: the data key is
// cluster-wide and immutable, so a second unseal (an operator racing the
// startup auto-unseal) must not swap the sealer under in-flight work.
func TestVaultInstallIsIdempotent(t *testing.T) {
	v := NewVault()
	first := testKeyring(t)
	if err := v.Install(first); err != nil {
		t.Fatalf("install: %v", err)
	}
	ev, _ := v.Seal([]byte("before"))

	if err := v.Install(testKeyring(t)); err != nil {
		t.Fatalf("second install: %v", err)
	}
	// Values sealed before the second install must still open — proof the
	// original key is still in force.
	pt, err := v.Open(ev)
	if err != nil || string(pt) != "before" {
		t.Fatalf("second Install replaced the key: open = %q, %v", pt, err)
	}
}

func TestVaultInstallNilIsAnError(t *testing.T) {
	if err := NewVault().Install(nil); err == nil {
		t.Fatal("Install(nil) should error")
	}
}

// TestVaultOnUnsealHooks covers the recovery path for subsystems that memoize
// something derived from the key (the GitHub App loader).
func TestVaultOnUnsealHooks(t *testing.T) {
	v := NewVault()
	var pending, immediate int

	v.OnUnseal(func() { pending++ })
	if pending != 0 {
		t.Fatal("hook ran while still sealed")
	}
	if err := v.Install(testKeyring(t)); err != nil {
		t.Fatalf("install: %v", err)
	}
	if pending != 1 {
		t.Fatalf("pending hook ran %d times, want 1", pending)
	}

	// Registering after the fact runs immediately.
	v.OnUnseal(func() { immediate++ })
	if immediate != 1 {
		t.Fatalf("late hook ran %d times, want 1", immediate)
	}

	// A second Install is a no-op and must not re-run hooks.
	_ = v.Install(testKeyring(t))
	if pending != 1 {
		t.Fatalf("hooks re-ran on a redundant install: %d", pending)
	}
}

func TestVaultUnsealWithPassphrase(t *testing.T) {
	dataKey, _ := GenerateDataKey()
	km, err := SealDataKey(dataKey, "correct horse battery", 7)
	if err != nil {
		t.Fatalf("seal data key: %v", err)
	}

	v := NewVault()
	if err := v.UnsealWithPassphrase(km, "wrong passphrase"); !errors.Is(err, ErrSealedDataInvalid) {
		t.Fatalf("wrong passphrase = %v, want ErrSealedDataInvalid", err)
	}
	if v.Unsealed() {
		t.Fatal("a failed unseal must leave the vault sealed")
	}

	if err := v.UnsealWithPassphrase(km, "correct horse battery"); err != nil {
		t.Fatalf("unseal: %v", err)
	}
	if !v.Unsealed() {
		t.Fatal("vault should be unsealed")
	}
	if v.KeyVersion() != 7 {
		t.Errorf("key version = %d, want 7", v.KeyVersion())
	}

	// The recovered key must be the original, not merely a valid one.
	s, _ := NewSealer(dataKey, 7)
	ev, _ := s.Seal([]byte("round trip"))
	pt, err := v.Open(ev)
	if err != nil || string(pt) != "round trip" {
		t.Fatalf("recovered key differs from the original: %q %v", pt, err)
	}
}

func TestVaultUnsealWithNilMaterial(t *testing.T) {
	if err := NewVault().UnsealWithPassphrase(nil, "x"); err == nil {
		t.Fatal("nil key material should error")
	}
}

// TestVaultConcurrent exercises the lock under -race: readers must never see a
// half-installed vault.
func TestVaultConcurrent(t *testing.T) {
	v := NewVault()
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if v.Unsealed() {
					if _, err := v.Seal([]byte("x")); err != nil {
						t.Errorf("seal after Unsealed()==true: %v", err)
						return
					}
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = v.Install(testKeyring(t))
	}()
	wg.Wait()
}

// TestNilVaultIsSealed pins the fail-safe: a call site that misses a vault must
// degrade to "cannot touch secrets" rather than crashing the node. A nil deref
// here would take down a control plane over a missing wire-up.
func TestNilVaultIsSealed(t *testing.T) {
	var v *Vault
	if v.Unsealed() {
		t.Error("nil vault reported unsealed")
	}
	if _, err := v.Seal([]byte("x")); !errors.Is(err, ErrSealed) {
		t.Errorf("nil Seal = %v, want ErrSealed", err)
	}
	if _, err := v.Open(nil); !errors.Is(err, ErrSealed) {
		t.Errorf("nil Open = %v, want ErrSealed", err)
	}
	if v.DataKey() != nil || v.KeyVersion() != 0 {
		t.Error("nil vault exposed key material")
	}
	v.OnUnseal(func() { t.Error("nil vault ran an unseal hook") })
	if err := v.Install(testKeyring(t)); err == nil {
		t.Error("Install into a nil vault should error")
	}
}
