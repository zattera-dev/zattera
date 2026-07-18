package secrets

import (
	"errors"
	"sync"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

// ErrSealed is returned by a Vault that does not (yet) hold the cluster data
// key. Callers that can degrade gracefully should check Unsealed() first and
// report a clear FailedPrecondition rather than surfacing this raw.
var ErrSealed = errors.New("secrets: cluster key is sealed")

// Vault holds the cluster keyring behind a stable, always-non-nil handle.
//
// It exists because the data key is not available at every startup: only the
// process that bootstraps a cluster (or first-joins as a control node) is
// handed one, so a restarted node comes up sealed and must acquire the key
// later — from an operator's passphrase (`zattera unseal`) or from a control
// peer (T-112). Passing a nil Sealer around made that unrepresentable: every
// consumer had to nil-check, several construction sites skipped whole
// subsystems, and nothing could recover without a restart.
//
// Vault implements Sealer, so a sealed cluster returns ErrSealed from Seal and
// Open instead of panicking on a nil interface. It is safe for concurrent use.
type Vault struct {
	mu      sync.RWMutex
	keyring *Keyring
	sealer  Sealer
	// onUnseal runs once, after the keyring is installed, for subsystems that
	// cached something derived from the key.
	onUnseal []func()
}

// NewVault returns a sealed Vault.
func NewVault() *Vault { return &Vault{} }

// NewUnsealedVault returns a Vault already holding kr. A nil keyring yields a
// sealed Vault, which is what a restarted node gets from Bootstrap.
func NewUnsealedVault(kr *Keyring) (*Vault, error) {
	v := NewVault()
	if kr == nil {
		return v, nil
	}
	return v, v.Install(kr)
}

// Install stores the keyring and derives a sealer. Installing over an already
// unsealed vault is a no-op: the data key is cluster-wide and immutable, so a
// second unseal (an operator racing the auto-unseal path, say) must not swap
// the sealer out from under in-flight work.
func (v *Vault) Install(kr *Keyring) error {
	if v == nil {
		return errors.New("secrets: install into a nil vault")
	}
	if kr == nil {
		return errors.New("secrets: install nil keyring")
	}
	s, err := kr.Sealer()
	if err != nil {
		return err
	}
	v.mu.Lock()
	if v.sealer != nil {
		v.mu.Unlock()
		return nil
	}
	v.keyring, v.sealer = kr, s
	hooks := v.onUnseal
	v.onUnseal = nil
	v.mu.Unlock()

	// Outside the lock: a hook may call back into the vault.
	for _, fn := range hooks {
		fn()
	}
	return nil
}

// Unsealed reports whether the cluster data key is available. A nil Vault
// reports sealed rather than panicking: every method here is nil-safe on
// purpose, so a call site that misses a vault degrades to "cannot touch
// secrets" instead of crashing the node.
func (v *Vault) Unsealed() bool {
	if v == nil {
		return false
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.sealer != nil
}

// OnUnseal registers fn to run when the vault is unsealed. If it already is,
// fn runs immediately. Used by subsystems that memoize something derived from
// the key and would otherwise stay broken after a late unseal.
func (v *Vault) OnUnseal(fn func()) {
	if v == nil {
		return
	}
	v.mu.Lock()
	if v.sealer != nil {
		v.mu.Unlock()
		fn()
		return
	}
	v.onUnseal = append(v.onUnseal, fn)
	v.mu.Unlock()
}

// Seal implements Sealer.
func (v *Vault) Seal(plaintext []byte) (*zatterav1.EncryptedValue, error) {
	if v == nil {
		return nil, ErrSealed
	}
	v.mu.RLock()
	s := v.sealer
	v.mu.RUnlock()
	if s == nil {
		return nil, ErrSealed
	}
	return s.Seal(plaintext)
}

// Open implements Sealer.
func (v *Vault) Open(val *zatterav1.EncryptedValue) ([]byte, error) {
	if v == nil {
		return nil, ErrSealed
	}
	v.mu.RLock()
	s := v.sealer
	v.mu.RUnlock()
	if s == nil {
		return nil, ErrSealed
	}
	return s.Open(val)
}

// DataKey returns a copy of the plaintext data key, or nil when sealed. Handle
// with care: it is handed to joining control nodes over mTLS and to nothing
// else.
func (v *Vault) DataKey() []byte {
	if v == nil {
		return nil
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.keyring == nil {
		return nil
	}
	return v.keyring.DataKey()
}

// KeyVersion returns the data key version, or 0 when sealed.
func (v *Vault) KeyVersion() uint32 {
	if v == nil {
		return 0
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.keyring == nil {
		return 0
	}
	return v.keyring.KeyVersion()
}

// UnsealWithPassphrase derives the data key from the cluster key material and
// installs it. Returns ErrSealedDataInvalid for a wrong passphrase.
func (v *Vault) UnsealWithPassphrase(m *zatterav1.ClusterKeyMaterial, passphrase string) error {
	if m == nil {
		return errors.New("secrets: no cluster key material")
	}
	dataKey, err := UnsealDataKey(m, passphrase)
	if err != nil {
		return err
	}
	kr, err := NewKeyring(dataKey, m.GetKeyVersion())
	if err != nil {
		return err
	}
	return v.Install(kr)
}
