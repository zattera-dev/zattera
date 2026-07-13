package mesh

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
)

// loadOrCreatePrivateKey returns the node's Curve25519 WireGuard private key,
// generating and persisting it (base64, 0600) on first use. A read error other
// than not-exist is fatal — we never silently regenerate an unreadable key
// (that would orphan the node's identity in existing peers).
func loadOrCreatePrivateKey(path string) (wgtypes.Key, error) {
	b, err := os.ReadFile(path)
	switch {
	case err == nil:
		k, perr := wgtypes.ParseKey(strings.TrimSpace(string(b)))
		if perr != nil {
			return wgtypes.Key{}, fmt.Errorf("mesh: parse private key %s: %w", path, perr)
		}
		return k, nil
	case !os.IsNotExist(err):
		return wgtypes.Key{}, fmt.Errorf("mesh: read private key %s: %w", path, err)
	}

	k, err := wgtypes.GeneratePrivateKey()
	if err != nil {
		return wgtypes.Key{}, fmt.Errorf("mesh: generate private key: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return wgtypes.Key{}, fmt.Errorf("mesh: key dir: %w", err)
	}
	if err := os.WriteFile(path, []byte(k.String()), 0o600); err != nil {
		return wgtypes.Key{}, fmt.Errorf("mesh: write private key %s: %w", path, err)
	}
	return k, nil
}

// hexKey encodes a key as lowercase hex — the form the WireGuard uapi expects
// (device.IpcSet), distinct from the base64 form Key.String() emits.
func hexKey(k wgtypes.Key) string { return hex.EncodeToString(k[:]) }

// EnsureNodeKey loads or creates the WG private key at path and returns its
// public key (base64). A joining node calls this to advertise its public key in
// the join request before the device is brought up; Up reuses the same path so
// the advertised key matches the running device.
func EnsureNodeKey(path string) (string, error) {
	k, err := loadOrCreatePrivateKey(path)
	if err != nil {
		return "", err
	}
	return k.PublicKey().String(), nil
}
