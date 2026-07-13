// Package secrets implements envelope encryption for cluster secrets
// (spec §3.13, design §2.10):
//
//	recovery passphrase ──argon2id──▶ KEK ──AES-GCM──▶ sealed data key (in Raft)
//	data key (memory only) ──AES-GCM──▶ every secret value (in Raft)
//
// Control nodes hold the plaintext data key in memory; a joining control node
// receives it from the leader over mTLS. Restore from backup requires the
// passphrase.
package secrets

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"

	"golang.org/x/crypto/argon2"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

// Argon2id parameters (RFC 9106 second recommended option; interactive-ish
// but this runs once per unseal, so err on the strong side).
const (
	argonTime      = 3
	argonMemoryKiB = 64 * 1024
	argonThreads   = 4
	keyLen         = 32
	saltLen        = 16
)

// ErrSealedDataInvalid covers wrong passphrase and corrupted material —
// AES-GCM cannot distinguish them.
var ErrSealedDataInvalid = errors.New("secrets: wrong passphrase or corrupted key material")

// Sealer encrypts/decrypts individual secret values with the cluster data
// key. The zero Sealer is unusable; obtain one via NewSealer.
type Sealer interface {
	Seal(plaintext []byte) (*zatterav1.EncryptedValue, error)
	Open(v *zatterav1.EncryptedValue) ([]byte, error)
}

type sealer struct {
	aead       cipher.AEAD
	keyVersion uint32
}

// NewSealer wraps a 32-byte data key.
func NewSealer(dataKey []byte, keyVersion uint32) (Sealer, error) {
	if len(dataKey) != keyLen {
		return nil, fmt.Errorf("secrets: data key must be %d bytes, got %d", keyLen, len(dataKey))
	}
	block, err := aes.NewCipher(dataKey)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &sealer{aead: aead, keyVersion: keyVersion}, nil
}

func (s *sealer) Seal(plaintext []byte) (*zatterav1.EncryptedValue, error) {
	nonce := make([]byte, s.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return &zatterav1.EncryptedValue{
		Nonce:      nonce,
		Ciphertext: s.aead.Seal(nil, nonce, plaintext, nil),
		KeyVersion: s.keyVersion,
	}, nil
}

func (s *sealer) Open(v *zatterav1.EncryptedValue) ([]byte, error) {
	if v.GetKeyVersion() != s.keyVersion {
		return nil, fmt.Errorf("secrets: value sealed with key version %d, sealer has %d", v.GetKeyVersion(), s.keyVersion)
	}
	pt, err := s.aead.Open(nil, v.GetNonce(), v.GetCiphertext(), nil)
	if err != nil {
		return nil, ErrSealedDataInvalid
	}
	return pt, nil
}

// GenerateDataKey returns a fresh random 32-byte cluster data key.
func GenerateDataKey() ([]byte, error) {
	key := make([]byte, keyLen)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	return key, nil
}

// GeneratePassphrase returns a human-typable recovery passphrase
// (8 groups of 4 base32 chars ≈ 160 bits).
func GeneratePassphrase() (string, error) {
	const alphabet = "abcdefghijklmnopqrstuvwxyz234567"
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	out := make([]byte, 0, 39)
	for i, b := range raw {
		if i > 0 && i%4 == 0 {
			out = append(out, '-')
		}
		out = append(out, alphabet[int(b)%len(alphabet)])
	}
	return string(out), nil
}

// SealDataKey encrypts the data key under a passphrase-derived KEK, producing
// the ClusterKeyMaterial stored in Raft.
func SealDataKey(dataKey []byte, passphrase string, keyVersion uint32) (*zatterav1.ClusterKeyMaterial, error) {
	if len(dataKey) != keyLen {
		return nil, fmt.Errorf("secrets: data key must be %d bytes", keyLen)
	}
	salt := make([]byte, saltLen)
	if _, err := rand.Read(salt); err != nil {
		return nil, err
	}
	kek := argon2.IDKey([]byte(passphrase), salt, argonTime, argonMemoryKiB, argonThreads, keyLen)
	block, err := aes.NewCipher(kek)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return &zatterav1.ClusterKeyMaterial{
		SealedDataKey:   aead.Seal(nil, nonce, dataKey, nil),
		Nonce:           nonce,
		Argon2Salt:      salt,
		Argon2Time:      argonTime,
		Argon2MemoryKib: argonMemoryKiB,
		Argon2Threads:   argonThreads,
		KeyVersion:      keyVersion,
	}, nil
}

// UnsealDataKey recovers the data key from ClusterKeyMaterial + passphrase.
// Uses the argon2 parameters recorded in the material (forward compat).
func UnsealDataKey(m *zatterav1.ClusterKeyMaterial, passphrase string) ([]byte, error) {
	kek := argon2.IDKey(
		[]byte(passphrase),
		m.GetArgon2Salt(),
		m.GetArgon2Time(),
		m.GetArgon2MemoryKib(),
		uint8(m.GetArgon2Threads()),
		keyLen,
	)
	block, err := aes.NewCipher(kek)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	dataKey, err := aead.Open(nil, m.GetNonce(), m.GetSealedDataKey(), nil)
	if err != nil {
		return nil, ErrSealedDataInvalid
	}
	return dataKey, nil
}
