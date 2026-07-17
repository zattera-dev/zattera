package volumes

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
)

// nonceSize is the AES-GCM nonce length; a fresh random nonce is prepended to
// every sealed object (its "header"). Deriving the nonce from the plaintext hash
// is forbidden — identical chunks must still get distinct nonces.
const nonceSize = 12

// seal encrypts plaintext with AES-256-GCM under key, returning nonce||ciphertext.
func seal(key, plaintext []byte) ([]byte, error) {
	aead, err := newAEAD(key)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, nonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("volumes: nonce: %w", err)
	}
	// Seal appends the ciphertext to nonce, so the nonce is the object header.
	return aead.Seal(nonce, nonce, plaintext, nil), nil
}

// open reverses seal.
func open(key, blob []byte) ([]byte, error) {
	aead, err := newAEAD(key)
	if err != nil {
		return nil, err
	}
	if len(blob) < nonceSize {
		return nil, errors.New("volumes: sealed object too short")
	}
	nonce, ciphertext := blob[:nonceSize], blob[nonceSize:]
	pt, err := aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("volumes: decrypt: %w", err)
	}
	return pt, nil
}

func newAEAD(key []byte) (cipher.AEAD, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("volumes: data key must be 32 bytes, got %d", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
