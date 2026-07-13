package secrets

import (
	"bytes"
	"errors"
	"testing"
)

func TestSealOpenRoundTrip(t *testing.T) {
	key, err := GenerateDataKey()
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewSealer(key, 1)
	if err != nil {
		t.Fatal(err)
	}

	plaintext := []byte("postgres://user:secret@db:5432/app")
	v, err := s.Seal(plaintext)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(v.GetCiphertext(), []byte("secret")) {
		t.Fatal("ciphertext leaks plaintext")
	}
	got, err := s.Open(v)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatalf("round trip mismatch: %q", got)
	}

	// Two seals of the same plaintext must differ (fresh nonces).
	v2, _ := s.Seal(plaintext)
	if bytes.Equal(v.GetCiphertext(), v2.GetCiphertext()) || bytes.Equal(v.GetNonce(), v2.GetNonce()) {
		t.Fatal("nonce reuse")
	}
}

func TestOpenRejectsTamperingAndWrongKey(t *testing.T) {
	key, _ := GenerateDataKey()
	s, _ := NewSealer(key, 1)
	v, _ := s.Seal([]byte("data"))

	v.Ciphertext[0] ^= 0xff
	if _, err := s.Open(v); !errors.Is(err, ErrSealedDataInvalid) {
		t.Fatalf("tampered ciphertext accepted: %v", err)
	}
	v.Ciphertext[0] ^= 0xff

	otherKey, _ := GenerateDataKey()
	s2, _ := NewSealer(otherKey, 1)
	if _, err := s2.Open(v); !errors.Is(err, ErrSealedDataInvalid) {
		t.Fatalf("wrong key accepted: %v", err)
	}

	// Wrong key version is rejected before decryption.
	s3, _ := NewSealer(key, 2)
	if _, err := s3.Open(v); err == nil {
		t.Fatal("wrong key version accepted")
	}
}

func TestDataKeySealUnseal(t *testing.T) {
	dataKey, _ := GenerateDataKey()
	pass, err := GeneratePassphrase()
	if err != nil {
		t.Fatal(err)
	}
	if len(pass) < 30 {
		t.Fatalf("passphrase too short: %q", pass)
	}

	material, err := SealDataKey(dataKey, pass, 1)
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnsealDataKey(material, pass)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, dataKey) {
		t.Fatal("data key round trip mismatch")
	}

	if _, err := UnsealDataKey(material, "wrong-passphrase"); !errors.Is(err, ErrSealedDataInvalid) {
		t.Fatalf("wrong passphrase accepted: %v", err)
	}
}
