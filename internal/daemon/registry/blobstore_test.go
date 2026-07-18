package registry

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

func TestParseDigest(t *testing.T) {
	good := digestOf([]byte("hi"))
	if _, err := parseDigest(good); err != nil {
		t.Fatalf("good digest rejected: %v", err)
	}
	bad := []string{
		"",
		"sha256:",
		"sha256:zz",
		"md5:" + strings.Repeat("a", 64),
		strings.Repeat("a", 64), // no algorithm
		"sha256:" + strings.Repeat("g", 64),
	}
	for _, d := range bad {
		if _, err := parseDigest(d); err == nil {
			t.Errorf("expected %q to be rejected", d)
		}
	}
}

func TestBlobStoreCommitAndRead(t *testing.T) {
	store, err := NewBlobStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := NewUploads(store, clock.Real{})

	payload := []byte("multi-arch layer bytes")
	dgst := digestOf(payload)

	got, err := m.Ingest(bytes.NewReader(payload), dgst)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if got != dgst {
		t.Fatalf("digest = %s, want %s", got, dgst)
	}
	if !store.Has(dgst) {
		t.Fatal("store should have the blob")
	}
	size, err := store.Stat(dgst)
	if err != nil || size != int64(len(payload)) {
		t.Fatalf("stat = %d,%v want %d", size, err, len(payload))
	}
	f, err := store.Open(dgst)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = f.Close() }()
	buf, _ := os.ReadFile(f.Name())
	if !bytes.Equal(buf, payload) {
		t.Fatal("blob content mismatch")
	}
}

func TestBlobStoreUnknown(t *testing.T) {
	store, _ := NewBlobStore(t.TempDir())
	miss := digestOf([]byte("absent"))
	if store.Has(miss) {
		t.Fatal("Has should be false for missing blob")
	}
	if _, err := store.Stat(miss); err != ErrBlobUnknown {
		t.Fatalf("Stat err = %v, want ErrBlobUnknown", err)
	}
	if _, err := store.Open(miss); err != ErrBlobUnknown {
		t.Fatalf("Open err = %v, want ErrBlobUnknown", err)
	}
}

func TestBlobStoreDedup(t *testing.T) {
	store, _ := NewBlobStore(t.TempDir())
	m := NewUploads(store, clock.Real{})
	payload := []byte("shared across two architectures")
	dgst := digestOf(payload)

	if _, err := m.Ingest(bytes.NewReader(payload), dgst); err != nil {
		t.Fatal(err)
	}
	// A second push of identical bytes must succeed and leave exactly one blob.
	if _, err := m.Ingest(bytes.NewReader(payload), dgst); err != nil {
		t.Fatalf("second ingest: %v", err)
	}
	h, _ := parseDigest(dgst)
	if _, err := os.Stat(store.blobPath(h)); err != nil {
		t.Fatalf("blob missing after dedup: %v", err)
	}
	// No temp files leaked in uploads/.
	ups, _ := os.ReadDir(uploadsDir(store))
	if len(ups) != 0 {
		t.Fatalf("expected no leftover upload temp files, got %d", len(ups))
	}
}

// uploadsDir is a test helper reaching into the store layout.
func uploadsDir(s *BlobStore) string { return filepath.Join(s.root, "uploads") }

func TestDigestMismatchLeavesNoBlob(t *testing.T) {
	store, _ := NewBlobStore(t.TempDir())
	m := NewUploads(store, clock.Real{})

	payload := []byte("real content")
	wrong := digestOf([]byte("different content"))

	_, err := m.Ingest(bytes.NewReader(payload), wrong)
	if err != ErrDigestMismatch {
		t.Fatalf("err = %v, want ErrDigestMismatch", err)
	}
	if store.Has(wrong) || store.Has(digestOf(payload)) {
		t.Fatal("no blob should be committed on mismatch")
	}
	// The rejected upload temp file must be gone (crash-safety invariant).
	ups, _ := os.ReadDir(uploadsDir(store))
	if len(ups) != 0 {
		t.Fatalf("mismatched upload temp not cleaned: %d files", len(ups))
	}
}
