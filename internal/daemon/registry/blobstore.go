// Package registry implements Zattera's embedded OCI image registry (spec
// §3.5). T-31 covers the content-addressed blob store and the OCI push
// protocol (blob uploads); manifests, tags, pull, auth and GC arrive in T-32.
//
// Blob storage is content-addressed by digest, so identical layers — shared
// across the architectures of a multi-arch image, or across repos — are stored
// exactly once. Nothing here is keyed by repository or platform; the push path
// is media-type agnostic (an image index is just bytes at PUT time, its
// validation is T-32's job).
package registry

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
)

// Sentinel errors returned by the store and upload manager. The HTTP layer
// maps these onto OCI error codes.
var (
	ErrBlobUnknown         = errors.New("registry: blob unknown")
	ErrUploadUnknown       = errors.New("registry: upload unknown")
	ErrDigestInvalid       = errors.New("registry: invalid digest")
	ErrDigestMismatch      = errors.New("registry: digest mismatch")
	ErrRangeNotSatisfiable = errors.New("registry: range not satisfiable")
)

// BlobStore keeps immutable, digest-addressed blobs on disk. Layout:
//
//	<root>/blobs/sha256/<first2>/<hex>   committed blobs
//	<root>/uploads/<id>                  in-progress upload temp files
//
// Writes go to a temp file, get digest-verified, then atomically rename into
// place — a crash mid-upload can never surface as a corrupt blob.
type BlobStore struct {
	root string
}

// NewBlobStore prepares the blob and upload directories under root (which is
// typically <data-dir>/registry).
func NewBlobStore(root string) (*BlobStore, error) {
	for _, d := range []string{
		filepath.Join(root, "blobs", "sha256"),
		filepath.Join(root, "uploads"),
	} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, fmt.Errorf("registry: prepare %s: %w", d, err)
		}
	}
	return &BlobStore{root: root}, nil
}

// parseDigest validates a `sha256:<64-hex>` digest and returns its hex part.
// Only sha256 is supported (the only algorithm we produce or accept).
func parseDigest(dgst string) (hexPart string, err error) {
	algo, h, ok := strings.Cut(dgst, ":")
	if !ok || algo != "sha256" || len(h) != 64 {
		return "", ErrDigestInvalid
	}
	if _, err := hex.DecodeString(h); err != nil {
		return "", ErrDigestInvalid
	}
	return strings.ToLower(h), nil
}

// blobPath returns the on-disk path for a validated hex digest.
func (s *BlobStore) blobPath(hexPart string) string {
	return filepath.Join(s.root, "blobs", "sha256", hexPart[:2], hexPart)
}

// Has reports whether a committed blob exists for the digest.
func (s *BlobStore) Has(dgst string) bool {
	h, err := parseDigest(dgst)
	if err != nil {
		return false
	}
	_, err = os.Stat(s.blobPath(h))
	return err == nil
}

// Stat returns the byte size of a committed blob. ErrBlobUnknown if absent.
func (s *BlobStore) Stat(dgst string) (int64, error) {
	h, err := parseDigest(dgst)
	if err != nil {
		return 0, err
	}
	fi, err := os.Stat(s.blobPath(h))
	if errors.Is(err, os.ErrNotExist) {
		return 0, ErrBlobUnknown
	}
	if err != nil {
		return 0, fmt.Errorf("registry: stat blob: %w", err)
	}
	return fi.Size(), nil
}

// Open returns a read handle to a committed blob. ErrBlobUnknown if absent.
// The caller closes the returned file.
func (s *BlobStore) Open(dgst string) (*os.File, error) {
	h, err := parseDigest(dgst)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(s.blobPath(h))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrBlobUnknown
	}
	if err != nil {
		return nil, fmt.Errorf("registry: open blob: %w", err)
	}
	return f, nil
}

// newUploadFile creates a fresh temp file under uploads/ for a streaming
// upload. The caller is responsible for eventually committing or removing it.
func (s *BlobStore) newUploadFile(id string) (*os.File, error) {
	p := filepath.Join(s.root, "uploads", id)
	f, err := os.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("registry: create upload: %w", err)
	}
	return f, nil
}

// Write streams r into the blob store, computing its digest as it goes, and
// commits it under that digest. Used for content whose digest we do not know
// in advance (manifests and image indexes, which are addressed by the sha256
// of their own bytes). Returns the resulting digest and byte size.
func (s *BlobStore) Write(r io.Reader) (string, int64, error) {
	f, err := s.newUploadFile(ids.New())
	if err != nil {
		return "", 0, err
	}
	tmp := f.Name()
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, h), r)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(tmp)
		return "", 0, fmt.Errorf("registry: write blob: %w", err)
	}
	hexPart := hex.EncodeToString(h.Sum(nil))
	if err := s.commit(tmp, hexPart); err != nil {
		_ = os.Remove(tmp)
		return "", 0, err
	}
	return "sha256:" + hexPart, n, nil
}

// Delete removes a blob by digest. A missing blob is not an error (GC calls
// this idempotently).
func (s *BlobStore) Delete(dgst string) error {
	h, err := parseDigest(dgst)
	if err != nil {
		return err
	}
	if err := os.Remove(s.blobPath(h)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("registry: delete blob: %w", err)
	}
	return nil
}

// commit moves a fully-written, digest-verified temp file into the blob store.
// If the blob already exists (a concurrent or repeat push of identical bytes)
// the temp file is discarded — content-addressing makes this a no-op dedupe.
func (s *BlobStore) commit(tmpPath, hexPart string) error {
	dst := s.blobPath(hexPart)
	if _, err := os.Stat(dst); err == nil {
		return os.Remove(tmpPath) // already have it; drop the duplicate
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("registry: blob dir: %w", err)
	}
	if err := os.Rename(tmpPath, dst); err != nil {
		return fmt.Errorf("registry: commit blob: %w", err)
	}
	return nil
}
