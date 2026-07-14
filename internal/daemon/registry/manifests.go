package registry

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	bolt "go.etcd.io/bbolt"

	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// OCI / Docker media types we accept and produce. Manifest lists and image
// indexes are the multi-arch descriptors; a single-arch image is a plain
// manifest.
const (
	MediaTypeOCIManifest    = "application/vnd.oci.image.manifest.v1+json"
	MediaTypeOCIIndex       = "application/vnd.oci.image.index.v1+json"
	MediaTypeDockerManifest = "application/vnd.docker.distribution.manifest.v2+json"
	MediaTypeDockerList     = "application/vnd.docker.distribution.manifest.list.v2+json"
)

// Manifest-layer sentinel errors, mapped to OCI error codes by the HTTP layer.
var (
	ErrManifestUnknown   = errors.New("registry: manifest unknown")
	ErrManifestInvalid   = errors.New("registry: manifest invalid")
	ErrManifestBlobUnkn  = errors.New("registry: referenced blob unknown")
	ErrManifestChildUnkn = errors.New("registry: referenced child manifest unknown")
)

// bbolt buckets (registry/meta.db). Refcounts and the manifest graph live here,
// NOT in raft — the registry is node-local content-addressed storage.
var (
	bktTags    = []byte("tags")    // repo\x00tag        -> digest
	bktRepoMan = []byte("repoman") // repo\x00digest     -> mediaType (exists-in-repo)
	bktManMeta = []byte("manmeta") // digest             -> manMeta JSON (global graph)
	bktRefs    = []byte("refs")    // digest             -> uint64 refcount (global)
)

const nul = "\x00"

// Manifests stores image manifests and indexes (as content-addressed blobs)
// plus the tag index and a reference-counted object graph for GC.
type Manifests struct {
	db    *bolt.DB
	blobs *BlobStore
	clk   clock.Clock
}

// NewManifests opens (creating if needed) the registry metadata db under dir
// and prepares its buckets.
func NewManifests(dir string, blobs *BlobStore, clk clock.Clock) (*Manifests, error) {
	db, err := bolt.Open(filepath.Join(dir, "meta.db"), 0o644, nil)
	if err != nil {
		return nil, fmt.Errorf("registry: open meta.db: %w", err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bktTags, bktRepoMan, bktManMeta, bktRefs} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("registry: init meta.db: %w", err)
	}
	return &Manifests{db: db, blobs: blobs, clk: clk}, nil
}

// Close releases the metadata db.
func (m *Manifests) Close() error { return m.db.Close() }

// --- manifest wire structs ---

type ociPlatform struct {
	Architecture string `json:"architecture"`
	OS           string `json:"os"`
	Variant      string `json:"variant,omitempty"`
}

type ociDescriptor struct {
	MediaType string       `json:"mediaType"`
	Digest    string       `json:"digest"`
	Size      int64        `json:"size"`
	Platform  *ociPlatform `json:"platform,omitempty"`
}

type parsedManifest struct {
	MediaType string          `json:"mediaType"`
	Config    ociDescriptor   `json:"config"`
	Layers    []ociDescriptor `json:"layers"`
	Manifests []ociDescriptor `json:"manifests"` // present for indexes
}

// childDesc records one entry of an image index, kept so pulls can select the
// child manifest for a platform without re-parsing (T-88's ResolveManifest).
type childDesc struct {
	Digest    string `json:"d"`
	MediaType string `json:"m"`
	OS        string `json:"os"`
	Arch      string `json:"a"`
	Variant   string `json:"v,omitempty"`
}

// manMeta is the stored graph node for one manifest/index digest.
type manMeta struct {
	MediaType string      `json:"mediaType"`
	Index     bool        `json:"index"`
	Edges     []string    `json:"edges"`              // digests this object references
	Children  []childDesc `json:"children,omitempty"` // index children (with platforms)
}

// isIndexMediaType reports whether a media type names a manifest list / index.
func isIndexMediaType(mt string) bool {
	return mt == MediaTypeOCIIndex || mt == MediaTypeDockerList
}

// PutManifest validates and stores a manifest or image index under repo, then
// (when ref is a tag) points the tag at it. Returns the manifest digest.
//
// Validation: an image manifest requires its config and every layer blob to
// already exist; an index requires every referenced child manifest to already
// exist in this repo (children are pushed before the index in OCI push order).
func (m *Manifests) PutManifest(repo, ref, contentType string, body []byte) (string, error) {
	sum := sha256.Sum256(body)
	digest := "sha256:" + hex.EncodeToString(sum[:])

	var pm parsedManifest
	if err := json.Unmarshal(body, &pm); err != nil {
		return "", ErrManifestInvalid
	}
	mediaType := contentType
	if mediaType == "" {
		mediaType = pm.MediaType
	}
	index := isIndexMediaType(mediaType) || (mediaType == "" && len(pm.Manifests) > 0)
	if index && mediaType == "" {
		mediaType = MediaTypeOCIIndex
	} else if mediaType == "" {
		mediaType = MediaTypeOCIManifest
	}

	meta := manMeta{MediaType: mediaType, Index: index}
	if index {
		if len(pm.Manifests) == 0 {
			return "", ErrManifestInvalid
		}
		for _, c := range pm.Manifests {
			if c.Digest == "" {
				return "", ErrManifestInvalid
			}
			meta.Edges = append(meta.Edges, c.Digest)
			cd := childDesc{Digest: c.Digest, MediaType: c.MediaType}
			if c.Platform != nil {
				cd.OS, cd.Arch, cd.Variant = c.Platform.OS, c.Platform.Architecture, c.Platform.Variant
			}
			meta.Children = append(meta.Children, cd)
		}
	} else {
		if pm.Config.Digest == "" {
			return "", ErrManifestInvalid
		}
		meta.Edges = append(meta.Edges, pm.Config.Digest)
		for _, l := range pm.Layers {
			if l.Digest == "" {
				return "", ErrManifestInvalid
			}
			meta.Edges = append(meta.Edges, l.Digest)
		}
	}

	// Existence validation before we mutate anything.
	if index {
		if err := m.validateChildren(repo, meta.Edges); err != nil {
			return "", err
		}
	} else {
		for _, d := range meta.Edges {
			if !m.blobs.Has(d) {
				return "", fmt.Errorf("%w: %s", ErrManifestBlobUnkn, d)
			}
		}
	}

	// Persist the manifest body as a blob (idempotent, content-addressed).
	if _, _, err := m.blobs.Write(bytes.NewReader(body)); err != nil {
		return "", err
	}

	var toDelete []string
	err := m.db.Update(func(tx *bolt.Tx) error {
		metaBkt := tx.Bucket(bktManMeta)
		if metaBkt.Get([]byte(digest)) == nil {
			enc, _ := json.Marshal(meta)
			if err := metaBkt.Put([]byte(digest), enc); err != nil {
				return err
			}
			for _, e := range meta.Edges {
				refInc(tx, e)
			}
		}
		if err := tx.Bucket(bktRepoMan).Put([]byte(repo+nul+digest), []byte(mediaType)); err != nil {
			return err
		}
		// If ref is a tag (not a digest and non-empty), (re)point it here.
		// Pushes addressed by digest (children of an index) carry no tag.
		if _, derr := parseDigest(ref); ref != "" && derr != nil {
			setTag(tx, repo, ref, digest, &toDelete)
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("registry: put manifest: %w", err)
	}
	m.sweepBlobs(toDelete)
	return digest, nil
}

// validateChildren ensures every child digest exists as a manifest in repo.
func (m *Manifests) validateChildren(repo string, children []string) error {
	return m.db.View(func(tx *bolt.Tx) error {
		rb := tx.Bucket(bktRepoMan)
		for _, d := range children {
			if rb.Get([]byte(repo+nul+d)) == nil {
				return fmt.Errorf("%w: %s", ErrManifestChildUnkn, d)
			}
		}
		return nil
	})
}

// Resolve turns a tag or digest reference into a concrete manifest digest that
// exists in repo. ErrManifestUnknown when absent.
func (m *Manifests) Resolve(repo, ref string) (string, error) {
	var digest string
	err := m.db.View(func(tx *bolt.Tx) error {
		digest = resolveRef(tx, repo, ref)
		if digest == "" || tx.Bucket(bktRepoMan).Get([]byte(repo+nul+digest)) == nil {
			return ErrManifestUnknown
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return digest, nil
}

// GetManifest returns the stored bytes, media type and digest for a reference.
func (m *Manifests) GetManifest(repo, ref string) (body []byte, mediaType, digest string, err error) {
	err = m.db.View(func(tx *bolt.Tx) error {
		digest = resolveRef(tx, repo, ref)
		if digest == "" {
			return ErrManifestUnknown
		}
		mt := tx.Bucket(bktRepoMan).Get([]byte(repo + nul + digest))
		if mt == nil {
			return ErrManifestUnknown
		}
		mediaType = string(mt)
		return nil
	})
	if err != nil {
		return nil, "", "", err
	}
	f, err := m.blobs.Open(digest)
	if err != nil {
		return nil, "", "", ErrManifestUnknown
	}
	defer func() { _ = f.Close() }()
	body, err = io.ReadAll(f)
	if err != nil {
		return nil, "", "", err
	}
	return body, mediaType, digest, nil
}

// ResolveManifest maps (repo, ref, platform) to the concrete manifest digest a
// client should pull for that platform: for an index it returns the matching
// child; for a plain manifest (or empty platform) it returns the reference
// itself. This lets the control plane learn a release's arch without a docker
// client (T-88).
func (m *Manifests) ResolveManifest(repo, ref, platform string) (digest, mediaType string, err error) {
	err = m.db.View(func(tx *bolt.Tx) error {
		d := resolveRef(tx, repo, ref)
		if d == "" || tx.Bucket(bktRepoMan).Get([]byte(repo+nul+d)) == nil {
			return ErrManifestUnknown
		}
		meta, ok := loadMeta(tx, d)
		if !ok {
			return ErrManifestUnknown
		}
		if !meta.Index || platform == "" {
			digest, mediaType = d, meta.MediaType
			return nil
		}
		for _, c := range meta.Children {
			if platformMatches(platform, c) {
				digest, mediaType = c.Digest, c.MediaType
				return nil
			}
		}
		return fmt.Errorf("%w: no child for platform %s", ErrManifestUnknown, platform)
	})
	return digest, mediaType, err
}

// Platforms lists the OCI platform strings a reference can run on: an index's
// child platforms, or a single-element list for a plain manifest (empty when
// unknown). Used by T-88 to populate Release.platforms.
func (m *Manifests) Platforms(repo, ref string) ([]string, error) {
	var out []string
	err := m.db.View(func(tx *bolt.Tx) error {
		d := resolveRef(tx, repo, ref)
		if d == "" || tx.Bucket(bktRepoMan).Get([]byte(repo+nul+d)) == nil {
			return ErrManifestUnknown
		}
		meta, ok := loadMeta(tx, d)
		if !ok {
			return ErrManifestUnknown
		}
		if !meta.Index {
			return nil
		}
		for _, c := range meta.Children {
			if c.OS == "" || c.Arch == "" {
				continue
			}
			p := c.OS + "/" + c.Arch
			if c.Variant != "" {
				p += "/" + c.Variant
			}
			out = append(out, p)
		}
		return nil
	})
	return out, err
}

// Tags lists the tags defined in repo (sorted by bbolt key order).
func (m *Manifests) Tags(repo string) ([]string, error) {
	var tags []string
	prefix := []byte(repo + nul)
	err := m.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bktTags).Cursor()
		for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			tags = append(tags, string(k[len(prefix):]))
		}
		return nil
	})
	return tags, err
}

// platformMatches reports whether an index child satisfies a wanted "os/arch"
// or "os/arch/variant" platform string.
func platformMatches(want string, c childDesc) bool {
	parts := strings.Split(want, "/")
	if len(parts) < 2 {
		return false
	}
	if c.OS != parts[0] || c.Arch != parts[1] {
		return false
	}
	if len(parts) >= 3 && c.Variant != "" && c.Variant != parts[2] {
		return false
	}
	return true
}

// resolveRef returns the digest for a reference: the ref itself if it is a
// digest, otherwise the tag's target ("" if the tag is unknown).
func resolveRef(tx *bolt.Tx, repo, ref string) string {
	if _, err := parseDigest(ref); err == nil {
		return ref
	}
	if v := tx.Bucket(bktTags).Get([]byte(repo + nul + ref)); v != nil {
		return string(v)
	}
	return ""
}

func loadMeta(tx *bolt.Tx, digest string) (manMeta, bool) {
	raw := tx.Bucket(bktManMeta).Get([]byte(digest))
	if raw == nil {
		return manMeta{}, false
	}
	var meta manMeta
	if err := json.Unmarshal(raw, &meta); err != nil {
		return manMeta{}, false
	}
	return meta, true
}

// --- refcount primitives (must run inside a bbolt Update tx) ---

func refGet(tx *bolt.Tx, digest string) uint64 {
	v := tx.Bucket(bktRefs).Get([]byte(digest))
	if len(v) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(v)
}

func refPut(tx *bolt.Tx, digest string, n uint64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], n)
	_ = tx.Bucket(bktRefs).Put([]byte(digest), buf[:])
}

func refInc(tx *bolt.Tx, digest string) {
	refPut(tx, digest, refGet(tx, digest)+1)
}

// sweepBlobs deletes blob files whose refcount reached zero. Called AFTER the
// bbolt transaction commits so a rolled-back txn never orphans deletes.
func (m *Manifests) sweepBlobs(digests []string) {
	for _, d := range digests {
		_ = m.blobs.Delete(d)
	}
}
