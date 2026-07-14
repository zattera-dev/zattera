package registry

import (
	"bytes"

	bolt "go.etcd.io/bbolt"
)

// GC in the embedded registry is reference-counted at the digest level. Every
// edge — a tag pointing at a manifest, an index pointing at a child manifest, a
// manifest pointing at its config and layers — is one reference. When a
// digest's refcount reaches zero its blob is deleted and, if it is itself a
// manifest/index, its outgoing edges are released too (a recursive cascade).
// This means untagging a multi-arch image frees every architecture's child
// manifests and their unshared layers, while layers still referenced by another
// image survive.

// setTag points repo:tag at digest, adjusting refcounts. Retagging releases the
// previous target (which may cascade). Digests to delete post-commit are
// appended to toDelete. Must run inside a bbolt Update transaction.
func setTag(tx *bolt.Tx, repo, tag, digest string, toDelete *[]string) {
	key := []byte(repo + nul + tag)
	old := tx.Bucket(bktTags).Get(key)
	if old != nil && string(old) == digest {
		return // no change
	}
	oldStr := ""
	if old != nil {
		oldStr = string(old)
	}
	_ = tx.Bucket(bktTags).Put(key, []byte(digest))
	refInc(tx, digest)
	if oldStr != "" {
		refDec(tx, oldStr, toDelete)
	}
}

// refDec decrements a digest's refcount. On reaching zero it removes the
// digest's metadata and repo listings, recurses into its edges, and schedules
// its blob for deletion. Must run inside a bbolt Update transaction.
func refDec(tx *bolt.Tx, digest string, toDelete *[]string) {
	c := refGet(tx, digest)
	if c > 1 {
		refPut(tx, digest, c-1)
		return
	}
	// Reaching zero: tear the node down.
	_ = tx.Bucket(bktRefs).Delete([]byte(digest))
	if meta, ok := loadMeta(tx, digest); ok {
		_ = tx.Bucket(bktManMeta).Delete([]byte(digest))
		removeRepoListings(tx, digest)
		for _, e := range meta.Edges {
			refDec(tx, e, toDelete)
		}
	}
	*toDelete = append(*toDelete, digest)
}

// removeRepoListings deletes every repoman entry (across repos) for a digest
// whose global refcount hit zero.
func removeRepoListings(tx *bolt.Tx, digest string) {
	suffix := []byte(nul + digest)
	rb := tx.Bucket(bktRepoMan)
	var keys [][]byte
	c := rb.Cursor()
	for k, _ := c.First(); k != nil; k, _ = c.Next() {
		if bytes.HasSuffix(k, suffix) {
			keys = append(keys, append([]byte(nil), k...))
		}
	}
	for _, k := range keys {
		_ = rb.Delete(k)
	}
}

// UntagAndSweep removes a tag and reclaims anything it solely kept alive. This
// is the retention hook (T-38): drop an old release's image tag and let GC free
// its blobs.
func (m *Manifests) UntagAndSweep(repo, tag string) error {
	var toDelete []string
	err := m.db.Update(func(tx *bolt.Tx) error {
		key := []byte(repo + nul + tag)
		old := tx.Bucket(bktTags).Get(key)
		if old == nil {
			return nil // idempotent: nothing tagged
		}
		_ = tx.Bucket(bktTags).Delete(key)
		refDec(tx, string(old), &toDelete)
		return nil
	})
	if err != nil {
		return err
	}
	m.sweepBlobs(toDelete)
	return nil
}

// DeleteManifest removes a manifest by digest from repo: it untags every tag in
// the repo pointing at it and releases those references (cascading GC). Absent
// manifests return ErrManifestUnknown.
func (m *Manifests) DeleteManifest(repo, digest string) error {
	var toDelete []string
	err := m.db.Update(func(tx *bolt.Tx) error {
		if tx.Bucket(bktRepoMan).Get([]byte(repo+nul+digest)) == nil {
			return ErrManifestUnknown
		}
		prefix := []byte(repo + nul)
		var keys [][]byte
		c := tx.Bucket(bktTags).Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			if string(v) == digest {
				keys = append(keys, append([]byte(nil), k...))
			}
		}
		for _, k := range keys {
			_ = tx.Bucket(bktTags).Delete(k)
			refDec(tx, digest, &toDelete)
		}
		return nil
	})
	if err != nil {
		return err
	}
	m.sweepBlobs(toDelete)
	return nil
}
