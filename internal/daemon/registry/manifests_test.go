package registry

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

func newTestRegistry(t *testing.T) *Registry {
	t.Helper()
	rg, err := New(t.TempDir(), clock.NewFake(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rg.Close() })
	return rg
}

// pushBlob writes content to the blob store and returns its digest.
func pushBlob(t *testing.T, rg *Registry, content string) string {
	t.Helper()
	d, _, err := rg.Blobs.Write(strings.NewReader(content))
	if err != nil {
		t.Fatalf("push blob: %v", err)
	}
	return d
}

func imageManifest(config string, layers ...string) []byte {
	descs := make([]map[string]any, 0, len(layers))
	for _, l := range layers {
		descs = append(descs, map[string]any{
			"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
			"digest":    l,
			"size":      1,
		})
	}
	b, _ := json.Marshal(map[string]any{
		"schemaVersion": 2,
		"mediaType":     MediaTypeOCIManifest,
		"config":        map[string]any{"mediaType": "application/vnd.oci.image.config.v1+json", "digest": config, "size": 1},
		"layers":        descs,
	})
	return b
}

type childRef struct {
	digest, os, arch string
}

func indexManifest(children ...childRef) []byte {
	descs := make([]map[string]any, 0, len(children))
	for _, c := range children {
		descs = append(descs, map[string]any{
			"mediaType": MediaTypeOCIManifest,
			"digest":    c.digest,
			"size":      1,
			"platform":  map[string]any{"os": c.os, "architecture": c.arch},
		})
	}
	b, _ := json.Marshal(map[string]any{
		"schemaVersion": 2,
		"mediaType":     MediaTypeOCIIndex,
		"manifests":     descs,
	})
	return b
}

func TestPutGetImageManifest(t *testing.T) {
	rg := newTestRegistry(t)
	cfg := pushBlob(t, rg, "config-json")
	layer := pushBlob(t, rg, "layer-bytes")
	body := imageManifest(cfg, layer)

	digest, err := rg.Manifests.PutManifest("proj/app", "v1", MediaTypeOCIManifest, body)
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	// GET by tag returns the same bytes, media type and digest.
	got, mt, d, err := rg.Manifests.GetManifest("proj/app", "v1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got) != string(body) || mt != MediaTypeOCIManifest || d != digest {
		t.Fatalf("get mismatch: mt=%s d=%s", mt, d)
	}
	// GET by digest works too.
	if _, _, _, err := rg.Manifests.GetManifest("proj/app", digest); err != nil {
		t.Fatalf("get by digest: %v", err)
	}
	// Tags list.
	tags, _ := rg.Manifests.Tags("proj/app")
	if len(tags) != 1 || tags[0] != "v1" {
		t.Fatalf("tags = %v", tags)
	}
}

func TestPutManifestMissingBlob(t *testing.T) {
	rg := newTestRegistry(t)
	// Reference a config/layer that were never uploaded.
	body := imageManifest("sha256:"+strings.Repeat("a", 64), "sha256:"+strings.Repeat("b", 64))
	_, err := rg.Manifests.PutManifest("proj/app", "v1", MediaTypeOCIManifest, body)
	if err == nil || !strings.Contains(err.Error(), "blob unknown") {
		t.Fatalf("expected blob-unknown error, got %v", err)
	}
}

func TestGetUnknownManifest(t *testing.T) {
	rg := newTestRegistry(t)
	if _, _, _, err := rg.Manifests.GetManifest("proj/app", "nope"); err != ErrManifestUnknown {
		t.Fatalf("err = %v, want ErrManifestUnknown", err)
	}
}

func TestSharedLayerSurvivesUntag(t *testing.T) {
	rg := newTestRegistry(t)
	shared := pushBlob(t, rg, "shared-layer")
	cfgA := pushBlob(t, rg, "config-a")
	cfgB := pushBlob(t, rg, "config-b")

	dA, err := rg.Manifests.PutManifest("proj/app", "a", MediaTypeOCIManifest, imageManifest(cfgA, shared))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rg.Manifests.PutManifest("proj/app", "b", MediaTypeOCIManifest, imageManifest(cfgB, shared)); err != nil {
		t.Fatal(err)
	}

	// Removing tag a frees config-a and manifest a, but the shared layer and
	// config-b remain (still referenced by b).
	if err := rg.Manifests.UntagAndSweep("proj/app", "a"); err != nil {
		t.Fatal(err)
	}
	if rg.Blobs.Has(cfgA) {
		t.Error("config-a should be swept")
	}
	if rg.Blobs.Has(dA) {
		t.Error("manifest a should be swept")
	}
	if !rg.Blobs.Has(shared) {
		t.Error("shared layer must survive while b references it")
	}
	if !rg.Blobs.Has(cfgB) {
		t.Error("config-b must survive")
	}

	// Removing b now frees everything.
	if err := rg.Manifests.UntagAndSweep("proj/app", "b"); err != nil {
		t.Fatal(err)
	}
	if rg.Blobs.Has(shared) || rg.Blobs.Has(cfgB) {
		t.Error("everything should be swept after last tag removed")
	}
}

func TestIndexRejectsMissingChild(t *testing.T) {
	rg := newTestRegistry(t)
	// An index referencing a child manifest that was never pushed.
	body := indexManifest(childRef{"sha256:" + strings.Repeat("c", 64), "linux", "amd64"})
	_, err := rg.Manifests.PutManifest("proj/app", "latest", MediaTypeOCIIndex, body)
	if err == nil || !strings.Contains(err.Error(), "child") {
		t.Fatalf("expected child-unknown error, got %v", err)
	}
}

// TestMultiArchIndex exercises the full multi-arch flow: two per-arch child
// manifests (sharing a base layer) plus an index; resolution per platform; and
// GC of the tag freeing every architecture.
func TestMultiArchIndex(t *testing.T) {
	rg := newTestRegistry(t)
	base := pushBlob(t, rg, "shared-base-layer")
	cfgAmd := pushBlob(t, rg, "config-amd64")
	cfgArm := pushBlob(t, rg, "config-arm64")
	layAmd := pushBlob(t, rg, "layer-amd64")
	layArm := pushBlob(t, rg, "layer-arm64")

	// Children are pushed by digest first (OCI push order).
	amdBody := imageManifest(cfgAmd, base, layAmd)
	armBody := imageManifest(cfgArm, base, layArm)
	dAmd := putByDigest(t, rg, "proj/app", amdBody)
	// Re-push by its own digest ref (as docker does) — must be idempotent.
	if _, err := rg.Manifests.PutManifest("proj/app", dAmd, MediaTypeOCIManifest, amdBody); err != nil {
		t.Fatal(err)
	}
	dArm := putByDigest(t, rg, "proj/app", armBody)

	idx := indexManifest(childRef{dAmd, "linux", "amd64"}, childRef{dArm, "linux", "arm64"})
	dIdx, err := rg.Manifests.PutManifest("proj/app", "latest", MediaTypeOCIIndex, idx)
	if err != nil {
		t.Fatalf("put index: %v", err)
	}

	// GET latest returns the index media type (clients then fetch a child).
	_, mt, d, err := rg.Manifests.GetManifest("proj/app", "latest")
	if err != nil || mt != MediaTypeOCIIndex || d != dIdx {
		t.Fatalf("index get: mt=%s d=%s err=%v", mt, d, err)
	}

	// Platform resolution picks the right child.
	if got, _, err := rg.Manifests.ResolveManifest("proj/app", "latest", "linux/arm64"); err != nil || got != dArm {
		t.Fatalf("resolve arm64 = %s err=%v, want %s", got, err, dArm)
	}
	if got, _, err := rg.Manifests.ResolveManifest("proj/app", "latest", "linux/amd64"); err != nil || got != dAmd {
		t.Fatalf("resolve amd64 = %s err=%v, want %s", got, err, dAmd)
	}
	if _, _, err := rg.Manifests.ResolveManifest("proj/app", "latest", "windows/arm64"); err == nil {
		t.Fatal("resolve of an unsupported platform should error")
	}
	// Empty platform → the index itself.
	if got, _, _ := rg.Manifests.ResolveManifest("proj/app", "latest", ""); got != dIdx {
		t.Fatalf("resolve empty = %s, want index %s", got, dIdx)
	}

	// Platforms() lists both arches (for T-88's Release.platforms).
	plats, _ := rg.Manifests.Platforms("proj/app", "latest")
	if len(plats) != 2 || !contains(plats, "linux/amd64") || !contains(plats, "linux/arm64") {
		t.Fatalf("platforms = %v", plats)
	}

	// Untagging the multi-arch tag frees every architecture's blobs.
	if err := rg.Manifests.UntagAndSweep("proj/app", "latest"); err != nil {
		t.Fatal(err)
	}
	for name, d := range map[string]string{
		"base": base, "cfgAmd": cfgAmd, "cfgArm": cfgArm, "layAmd": layAmd, "layArm": layArm,
		"childAmd": dAmd, "childArm": dArm, "index": dIdx,
	} {
		if rg.Blobs.Has(d) {
			t.Errorf("%s (%s) should be swept after untag", name, d)
		}
	}
}

func TestDeleteManifestByDigest(t *testing.T) {
	rg := newTestRegistry(t)
	cfg := pushBlob(t, rg, "cfg")
	layer := pushBlob(t, rg, "layer")
	d, err := rg.Manifests.PutManifest("proj/app", "v1", MediaTypeOCIManifest, imageManifest(cfg, layer))
	if err != nil {
		t.Fatal(err)
	}
	if err := rg.Manifests.DeleteManifest("proj/app", d); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if rg.Blobs.Has(cfg) || rg.Blobs.Has(layer) || rg.Blobs.Has(d) {
		t.Error("delete should sweep manifest and its blobs")
	}
	if err := rg.Manifests.DeleteManifest("proj/app", d); err != ErrManifestUnknown {
		t.Fatalf("second delete err = %v, want ErrManifestUnknown", err)
	}
}

// putByDigest mirrors a docker client pushing a child manifest to
// /manifests/<digest> (computed client-side): the ref is the content digest,
// so no tag is created.
func putByDigest(t *testing.T, rg *Registry, repo string, body []byte) string {
	t.Helper()
	sum := sha256.Sum256(body)
	ref := "sha256:" + hex.EncodeToString(sum[:])
	d, err := rg.Manifests.PutManifest(repo, ref, MediaTypeOCIManifest, body)
	if err != nil {
		t.Fatalf("put by digest: %v", err)
	}
	return d
}

func contains(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}
