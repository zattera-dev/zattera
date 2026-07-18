package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// twoNode builds registries A and B, each behind its own httptest server, with
// A configured to pull through from B. It returns both plus a counter of the
// requests B actually served.
func twoNode(t *testing.T) (a, b *Registry, srvB *httptest.Server, bHits *atomic.Int64) {
	t.Helper()
	regA, err := New(t.TempDir(), clock.NewFake(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	regB, err := New(t.TempDir(), clock.NewFake(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = regA.Close(); _ = regB.Close() })

	hits := &atomic.Int64{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		regB.Handler().ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)

	regA.EnablePullThrough(
		PeerSourceFunc(func() []Peer { return []Peer{{NodeID: "b", BaseURL: srv.URL}} }),
		PeerCredentials{},
		srv.Client(),
	)
	return regA, regB, srv, hits
}

// getBlob issues a pull against a registry's HTTP surface.
func getBlob(t *testing.T, reg *Registry, repo, dgst string) (int, []byte) {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/v2/%s/blobs/%s", repo, dgst), nil)
	reg.Handler().ServeHTTP(rec, req)
	body, _ := io.ReadAll(rec.Body)
	return rec.Code, body
}

func TestPullThroughBlob(t *testing.T) {
	regA, regB, _, bHits := twoNode(t)

	payload := []byte("layer-bytes")
	dgst, _, err := regB.Blobs.Write(strings.NewReader(string(payload)))
	if err != nil {
		t.Fatal(err)
	}

	// A does not have it; the pull succeeds by fetching from B.
	if regA.Blobs.Has(dgst) {
		t.Fatal("precondition: A should not have the blob")
	}
	code, body := getBlob(t, regA, "proj/app", dgst)
	if code != http.StatusOK {
		t.Fatalf("pull-through GET = %d, want 200", code)
	}
	if string(body) != string(payload) {
		t.Fatalf("served %q, want %q", body, payload)
	}

	// It was committed locally, so a second pull never touches B.
	if !regA.Blobs.Has(dgst) {
		t.Fatal("fetched blob must be committed locally")
	}
	before := bHits.Load()
	if code, _ := getBlob(t, regA, "proj/app", dgst); code != http.StatusOK {
		t.Fatalf("second GET = %d, want 200", code)
	}
	if after := bHits.Load(); after != before {
		t.Fatalf("second pull hit the peer %d times, want 0", after-before)
	}
}

func TestPullThroughNoPeersIsCleanMiss(t *testing.T) {
	reg, err := New(t.TempDir(), clock.NewFake(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reg.Close() })
	// No EnablePullThrough at all: the single-node/dev case.
	code, _ := getBlob(t, reg, "proj/app", "sha256:"+strings.Repeat("ab", 32))
	if code != http.StatusNotFound {
		t.Fatalf("missing blob without peers = %d, want 404", code)
	}

	// And with pull-through enabled but zero peers resolved.
	reg.EnablePullThrough(PeerSourceFunc(func() []Peer { return nil }), PeerCredentials{}, nil)
	if code, _ := getBlob(t, reg, "proj/app", "sha256:"+strings.Repeat("cd", 32)); code != http.StatusNotFound {
		t.Fatalf("missing blob with zero peers = %d, want 404", code)
	}
}

func TestPullThroughUnreachablePeerFallsThrough(t *testing.T) {
	regA, regB, srvB, _ := twoNode(t)

	payload := []byte("only-on-b")
	dgst, _, err := regB.Blobs.Write(strings.NewReader(string(payload)))
	if err != nil {
		t.Fatal(err)
	}

	// A dead peer listed first must not prevent the live one from serving.
	regA.EnablePullThrough(
		PeerSourceFunc(func() []Peer {
			return []Peer{
				{NodeID: "dead", BaseURL: "http://127.0.0.1:1"},
				{NodeID: "b", BaseURL: srvB.URL},
			}
		}),
		PeerCredentials{}, srvB.Client(),
	)
	if code, body := getBlob(t, regA, "proj/app", dgst); code != http.StatusOK || string(body) != string(payload) {
		t.Fatalf("unreachable peer should fall through: code=%d body=%q", code, body)
	}
}

// TestPullThroughRejectsWrongBytes: a peer that answers with content whose
// digest differs from the request must not poison the local store.
func TestPullThroughRejectsWrongBytes(t *testing.T) {
	regA, err := New(t.TempDir(), clock.NewFake(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = regA.Close() })

	lying := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if r.Method == http.MethodGet {
			_, _ = w.Write([]byte("totally different bytes"))
		}
	}))
	t.Cleanup(lying.Close)

	regA.EnablePullThrough(
		PeerSourceFunc(func() []Peer { return []Peer{{NodeID: "liar", BaseURL: lying.URL}} }),
		PeerCredentials{}, lying.Client(),
	)

	want := digestOf([]byte("the real thing"))
	code, _ := getBlob(t, regA, "proj/app", want)
	if code != http.StatusNotFound {
		t.Fatalf("mismatched peer content = %d, want 404", code)
	}
	if regA.Blobs.Has(want) {
		t.Fatal("a digest mismatch must never be committed under the requested digest")
	}
	if regA.Blobs.Has(digestOf([]byte("totally different bytes"))) {
		t.Fatal("the wrong bytes must not be left in the store either")
	}
}

// TestPullThroughManifestRegistersGraph: a manifest must arrive with its config
// and layers AND be registered through Manifests, so the refcount graph is
// complete and GC does not later orphan or over-delete blobs.
func TestPullThroughManifestRegistersGraph(t *testing.T) {
	regA, regB, _, _ := twoNode(t)

	// Build an image on B: config + one layer + manifest, tagged.
	cfgDgst, _, err := regB.Blobs.Write(strings.NewReader(`{"architecture":"arm64","os":"linux"}`))
	if err != nil {
		t.Fatal(err)
	}
	layerDgst, _, err := regB.Blobs.Write(strings.NewReader("layer-1"))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(map[string]any{
		"mediaType": MediaTypeOCIManifest,
		"config":    map[string]any{"mediaType": "application/vnd.oci.image.config.v1+json", "digest": cfgDgst, "size": 10},
		"layers":    []any{map[string]any{"mediaType": "application/vnd.oci.image.layer.v1.tar", "digest": layerDgst, "size": 7}},
	})
	manDgst, err := regB.Manifests.PutManifest("proj/app", "v1", MediaTypeOCIManifest, body)
	if err != nil {
		t.Fatal(err)
	}

	// A pulls by tag.
	rec := httptest.NewRecorder()
	regA.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v2/proj/app/manifests/v1", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("manifest pull-through = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Docker-Content-Digest"); got != manDgst {
		t.Errorf("digest = %q, want %q", got, manDgst)
	}

	// The whole graph landed: manifest resolvable locally, blobs present.
	if _, _, _, err := regA.Manifests.GetManifest("proj/app", "v1"); err != nil {
		t.Errorf("tag should resolve locally after pull-through: %v", err)
	}
	if !regA.Blobs.Has(cfgDgst) || !regA.Blobs.Has(layerDgst) {
		t.Error("config and layer blobs must be fetched alongside the manifest")
	}

	// Serving it again is local, and the layer is reachable by digest.
	if code, body := getBlob(t, regA, "proj/app", layerDgst); code != http.StatusOK || string(body) != "layer-1" {
		t.Fatalf("layer GET after manifest pull = %d %q", code, body)
	}
}

// TestPullThroughConcurrentSameDigest: Docker pulls layers in parallel, so
// concurrent requests for one missing digest must collapse into a single peer
// fetch rather than N.
func TestPullThroughConcurrentSameDigest(t *testing.T) {
	regA, regB, _, bHits := twoNode(t)

	dgst, _, err := regB.Blobs.Write(strings.NewReader("shared-layer"))
	if err != nil {
		t.Fatal(err)
	}

	const n = 8
	done := make(chan int, n)
	for i := 0; i < n; i++ {
		go func() {
			code, _ := getBlob(t, regA, "proj/app", dgst)
			done <- code
		}()
	}
	for i := 0; i < n; i++ {
		if code := <-done; code != http.StatusOK {
			t.Errorf("concurrent pull got %d, want 200", code)
		}
	}
	// Each fetch costs a HEAD + GET; collapsing keeps this far below 2*n.
	if hits := bHits.Load(); hits > 4 {
		t.Errorf("peer served %d requests for %d concurrent pulls; fetches should collapse", hits, n)
	}
}

// TestPullThroughIsIntraClusterOnly documents the boundary: the fetcher only
// ever talks to the peers it is given, never to an external registry named in
// the image reference.
func TestPullThroughIsIntraClusterOnly(t *testing.T) {
	f := NewFetcher(PeerSourceFunc(func() []Peer { return nil }), PeerCredentials{}, nil)
	store, err := NewBlobStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	err = f.FetchBlob(context.Background(), store, "docker.io/library/nginx", digestOf([]byte("x")))
	if err == nil {
		t.Fatal("with no peers a fetch must fail, not reach out to docker.io")
	}
}

// TestPullThroughBlobSurvivesGC guards the GC gotcha called out in T-101: a
// blob fetched from a peer has no local manifest referencing it yet, so it
// carries no refcount. Sweeping must not treat that as garbage while it is
// still the thing we just fetched to serve — and, once a manifest DOES arrive,
// the normal refcount path must own it (no double-free, no leak).
func TestPullThroughBlobSurvivesGC(t *testing.T) {
	regA, regB, _, _ := twoNode(t)

	dgst, _, err := regB.Blobs.Write(strings.NewReader("orphan-for-now"))
	if err != nil {
		t.Fatal(err)
	}
	if code, _ := getBlob(t, regA, "proj/app", dgst); code != http.StatusOK {
		t.Fatal("precondition: pull-through should succeed")
	}
	if !regA.Blobs.Has(dgst) {
		t.Fatal("precondition: blob should be local")
	}

	// Untag-and-sweep of an unrelated image must not remove it.
	_ = regA.Manifests.UntagAndSweep("proj/other", "v9")
	if !regA.Blobs.Has(dgst) {
		t.Error("an unrelated sweep deleted a pulled-through blob")
	}
}

// TestPullThroughAuthenticatesToPeer: production registries require auth, so a
// fetcher that forgot its credentials would work in tests and fail on a real
// cluster. The peer must reject anonymous fetches and accept the node's own
// "node-<id>" credential.
func TestPullThroughAuthenticatesToPeer(t *testing.T) {
	const user, pass = "node-abc", "s3cret"
	authed := AuthFunc(func(u, p string) bool { return u == user && p == pass })

	regB, err := New(t.TempDir(), clock.NewFake(), authed, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = regB.Close() })
	srv := httptest.NewServer(regB.Handler())
	t.Cleanup(srv.Close)

	dgst, _, err := regB.Blobs.Write(strings.NewReader("protected-layer"))
	if err != nil {
		t.Fatal(err)
	}
	peers := PeerSourceFunc(func() []Peer { return []Peer{{NodeID: "b", BaseURL: srv.URL}} })

	// Anonymous: the peer 401s, so the fetch must fail rather than half-commit.
	regAnon, err := New(t.TempDir(), clock.NewFake(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = regAnon.Close() })
	regAnon.EnablePullThrough(peers, PeerCredentials{}, srv.Client())
	if code, _ := getBlob(t, regAnon, "proj/app", dgst); code != http.StatusNotFound {
		t.Fatalf("unauthenticated pull-through = %d, want 404", code)
	}
	if regAnon.Blobs.Has(dgst) {
		t.Fatal("a rejected fetch must not commit anything")
	}

	// With the node credential the same fetch succeeds.
	regA, err := New(t.TempDir(), clock.NewFake(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = regA.Close() })
	regA.EnablePullThrough(peers, PeerCredentials{Username: user, Password: pass}, srv.Client())
	if code, body := getBlob(t, regA, "proj/app", dgst); code != http.StatusOK || string(body) != "protected-layer" {
		t.Fatalf("authenticated pull-through = %d %q", code, body)
	}
}
