package registry

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// fullServer starts an httptest server over the complete T-32 handler
// (manifests + optional auth).
func fullServer(t *testing.T, auth Authenticator) (*httptest.Server, *Registry) {
	t.Helper()
	rg, err := New(t.TempDir(), clock.NewFake(), auth, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rg.Close() })
	srv := httptest.NewServer(rg.handler)
	t.Cleanup(srv.Close)
	return srv, rg
}

// TestManifestHTTPRoundTrip pushes blobs + a manifest over HTTP and pulls the
// manifest back with the right content type and digest.
func TestManifestHTTPRoundTrip(t *testing.T) {
	srv, _ := fullServer(t, nil)
	name := "proj/api"

	cfg := []byte(`{"config":true}`)
	layer := []byte("layerdata")
	cfgD := digestOf(cfg)
	layerD := digestOf(layer)
	postBlob(t, srv.URL, name, cfg, cfgD)
	postBlob(t, srv.URL, name, layer, layerD)

	body := imageManifest(cfgD, layerD)
	resp := do(t, http.MethodPut, srv.URL+"/v2/"+name+"/manifests/v1",
		"", body, ctHeader(MediaTypeOCIManifest))
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("PUT manifest status = %d", resp.StatusCode)
	}
	dig := resp.Header.Get("Docker-Content-Digest")
	if dig == "" {
		t.Fatal("missing content-digest on PUT")
	}

	// GET by tag.
	resp = do(t, http.MethodGet, srv.URL+"/v2/"+name+"/manifests/v1", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET manifest status = %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != MediaTypeOCIManifest {
		t.Errorf("content-type = %q, want %q", ct, MediaTypeOCIManifest)
	}
	got, _ := io.ReadAll(resp.Body)
	if string(got) != string(body) {
		t.Errorf("manifest body mismatch")
	}

	// tags/list.
	resp = do(t, http.MethodGet, srv.URL+"/v2/"+name+"/tags/list", "", nil)
	var tl struct {
		Name string   `json:"name"`
		Tags []string `json:"tags"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&tl)
	if tl.Name != name || len(tl.Tags) != 1 || tl.Tags[0] != "v1" {
		t.Fatalf("tags/list = %+v", tl)
	}
}

func TestManifestHTTPMissingBlob(t *testing.T) {
	srv, _ := fullServer(t, nil)
	body := imageManifest(digestOf([]byte("nope-cfg")), digestOf([]byte("nope-layer")))
	resp := do(t, http.MethodPut, srv.URL+"/v2/app/manifests/v1", "", body, ctHeader(MediaTypeOCIManifest))
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	assertOCIError(t, resp, "MANIFEST_BLOB_UNKNOWN")
}

func TestAuthRequired(t *testing.T) {
	auth := AuthFunc(func(u, p string) bool { return u == "node-x" && p == "secret" })
	srv, _ := fullServer(t, auth)

	// No credentials → 401 with a Basic challenge.
	resp := do(t, http.MethodGet, srv.URL+"/v2/", "", nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
	if resp.Header.Get("WWW-Authenticate") == "" {
		t.Error("missing WWW-Authenticate challenge")
	}

	// Wrong credentials → 401.
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v2/", nil)
	req.SetBasicAuth("node-x", "wrong")
	resp2, _ := http.DefaultClient.Do(req)
	defer func() { _ = resp2.Body.Close() }()
	if resp2.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong-cred status = %d, want 401", resp2.StatusCode)
	}

	// Correct credentials → 200.
	req, _ = http.NewRequest(http.MethodGet, srv.URL+"/v2/", nil)
	req.SetBasicAuth("node-x", "secret")
	resp3, _ := http.DefaultClient.Do(req)
	defer func() { _ = resp3.Body.Close() }()
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("good-cred status = %d, want 200", resp3.StatusCode)
	}
}

// postBlob uploads a blob via monolithic POST ?digest=.
func postBlob(t *testing.T, base, name string, content []byte, dgst string) {
	t.Helper()
	resp := do(t, http.MethodPost, base+"/v2/"+name+"/blobs/uploads/?digest="+dgst, "", content)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("post blob status = %d", resp.StatusCode)
	}
}

func ctHeader(mt string) http.Header {
	h := http.Header{}
	h.Set("Content-Type", mt)
	return h
}
