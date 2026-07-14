package registry

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

func newTestServer(t *testing.T) (*httptest.Server, *BlobStore, *Uploads, clock.Clock) {
	t.Helper()
	store, err := NewBlobStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	clk := clock.NewFake()
	m := NewUploads(store, clk)
	srv := httptest.NewServer(NewHandler(store, m, nil))
	t.Cleanup(srv.Close)
	return srv, store, m, clk
}

func TestVersionProbe(t *testing.T) {
	srv, _, _, _ := newTestServer(t)
	resp, err := http.Get(srv.URL + "/v2/")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if resp.Header.Get("Docker-Distribution-Api-Version") != apiVersionHeader {
		t.Errorf("missing api version header")
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "{}" {
		t.Errorf("body = %q, want {}", body)
	}
}

// TestChunkedUpload drives the full session flow: POST → PATCH ×2 → PUT.
func TestChunkedUpload(t *testing.T) {
	srv, store, _, _ := newTestServer(t)
	name := "myproject/api"
	part1 := []byte("hello ")
	part2 := []byte("multi-arch world")
	full := append(append([]byte{}, part1...), part2...)
	dgst := digestOf(full)

	// POST to open a session.
	resp := do(t, http.MethodPost, srv.URL+"/v2/"+name+"/blobs/uploads/", "", nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST status = %d, want 202", resp.StatusCode)
	}
	loc := resp.Header.Get("Location")
	if loc == "" || resp.Header.Get("Docker-Upload-UUID") == "" {
		t.Fatalf("missing Location/UUID: %+v", resp.Header)
	}
	if got := resp.Header.Get("Range"); got != "0-0" {
		t.Errorf("initial Range = %q, want 0-0", got)
	}

	// PATCH part 1 with an explicit content range.
	resp = do(t, http.MethodPatch, srv.URL+loc, "0-", part1)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("PATCH1 status = %d", resp.StatusCode)
	}
	if got, want := resp.Header.Get("Range"), "0-5"; got != want {
		t.Errorf("Range after part1 = %q, want %q", got, want)
	}

	// PATCH part 2 continuing at offset 6.
	resp = do(t, http.MethodPatch, srv.URL+loc, "6-", part2)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("PATCH2 status = %d", resp.StatusCode)
	}

	// PUT to finalize with the digest.
	resp = do(t, http.MethodPut, srv.URL+loc+"?digest="+dgst, "", nil)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("PUT status = %d, want 201", resp.StatusCode)
	}
	if resp.Header.Get("Docker-Content-Digest") != dgst {
		t.Errorf("content-digest = %q, want %q", resp.Header.Get("Docker-Content-Digest"), dgst)
	}
	if !store.Has(dgst) {
		t.Fatal("blob not committed after PUT")
	}
}

// TestMonolithicPut uploads all bytes in the finalizing PUT body.
func TestMonolithicPut(t *testing.T) {
	srv, store, _, _ := newTestServer(t)
	name := "app"
	blob := []byte("single request layer")
	dgst := digestOf(blob)

	resp := do(t, http.MethodPost, srv.URL+"/v2/"+name+"/blobs/uploads/", "", nil)
	loc := resp.Header.Get("Location")
	resp = do(t, http.MethodPut, srv.URL+loc+"?digest="+dgst, "", blob)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("PUT status = %d, want 201", resp.StatusCode)
	}
	if !store.Has(dgst) {
		t.Fatal("blob not committed")
	}
}

// TestMonolithicPost uploads via POST ?digest= in one request.
func TestMonolithicPost(t *testing.T) {
	srv, store, _, _ := newTestServer(t)
	blob := []byte("posted layer")
	dgst := digestOf(blob)

	resp := do(t, http.MethodPost, srv.URL+"/v2/app/blobs/uploads/?digest="+dgst, "", blob)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST status = %d, want 201", resp.StatusCode)
	}
	if !store.Has(dgst) {
		t.Fatal("blob not committed")
	}
}

func TestPutDigestMismatch(t *testing.T) {
	srv, store, _, _ := newTestServer(t)
	blob := []byte("content")
	wrong := digestOf([]byte("other"))

	resp := do(t, http.MethodPost, srv.URL+"/v2/app/blobs/uploads/", "", nil)
	loc := resp.Header.Get("Location")
	resp = do(t, http.MethodPut, srv.URL+loc+"?digest="+wrong, "", blob)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	assertOCIError(t, resp, "DIGEST_INVALID")
	if store.Has(wrong) {
		t.Fatal("no blob should be committed on mismatch")
	}
}

func TestRangeOutOfOrder(t *testing.T) {
	srv, _, _, _ := newTestServer(t)
	resp := do(t, http.MethodPost, srv.URL+"/v2/app/blobs/uploads/", "", nil)
	loc := resp.Header.Get("Location")
	// Declaring a start of 10 when the offset is 0 must be rejected.
	resp = do(t, http.MethodPatch, srv.URL+loc, "10-", []byte("xxxx"))
	if resp.StatusCode != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("status = %d, want 416", resp.StatusCode)
	}
}

func TestHeadBlob(t *testing.T) {
	srv, _, m, _ := newTestServer(t)
	blob := []byte("existing")
	dgst := digestOf(blob)
	if _, err := m.Ingest(bytes.NewReader(blob), dgst); err != nil {
		t.Fatal(err)
	}

	resp := do(t, http.MethodHead, srv.URL+"/v2/app/blobs/"+dgst, "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("HEAD present status = %d, want 200", resp.StatusCode)
	}
	if resp.Header.Get("Docker-Content-Digest") != dgst {
		t.Errorf("missing content-digest")
	}

	miss := digestOf([]byte("nope"))
	resp = do(t, http.MethodHead, srv.URL+"/v2/app/blobs/"+miss, "", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("HEAD missing status = %d, want 404", resp.StatusCode)
	}
}

func TestGetBlob(t *testing.T) {
	srv, _, m, _ := newTestServer(t)
	blob := []byte("downloadable bytes")
	dgst := digestOf(blob)
	if _, err := m.Ingest(bytes.NewReader(blob), dgst); err != nil {
		t.Fatal(err)
	}
	resp := do(t, http.MethodGet, srv.URL+"/v2/app/blobs/"+dgst, "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d", resp.StatusCode)
	}
	got, _ := io.ReadAll(resp.Body)
	if !bytes.Equal(got, blob) {
		t.Fatalf("body mismatch: %q", got)
	}
}

func TestCancelUpload(t *testing.T) {
	srv, store, _, _ := newTestServer(t)
	resp := do(t, http.MethodPost, srv.URL+"/v2/app/blobs/uploads/", "", nil)
	loc := resp.Header.Get("Location")
	do(t, http.MethodPatch, srv.URL+loc, "0-", []byte("partial"))
	resp = do(t, http.MethodDelete, srv.URL+loc, "", nil)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE status = %d, want 204", resp.StatusCode)
	}
	ups, _ := readDirNames(uploadsDir(store))
	if len(ups) != 0 {
		t.Fatalf("canceled upload temp not removed: %v", ups)
	}
}

func TestReapExpiresSessions(t *testing.T) {
	store, _ := NewBlobStore(t.TempDir())
	clk := clock.NewFake()
	m := NewUploads(store, clk)

	up, err := m.Start()
	if err != nil {
		t.Fatal(err)
	}
	if n := m.Reap(); n != 0 {
		t.Fatalf("nothing should expire yet, reaped %d", n)
	}
	clk.Advance(uploadTTL + time.Minute)
	if n := m.Reap(); n != 1 {
		t.Fatalf("expected 1 expired, got %d", n)
	}
	if _, ok := m.Get(up.ID); ok {
		t.Fatal("session should be gone after reap")
	}
}

// --- helpers ---

func do(t *testing.T, method, url, contentRange string, body []byte, headers ...http.Header) *http.Response {
	t.Helper()
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, url, r)
	if err != nil {
		t.Fatal(err)
	}
	if contentRange != "" {
		req.Header.Set("Content-Range", contentRange)
	}
	for _, hdr := range headers {
		for k, vs := range hdr {
			for _, v := range vs {
				req.Header.Set(k, v)
			}
		}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func assertOCIError(t *testing.T, resp *http.Response, code string) {
	t.Helper()
	var env struct {
		Errors []ociError `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if len(env.Errors) == 0 || env.Errors[0].Code != code {
		t.Fatalf("error code = %+v, want %s", env.Errors, code)
	}
}

func readDirNames(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names, nil
}
