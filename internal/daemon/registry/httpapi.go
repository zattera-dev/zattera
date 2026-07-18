package registry

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// apiVersionHeader is advertised on every response so docker/OCI clients know
// this is a v2 registry.
const apiVersionHeader = "registry/2.0"

// maxManifestBytes caps a manifest/index body (they are small JSON documents;
// blobs stream through the upload path instead).
const maxManifestBytes = 8 << 20

// Handler serves the OCI distribution API: the version probe and blob upload
// flow (T-31) plus manifests, tags, pull and delete (T-32). A nil manifests
// store leaves the manifest routes disabled (the T-31-only surface); a nil
// Authenticator disables auth.
type Handler struct {
	store     *BlobStore
	uploads   *Uploads
	manifests *Manifests
	auth      Authenticator
	log       *slog.Logger
	// fetcher, when set, fetches blobs/manifests this node lacks from peer
	// control nodes before answering a miss (T-101). Nil disables pull-through.
	fetcher *Fetcher
}

// NewHandler wires the blob/upload push surface (T-31) with no manifest store
// or auth. Use newHandler (or registry.New) for the full T-32 surface.
func NewHandler(store *BlobStore, uploads *Uploads, log *slog.Logger) *Handler {
	return newHandler(store, uploads, nil, nil, log)
}

func newHandler(store *BlobStore, uploads *Uploads, manifests *Manifests, auth Authenticator, log *slog.Logger) *Handler {
	if log == nil {
		log = slog.Default()
	}
	return &Handler{store: store, uploads: uploads, manifests: manifests, auth: auth, log: log}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Docker-Distribution-Api-Version", apiVersionHeader)

	if !h.authorize(w, r) {
		return
	}

	// Version probe: GET/HEAD /v2/.
	if r.URL.Path == "/v2" || r.URL.Path == "/v2/" {
		h.version(w, r)
		return
	}

	rest, ok := strings.CutPrefix(r.URL.Path, "/v2/")
	if ok {
		// POST /v2/<name>/blobs/uploads/
		if name, ok := strings.CutSuffix(rest, "/blobs/uploads/"); ok {
			h.startUpload(w, r, name)
			return
		}
		// PATCH/PUT/GET/DELETE /v2/<name>/blobs/uploads/<id>
		if i := strings.Index(rest, "/blobs/uploads/"); i >= 0 {
			name := rest[:i]
			id := rest[i+len("/blobs/uploads/"):]
			h.uploadSession(w, r, name, id)
			return
		}
		// HEAD/GET /v2/<name>/blobs/<digest>
		if i := strings.Index(rest, "/blobs/"); i >= 0 {
			dgst := rest[i+len("/blobs/"):]
			h.blob(w, r, dgst)
			return
		}
		// GET /v2/<name>/tags/list
		if name, ok := strings.CutSuffix(rest, "/tags/list"); ok {
			h.tagsList(w, r, name)
			return
		}
		// GET/HEAD/PUT/DELETE /v2/<name>/manifests/<ref>
		if i := strings.Index(rest, "/manifests/"); i >= 0 {
			name := rest[:i]
			ref := rest[i+len("/manifests/"):]
			h.manifest(w, r, name, ref)
			return
		}
	}
	h.writeError(w, http.StatusNotFound, "UNSUPPORTED", "unsupported registry endpoint")
}

func (h *Handler) version(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		h.writeError(w, http.StatusMethodNotAllowed, "UNSUPPORTED", "method not allowed")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodGet {
		_, _ = w.Write([]byte("{}"))
	}
}

// startUpload handles POST /v2/<name>/blobs/uploads/. It supports three modes:
//   - ?digest=<d>          monolithic single-request upload → 201 Created
//   - ?mount=<d>&from=<r>  cross-repo mount; we don't track cross-repo layers
//     here, so fall back to a fresh upload session (spec-legal 202)
//   - (none)               open a resumable session → 202 Accepted
func (h *Handler) startUpload(w http.ResponseWriter, r *http.Request, name string) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "UNSUPPORTED", "method not allowed")
		return
	}
	if dgst := r.URL.Query().Get("digest"); dgst != "" {
		digest, err := h.uploads.Ingest(r.Body, dgst)
		if err != nil {
			h.writeUploadError(w, err)
			return
		}
		h.writeBlobCreated(w, name, digest)
		return
	}

	// mount= is handled by falling through to a normal session (202).
	up, err := h.uploads.Start()
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "UNKNOWN", err.Error())
		return
	}
	h.writeUploadAccepted(w, name, up, http.StatusAccepted)
}

func (h *Handler) uploadSession(w http.ResponseWriter, r *http.Request, name, id string) {
	switch r.Method {
	case http.MethodPatch:
		start, err := parseContentRangeStart(r.Header.Get("Content-Range"))
		if err != nil {
			h.writeError(w, http.StatusRequestedRangeNotSatisfiable, "BLOB_UPLOAD_INVALID", err.Error())
			return
		}
		if _, err := h.uploads.Append(id, r.Body, start); err != nil {
			h.writeUploadError(w, err)
			return
		}
		up, _ := h.uploads.Get(id)
		h.writeUploadAccepted(w, name, up, http.StatusAccepted)

	case http.MethodPut:
		dgst := r.URL.Query().Get("digest")
		if dgst == "" {
			h.writeError(w, http.StatusBadRequest, "DIGEST_INVALID", "missing digest query parameter")
			return
		}
		digest, err := h.uploads.Finalize(id, dgst, r.Body)
		if err != nil {
			h.writeUploadError(w, err)
			return
		}
		h.writeBlobCreated(w, name, digest)

	case http.MethodGet:
		up, ok := h.uploads.Get(id)
		if !ok {
			h.writeError(w, http.StatusNotFound, "BLOB_UPLOAD_UNKNOWN", "upload session not found")
			return
		}
		h.writeUploadAccepted(w, name, up, http.StatusNoContent)

	case http.MethodDelete:
		if err := h.uploads.Cancel(id); err != nil {
			h.writeUploadError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		h.writeError(w, http.StatusMethodNotAllowed, "UNSUPPORTED", "method not allowed")
	}
}

func (h *Handler) blob(w http.ResponseWriter, r *http.Request, dgst string) {
	switch r.Method {
	case http.MethodHead, http.MethodGet:
		size, err := h.store.Stat(dgst)
		if errors.Is(err, ErrBlobUnknown) {
			// Pull-through: another control node may hold this blob. Fetch and
			// commit it, then serve from local storage (T-101).
			if ferr := h.pullBlob(r, dgst); ferr == nil {
				size, err = h.store.Stat(dgst)
			}
		}
		if err != nil {
			h.writeBlobStatError(w, err)
			return
		}
		w.Header().Set("Docker-Content-Digest", dgst)
		w.Header().Set("Content-Type", "application/octet-stream")
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
			w.WriteHeader(http.StatusOK)
			return
		}
		f, err := h.store.Open(dgst)
		if err != nil {
			h.writeBlobStatError(w, err)
			return
		}
		defer func() { _ = f.Close() }()
		http.ServeContent(w, r, "", time.Time{}, f)

	default:
		h.writeError(w, http.StatusMethodNotAllowed, "UNSUPPORTED", "method not allowed")
	}
}

func (h *Handler) manifest(w http.ResponseWriter, r *http.Request, name, ref string) {
	if h.manifests == nil {
		h.writeError(w, http.StatusNotFound, "UNSUPPORTED", "manifests not available")
		return
	}
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		body, mediaType, digest, err := h.manifests.GetManifest(name, ref)
		if errors.Is(err, ErrManifestUnknown) {
			// Pull-through, including the manifest's children and blobs so the
			// local refcount graph stays complete (T-101).
			if ferr := h.pullManifest(r, name, ref); ferr == nil {
				body, mediaType, digest, err = h.manifests.GetManifest(name, ref)
			}
		}
		if err != nil {
			h.writeManifestError(w, err)
			return
		}
		w.Header().Set("Content-Type", mediaType)
		w.Header().Set("Docker-Content-Digest", digest)
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)

	case http.MethodPut:
		body, err := io.ReadAll(io.LimitReader(r.Body, maxManifestBytes))
		if err != nil {
			h.writeError(w, http.StatusBadRequest, "MANIFEST_INVALID", "read manifest body")
			return
		}
		digest, err := h.manifests.PutManifest(name, ref, r.Header.Get("Content-Type"), body)
		if err != nil {
			h.writeManifestError(w, err)
			return
		}
		w.Header().Set("Location", fmt.Sprintf("/v2/%s/manifests/%s", name, digest))
		w.Header().Set("Docker-Content-Digest", digest)
		w.WriteHeader(http.StatusCreated)

	case http.MethodDelete:
		// DELETE is by digest per the distribution spec.
		if err := h.manifests.DeleteManifest(name, ref); err != nil {
			h.writeManifestError(w, err)
			return
		}
		w.WriteHeader(http.StatusAccepted)

	default:
		h.writeError(w, http.StatusMethodNotAllowed, "UNSUPPORTED", "method not allowed")
	}
}

func (h *Handler) tagsList(w http.ResponseWriter, r *http.Request, name string) {
	if h.manifests == nil {
		h.writeError(w, http.StatusNotFound, "UNSUPPORTED", "manifests not available")
		return
	}
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "UNSUPPORTED", "method not allowed")
		return
	}
	tags, err := h.manifests.Tags(name)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "UNKNOWN", err.Error())
		return
	}
	if tags == nil {
		tags = []string{}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(struct {
		Name string   `json:"name"`
		Tags []string `json:"tags"`
	}{Name: name, Tags: tags})
}

// --- response helpers ---

func (h *Handler) writeBlobCreated(w http.ResponseWriter, name, digest string) {
	w.Header().Set("Location", fmt.Sprintf("/v2/%s/blobs/%s", name, digest))
	w.Header().Set("Docker-Content-Digest", digest)
	w.WriteHeader(http.StatusCreated)
}

func (h *Handler) writeUploadAccepted(w http.ResponseWriter, name string, up *Upload, status int) {
	if up == nil {
		h.writeError(w, http.StatusNotFound, "BLOB_UPLOAD_UNKNOWN", "upload session not found")
		return
	}
	w.Header().Set("Location", fmt.Sprintf("/v2/%s/blobs/uploads/%s", name, up.ID))
	w.Header().Set("Docker-Upload-UUID", up.ID)
	w.Header().Set("Range", rangeHeader(up.Offset()))
	w.WriteHeader(status)
}

func (h *Handler) writeUploadError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrUploadUnknown):
		h.writeError(w, http.StatusNotFound, "BLOB_UPLOAD_UNKNOWN", "upload session not found")
	case errors.Is(err, ErrRangeNotSatisfiable):
		h.writeError(w, http.StatusRequestedRangeNotSatisfiable, "BLOB_UPLOAD_INVALID", "content range does not match upload offset")
	case errors.Is(err, ErrDigestInvalid):
		h.writeError(w, http.StatusBadRequest, "DIGEST_INVALID", "invalid digest")
	case errors.Is(err, ErrDigestMismatch):
		h.writeError(w, http.StatusBadRequest, "DIGEST_INVALID", "uploaded content does not match digest")
	default:
		h.log.Error("registry upload error", "err", err)
		h.writeError(w, http.StatusInternalServerError, "UNKNOWN", err.Error())
	}
}

func (h *Handler) writeManifestError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrManifestUnknown):
		h.writeError(w, http.StatusNotFound, "MANIFEST_UNKNOWN", "manifest unknown")
	case errors.Is(err, ErrManifestChildUnkn):
		h.writeError(w, http.StatusNotFound, "MANIFEST_UNKNOWN", err.Error())
	case errors.Is(err, ErrManifestBlobUnkn):
		h.writeError(w, http.StatusBadRequest, "MANIFEST_BLOB_UNKNOWN", err.Error())
	case errors.Is(err, ErrManifestInvalid):
		h.writeError(w, http.StatusBadRequest, "MANIFEST_INVALID", "manifest invalid")
	default:
		h.log.Error("registry manifest error", "err", err)
		h.writeError(w, http.StatusInternalServerError, "UNKNOWN", err.Error())
	}
}

func (h *Handler) writeBlobStatError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrBlobUnknown):
		h.writeError(w, http.StatusNotFound, "BLOB_UNKNOWN", "blob unknown to registry")
	case errors.Is(err, ErrDigestInvalid):
		h.writeError(w, http.StatusBadRequest, "DIGEST_INVALID", "invalid digest")
	default:
		h.writeError(w, http.StatusInternalServerError, "UNKNOWN", err.Error())
	}
}

// ociError is one entry in the OCI error response envelope.
type ociError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (h *Handler) writeError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Errors []ociError `json:"errors"`
	}{Errors: []ociError{{Code: code, Message: msg}}})
}

// rangeHeader formats the inclusive byte range accepted so far, per the
// distribution spec (a fresh session reports "0-0").
func rangeHeader(offset int64) string {
	if offset == 0 {
		return "0-0"
	}
	return "0-" + strconv.FormatInt(offset-1, 10)
}

// parseContentRangeStart parses a PATCH Content-Range header into its start
// offset. An empty header means "append at the current offset" and returns -1.
// Accepts "<start>-<end>" (distribution spec) and tolerates a "bytes " prefix.
func parseContentRangeStart(hdr string) (int64, error) {
	hdr = strings.TrimSpace(strings.TrimPrefix(hdr, "bytes "))
	if hdr == "" {
		return -1, nil
	}
	startStr, _, ok := strings.Cut(hdr, "-")
	if !ok {
		return 0, fmt.Errorf("malformed content-range %q", hdr)
	}
	start, err := strconv.ParseInt(strings.TrimSpace(startStr), 10, 64)
	if err != nil || start < 0 {
		return 0, fmt.Errorf("malformed content-range %q", hdr)
	}
	return start, nil
}

// pullBlob attempts a peer fetch for a blob this node does not have. It is a
// no-op miss when pull-through is disabled (single-node clusters).
func (h *Handler) pullBlob(r *http.Request, dgst string) error {
	if h.fetcher == nil {
		return ErrBlobUnknown
	}
	repo := repoFromPath(r.URL.Path, "/blobs/")
	if repo == "" {
		return ErrBlobUnknown
	}
	if err := h.fetcher.FetchBlob(r.Context(), h.store, repo, dgst); err != nil {
		h.log.Debug("registry: blob not available from peers", "digest", dgst, "err", err)
		return err
	}
	h.log.Info("registry: pulled blob through from a peer", "digest", dgst, "repo", repo)
	return nil
}

// pullManifest attempts a peer fetch for a manifest this node does not have.
func (h *Handler) pullManifest(r *http.Request, repo, ref string) error {
	if h.fetcher == nil {
		return ErrManifestUnknown
	}
	if err := h.fetcher.FetchManifest(r.Context(), h.manifests, h.store, repo, ref); err != nil {
		h.log.Debug("registry: manifest not available from peers", "repo", repo, "ref", ref, "err", err)
		return err
	}
	h.log.Info("registry: pulled manifest through from a peer", "repo", repo, "ref", ref)
	return nil
}

// repoFromPath recovers the repository name from a /v2/<name>/<kind>/... path.
// The router already split it, but the blob handler only receives the digest,
// and a peer fetch needs the repo for the peer's URL.
func repoFromPath(path, kind string) string {
	rest, ok := strings.CutPrefix(path, "/v2/")
	if !ok {
		return ""
	}
	i := strings.Index(rest, kind)
	if i < 0 {
		return ""
	}
	return rest[:i]
}
