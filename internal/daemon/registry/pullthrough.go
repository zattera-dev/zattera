package registry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Pull-through (T-101). Registry blobs are node-local by design: layers do not
// belong in a consensus log, and one copy per cluster is the point (ADR-0005).
// But the node that RECEIVES a push is not necessarily the node a worker was
// told to pull from — builds push to the raft leader, while a joining node is
// handed the registry address of whichever control node served its join. So a
// control node that is asked for a blob it does not have fetches it from a
// peer control node, commits it locally, and serves it. Storage stays ~1x
// cluster-wide (each blob lands only where it is actually served), the cost is
// one hop on a cold pull, and repeated pulls are local.
//
// Scope: strictly intra-cluster. This never fetches from Docker Hub or any
// external registry — that is a different feature with different security
// properties (T-101 step 6).

const (
	// peerFetchTimeout bounds a single peer exchange (HEAD + GET + commit).
	// Layers can be large, so this is generous; peerTotalTimeout is the real
	// backstop across all peers.
	peerFetchTimeout = 5 * time.Minute
	// peerTotalTimeout bounds the whole pull-through attempt across every peer.
	// It must stay comfortably under a Docker client's pull timeout so the
	// client sees a clean 404 rather than a hung connection.
	peerTotalTimeout = 10 * time.Minute
	// peerFetchConcurrency caps concurrent peer fetches per node. A cold
	// multi-layer image otherwise opens one fetch per layer per peer.
	peerFetchConcurrency = 4
	// peerProbeTimeout bounds the cheap "do you have it?" HEAD.
	peerProbeTimeout = 10 * time.Second
)

// Peer is one other control node's registry endpoint.
type Peer struct {
	// NodeID identifies the peer (logging only).
	NodeID string
	// BaseURL is its registry root, e.g. "https://10.90.0.2:5000".
	BaseURL string
}

// PeerSource lists the OTHER control-node registries this node may fetch from.
// It is resolved per call from cluster state (never a static list) so it
// follows joins, removals and cordons without restarts. Returning nil disables
// pull-through — which is the single-node and dev case.
type PeerSource interface {
	Peers() []Peer
}

// PeerSourceFunc adapts a plain function to PeerSource.
type PeerSourceFunc func() []Peer

// Peers implements PeerSource.
func (f PeerSourceFunc) Peers() []Peer { return f() }

// PeerCredentials is the identity a node presents to a peer's registry. In
// production this is the node's own "node-<id>" credential (already minted at
// join and validated by the registry authenticator) over the mesh with the
// cluster CA — never anonymous, never a user PAT.
type PeerCredentials struct {
	Username string
	Password string
}

// Fetcher pulls blobs and manifests from peer control nodes on demand.
type Fetcher struct {
	peers  PeerSource
	creds  PeerCredentials
	client *http.Client
	sem    chan struct{}

	// inflight collapses concurrent fetches of the same digest on this node:
	// Docker pulls layers in parallel and a retried pull can race the first.
	mu       sync.Mutex
	inflight map[string]*fetchWait
}

// fetchWait lets callers wait on an in-progress fetch of the same digest.
type fetchWait struct {
	done chan struct{}
	err  error
}

// NewFetcher builds a pull-through fetcher. A nil PeerSource (or one that
// returns no peers) makes every method a no-op miss, which is exactly what a
// single-node cluster wants.
func NewFetcher(peers PeerSource, creds PeerCredentials, client *http.Client) *Fetcher {
	if client == nil {
		client = &http.Client{Timeout: peerFetchTimeout}
	}
	return &Fetcher{
		peers:    peers,
		creds:    creds,
		client:   client,
		sem:      make(chan struct{}, peerFetchConcurrency),
		inflight: map[string]*fetchWait{},
	}
}

// enabled reports whether any peer is currently known.
func (f *Fetcher) enabled() bool {
	return f != nil && f.peers != nil && len(f.peers.Peers()) > 0
}

// FetchBlob fetches dgst from the first peer that has it and commits it to
// store. It returns nil when the blob is now present locally. Callers re-read
// from the local store afterwards: committing first and serving from disk
// keeps the write path atomic (BlobStore.Write digest-verifies and renames)
// and avoids half-serving a blob whose transfer failed midway.
func (f *Fetcher) FetchBlob(ctx context.Context, store *BlobStore, repo, dgst string) error {
	if !f.enabled() {
		return ErrBlobUnknown
	}
	// Collapse duplicate concurrent fetches of the same digest.
	f.mu.Lock()
	if w, ok := f.inflight[dgst]; ok {
		f.mu.Unlock()
		select {
		case <-w.done:
			return w.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	w := &fetchWait{done: make(chan struct{})}
	f.inflight[dgst] = w
	f.mu.Unlock()

	err := f.fetchBlobLocked(ctx, store, repo, dgst)

	f.mu.Lock()
	delete(f.inflight, dgst)
	f.mu.Unlock()
	w.err = err
	close(w.done)
	return err
}

func (f *Fetcher) fetchBlobLocked(ctx context.Context, store *BlobStore, repo, dgst string) error {
	if store.Has(dgst) { // a concurrent push/fetch won the race
		return nil
	}
	select {
	case f.sem <- struct{}{}:
		defer func() { <-f.sem }()
	case <-ctx.Done():
		return ctx.Err()
	}

	ctx, cancel := context.WithTimeout(ctx, peerTotalTimeout)
	defer cancel()

	for _, p := range f.peers.Peers() {
		body, err := f.get(ctx, p, fmt.Sprintf("/v2/%s/blobs/%s", repo, dgst))
		if err != nil {
			continue // unreachable peer or a peer that lacks it: try the next
		}
		got, _, werr := store.Write(body)
		_ = body.Close()
		if werr != nil {
			continue
		}
		// BlobStore.Write returns the digest of what actually arrived. A peer
		// serving different bytes than asked for must never be trusted: drop it
		// rather than commit a mislabelled blob.
		if got != dgst {
			_ = store.Delete(got)
			continue
		}
		return nil
	}
	return ErrBlobUnknown
}

// FetchManifest fetches a manifest from a peer and registers it locally so the
// tag index and refcount graph stay correct (a manifest is a blob plus
// bookkeeping — copying only the bytes would leave GC blind to its children).
//
// ref may be a tag or a digest. A tag is resolved on the peer first and the
// local tag is bound only to the digest the peer actually served, so a stale
// or racing tag on one node cannot silently repoint another node's tag.
func (f *Fetcher) FetchManifest(ctx context.Context, man *Manifests, store *BlobStore, repo, ref string) error {
	if !f.enabled() || man == nil {
		return ErrManifestUnknown
	}
	ctx, cancel := context.WithTimeout(ctx, peerTotalTimeout)
	defer cancel()

	for _, p := range f.peers.Peers() {
		body, mediaType, digest, err := f.getManifest(ctx, p, repo, ref)
		if err != nil {
			continue
		}
		// Pull the manifest's children first: PutManifest validates that every
		// referenced blob and child manifest exists locally, so a manifest whose
		// layers are missing would be rejected.
		if err := f.fetchManifestChildren(ctx, man, store, p, repo, body, mediaType); err != nil {
			continue
		}
		// Register by digest, then bind the tag only if the request was by tag.
		if _, err := man.PutManifest(repo, digest, mediaType, body); err != nil {
			continue
		}
		if !strings.HasPrefix(ref, "sha256:") {
			if _, err := man.PutManifest(repo, ref, mediaType, body); err != nil {
				continue
			}
		}
		return nil
	}
	return ErrManifestUnknown
}

// fetchManifestChildren pulls everything a manifest references: for an index,
// each child manifest (recursively); for an image manifest, its config and
// layers.
func (f *Fetcher) fetchManifestChildren(ctx context.Context, man *Manifests, store *BlobStore, p Peer, repo string, body []byte, mediaType string) error {
	children, blobs, err := manifestRefs(body, mediaType)
	if err != nil {
		return err
	}
	for _, child := range children {
		if _, _, _, err := man.GetManifest(repo, child); err == nil {
			continue // already local
		}
		cbody, cmt, cdgst, err := f.getManifest(ctx, p, repo, child)
		if err != nil {
			return err
		}
		if err := f.fetchManifestChildren(ctx, man, store, p, repo, cbody, cmt); err != nil {
			return err
		}
		if _, err := man.PutManifest(repo, cdgst, cmt, cbody); err != nil {
			return err
		}
	}
	for _, dgst := range blobs {
		if store.Has(dgst) {
			continue
		}
		if err := f.FetchBlob(ctx, store, repo, dgst); err != nil {
			return err
		}
	}
	return nil
}

// getManifest fetches one manifest from a peer, returning its body, media type
// and digest.
func (f *Fetcher) getManifest(ctx context.Context, p Peer, repo, ref string) (body []byte, mediaType, digest string, err error) {
	rc, hdr, err := f.getWithHeader(ctx, p, fmt.Sprintf("/v2/%s/manifests/%s", repo, ref))
	if err != nil {
		return nil, "", "", err
	}
	defer func() { _ = rc.Close() }()
	body, err = io.ReadAll(io.LimitReader(rc, maxManifestBytes))
	if err != nil {
		return nil, "", "", err
	}
	digest = hdr.Get("Docker-Content-Digest")
	if digest == "" {
		digest = digestOf(body)
	}
	// Never trust the peer's digest header over the bytes it sent.
	if computed := digestOf(body); computed != digest {
		return nil, "", "", fmt.Errorf("registry: peer manifest digest mismatch (%s != %s)", computed, digest)
	}
	return body, hdr.Get("Content-Type"), digest, nil
}

// get issues an authenticated GET against a peer, returning the response body
// on 200. The caller closes it.
func (f *Fetcher) get(ctx context.Context, p Peer, path string) (io.ReadCloser, error) {
	rc, _, err := f.getWithHeader(ctx, p, path)
	return rc, err
}

func (f *Fetcher) getWithHeader(ctx context.Context, p Peer, path string) (io.ReadCloser, http.Header, error) {
	// Cheap existence probe first so a peer that lacks the object costs a HEAD,
	// not a connection held open for a body that will never come.
	probeCtx, cancelProbe := context.WithTimeout(ctx, peerProbeTimeout)
	head, err := f.do(probeCtx, http.MethodHead, p, path)
	if err != nil {
		cancelProbe()
		return nil, nil, err
	}
	_ = head.Body.Close()
	cancelProbe()
	if head.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("registry: peer %s: %s", p.NodeID, head.Status)
	}

	resp, err := f.do(ctx, http.MethodGet, p, path)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, nil, fmt.Errorf("registry: peer %s: %s", p.NodeID, resp.Status)
	}
	return resp.Body, resp.Header, nil
}

func (f *Fetcher) do(ctx context.Context, method string, p Peer, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(p.BaseURL, "/")+path, nil)
	if err != nil {
		return nil, err
	}
	if f.creds.Username != "" {
		req.SetBasicAuth(f.creds.Username, f.creds.Password)
	}
	req.Header.Set("Accept", strings.Join([]string{
		MediaTypeOCIIndex, MediaTypeOCIManifest,
		MediaTypeDockerList, MediaTypeDockerManifest,
	}, ", "))
	return f.client.Do(req)
}

// digestOf is the canonical digest of a manifest body.
func digestOf(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// manifestRefs splits a manifest body into the child manifests it indexes and
// the blobs (config + layers) it references. Mirrors PutManifest's parsing so
// pull-through fetches exactly what validation will require.
func manifestRefs(body []byte, mediaType string) (children, blobs []string, err error) {
	var pm parsedManifest
	if uerr := json.Unmarshal(body, &pm); uerr != nil {
		return nil, nil, ErrManifestInvalid
	}
	mt := mediaType
	if mt == "" {
		mt = pm.MediaType
	}
	if isIndexMediaType(mt) || (mt == "" && len(pm.Manifests) > 0) {
		for _, c := range pm.Manifests {
			if c.Digest == "" {
				return nil, nil, ErrManifestInvalid
			}
			children = append(children, c.Digest)
		}
		return children, nil, nil
	}
	if pm.Config.Digest == "" {
		return nil, nil, ErrManifestInvalid
	}
	blobs = append(blobs, pm.Config.Digest)
	for _, l := range pm.Layers {
		if l.Digest == "" {
			return nil, nil, ErrManifestInvalid
		}
		blobs = append(blobs, l.Digest)
	}
	return nil, blobs, nil
}
