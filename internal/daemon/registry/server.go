package registry

import (
	"fmt"
	"log/slog"
	"net/http"

	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// Registry bundles the embedded OCI registry's storage (blobs, upload sessions,
// manifests) behind a single http.Handler mounted on :5000 by the daemon on
// control nodes.
type Registry struct {
	Blobs     *BlobStore
	Uploads   *Uploads
	Manifests *Manifests
	handler   *Handler
}

// New opens the registry rooted at dir (typically <data-dir>/registry). auth
// may be nil to disable authentication (dev / tests).
func New(dir string, clk clock.Clock, auth Authenticator, log *slog.Logger) (*Registry, error) {
	blobs, err := NewBlobStore(dir)
	if err != nil {
		return nil, err
	}
	uploads := NewUploads(blobs, clk)
	man, err := NewManifests(dir, blobs, clk)
	if err != nil {
		return nil, err
	}
	return &Registry{
		Blobs:     blobs,
		Uploads:   uploads,
		Manifests: man,
		handler:   newHandler(blobs, uploads, man, auth, log),
	}, nil
}

// Handler returns the OCI distribution HTTP handler.
func (rg *Registry) Handler() http.Handler { return rg.handler }

// ReapUploads expires idle upload sessions; call it periodically from a janitor.
func (rg *Registry) ReapUploads() int { return rg.Uploads.Reap() }

// Close releases the manifest metadata database.
func (rg *Registry) Close() error {
	if err := rg.Manifests.Close(); err != nil {
		return fmt.Errorf("registry: close: %w", err)
	}
	return nil
}
