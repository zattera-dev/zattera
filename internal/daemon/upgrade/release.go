// Package upgrade resolves release artifacts for a rolling cluster upgrade
// (T-95).
//
// The control plane, not the node, decides what a node should install: it
// resolves the target version to a per-architecture asset URL and its SHA-256,
// and hands both to the agent. The node then verifies the digest before it
// executes anything. That ordering matters — a node that fetched its own
// checksum from the same host it fetched the binary from would be trusting one
// source to vouch for itself.
package upgrade

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// DefaultBaseURL is the official release host. Overridable for mirrors and
// air-gapped clusters; the agent independently refuses any URL outside its own
// configured base, so changing this alone cannot redirect a node.
//
// This is NOT get.zattera.dev — that serves the install script only. Binaries
// live on GitHub Releases, and this must stay in step with GITHUB_REPO in
// install/install.sh: the two are the only ways a binary reaches a node, and
// they must install the same artifact.
const DefaultBaseURL = "https://github.com/zattera-dev/zattera/releases"

// assetPrefix matches the names produced by `make cross` and consumed by
// install/install.sh: zattera-<goos>-<goarch>.
const assetPrefix = "zattera-"

// checksumFile is the manifest published alongside the binaries.
const checksumFile = "sha256sums.txt"

// Asset is one downloadable binary.
type Asset struct {
	URL    string
	SHA256 string
}

// Release is a resolved version and its per-os/arch assets, keyed "linux/amd64".
type Release struct {
	Version string
	Assets  map[string]Asset
}

// Asset returns the artifact for an os/arch as reported by Node.os_arch.
func (r Release) Asset(osArch string) (Asset, bool) {
	a, ok := r.Assets[osArch]
	return a, ok
}

// Resolver turns a version (or "" for latest) into a Release.
type Resolver interface {
	Resolve(ctx context.Context, version string) (Release, error)
}

// HTTPResolver reads the release's checksum manifest over HTTP.
type HTTPResolver struct {
	BaseURL string
	Client  *http.Client

	mu    sync.Mutex
	cache map[string]Release
}

// NewHTTPResolver builds a resolver against a release host.
func NewHTTPResolver(baseURL string) *HTTPResolver {
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	return &HTTPResolver{
		BaseURL: strings.TrimRight(baseURL, "/"),
		Client:  &http.Client{Timeout: 30 * time.Second},
		cache:   map[string]Release{},
	}
}

// Resolve fetches and parses the checksum manifest for a version. Results are
// cached: an upgrade run resolves once and reuses it for every node, so a
// release retagged mid-rollout cannot split the cluster across two builds.
func (r *HTTPResolver) Resolve(ctx context.Context, version string) (Release, error) {
	r.mu.Lock()
	if rel, ok := r.cache[version]; ok {
		r.mu.Unlock()
		return rel, nil
	}
	r.mu.Unlock()

	resolved, base, err := r.downloadBase(ctx, version)
	if err != nil {
		return Release{}, err
	}
	sums, err := r.fetch(ctx, base+"/"+checksumFile)
	if err != nil {
		return Release{}, fmt.Errorf("upgrade: fetch %s: %w", checksumFile, err)
	}
	rel := Release{Version: resolved, Assets: map[string]Asset{}}
	for name, sum := range parseChecksums(sums) {
		osArch, ok := osArchFromAsset(name)
		if !ok {
			continue
		}
		rel.Assets[osArch] = Asset{URL: base + "/" + name, SHA256: sum}
	}
	if len(rel.Assets) == 0 {
		return Release{}, fmt.Errorf("upgrade: release %s publishes no recognizable binaries", resolved)
	}

	r.mu.Lock()
	r.cache[version] = rel
	r.mu.Unlock()
	return rel, nil
}

// downloadBase returns the resolved version and the directory its assets live
// in. An empty version means latest, which GitHub serves behind a redirect —
// following it is how we learn the actual tag, and knowing the concrete tag is
// what lets the plan report "already up to date" honestly.
func (r *HTTPResolver) downloadBase(ctx context.Context, version string) (resolved, base string, err error) {
	if version != "" {
		return version, r.BaseURL + "/download/" + version, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.BaseURL+"/latest", nil)
	if err != nil {
		return "", "", err
	}
	client := *r.Client
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := client.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("upgrade: resolve latest release: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	loc := resp.Header.Get("Location")
	if loc == "" {
		loc = resp.Request.URL.String()
	}
	tag, ok := tagFromLocation(loc)
	if !ok {
		return "", "", fmt.Errorf("upgrade: cannot determine the latest version from %q; pass --version", loc)
	}
	return tag, r.BaseURL + "/download/" + tag, nil
}

func (r *HTTPResolver) fetch(ctx context.Context, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	resp, err := r.Client.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return string(body), err
}

// tagFromLocation pulls "v0.3.1" out of ".../releases/tag/v0.3.1".
func tagFromLocation(loc string) (string, bool) {
	i := strings.LastIndex(loc, "/tag/")
	if i < 0 {
		return "", false
	}
	tag := strings.Trim(loc[i+len("/tag/"):], "/")
	if tag == "" {
		return "", false
	}
	return tag, true
}

// parseChecksums reads "<sha256>  <name>" lines (the sha256sum format the
// release workflow produces).
func parseChecksums(body string) map[string]string {
	out := map[string]string{}
	sc := bufio.NewScanner(strings.NewReader(body))
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) != 2 || len(fields[0]) != 64 {
			continue
		}
		out[strings.TrimPrefix(fields[1], "*")] = fields[0]
	}
	return out
}

// osArchFromAsset maps "zattera-linux-arm64" to "linux/arm64". CLI-only builds
// for darwin/windows are listed too; the planner simply never asks for them,
// since Node.os_arch on a real node is always linux.
func osArchFromAsset(name string) (string, bool) {
	if !strings.HasPrefix(name, assetPrefix) || strings.HasSuffix(name, ".txt") {
		return "", false
	}
	rest := strings.TrimSuffix(strings.TrimPrefix(name, assetPrefix), ".exe")
	os, arch, ok := strings.Cut(rest, "-")
	if !ok || os == "" || arch == "" {
		return "", false
	}
	return os + "/" + arch, true
}
