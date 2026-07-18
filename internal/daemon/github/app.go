package github

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// defaultAPIBase is GitHub's REST API root; injectable for tests.
const defaultAPIBase = "https://api.github.com"

// GitHubApp authenticates as a GitHub App: it signs a short-lived JWT with the
// app private key, exchanges it for per-installation access tokens (cached
// until just before expiry), and posts commit statuses.
type GitHubApp struct {
	appID   int64
	key     *rsa.PrivateKey
	http    *http.Client
	baseURL string
	clk     clock.Clock

	mu    sync.Mutex
	cache map[int64]cachedToken
}

type cachedToken struct {
	token   string
	expires time.Time
}

// Option configures a GitHubApp.
type Option func(*GitHubApp)

// WithBaseURL overrides the GitHub API base (tests point it at httptest).
func WithBaseURL(u string) Option {
	return func(g *GitHubApp) { g.baseURL = strings.TrimRight(u, "/") }
}

// WithHTTPClient overrides the HTTP client.
func WithHTTPClient(c *http.Client) Option { return func(g *GitHubApp) { g.http = c } }

// WithClock overrides the clock (tests use a fake).
func WithClock(c clock.Clock) Option { return func(g *GitHubApp) { g.clk = c } }

// NewGitHubApp builds an app authenticator from a PEM-encoded RSA private key.
func NewGitHubApp(appID int64, pemKey []byte, opts ...Option) (*GitHubApp, error) {
	key, err := parseRSAKey(pemKey)
	if err != nil {
		return nil, err
	}
	g := &GitHubApp{
		appID: appID, key: key,
		http: http.DefaultClient, baseURL: defaultAPIBase, clk: clock.Real{},
		cache: map[int64]cachedToken{},
	}
	for _, o := range opts {
		o(g)
	}
	return g, nil
}

// InstallationToken returns a cached-or-fresh access token for an installation.
func (g *GitHubApp) InstallationToken(ctx context.Context, installationID int64) (string, error) {
	now := g.clk.Now()
	g.mu.Lock()
	if t, ok := g.cache[installationID]; ok && now.Before(t.expires.Add(-5*time.Minute)) {
		g.mu.Unlock()
		return t.token, nil
	}
	g.mu.Unlock()

	jwt, err := g.appJWT(now)
	if err != nil {
		return "", err
	}
	url := fmt.Sprintf("%s/app/installations/%d/access_tokens", g.baseURL, installationID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+jwt)
	req.Header.Set("Accept", "application/vnd.github+json")
	resp, err := g.http.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("github: installation token: HTTP %d", resp.StatusCode)
	}
	var out struct {
		Token     string    `json:"token"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	g.mu.Lock()
	g.cache[installationID] = cachedToken{token: out.Token, expires: out.ExpiresAt}
	g.mu.Unlock()
	return out.Token, nil
}

// CommitStatus states for SetCommitStatus.
const (
	StatusPending = "pending"
	StatusSuccess = "success"
	StatusFailure = "failure"
)

// SetCommitStatus posts a commit status (pending/success/failure) using an
// installation token.
func (g *GitHubApp) SetCommitStatus(ctx context.Context, token, repo, sha, state, targetURL, description string) error {
	body, _ := json.Marshal(map[string]string{
		"state": state, "target_url": targetURL, "description": description, "context": "zattera",
	})
	url := fmt.Sprintf("%s/repos/%s/statuses/%s", g.baseURL, repo, sha)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "token "+token)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Content-Type", "application/json")
	resp, err := g.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("github: set commit status: HTTP %d", resp.StatusCode)
	}
	return nil
}

// CommentPR posts an issue comment on a pull request (PRs are issues in the
// REST API) using an installation token — used to announce preview URLs (T-75).
func (g *GitHubApp) CommentPR(ctx context.Context, token, repo string, pr int64, comment string) error {
	body, _ := json.Marshal(map[string]string{"body": comment})
	url := fmt.Sprintf("%s/repos/%s/issues/%d/comments", g.baseURL, repo, pr)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "token "+token)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Content-Type", "application/json")
	resp, err := g.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("github: comment on PR %d: HTTP %d", pr, resp.StatusCode)
	}
	return nil
}

// appJWT builds a signed RS256 JWT for the app (10-minute validity, per
// GitHub's contract). Hand-rolled to avoid a JWT dependency.
func (g *GitHubApp) appJWT(now time.Time) (string, error) {
	header := b64url([]byte(`{"alg":"RS256","typ":"JWT"}`))
	claims, _ := json.Marshal(map[string]any{
		"iat": now.Add(-30 * time.Second).Unix(), // clock-skew allowance
		"exp": now.Add(9 * time.Minute).Unix(),
		"iss": strconv.FormatInt(g.appID, 10),
	})
	signingInput := header + "." + b64url(claims)
	digest := sha256.Sum256([]byte(signingInput))
	sig, err := rsa.SignPKCS1v15(rand.Reader, g.key, crypto.SHA256, digest[:])
	if err != nil {
		return "", fmt.Errorf("github: sign jwt: %w", err)
	}
	return signingInput + "." + b64url(sig), nil
}

func parseRSAKey(pemKey []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(pemKey)
	if block == nil {
		return nil, fmt.Errorf("github: no PEM block in app key")
	}
	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("github: parse app key: %w", err)
	}
	rsaKey, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("github: app key is not RSA")
	}
	return rsaKey, nil
}

func b64url(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }
