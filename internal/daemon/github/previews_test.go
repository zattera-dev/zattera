package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// --- fake preview store ---

// fakePreviewStore is an in-memory PreviewStore. It mirrors the real adapter's
// contract: HeadSHA reflects the last deploy, so the test must record deploys
// through it (recordDeploy) for the SHA-dedupe path to be exercised honestly.
type fakePreviewStore struct {
	mu   sync.Mutex
	envs []PreviewEnv
	next int
}

func (s *fakePreviewStore) ListPreviews(appID string) []PreviewEnv {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []PreviewEnv
	for _, e := range s.envs {
		if e.AppID == appID {
			out = append(out, e)
		}
	}
	return out
}

func (s *fakePreviewStore) AllPreviews() []PreviewEnv {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]PreviewEnv(nil), s.envs...)
}

func (s *fakePreviewStore) CreatePreview(_ context.Context, app *App, name string, pr int64, exp time.Time) (PreviewEnv, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.next++
	env := PreviewEnv{
		ID: fmt.Sprintf("env-%d", s.next), Name: name, AppID: app.AppID,
		ProjectID: app.ProjectID, PRNumber: pr, ExpiresAt: exp,
	}
	s.envs = append(s.envs, env)
	return env, nil
}

func (s *fakePreviewStore) TouchPreview(_ context.Context, env PreviewEnv, exp time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.envs {
		if s.envs[i].ID == env.ID {
			s.envs[i].ExpiresAt = exp
			return nil
		}
	}
	return errors.New("not found")
}

func (s *fakePreviewStore) DeletePreview(_ context.Context, env PreviewEnv) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.envs {
		if s.envs[i].ID == env.ID {
			s.envs = append(s.envs[:i], s.envs[i+1:]...)
			return nil
		}
	}
	return nil
}

func (s *fakePreviewStore) PreviewURL(_ *App, envName string) string {
	return "https://api-" + envName + ".example.com"
}

// recordDeploy stamps the deployed SHA on the env, the way a real release does.
func (s *fakePreviewStore) recordDeploy(envName, sha string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.envs {
		if s.envs[i].Name == envName {
			s.envs[i].HeadSHA = sha
		}
	}
}

func (s *fakePreviewStore) names() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.envs))
	for _, e := range s.envs {
		out = append(out, e.Name)
	}
	return out
}

// shaRecordingDeployer feeds deploys back into the store like the control plane
// does (build → release → env head SHA).
type shaRecordingDeployer struct {
	*fakeDeployer
	store *fakePreviewStore
}

func (d shaRecordingDeployer) DeployGit(ctx context.Context, app *App, env, branch, sha, cloneURL, token string) (string, error) {
	id, err := d.fakeDeployer.DeployGit(ctx, app, env, branch, sha, cloneURL, token)
	if err == nil {
		d.store.recordDeploy(env, sha)
	}
	return id, err
}

type fakeComments struct {
	mu   sync.Mutex
	body []string
}

func (c *fakeComments) CommentPR(_ context.Context, _, _ string, pr int64, body string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.body = append(c.body, fmt.Sprintf("#%d %s", pr, body))
	return nil
}

func (c *fakeComments) all() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.body...)
}

func newPreviewsFixture(t *testing.T) (*Previews, *fakePreviewStore, *fakeDeployer, *fakeComments, *clock.Fake) {
	t.Helper()
	store := &fakePreviewStore{}
	dep := &fakeDeployer{}
	comments := &fakeComments{}
	clk := clock.NewFake()
	p := NewPreviews(store, shaRecordingDeployer{dep, store}, fakeTokens{token: "tok"}, comments, clk, nil)
	return p, store, dep, comments, clk
}

func prEvent(action string, n int64, sha string) PullRequestEvent {
	return PullRequestEvent{
		Action: action, Number: n, Branch: "feature", HeadSHA: sha,
		CloneURL: "https://github.com/acme/api.git",
	}
}

// TestPreviewsLifecycle walks a PR through opened → synchronize → closed: the
// env is created and deployed, redeployed on a new commit, and deleted on close.
func TestPreviewsLifecycle(t *testing.T) {
	p, store, dep, comments, _ := newPreviewsFixture(t)
	app := testApp()
	ctx := context.Background()

	if err := p.OnPullRequest(ctx, app, prEvent("opened", 42, "sha1")); err != nil {
		t.Fatalf("opened: %v", err)
	}
	if got := store.names(); len(got) != 1 || got[0] != "preview-42" {
		t.Fatalf("environments after open = %v, want [preview-42]", got)
	}
	if len(dep.calls) != 1 || dep.calls[0].env != "preview-42" || dep.calls[0].sha != "sha1" {
		t.Fatalf("deploy calls after open = %+v", dep.calls)
	}
	if dep.calls[0].token != "tok" {
		t.Fatalf("deploy used token %q, want the installation token", dep.calls[0].token)
	}
	// The URL is announced exactly once, on creation.
	if c := comments.all(); len(c) != 1 || !strings.Contains(c[0], "https://api-preview-42.example.com") {
		t.Fatalf("comments after open = %v", c)
	}

	if err := p.OnPullRequest(ctx, app, prEvent("synchronize", 42, "sha2")); err != nil {
		t.Fatalf("synchronize: %v", err)
	}
	if len(dep.calls) != 2 || dep.calls[1].sha != "sha2" {
		t.Fatalf("deploy calls after synchronize = %+v", dep.calls)
	}
	if got := store.names(); len(got) != 1 {
		t.Fatalf("synchronize must reuse the environment, got %v", got)
	}
	if c := comments.all(); len(c) != 1 {
		t.Fatalf("synchronize must not re-comment, got %v", c)
	}

	if err := p.OnPullRequest(ctx, app, prEvent("closed", 42, "sha2")); err != nil {
		t.Fatalf("closed: %v", err)
	}
	if got := store.names(); len(got) != 0 {
		t.Fatalf("close must delete the preview, got %v", got)
	}
	// Closing an already-reaped PR is a no-op, not an error.
	if err := p.OnPullRequest(ctx, app, prEvent("closed", 42, "sha2")); err != nil {
		t.Fatalf("repeat close: %v", err)
	}
}

// TestPreviewsSHADedupe covers the force-push storm guard: a synchronize for a
// SHA already deployed extends the TTL but does not rebuild.
func TestPreviewsSHADedupe(t *testing.T) {
	p, store, dep, _, clk := newPreviewsFixture(t)
	app := testApp()
	ctx := context.Background()

	if err := p.OnPullRequest(ctx, app, prEvent("opened", 7, "sha1")); err != nil {
		t.Fatalf("opened: %v", err)
	}
	firstExpiry := store.AllPreviews()[0].ExpiresAt

	clk.Advance(time.Hour)
	for i := 0; i < 3; i++ {
		if err := p.OnPullRequest(ctx, app, prEvent("synchronize", 7, "sha1")); err != nil {
			t.Fatalf("synchronize %d: %v", i, err)
		}
	}
	if len(dep.calls) != 1 {
		t.Fatalf("redelivered synchronize rebuilt: %d deploys, want 1", len(dep.calls))
	}
	if got := store.AllPreviews()[0].ExpiresAt; !got.After(firstExpiry) {
		t.Fatalf("TTL not extended: %v <= %v", got, firstExpiry)
	}

	// A genuinely new commit does rebuild.
	if err := p.OnPullRequest(ctx, app, prEvent("synchronize", 7, "sha2")); err != nil {
		t.Fatalf("new sha: %v", err)
	}
	if len(dep.calls) != 2 {
		t.Fatalf("new commit did not rebuild: %d deploys", len(dep.calls))
	}
}

// TestPreviewsCap proves the per-app cap holds: the cap exists to protect the
// cluster's Let's Encrypt rate limit, so the (N+1)th PR must be refused rather
// than provisioning another certificate-bearing hostname.
func TestPreviewsCap(t *testing.T) {
	p, store, dep, comments, _ := newPreviewsFixture(t)
	app := testApp()
	ctx := context.Background()

	for i := int64(1); i <= DefaultMaxPreviewsPerApp; i++ {
		if err := p.OnPullRequest(ctx, app, prEvent("opened", i, fmt.Sprintf("sha%d", i))); err != nil {
			t.Fatalf("open pr %d: %v", i, err)
		}
	}
	if got := len(store.names()); got != DefaultMaxPreviewsPerApp {
		t.Fatalf("previews at cap = %d, want %d", got, DefaultMaxPreviewsPerApp)
	}

	err := p.OnPullRequest(ctx, app, prEvent("opened", 99, "sha99"))
	if !errors.Is(err, ErrPreviewCapReached) {
		t.Fatalf("over-cap open error = %v, want ErrPreviewCapReached", err)
	}
	if got := len(store.names()); got != DefaultMaxPreviewsPerApp {
		t.Fatalf("over-cap open created an environment: %v", store.names())
	}
	if got := len(dep.calls); got != DefaultMaxPreviewsPerApp {
		t.Fatalf("over-cap open deployed: %d builds", got)
	}
	// The author is told why, rather than the PR silently getting no preview.
	last := comments.all()[len(comments.all())-1]
	if !strings.Contains(last, "#99") || !strings.Contains(last, "cap") {
		t.Fatalf("no cap explanation commented, got %q", last)
	}

	// Closing one frees a slot.
	if err := p.OnPullRequest(ctx, app, prEvent("closed", 1, "sha1")); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := p.OnPullRequest(ctx, app, prEvent("opened", 99, "sha99")); err != nil {
		t.Fatalf("open after freeing a slot: %v", err)
	}
}

// TestPreviewsJanitor drives the TTL sweep with a fake clock.
func TestPreviewsJanitor(t *testing.T) {
	p, store, _, _, clk := newPreviewsFixture(t)
	app := testApp()
	ctx := context.Background()

	if err := p.OnPullRequest(ctx, app, prEvent("opened", 1, "sha1")); err != nil {
		t.Fatalf("open 1: %v", err)
	}
	clk.Advance(DefaultPreviewTTL - time.Hour)
	if err := p.OnPullRequest(ctx, app, prEvent("opened", 2, "sha2")); err != nil {
		t.Fatalf("open 2: %v", err)
	}

	if n := p.SweepExpired(ctx); n != 0 {
		t.Fatalf("swept %d before any TTL elapsed", n)
	}

	// Cross PR 1's deadline but not PR 2's.
	clk.Advance(2 * time.Hour)
	if n := p.SweepExpired(ctx); n != 1 {
		t.Fatalf("swept %d, want 1 (only PR 1 expired)", n)
	}
	if got := store.names(); len(got) != 1 || got[0] != "preview-2" {
		t.Fatalf("survivors = %v, want [preview-2]", got)
	}

	clk.Advance(DefaultPreviewTTL)
	if n := p.SweepExpired(ctx); n != 1 {
		t.Fatalf("second sweep = %d, want 1", n)
	}
	if got := store.names(); len(got) != 0 {
		t.Fatalf("expected all previews reaped, got %v", got)
	}
}

// TestPreviewsWebhookRouting checks the HTTP layer: a signed pull_request
// delivery reaches the preview manager, duplicates are dropped, and the handler
// is inert when previews are not enabled.
func TestPreviewsWebhookRouting(t *testing.T) {
	p, store, _, _, _ := newPreviewsFixture(t)
	app := testApp()
	dedup := &fakeDedup{}
	h := NewWebhook(fakeApps{app: app}, &fakeDeployer{}, dedup, fakeTokens{token: "tok"}, nil)

	body, _ := json.Marshal(map[string]any{
		"action":     "opened",
		"number":     11,
		"repository": map[string]string{"full_name": "acme/api", "clone_url": "https://github.com/acme/api.git"},
		"pull_request": map[string]any{
			"head": map[string]string{"ref": "feature", "sha": "abc123"},
		},
	})
	post := func(delivery string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/v1/github/webhook", strings.NewReader(string(body)))
		req.Header.Set("X-GitHub-Event", "pull_request")
		req.Header.Set("X-GitHub-Delivery", delivery)
		req.Header.Set("X-Hub-Signature-256", SignPayload(app.WebhookSecret, body))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		h.Wait()
		return rec
	}

	// Previews disabled: accepted and ignored, no environment.
	if rec := post("d0"); rec.Code != http.StatusOK {
		t.Fatalf("disabled status = %d", rec.Code)
	}
	if got := store.names(); len(got) != 0 {
		t.Fatalf("environment created while previews disabled: %v", got)
	}

	h.EnablePreviews(p)
	if rec := post("d1"); rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if got := store.names(); len(got) != 1 || got[0] != "preview-11" {
		t.Fatalf("environments = %v, want [preview-11]", got)
	}

	// A redelivery of the same id must not touch anything.
	post("d1")
	if got := store.names(); len(got) != 1 {
		t.Fatalf("redelivery created a second environment: %v", got)
	}
}
