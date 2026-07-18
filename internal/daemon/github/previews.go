package github

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// Preview environment policy (spec §3.6, T-75).
const (
	// DefaultPreviewTTL is how long a preview survives without a new push. Every
	// PR event extends it; the janitor deletes previews past their deadline.
	DefaultPreviewTTL = 7 * 24 * time.Hour
	// DefaultMaxPreviewsPerApp caps concurrent previews per app. The cap exists
	// to protect the cluster's Let's Encrypt rate limit: every preview needs a
	// certificate for its own <app>-preview-<n>.<domain> hostname.
	DefaultMaxPreviewsPerApp = 5
	// previewEnvPrefix names preview environments; the env name IS the hostname
	// label, so it must stay DNS-safe.
	previewEnvPrefix = "preview-"
)

// ErrPreviewCapReached is returned when an app already has the maximum number
// of concurrent preview environments.
var ErrPreviewCapReached = errors.New("github: preview environment cap reached")

// PreviewEnvName is the environment name for a pull request.
func PreviewEnvName(pr int64) string { return fmt.Sprintf("%s%d", previewEnvPrefix, pr) }

// IsPreviewEnvName reports whether name belongs to a preview environment.
func IsPreviewEnvName(name string) bool { return strings.HasPrefix(name, previewEnvPrefix) }

// PreviewEnv is the control plane's view of one preview environment.
type PreviewEnv struct {
	ID        string
	Name      string
	AppID     string
	ProjectID string
	PRNumber  int64
	// HeadSHA is the commit of the environment's newest release, used to skip
	// redundant rebuilds when GitHub replays a synchronize for a SHA we already
	// deployed (force-push storms send several in a row).
	HeadSHA   string
	ExpiresAt time.Time
}

// PreviewStore is the control-plane port for preview environment lifecycle. The
// api package implements it against cluster state + raft.
type PreviewStore interface {
	// ListPreviews returns the app's current preview environments.
	ListPreviews(appID string) []PreviewEnv
	// AllPreviews returns every preview environment in the cluster (janitor).
	AllPreviews() []PreviewEnv
	// CreatePreview creates a PREVIEW environment named name, cloning its
	// service spec from the app's base (staging) environment.
	CreatePreview(ctx context.Context, app *App, name string, pr int64, expiresAt time.Time) (PreviewEnv, error)
	// TouchPreview extends an existing preview's TTL.
	TouchPreview(ctx context.Context, env PreviewEnv, expiresAt time.Time) error
	// DeletePreview removes the environment; teardown of its containers is a
	// cascade handled by the scheduler's orphan reconciler.
	DeletePreview(ctx context.Context, env PreviewEnv) error
	// PreviewURL renders the public URL an environment is reachable at.
	PreviewURL(app *App, envName string) string
}

// Commenter posts a comment on a pull request.
type Commenter interface {
	CommentPR(ctx context.Context, token, repo string, pr int64, body string) error
}

// PullRequestEvent is the subset of GitHub's pull_request payload we act on.
type PullRequestEvent struct {
	Action   string
	Number   int64
	Branch   string // head ref
	HeadSHA  string
	CloneURL string
}

// Previews turns pull-request events into preview environments and reaps
// expired ones. All policy (cap, TTL, SHA dedupe) lives here; storage and
// GitHub I/O are ports so this is unit-testable end to end.
type Previews struct {
	store    PreviewStore
	deployer Deployer
	tokens   TokenSource
	comments Commenter
	clk      clock.Clock
	log      *slog.Logger

	ttl time.Duration
	max int
}

// NewPreviews builds the preview manager. comments may be nil (no PR comments).
func NewPreviews(store PreviewStore, deployer Deployer, tokens TokenSource, comments Commenter, clk clock.Clock, log *slog.Logger) *Previews {
	if clk == nil {
		clk = clock.Real{}
	}
	if log == nil {
		log = slog.Default()
	}
	return &Previews{
		store: store, deployer: deployer, tokens: tokens, comments: comments,
		clk: clk, log: log, ttl: DefaultPreviewTTL, max: DefaultMaxPreviewsPerApp,
	}
}

// OnPullRequest applies a pull_request event: open/reopen/synchronize ensure a
// preview exists and is deployed at the head SHA; close deletes it.
func (p *Previews) OnPullRequest(ctx context.Context, app *App, ev PullRequestEvent) error {
	switch ev.Action {
	case "opened", "reopened", "synchronize":
		return p.ensure(ctx, app, ev)
	case "closed":
		return p.remove(ctx, app, ev.Number)
	default:
		return nil
	}
}

func (p *Previews) ensure(ctx context.Context, app *App, ev PullRequestEvent) error {
	name := PreviewEnvName(ev.Number)
	current := p.store.ListPreviews(app.AppID)
	existing, found := findPR(current, ev.Number)
	expires := p.clk.Now().Add(p.ttl)

	token, err := p.tokens.InstallationToken(ctx, app.InstallationID)
	if err != nil {
		return fmt.Errorf("github: installation token: %w", err)
	}

	created := false
	switch {
	case !found:
		if len(current) >= p.max {
			p.log.Warn("preview cap reached", "repo", app.Repo, "app", app.AppID, "pr", ev.Number, "cap", p.max)
			p.comment(ctx, token, app.Repo, ev.Number, fmt.Sprintf(
				"Preview environment not created: this app already has %d active previews (the cap). Close another preview and push again.", p.max))
			return ErrPreviewCapReached
		}
		if _, err := p.store.CreatePreview(ctx, app, name, ev.Number, expires); err != nil {
			return err
		}
		created = true
	case existing.HeadSHA != "" && existing.HeadSHA == ev.HeadSHA:
		// Already deployed at this commit (redelivery / force-push storm):
		// keep the preview alive but skip the rebuild.
		p.log.Debug("preview already at head sha", "repo", app.Repo, "pr", ev.Number, "sha", ev.HeadSHA)
		return p.store.TouchPreview(ctx, existing, expires)
	default:
		if err := p.store.TouchPreview(ctx, existing, expires); err != nil {
			return err
		}
	}

	depID, err := p.deployer.DeployGit(ctx, app, name, ev.Branch, ev.HeadSHA, ev.CloneURL, token)
	if err != nil {
		return err
	}
	p.log.Info("preview deployed", "repo", app.Repo, "pr", ev.Number, "env", name, "sha", ev.HeadSHA, "deployment", depID)

	if created {
		url := p.store.PreviewURL(app, name)
		p.comment(ctx, token, app.Repo, ev.Number, fmt.Sprintf(
			"Preview environment `%s` is deploying — it will be live at %s (expires in %s of inactivity).",
			name, url, p.ttl.Round(time.Hour)))
	}
	return nil
}

func (p *Previews) remove(ctx context.Context, app *App, pr int64) error {
	env, found := findPR(p.store.ListPreviews(app.AppID), pr)
	if !found {
		return nil // never created, or already reaped
	}
	if err := p.store.DeletePreview(ctx, env); err != nil {
		return err
	}
	p.log.Info("preview deleted", "repo", app.Repo, "pr", pr, "env", env.Name)
	return nil
}

// SweepExpired deletes every preview past its deadline. Returns the number
// deleted. Called from a leader-gated janitor loop.
func (p *Previews) SweepExpired(ctx context.Context) int {
	now := p.clk.Now()
	n := 0
	for _, env := range p.store.AllPreviews() {
		if env.ExpiresAt.IsZero() || !now.After(env.ExpiresAt) {
			continue
		}
		if err := p.store.DeletePreview(ctx, env); err != nil {
			p.log.Warn("preview janitor delete failed", "env", env.Name, "err", err)
			continue
		}
		p.log.Info("preview expired", "env", env.Name, "pr", env.PRNumber, "app", env.AppID)
		n++
	}
	return n
}

func (p *Previews) comment(ctx context.Context, token, repo string, pr int64, body string) {
	if p.comments == nil {
		return
	}
	if err := p.comments.CommentPR(ctx, token, repo, pr, body); err != nil {
		p.log.Warn("preview PR comment failed", "repo", repo, "pr", pr, "err", err)
	}
}

func findPR(envs []PreviewEnv, pr int64) (PreviewEnv, bool) {
	for _, e := range envs {
		if e.PRNumber == pr {
			return e, true
		}
	}
	return PreviewEnv{}, false
}
