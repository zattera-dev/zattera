// Package github implements push-to-deploy: a webhook endpoint that turns
// GitHub push events into builds, and GitHub App authentication for cloning
// private repos and posting commit statuses (spec F9, T-37).
package github

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

// maxWebhookBytes bounds a webhook body (GitHub payloads are well under this).
const maxWebhookBytes = 5 << 20

// App is the resolved GitHub configuration for a repository.
type App struct {
	ProjectID          string
	AppID              string
	Repo               string // "owner/name"
	InstallationID     int64
	WebhookSecret      []byte // unsealed HMAC secret
	BranchEnvironments map[string]string
}

// AppStore resolves a repo ("owner/name") to its configured app.
type AppStore interface {
	AppByRepo(repo string) (*App, bool)
}

// Deployer creates a build + deployment for a pushed commit.
type Deployer interface {
	DeployGit(ctx context.Context, app *App, envName, branch, sha, cloneURL, token string) (deploymentID string, err error)
}

// Deduper records processed delivery ids (with a TTL) so redelivered webhooks
// are not built twice. Seen returns true if the id was already recorded.
type Deduper interface {
	Seen(deliveryID string) bool
}

// TokenSource mints GitHub App installation access tokens for cloning.
type TokenSource interface {
	InstallationToken(ctx context.Context, installationID int64) (string, error)
}

// Webhook is the POST /v1/github/webhook handler.
type Webhook struct {
	apps     AppStore
	deployer Deployer
	dedup    Deduper
	tokens   TokenSource
	previews *Previews // nil until EnablePreviews (T-75)
	log      *slog.Logger

	inflight sync.WaitGroup
}

// EnablePreviews turns on PR → preview-<n> environments (T-75). Without it
// pull_request events are acknowledged and ignored.
func (h *Webhook) EnablePreviews(p *Previews) { h.previews = p }

// NewWebhook builds the webhook handler.
func NewWebhook(apps AppStore, deployer Deployer, dedup Deduper, tokens TokenSource, log *slog.Logger) *Webhook {
	if log == nil {
		log = slog.Default()
	}
	return &Webhook{apps: apps, deployer: deployer, dedup: dedup, tokens: tokens, log: log}
}

// Wait blocks until all async deploy jobs finish (used by tests).
func (h *Webhook) Wait() { h.inflight.Wait() }

type repoEnvelope struct {
	Repository struct {
		FullName string `json:"full_name"`
		CloneURL string `json:"clone_url"`
	} `json:"repository"`
}

type pushPayload struct {
	Ref   string `json:"ref"`   // "refs/heads/<branch>"
	After string `json:"after"` // pushed SHA
}

type prPayload struct {
	Action      string `json:"action"`
	Number      int64  `json:"number"`
	PullRequest struct {
		Head struct {
			Ref string `json:"ref"`
			SHA string `json:"sha"`
		} `json:"head"`
	} `json:"pull_request"`
}

func (h *Webhook) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxWebhookBytes))
	if err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}

	// Identify the app from the (still unverified) payload so we can load its
	// secret, then verify the signature over the raw body.
	var env repoEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		http.Error(w, "bad payload", http.StatusBadRequest)
		return
	}
	app, ok := h.apps.AppByRepo(env.Repository.FullName)
	if !ok {
		// Nothing configured for this repo: accept and ignore (no retries).
		h.log.Debug("github webhook for unconfigured repo", "repo", env.Repository.FullName)
		writeJSON(w, http.StatusAccepted, map[string]string{"status": "ignored"})
		return
	}
	if !validSignature(r.Header.Get("X-Hub-Signature-256"), app.WebhookSecret, body) {
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}

	switch r.Header.Get("X-GitHub-Event") {
	case "ping":
		writeJSON(w, http.StatusOK, map[string]bool{"pong": true})
	case "push":
		h.handlePush(r, app, env, body, w)
	case "pull_request":
		h.handlePullRequest(r, app, env, body, w)
	default:
		writeJSON(w, http.StatusOK, map[string]string{"status": "ignored"})
	}
}

func (h *Webhook) handlePush(r *http.Request, app *App, env repoEnvelope, body []byte, w http.ResponseWriter) {
	delivery := r.Header.Get("X-GitHub-Delivery")
	if delivery != "" && h.dedup.Seen(delivery) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "duplicate"})
		return
	}
	var push pushPayload
	if err := json.Unmarshal(body, &push); err != nil {
		http.Error(w, "bad push payload", http.StatusBadRequest)
		return
	}
	branch, ok := strings.CutPrefix(push.Ref, "refs/heads/")
	if !ok {
		writeJSON(w, http.StatusOK, map[string]string{"status": "not a branch push"}) // tag/other ref
		return
	}
	envName, ok := app.BranchEnvironments[branch]
	if !ok {
		h.log.Debug("github push to unmapped branch", "repo", app.Repo, "branch", branch)
		writeJSON(w, http.StatusOK, map[string]string{"status": "branch not mapped"})
		return
	}

	// Respond fast; fetch the installation token and create the deployment in
	// the background (spec: webhook must return within ~1s).
	cloneURL := env.Repository.CloneURL
	h.inflight.Add(1)
	go func() {
		defer h.inflight.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		token, err := h.tokens.InstallationToken(ctx, app.InstallationID)
		if err != nil {
			h.log.Error("github installation token", "repo", app.Repo, "err", err)
			return
		}
		depID, err := h.deployer.DeployGit(ctx, app, envName, branch, push.After, cloneURL, token)
		if err != nil {
			h.log.Error("github deploy", "repo", app.Repo, "branch", branch, "err", err)
			return
		}
		h.log.Info("github push deployed", "repo", app.Repo, "branch", branch, "env", envName, "deployment", depID)
	}()
	writeJSON(w, http.StatusOK, map[string]string{"status": "accepted", "environment": envName})
}

// handlePullRequest drives preview environments (T-75). Like push, the reply is
// immediate and the work happens in the background.
func (h *Webhook) handlePullRequest(r *http.Request, app *App, env repoEnvelope, body []byte, w http.ResponseWriter) {
	if h.previews == nil {
		writeJSON(w, http.StatusOK, map[string]string{"status": "previews disabled"})
		return
	}
	delivery := r.Header.Get("X-GitHub-Delivery")
	if delivery != "" && h.dedup.Seen(delivery) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "duplicate"})
		return
	}
	var pr prPayload
	if err := json.Unmarshal(body, &pr); err != nil {
		http.Error(w, "bad pull_request payload", http.StatusBadRequest)
		return
	}
	if pr.Number <= 0 {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ignored"})
		return
	}

	ev := PullRequestEvent{
		Action:   pr.Action,
		Number:   pr.Number,
		Branch:   pr.PullRequest.Head.Ref,
		HeadSHA:  pr.PullRequest.Head.SHA,
		CloneURL: env.Repository.CloneURL,
	}
	h.inflight.Add(1)
	go func() {
		defer h.inflight.Done()
		// Previews create a build + a deployment; allow more room than a push.
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := h.previews.OnPullRequest(ctx, app, ev); err != nil {
			h.log.Error("github preview", "repo", app.Repo, "pr", ev.Number, "action", ev.Action, "err", err)
		}
	}()
	writeJSON(w, http.StatusOK, map[string]string{"status": "accepted", "environment": PreviewEnvName(pr.Number)})
}

// validSignature checks the "sha256=<hex>" X-Hub-Signature-256 header against
// HMAC-SHA256(secret, body) in constant time.
func validSignature(header string, secret, body []byte) bool {
	got, ok := strings.CutPrefix(header, "sha256=")
	if !ok || len(secret) == 0 {
		return false
	}
	gotBytes, err := hex.DecodeString(got)
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	return hmac.Equal(gotBytes, mac.Sum(nil))
}

// SignPayload returns the "sha256=<hex>" signature for a body (used by tests and
// the CLI setup helper).
func SignPayload(secret, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
