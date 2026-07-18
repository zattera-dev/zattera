package api

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/appconfig"
	"github.com/zattera-dev/zattera/internal/daemon/github"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

// KV keys for GitHub App integration.
const (
	kvGitHubAppKey     = "github/app-key"         // sealed RSA private key + app id
	kvGitHubSecretPfx  = "github/webhook-secret/" // + appID → sealed HMAC secret
	kvGitHubDeliveries = "github/delivery/"       // + delivery id → dedupe marker (TTL)
	kvGitHubBuildPfx   = "github/build/"          // + buildID → clone url (for the git builder)
	deliveryDedupeTTL  = time.Hour
)

// NewGitHubWebhook builds the POST /v1/github/webhook handler backed by cluster
// state. The webhook secret and App private key are read (sealed) from the KV
// store; a sealed vault or missing key degrades gracefully (deploys are skipped).
// The returned *github.Previews is the preview-environment manager; the daemon
// runs its SweepExpired in a leader-gated janitor loop (T-75).
func NewGitHubWebhook(store *state.Store, raft Applier, vault *secrets.Vault, clk clock.Clock, domain string, log *slog.Logger) (http.Handler, *github.Previews) {
	gw := &githubWebhook{store: store, raft: raft, vault: vault, clk: clk, domain: domain, log: log}
	// A late unseal (operator or peer) must clear the memoized failure, or the
	// App stays broken for the process lifetime even once the key is available.
	vault.OnUnseal(gw.resetAppCache)
	previews := github.NewPreviews(gw, gw, gw, gw, clk, log)
	h := github.NewWebhook(gw, gw, gw, gw, log)
	h.EnablePreviews(previews)
	return h, previews
}

// githubWebhook adapts cluster state to the github package's interfaces.
type githubWebhook struct {
	store  *state.Store
	raft   Applier
	vault  *secrets.Vault
	clk    clock.Clock
	domain string
	log    *slog.Logger

	mu     sync.Mutex
	ghApp  *github.GitHubApp // built lazily from the sealed app key
	ghErr  error
	loaded bool
}

// AppByRepo finds the app configured for a repo and unseals its webhook secret.
func (g *githubWebhook) AppByRepo(repo string) (*github.App, bool) {
	for _, proj := range g.store.ListProjects() {
		for _, app := range g.store.ListApps(proj.GetMeta().GetId()) {
			gh := app.GetGithub()
			if gh.GetRepo() != repo || repo == "" {
				continue
			}
			return &github.App{
				ProjectID:          app.GetProjectId(),
				AppID:              app.GetMeta().GetId(),
				Repo:               repo,
				InstallationID:     gh.GetInstallationId(),
				WebhookSecret:      g.webhookSecret(app.GetMeta().GetId()),
				BranchEnvironments: gh.GetBranchEnvironments(),
			}, true
		}
	}
	return nil, false
}

func (g *githubWebhook) webhookSecret(appID string) []byte {
	if !g.vault.Unsealed() {
		return nil
	}
	raw, _, _, ok := g.store.KV(kvGitHubSecretPfx + appID)
	if !ok {
		return nil
	}
	var enc zatterav1.EncryptedValue
	if err := proto.Unmarshal(raw, &enc); err != nil {
		return nil
	}
	pt, err := g.vault.Open(&enc)
	if err != nil {
		return nil
	}
	return pt
}

// Seen records a delivery id with a TTL so redelivered webhooks are ignored.
func (g *githubWebhook) Seen(deliveryID string) bool {
	if _, _, _, ok := g.store.KV(kvGitHubDeliveries + deliveryID); ok {
		return true
	}
	_ = g.apply(context.Background(), &clusterv1.Command{Mutation: &clusterv1.Command_PutKv{PutKv: &clusterv1.PutKV{
		Key:             kvGitHubDeliveries + deliveryID,
		Value:           []byte{1},
		ExpectedVersion: -1,
		ExpiresAt:       timestamppb.New(g.clk.Now().Add(deliveryDedupeTTL)),
	}}})
	return false
}

// InstallationToken mints a token via the App key stored (sealed) in the KV.
func (g *githubWebhook) InstallationToken(ctx context.Context, installationID int64) (string, error) {
	app, err := g.githubApp()
	if err != nil {
		return "", err
	}
	return app.InstallationToken(ctx, installationID)
}

// githubApp lazily builds the App authenticator from the sealed KV key.
func (g *githubWebhook) githubApp() (*github.GitHubApp, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.loaded {
		return g.ghApp, g.ghErr
	}
	if !g.vault.Unsealed() {
		// Deliberately not memoized: a sealed cluster is a transient state, and
		// caching this would survive the unseal that fixes it.
		return nil, fmt.Errorf("github: cluster key not unsealed")
	}
	g.loaded = true
	raw, _, _, ok := g.store.KV(kvGitHubAppKey)
	if !ok {
		g.ghErr = fmt.Errorf("github: app key not configured")
		return nil, g.ghErr
	}
	var enc zatterav1.EncryptedValue
	if err := proto.Unmarshal(raw, &enc); err != nil {
		g.ghErr = err
		return nil, err
	}
	pt, err := g.vault.Open(&enc)
	if err != nil {
		g.ghErr = err
		return nil, err
	}
	// Stored as "<appID>\n<PEM>".
	appID, pemKey, err := splitAppKey(pt)
	if err != nil {
		g.ghErr = err
		return nil, err
	}
	g.ghApp, g.ghErr = github.NewGitHubApp(appID, pemKey, github.WithClock(g.clk))
	return g.ghApp, g.ghErr
}

// DeployGit creates a QUEUED git Build + a PENDING Deployment for a push. Clone
// details are stashed in the KV for the builder's git path.
func (g *githubWebhook) DeployGit(ctx context.Context, app *github.App, envName, branch, sha, cloneURL, token string) (string, error) {
	env, ok := g.store.EnvironmentByName(app.AppID, envName)
	if !ok {
		return "", fmt.Errorf("github: environment %q not found", envName)
	}
	appObj, _ := g.store.App(app.AppID)

	now := g.clk.Now()
	build := &zatterav1.Build{
		Meta:          newMeta(ids.New(), now),
		AppId:         app.AppID,
		ProjectId:     app.ProjectID,
		EnvironmentId: env.GetMeta().GetId(),
		Type:          appObj.GetBuild().GetType(),
		Status:        zatterav1.BuildStatus_BUILD_STATUS_QUEUED,
		GitSha:        sha,
		Platforms:     appObj.GetBuild().GetPlatforms(),
	}
	spec := proto.Clone(env.GetService()).(*zatterav1.ServiceSpec)
	rel := &zatterav1.Release{
		Meta:          newMeta(ids.New(), now),
		EnvironmentId: env.GetMeta().GetId(),
		AppId:         app.AppID,
		ProjectId:     app.ProjectID,
		Version:       g.store.NextReleaseVersion(env.GetMeta().GetId()),
		ConfigHash:    appconfig.ConfigHash(spec, 0),
		Service:       spec,
		Source:        &zatterav1.ReleaseSource{GitSha: sha, GitBranch: branch, BuildId: build.GetMeta().GetId()},
	}
	dep := &zatterav1.Deployment{
		Meta:          newMeta(ids.New(), now),
		EnvironmentId: env.GetMeta().GetId(),
		AppId:         app.AppID,
		ProjectId:     app.ProjectID,
		ReleaseId:     rel.GetMeta().GetId(),
		BuildId:       build.GetMeta().GetId(),
		Phase:         zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PENDING,
	}

	for _, cmd := range []*clusterv1.Command{
		{Mutation: &clusterv1.Command_PutBuild{PutBuild: &clusterv1.PutBuild{Build: build}}},
		{Mutation: &clusterv1.Command_PutRelease{PutRelease: &clusterv1.PutRelease{Release: rel}}},
		{Mutation: &clusterv1.Command_PutDeployment{PutDeployment: &clusterv1.PutDeployment{Deployment: dep}}},
		{Mutation: &clusterv1.Command_PutKv{PutKv: &clusterv1.PutKV{Key: kvGitHubBuildPfx + build.GetMeta().GetId(), Value: []byte(cloneURL), ExpectedVersion: -1}}},
	} {
		if err := g.apply(ctx, cmd); err != nil {
			return "", err
		}
	}
	return dep.GetMeta().GetId(), nil
}

func (g *githubWebhook) apply(ctx context.Context, cmd *clusterv1.Command) error {
	cmd.RequestId = ids.New()
	cmd.Actor = "system:github"
	cmd.Time = timestamppb.New(g.clk.Now())
	return g.raft.Apply(ctx, cmd)
}

// splitAppKey parses the stored "<appID>\n<PEM>" blob.
func splitAppKey(b []byte) (int64, []byte, error) {
	for i, c := range b {
		if c == '\n' {
			var id int64
			if _, err := fmt.Sscanf(string(b[:i]), "%d", &id); err != nil {
				return 0, nil, fmt.Errorf("github: bad app id in key blob: %w", err)
			}
			return id, b[i+1:], nil
		}
	}
	return 0, nil, fmt.Errorf("github: malformed app key blob")
}

// resetAppCache clears the memoized GitHub App so it is rebuilt after an
// unseal. Registered as a vault OnUnseal hook.
func (g *githubWebhook) resetAppCache() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.ghApp, g.ghErr, g.loaded = nil, nil, false
}
