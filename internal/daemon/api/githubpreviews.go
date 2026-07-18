package api

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/github"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
)

// previewBaseEnvs is the preference order for the environment a preview clones
// its service spec from (spec §3.6: "copied from staging").
var previewBaseEnvs = []string{"staging", "production"}

// ListPreviews returns the app's preview environments, newest release SHA
// included so the caller can skip redundant rebuilds.
func (g *githubWebhook) ListPreviews(appID string) []github.PreviewEnv {
	var out []github.PreviewEnv
	for _, env := range g.store.ListEnvironments("", appID) {
		if pe, ok := g.toPreviewEnv(env); ok {
			out = append(out, pe)
		}
	}
	return out
}

// AllPreviews returns every preview environment in the cluster (janitor).
func (g *githubWebhook) AllPreviews() []github.PreviewEnv {
	var out []github.PreviewEnv
	for _, proj := range g.store.ListProjects() {
		for _, app := range g.store.ListApps(proj.GetMeta().GetId()) {
			out = append(out, g.ListPreviews(app.GetMeta().GetId())...)
		}
	}
	return out
}

func (g *githubWebhook) toPreviewEnv(env *zatterav1.Environment) (github.PreviewEnv, bool) {
	if env.GetType() != zatterav1.EnvironmentType_ENVIRONMENT_TYPE_PREVIEW || env.GetPreviewPrNumber() <= 0 {
		return github.PreviewEnv{}, false
	}
	return github.PreviewEnv{
		ID:        env.GetMeta().GetId(),
		Name:      env.GetName(),
		AppID:     env.GetAppId(),
		ProjectID: env.GetProjectId(),
		PRNumber:  env.GetPreviewPrNumber(),
		HeadSHA:   g.latestReleaseSHA(env.GetMeta().GetId()),
		ExpiresAt: env.GetPreviewExpiresAt().AsTime(),
	}, true
}

// latestReleaseSHA is the git commit of the environment's highest-versioned
// release, or "" if it has never been deployed.
func (g *githubWebhook) latestReleaseSHA(envID string) string {
	var best *zatterav1.Release
	for _, r := range g.store.ListReleases(envID) {
		if best == nil || r.GetVersion() > best.GetVersion() {
			best = r
		}
	}
	return best.GetSource().GetGitSha()
}

// CreatePreview creates a PREVIEW environment cloning the base env's spec. The
// env name IS the hostname label, so the route builder gives us
// <app>-preview-<n>.<domain> for free (T-45).
func (g *githubWebhook) CreatePreview(ctx context.Context, app *github.App, name string, pr int64, expiresAt time.Time) (github.PreviewEnv, error) {
	base, ok := g.previewBaseEnv(app.AppID)
	if !ok {
		return github.PreviewEnv{}, fmt.Errorf("github: app %s has no base environment to clone a preview from", app.AppID)
	}
	spec := proto.Clone(base.GetService()).(*zatterav1.ServiceSpec)
	env := &zatterav1.Environment{
		Meta:             newMeta(ids.New(), g.clk.Now()),
		AppId:            app.AppID,
		ProjectId:        app.ProjectID,
		Name:             name,
		Type:             zatterav1.EnvironmentType_ENVIRONMENT_TYPE_PREVIEW,
		Service:          spec,
		PreviewPrNumber:  pr,
		PreviewExpiresAt: timestamppb.New(expiresAt),
		IdleTimeout:      base.GetIdleTimeout(),
	}
	if err := g.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_PutEnvironment{
		PutEnvironment: &clusterv1.PutEnvironment{Environment: env},
	}}); err != nil {
		return github.PreviewEnv{}, err
	}
	// Preview env vars start as a copy of the base environment's so the app can
	// actually boot; they stay encrypted (the sealed values are copied as-is).
	if vars := g.store.EnvVars(base.GetMeta().GetId()); len(vars) > 0 {
		if err := g.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_SetEnvVars{
			SetEnvVars: &clusterv1.SetEnvVars{EnvironmentId: env.GetMeta().GetId(), Set: vars},
		}}); err != nil {
			return github.PreviewEnv{}, err
		}
	}
	return github.PreviewEnv{
		ID: env.GetMeta().GetId(), Name: name, AppID: app.AppID,
		ProjectID: app.ProjectID, PRNumber: pr, ExpiresAt: expiresAt,
	}, nil
}

// previewBaseEnv picks the environment a preview clones from: staging, then
// production, then any non-preview environment.
func (g *githubWebhook) previewBaseEnv(appID string) (*zatterav1.Environment, bool) {
	for _, name := range previewBaseEnvs {
		if env, ok := g.store.EnvironmentByName(appID, name); ok {
			return env, true
		}
	}
	for _, env := range g.store.ListEnvironments("", appID) {
		if env.GetType() != zatterav1.EnvironmentType_ENVIRONMENT_TYPE_PREVIEW {
			return env, true
		}
	}
	return nil, false
}

// TouchPreview extends a preview's TTL.
func (g *githubWebhook) TouchPreview(ctx context.Context, pe github.PreviewEnv, expiresAt time.Time) error {
	env, ok := g.store.Environment(pe.ID)
	if !ok {
		return fmt.Errorf("github: preview environment %s not found", pe.ID)
	}
	env.PreviewExpiresAt = timestamppb.New(expiresAt)
	return g.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_PutEnvironment{
		PutEnvironment: &clusterv1.PutEnvironment{Environment: env},
	}})
}

// DeletePreview removes the environment. Containers, releases and routes are
// reaped by the scheduler's orphan reconciler once the env is gone.
func (g *githubWebhook) DeletePreview(ctx context.Context, pe github.PreviewEnv) error {
	return g.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_DeleteEnvironment{
		DeleteEnvironment: &clusterv1.DeleteByID{Id: pe.ID},
	}})
}

// PreviewURL mirrors the route builder's cluster-subdomain format (T-45).
func (g *githubWebhook) PreviewURL(app *github.App, envName string) string {
	appObj, ok := g.store.App(app.AppID)
	if !ok || g.domain == "" {
		return ""
	}
	return fmt.Sprintf("https://%s-%s.%s", appObj.GetName(), envName, g.domain)
}

// CommentPR posts a comment on a pull request via the GitHub App.
func (g *githubWebhook) CommentPR(ctx context.Context, token, repo string, pr int64, body string) error {
	app, err := g.githubApp()
	if err != nil {
		return err
	}
	return app.CommentPR(ctx, token, repo, pr, body)
}
