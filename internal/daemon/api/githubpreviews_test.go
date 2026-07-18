package api

import (
	"context"
	"log/slog"
	"testing"
	"time"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/github"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
)

// previewHarness wires the real adapter over a real raft test store so the
// commands actually round-trip through the FSM.
func previewHarness(t *testing.T) (*githubWebhook, *github.App, string) {
	t.Helper()
	rs := raftstore.NewTestStore(t)
	st := rs.State()
	clk := clock.NewFake()

	projID, appID := ids.New(), ids.New()
	st.PutProject(&zatterav1.Project{Meta: &zatterav1.Meta{Id: projID}, Name: "acme"})
	st.PutApp(&zatterav1.App{
		Meta: &zatterav1.Meta{Id: appID}, ProjectId: projID, Name: "api",
	})
	stagingID := ids.New()
	st.PutEnvironment(&zatterav1.Environment{
		Meta: &zatterav1.Meta{Id: stagingID}, AppId: appID, ProjectId: projID,
		Name: "staging", Type: zatterav1.EnvironmentType_ENVIRONMENT_TYPE_STAGING,
		Service: &zatterav1.ServiceSpec{
			Ports:    []*zatterav1.PortSpec{{Name: "http", ContainerPort: 8080}},
			Replicas: &zatterav1.ReplicaRange{Min: 2, Max: 4},
		},
	})

	gw := &githubWebhook{store: st, raft: rs, clk: clk, domain: "example.com", log: nil}
	gw.log = slog.New(slog.DiscardHandler)
	return gw, &github.App{ProjectID: projID, AppID: appID, Repo: "acme/api"}, stagingID
}

// TestPreviewsAdapterLifecycle covers the control-plane side of T-75: creating a
// preview clones the staging spec into a PREVIEW environment, listing surfaces
// it with its head SHA, touch extends the TTL, and delete removes it.
func TestPreviewsAdapterLifecycle(t *testing.T) {
	gw, app, stagingID := previewHarness(t)
	ctx := context.Background()
	exp := gw.clk.Now().Add(github.DefaultPreviewTTL)

	pe, err := gw.CreatePreview(ctx, app, "preview-42", 42, exp)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	env, ok := gw.store.Environment(pe.ID)
	if !ok {
		t.Fatal("preview environment not in state")
	}
	if env.GetType() != zatterav1.EnvironmentType_ENVIRONMENT_TYPE_PREVIEW {
		t.Fatalf("type = %v, want PREVIEW", env.GetType())
	}
	if env.GetName() != "preview-42" || env.GetPreviewPrNumber() != 42 {
		t.Fatalf("name/pr = %q/%d", env.GetName(), env.GetPreviewPrNumber())
	}
	// Spec is cloned from staging, not shared with it.
	staging, _ := gw.store.Environment(stagingID)
	if got := env.GetService().GetReplicas().GetMin(); got != staging.GetService().GetReplicas().GetMin() {
		t.Fatalf("replicas not cloned from staging: %d", got)
	}
	if got := env.GetService().GetPorts(); len(got) != 1 || got[0].GetContainerPort() != 8080 {
		t.Fatalf("ports not cloned: %+v", got)
	}

	// ListPreviews sees it; head SHA is empty until a release exists.
	list := gw.ListPreviews(app.AppID)
	if len(list) != 1 || list[0].PRNumber != 42 || list[0].HeadSHA != "" {
		t.Fatalf("ListPreviews = %+v", list)
	}
	// Only previews are listed — staging must not leak in.
	if list[0].Name != "preview-42" {
		t.Fatalf("non-preview environment listed: %+v", list)
	}

	// A release stamps the head SHA the dedupe logic reads.
	gw.store.PutRelease(&zatterav1.Release{
		Meta: &zatterav1.Meta{Id: ids.New()}, EnvironmentId: pe.ID, Version: 1,
		Source: &zatterav1.ReleaseSource{GitSha: "sha1"},
	})
	gw.store.PutRelease(&zatterav1.Release{
		Meta: &zatterav1.Meta{Id: ids.New()}, EnvironmentId: pe.ID, Version: 2,
		Source: &zatterav1.ReleaseSource{GitSha: "sha2"},
	})
	if got := gw.ListPreviews(app.AppID)[0].HeadSHA; got != "sha2" {
		t.Fatalf("head SHA = %q, want the newest release's sha2", got)
	}

	// TouchPreview extends the deadline in state.
	newExp := exp.Add(24 * time.Hour)
	if err := gw.TouchPreview(ctx, pe, newExp); err != nil {
		t.Fatalf("touch: %v", err)
	}
	env, _ = gw.store.Environment(pe.ID)
	if got := env.GetPreviewExpiresAt().AsTime(); !got.Equal(newExp.UTC()) {
		t.Fatalf("expiry = %v, want %v", got, newExp.UTC())
	}

	// The URL matches the route builder's <app>-<env>.<domain> format (T-45).
	if got := gw.PreviewURL(app, "preview-42"); got != "https://api-preview-42.example.com" {
		t.Fatalf("PreviewURL = %q", got)
	}

	if err := gw.DeletePreview(ctx, pe); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, ok := gw.store.Environment(pe.ID); ok {
		t.Fatal("preview environment survived delete")
	}
	if got := gw.ListPreviews(app.AppID); len(got) != 0 {
		t.Fatalf("ListPreviews after delete = %+v", got)
	}
}

// TestPreviewsAdapterBaseEnv checks the base-environment preference order and
// that an app with nothing to clone from fails loudly instead of creating an
// empty preview.
func TestPreviewsAdapterBaseEnv(t *testing.T) {
	gw, app, stagingID := previewHarness(t)

	// staging wins while it exists.
	base, ok := gw.previewBaseEnv(app.AppID)
	if !ok || base.GetMeta().GetId() != stagingID {
		t.Fatalf("base env = %v, want staging", base.GetName())
	}

	// Falls back to production once staging is gone.
	gw.store.DeleteEnvironment(stagingID)
	prodID := ids.New()
	gw.store.PutEnvironment(&zatterav1.Environment{
		Meta: &zatterav1.Meta{Id: prodID}, AppId: app.AppID, ProjectId: app.ProjectID,
		Name: "production", Type: zatterav1.EnvironmentType_ENVIRONMENT_TYPE_PRODUCTION,
		Service: &zatterav1.ServiceSpec{},
	})
	if base, ok = gw.previewBaseEnv(app.AppID); !ok || base.GetMeta().GetId() != prodID {
		t.Fatalf("base env = %v, want production", base.GetName())
	}

	// With no non-preview environment left, creation is refused.
	gw.store.DeleteEnvironment(prodID)
	if _, ok := gw.previewBaseEnv(app.AppID); ok {
		t.Fatal("expected no base environment")
	}
	if _, err := gw.CreatePreview(context.Background(), app, "preview-1", 1, gw.clk.Now()); err == nil {
		t.Fatal("expected CreatePreview to fail without a base environment")
	}
}

// TestPreviewsAdapterAllPreviews checks the janitor's cluster-wide view.
func TestPreviewsAdapterAllPreviews(t *testing.T) {
	gw, app, _ := previewHarness(t)
	ctx := context.Background()
	exp := gw.clk.Now().Add(github.DefaultPreviewTTL)

	for _, pr := range []int64{1, 2} {
		if _, err := gw.CreatePreview(ctx, app, github.PreviewEnvName(pr), pr, exp); err != nil {
			t.Fatalf("create pr %d: %v", pr, err)
		}
	}
	all := gw.AllPreviews()
	if len(all) != 2 {
		t.Fatalf("AllPreviews = %d, want 2 (staging must not appear)", len(all))
	}
	for _, e := range all {
		if e.ExpiresAt.IsZero() {
			t.Fatalf("preview %s has no deadline — the janitor would never reap it", e.Name)
		}
	}
}
