package api

import (
	"context"
	"io"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
)

// seedProjectTree seeds org + project demo + app web (production/staging) with
// one sealed env var. Returns the store.
func seedProjectTree(t *testing.T) (*raftstore.Store, secrets.Sealer) {
	t.Helper()
	rs := raftstore.NewTestStore(t)
	dataKey, _ := secrets.GenerateDataKey()
	vault := mustVault(mustKeyring(dataKey, 1))

	pid := ids.New()
	appID := ids.New()
	prodID := ids.New()
	sealed, _ := vault.Seal([]byte("s3cret"))
	cmds := []*clusterv1.Command{
		{Mutation: &clusterv1.Command_PutOrg{PutOrg: &clusterv1.PutOrg{Org: &zatterav1.Org{Meta: metaID(ids.New()), Name: "default"}}}},
		{Mutation: &clusterv1.Command_PutProject{PutProject: &clusterv1.PutProject{Project: &zatterav1.Project{Meta: metaID(pid), Name: "demo"}}}},
		{Mutation: &clusterv1.Command_PutApp{PutApp: &clusterv1.PutApp{App: &zatterav1.App{Meta: metaID(appID), ProjectId: pid, Name: "web", Build: &zatterav1.BuildConfig{Type: zatterav1.BuildType_BUILD_TYPE_DOCKERFILE, DockerfilePath: "Dockerfile"}}}}},
		{Mutation: &clusterv1.Command_PutEnvironment{PutEnvironment: &clusterv1.PutEnvironment{Environment: &zatterav1.Environment{Meta: metaID(prodID), AppId: appID, ProjectId: pid, Name: "production", Type: zatterav1.EnvironmentType_ENVIRONMENT_TYPE_PRODUCTION, Service: defaultServiceSpec()}}}},
		{Mutation: &clusterv1.Command_PutEnvironment{PutEnvironment: &clusterv1.PutEnvironment{Environment: &zatterav1.Environment{Meta: metaID(ids.New()), AppId: appID, ProjectId: pid, Name: "staging", Type: zatterav1.EnvironmentType_ENVIRONMENT_TYPE_STAGING, Service: defaultServiceSpec()}}}},
		{Mutation: &clusterv1.Command_SetEnvVars{SetEnvVars: &clusterv1.SetEnvVars{EnvironmentId: prodID, Set: map[string]*zatterav1.EncryptedValue{"API_KEY": sealed}}}},
	}
	for _, c := range cmds {
		c.RequestId = ids.New()
		if err := rs.Apply(context.Background(), c); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	return rs, vault
}

func metaID(id string) *zatterav1.Meta {
	return &zatterav1.Meta{Id: id}
}

func TestStateRoundTrip(t *testing.T) {
	src, vault := seedProjectTree(t)
	srcSrv := NewStateServer(src.State(), src, clock.Real{})

	doc, err := srcSrv.buildDocument("")
	if err != nil {
		t.Fatalf("build document: %v", err)
	}

	// Apply into a fresh store (simulating export → wipe → apply).
	dst := raftstore.NewTestStore(t)
	dstSrv := NewStateServer(dst.State(), dst, clock.Real{})
	resp := &zatterav1.ApplyResponse{}
	if err := dstSrv.applyDocument(context.Background(), doc, false, resp); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if resp.GetCreated() == 0 {
		t.Fatalf("expected creations, got %+v", resp)
	}

	// Projects / apps / envs reproduced by name.
	proj, ok := dst.State().ProjectByName("demo")
	if !ok {
		t.Fatal("project demo not reproduced")
	}
	app, ok := dst.State().AppByName(proj.GetMeta().GetId(), "web")
	if !ok {
		t.Fatal("app web not reproduced")
	}
	if app.GetBuild().GetType() != zatterav1.BuildType_BUILD_TYPE_DOCKERFILE {
		t.Errorf("build not reproduced: %+v", app.GetBuild())
	}
	envs := dst.State().ListEnvironments(proj.GetMeta().GetId(), app.GetMeta().GetId())
	if len(envs) != 2 {
		t.Fatalf("want 2 envs, got %d", len(envs))
	}
	// Env var sealed value round-trips byte-identically.
	prod, _ := dst.State().EnvironmentByName(app.GetMeta().GetId(), "production")
	got := dst.State().EnvVars(prod.GetMeta().GetId())
	want := src.State().EnvVars(prodEnvID(t, src))
	if len(got) != 1 || !proto.Equal(got["API_KEY"], want["API_KEY"]) {
		t.Fatalf("env var not round-tripped: got %+v want %+v", got, want)
	}
	// And it still decrypts with the original vault.
	pt, err := vault.Open(got["API_KEY"])
	if err != nil || string(pt) != "s3cret" {
		t.Fatalf("sealed value corrupted: %q %v", pt, err)
	}

	// Second apply is fully idempotent: all unchanged.
	resp2 := &zatterav1.ApplyResponse{}
	if err := dstSrv.applyDocument(context.Background(), doc, false, resp2); err != nil {
		t.Fatalf("re-apply: %v", err)
	}
	if resp2.GetCreated() != 0 || resp2.GetUpdated() != 0 {
		t.Errorf("re-apply not idempotent: %+v", resp2)
	}
}

func prodEnvID(t *testing.T, rs *raftstore.Store) string {
	t.Helper()
	p, _ := rs.State().ProjectByName("demo")
	a, _ := rs.State().AppByName(p.GetMeta().GetId(), "web")
	e, _ := rs.State().EnvironmentByName(a.GetMeta().GetId(), "production")
	return e.GetMeta().GetId()
}

func TestStateApplyDryRun(t *testing.T) {
	src, _ := seedProjectTree(t)
	doc, _ := NewStateServer(src.State(), src, clock.Real{}).buildDocument("")

	dst := raftstore.NewTestStore(t)
	dstSrv := NewStateServer(dst.State(), dst, clock.Real{})
	resp := &zatterav1.ApplyResponse{}
	if err := dstSrv.applyDocument(context.Background(), doc, true, resp); err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if resp.GetCreated() == 0 {
		t.Errorf("dry run should still count creations: %+v", resp)
	}
	// Nothing was written.
	if _, ok := dst.State().ProjectByName("demo"); ok {
		t.Error("dry run wrote to the store")
	}
}

func TestStateExportApplyStreaming(t *testing.T) {
	src, _ := seedProjectTree(t)
	srcSrv := NewStateServer(src.State(), src, clock.Real{})

	// Export via the streaming wrapper.
	es := &fakeExportStream{ctx: context.Background()}
	if err := srcSrv.Export(&zatterav1.ExportRequest{}, es); err != nil {
		t.Fatalf("export: %v", err)
	}
	if len(es.chunks) == 0 {
		t.Fatal("export produced no chunks")
	}

	// Apply via the streaming wrapper into a fresh store.
	dst := raftstore.NewTestStore(t)
	dstSrv := NewStateServer(dst.State(), dst, clock.Real{})
	as := &fakeApplyStream{ctx: context.Background(), chunks: es.chunks}
	if err := dstSrv.Apply(as); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if as.resp == nil || as.resp.GetCreated() == 0 {
		t.Fatalf("streaming apply response: %+v", as.resp)
	}
	if _, ok := dst.State().ProjectByName("demo"); !ok {
		t.Error("streaming apply did not reproduce project")
	}
}

// --- fake gRPC streams ---

type fakeExportStream struct {
	grpc.ServerStream
	ctx    context.Context
	chunks [][]byte
}

func (f *fakeExportStream) Context() context.Context { return f.ctx }
func (f *fakeExportStream) Send(c *zatterav1.StateChunk) error {
	f.chunks = append(f.chunks, append([]byte(nil), c.GetData()...))
	return nil
}

type fakeApplyStream struct {
	grpc.ServerStream
	ctx    context.Context
	chunks [][]byte
	i      int
	resp   *zatterav1.ApplyResponse
}

func (f *fakeApplyStream) Context() context.Context { return f.ctx }
func (f *fakeApplyStream) Recv() (*zatterav1.StateChunk, error) {
	if f.i >= len(f.chunks) {
		return nil, io.EOF
	}
	c := &zatterav1.StateChunk{Data: f.chunks[f.i]}
	f.i++
	return c, nil
}
func (f *fakeApplyStream) SendAndClose(r *zatterav1.ApplyResponse) error {
	f.resp = r
	return nil
}

func TestMetadataFlag(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(dryRunMDKey, "true"))
	if !metadataFlag(ctx, dryRunMDKey) {
		t.Error("dry-run flag not detected")
	}
	if metadataFlag(context.Background(), dryRunMDKey) {
		t.Error("false positive on empty ctx")
	}
}
