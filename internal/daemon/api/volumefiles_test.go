package api

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
)

// fakeFileDialer records what the control plane asked the node for.
type fakeFileDialer struct {
	files    []*clusterv1.AgentFileInfo
	truncate bool
	blob     []byte
	err      error

	gotNode, gotEnv, gotVol, gotPath string
}

func (f *fakeFileDialer) ListFiles(_ context.Context, node *zatterav1.Node, envID, volName, path string) (*clusterv1.AgentListFilesResponse, error) {
	f.gotNode, f.gotEnv, f.gotVol, f.gotPath = node.GetMeta().GetId(), envID, volName, path
	if f.err != nil {
		return nil, f.err
	}
	return &clusterv1.AgentListFilesResponse{Files: f.files, Truncated: f.truncate}, nil
}

func (f *fakeFileDialer) ReadFile(_ context.Context, node *zatterav1.Node, envID, volName, path string, emit func([]byte) error) error {
	f.gotNode, f.gotEnv, f.gotVol, f.gotPath = node.GetMeta().GetId(), envID, volName, path
	if f.err != nil {
		return f.err
	}
	return emit(f.blob)
}

// chunkCollector captures a ReadFile server stream.
type chunkCollector struct {
	grpc.ServerStream
	ctx  context.Context
	data []byte
}

func (c *chunkCollector) Context() context.Context { return c.ctx }
func (c *chunkCollector) Send(ch *zatterav1.FileChunk) error {
	c.data = append(c.data, ch.GetData()...)
	return nil
}

// volumeFilesHarness seeds a project with a volume pinned to a live node, an
// org admin, a project member and an outsider.
func volumeFilesHarness(t *testing.T) (*VolumeServer, *fakeFileDialer, string, string, map[string]string) {
	t.Helper()
	rs := raftstore.NewTestStore(t)
	st := rs.State()

	projID, nodeID, volID := ids.New(), ids.New(), ids.New()
	st.PutProject(&zatterav1.Project{Meta: &zatterav1.Meta{Id: projID}, Name: "demo"})
	st.PutNode(&zatterav1.Node{
		Meta: &zatterav1.Meta{Id: nodeID}, Name: "node-1",
		Status: zatterav1.NodeStatus_NODE_STATUS_ALIVE,
	})
	st.PutVolume(&zatterav1.Volume{
		Meta: &zatterav1.Meta{Id: volID}, ProjectId: projID, EnvironmentId: "env1",
		Name: "pg-data", NodeId: nodeID,
	})

	users := map[string]string{"admin": ids.New(), "member": ids.New(), "outsider": ids.New()}
	st.PutUser(&zatterav1.User{Meta: &zatterav1.Meta{Id: users["admin"]}, Email: "a@x", OrgRole: zatterav1.Role_ROLE_ADMIN})
	st.PutUser(&zatterav1.User{Meta: &zatterav1.Meta{Id: users["member"]}, Email: "m@x", OrgRole: zatterav1.Role_ROLE_DEVELOPER})
	st.PutUser(&zatterav1.User{Meta: &zatterav1.Meta{Id: users["outsider"]}, Email: "o@x", OrgRole: zatterav1.Role_ROLE_DEVELOPER})
	st.PutProjectMember(&zatterav1.ProjectMember{ProjectId: projID, UserId: users["member"], Role: zatterav1.Role_ROLE_VIEWER})

	dialer := &fakeFileDialer{
		files: []*clusterv1.AgentFileInfo{
			{Name: "data", Dir: true, Mode: "drwxr-xr-x"},
			{Name: "dump.sql", SizeBytes: 2048, ModTimeUnixMs: 1700, Mode: "-rw-r--r--"},
		},
		blob: []byte("SELECT 1;"),
	}
	srv := NewVolumeServer(st, rs, nil, nil, nil)
	srv.SetFileDialer(dialer)
	return srv, dialer, projID, volID, users
}

// TestVolumeListFiles covers the happy path and the field mapping to the node.
func TestVolumeListFiles(t *testing.T) {
	srv, dialer, projID, volID, users := volumeFilesHarness(t)
	ctx := callerCtx(users["admin"])

	resp, err := srv.ListFiles(ctx, &zatterav1.ListFilesRequest{
		ProjectId: projID, VolumeId: volID, Path: "/data",
	})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(resp.GetFiles()) != 2 {
		t.Fatalf("got %d files", len(resp.GetFiles()))
	}
	if resp.GetFiles()[0].GetName() != "data" || !resp.GetFiles()[0].GetDir() {
		t.Errorf("first entry not carried through: %+v", resp.GetFiles()[0])
	}
	if got := resp.GetFiles()[1]; got.GetSizeBytes() != 2048 || got.GetMode() != "-rw-r--r--" {
		t.Errorf("metadata lost in mapping: %+v", got)
	}
	// The node leg gets the volume's environment and logical name, and derives
	// the docker name itself — same contract as RemoveVolume.
	if dialer.gotEnv != "env1" || dialer.gotVol != "pg-data" || dialer.gotPath != "/data" {
		t.Errorf("agent call = env %q vol %q path %q", dialer.gotEnv, dialer.gotVol, dialer.gotPath)
	}

	dialer.truncate = true
	resp, err = srv.ListFiles(ctx, &zatterav1.ListFilesRequest{ProjectId: projID, VolumeId: volID, Path: "/"})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.GetTruncated() {
		t.Error("truncated flag not propagated from the node")
	}
}

// TestVolumeReadFile covers the streaming download.
func TestVolumeReadFile(t *testing.T) {
	srv, dialer, projID, volID, users := volumeFilesHarness(t)
	sink := &chunkCollector{ctx: callerCtx(users["member"])}

	if err := srv.ReadFile(&zatterav1.ReadFileRequest{
		ProjectId: projID, VolumeId: volID, Path: "/data/dump.sql",
	}, sink); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(sink.data) != "SELECT 1;" {
		t.Fatalf("streamed %q", sink.data)
	}
	if dialer.gotPath != "/data/dump.sql" {
		t.Errorf("agent path = %q", dialer.gotPath)
	}
}

// TestVolumeFilesAuthorization is the important one: ReadFile is a server
// stream, and the RBAC interceptor is unary-only, so the handler must do the
// project scoping itself. If it does not, any authenticated user can read any
// project's volume files.
func TestVolumeFilesAuthorization(t *testing.T) {
	srv, _, projID, volID, users := volumeFilesHarness(t)

	t.Run("member may list and read", func(t *testing.T) {
		ctx := callerCtx(users["member"])
		if _, err := srv.ListFiles(ctx, &zatterav1.ListFilesRequest{ProjectId: projID, VolumeId: volID}); err != nil {
			t.Errorf("member list: %v", err)
		}
		sink := &chunkCollector{ctx: ctx}
		if err := srv.ReadFile(&zatterav1.ReadFileRequest{ProjectId: projID, VolumeId: volID, Path: "/f"}, sink); err != nil {
			t.Errorf("member read: %v", err)
		}
	})

	t.Run("non-member is refused on both", func(t *testing.T) {
		ctx := callerCtx(users["outsider"])
		if _, err := srv.ListFiles(ctx, &zatterav1.ListFilesRequest{ProjectId: projID, VolumeId: volID}); status.Code(err) != codes.NotFound {
			t.Errorf("outsider list = %v, want NotFound", err)
		}
		sink := &chunkCollector{ctx: ctx}
		err := srv.ReadFile(&zatterav1.ReadFileRequest{ProjectId: projID, VolumeId: volID, Path: "/f"}, sink)
		if status.Code(err) != codes.NotFound {
			t.Errorf("outsider read = %v, want NotFound", err)
		}
		if len(sink.data) != 0 {
			t.Errorf("outsider received %d bytes", len(sink.data))
		}
	})

	t.Run("anonymous is refused", func(t *testing.T) {
		if _, err := srv.ListFiles(context.Background(), &zatterav1.ListFilesRequest{ProjectId: projID, VolumeId: volID}); status.Code(err) != codes.Unauthenticated {
			t.Errorf("anonymous list = %v, want Unauthenticated", err)
		}
		sink := &chunkCollector{ctx: context.Background()}
		if err := srv.ReadFile(&zatterav1.ReadFileRequest{ProjectId: projID, VolumeId: volID}, sink); status.Code(err) != codes.Unauthenticated {
			t.Errorf("anonymous read = %v, want Unauthenticated", err)
		}
	})

	t.Run("project name resolves like an id", func(t *testing.T) {
		// The streaming path never goes through the RBAC name rewrite, so the
		// handler must accept a name too — otherwise `--project demo` silently
		// 404s on download but works on listing.
		sink := &chunkCollector{ctx: callerCtx(users["member"])}
		if err := srv.ReadFile(&zatterav1.ReadFileRequest{ProjectId: "demo", VolumeId: volID, Path: "/f"}, sink); err != nil {
			t.Errorf("read by project name: %v", err)
		}
	})

	t.Run("wrong project cannot reach the volume", func(t *testing.T) {
		ctx := callerCtx(users["admin"])
		other := ids.New()
		srv.store.PutProject(&zatterav1.Project{Meta: &zatterav1.Meta{Id: other}, Name: "other"})
		if _, err := srv.ListFiles(ctx, &zatterav1.ListFilesRequest{ProjectId: other, VolumeId: volID}); status.Code(err) != codes.NotFound {
			t.Errorf("cross-project list = %v, want NotFound", err)
		}
	})
}

// TestVolumeFilesUnavailable checks the states where there is nothing to talk
// to, so the operator gets a reason rather than a timeout.
func TestVolumeFilesUnavailable(t *testing.T) {
	srv, dialer, projID, volID, users := volumeFilesHarness(t)
	ctx := callerCtx(users["admin"])

	t.Run("node down", func(t *testing.T) {
		vol, _ := srv.store.Volume(volID)
		node, _ := srv.store.Node(vol.GetNodeId())
		node.Status = zatterav1.NodeStatus_NODE_STATUS_DOWN
		srv.store.PutNode(node)
		defer func() {
			node.Status = zatterav1.NodeStatus_NODE_STATUS_ALIVE
			srv.store.PutNode(node)
		}()

		_, err := srv.ListFiles(ctx, &zatterav1.ListFilesRequest{ProjectId: projID, VolumeId: volID})
		if status.Code(err) != codes.Unavailable {
			t.Errorf("list on a down node = %v, want Unavailable", err)
		}
	})

	t.Run("unplaced volume", func(t *testing.T) {
		id := ids.New()
		srv.store.PutVolume(&zatterav1.Volume{
			Meta: &zatterav1.Meta{Id: id}, ProjectId: projID, Name: "fresh",
		})
		_, err := srv.ListFiles(ctx, &zatterav1.ListFilesRequest{ProjectId: projID, VolumeId: id})
		if status.Code(err) != codes.FailedPrecondition {
			t.Errorf("unplaced volume = %v, want FailedPrecondition", err)
		}
	})

	t.Run("node error is surfaced", func(t *testing.T) {
		dialer.err = errors.New("dial node-1: connection refused")
		defer func() { dialer.err = nil }()
		if _, err := srv.ListFiles(ctx, &zatterav1.ListFilesRequest{ProjectId: projID, VolumeId: volID}); err == nil {
			t.Error("node failure was swallowed")
		}
	})

	t.Run("no file dialer configured", func(t *testing.T) {
		bare := NewVolumeServer(srv.store, nil, nil, nil, nil)
		if _, err := bare.ListFiles(ctx, &zatterav1.ListFilesRequest{ProjectId: projID, VolumeId: volID}); status.Code(err) != codes.Unimplemented {
			t.Errorf("without a dialer = %v, want Unimplemented", err)
		}
	})
}
