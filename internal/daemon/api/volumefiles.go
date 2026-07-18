package api

import (
	"context"
	"errors"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

// VolumeFileDialer reaches the AgentLocalService on a volume's pinned node for
// read-only file browsing (T-77). Split from VolumeAgentDialer so a test (or a
// control node with no agent connectivity) can supply one without the other.
type VolumeFileDialer interface {
	ListFiles(ctx context.Context, node *zatterav1.Node, envID, volName, path string) (*clusterv1.AgentListFilesResponse, error)
	ReadFile(ctx context.Context, node *zatterav1.Node, envID, volName, path string, emit func([]byte) error) error
}

// SetFileDialer wires the browse/download data path. Nil leaves ListFiles and
// ReadFile Unimplemented.
func (s *VolumeServer) SetFileDialer(d VolumeFileDialer) { s.files = d }

// ListFiles lists one directory inside a volume. Read-only, and deliberately
// allowed while the volume is mounted: looking at a live volume is the main
// reason to browse one (unlike snapshot/restore/delete, which refuse).
func (s *VolumeServer) ListFiles(ctx context.Context, req *zatterav1.ListFilesRequest) (*zatterav1.ListFilesResponse, error) {
	projectID, err := s.authorizeVolumeRead(ctx, req.GetProjectId())
	if err != nil {
		return nil, err
	}
	vol, node, err := s.volumeNode(projectID, req.GetVolumeId())
	if err != nil {
		return nil, err
	}
	resp, err := s.files.ListFiles(ctx, node, vol.GetEnvironmentId(), vol.GetName(), req.GetPath())
	if err != nil {
		return nil, toStatus(err)
	}
	out := make([]*zatterav1.FileInfo, 0, len(resp.GetFiles()))
	for _, f := range resp.GetFiles() {
		out = append(out, &zatterav1.FileInfo{
			Name:          f.GetName(),
			Dir:           f.GetDir(),
			SizeBytes:     f.GetSizeBytes(),
			ModTimeUnixMs: f.GetModTimeUnixMs(),
			Mode:          f.GetMode(),
		})
	}
	return &zatterav1.ListFilesResponse{Files: out, Truncated: resp.GetTruncated()}, nil
}

// ReadFile streams one file out of a volume.
func (s *VolumeServer) ReadFile(req *zatterav1.ReadFileRequest, stream grpc.ServerStreamingServer[zatterav1.FileChunk]) error {
	// Streams bypass the RBAC interceptor (it is unary-only), so this does the
	// project resolution and membership check itself. Without it ReadFile would
	// be weaker than ListFiles: any authenticated user could read any project's
	// volume files by guessing a path.
	projectID, err := s.authorizeVolumeRead(stream.Context(), req.GetProjectId())
	if err != nil {
		return err
	}
	vol, node, err := s.volumeNode(projectID, req.GetVolumeId())
	if err != nil {
		return err
	}
	return toStatus(s.files.ReadFile(stream.Context(), node, vol.GetEnvironmentId(), vol.GetName(), req.GetPath(), func(b []byte) error {
		return stream.Send(&zatterav1.FileChunk{Data: b})
	}))
}

// authorizeVolumeRead resolves a project name-or-id and confirms the caller may
// read it. The unary path has already been through RBAC; running it again is a
// no-op there and the only check on the streaming path.
func (s *VolumeServer) authorizeVolumeRead(ctx context.Context, projectRef string) (string, error) {
	if projectRef == "" {
		return "", status.Error(codes.InvalidArgument, "project is required")
	}
	projectID := projectRef
	if _, ok := s.store.Project(projectID); !ok {
		p, ok := s.store.ProjectByName(projectRef)
		if !ok {
			return "", status.Error(codes.NotFound, "project not found")
		}
		projectID = p.GetMeta().GetId()
	}
	id, ok := IdentityFrom(ctx)
	if !ok || id.UserID == "" {
		return "", status.Error(codes.Unauthenticated, "a user identity is required")
	}
	if isOrgAdminUser(s.store, id.UserID) {
		return projectID, nil
	}
	if _, member := s.store.ProjectMember(projectID, id.UserID); !member {
		// Non-members must not learn the project exists.
		return "", status.Error(codes.NotFound, "project not found")
	}
	return projectID, nil
}

// volumeNode resolves a project-scoped volume and the node it is pinned to.
func (s *VolumeServer) volumeNode(projectID, volumeID string) (*zatterav1.Volume, *zatterav1.Node, error) {
	if s.files == nil {
		return nil, nil, status.Error(codes.Unimplemented, "volume file access is not available on this node")
	}
	v, ok := s.store.Volume(volumeID)
	if !ok || v.GetProjectId() != projectID {
		return nil, nil, status.Errorf(codes.NotFound, "volume %q not found", volumeID)
	}
	if v.GetNodeId() == "" {
		return nil, nil, status.Errorf(codes.FailedPrecondition, "volume %q is not placed on a node yet", v.GetName())
	}
	node, ok := s.store.Node(v.GetNodeId())
	if !ok {
		return nil, nil, status.Errorf(codes.FailedPrecondition, "volume %q is pinned to node %s, which is not in the cluster", v.GetName(), v.GetNodeId())
	}
	if node.GetStatus() == zatterav1.NodeStatus_NODE_STATUS_DOWN {
		return nil, nil, status.Errorf(codes.Unavailable, "volume %q lives on node %s, which is down", v.GetName(), node.GetName())
	}
	return v, node, nil
}

// GRPCVolumeFileDialer is the production VolumeFileDialer.
type GRPCVolumeFileDialer struct {
	Connect func(ctx context.Context, node *zatterav1.Node) (*grpc.ClientConn, error)
}

func (g GRPCVolumeFileDialer) ListFiles(ctx context.Context, node *zatterav1.Node, envID, volName, path string) (*clusterv1.AgentListFilesResponse, error) {
	conn, err := g.Connect(ctx, node)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	return clusterv1.NewAgentLocalServiceClient(conn).ListVolumeFiles(ctx, &clusterv1.AgentListFilesRequest{
		EnvironmentId: envID, VolumeName: volName, Path: path,
	})
}

func (g GRPCVolumeFileDialer) ReadFile(ctx context.Context, node *zatterav1.Node, envID, volName, path string, emit func([]byte) error) error {
	conn, err := g.Connect(ctx, node)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	stream, err := clusterv1.NewAgentLocalServiceClient(conn).ReadVolumeFile(ctx, &clusterv1.AgentReadFileRequest{
		EnvironmentId: envID, VolumeName: volName, Path: path,
	})
	if err != nil {
		return err
	}
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if len(chunk.GetData()) > 0 {
			if err := emit(chunk.GetData()); err != nil {
				return err
			}
		}
	}
}
