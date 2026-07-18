package api

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

// volumeRemoveTimeout bounds the best-effort docker-volume cleanup so a slow or
// down node never stalls a DeleteVolume response.
const volumeRemoveTimeout = 3 * time.Second

// VolumeAgentDialer removes a deleted volume's docker volume on its pinned node
// (best effort; nil disables the cleanup). Production dials AgentLocalService.
type VolumeAgentDialer interface {
	RemoveVolume(ctx context.Context, node *zatterav1.Node, envID, volumeName string) error
}

// VolumeServer implements VolumeService: CRUD (T-62), snapshot/restore
// (T-64/T-65) and read-only file browsing (T-77).
type VolumeServer struct {
	zatterav1.UnimplementedVolumeServiceServer
	store *state.Store
	raft  Applier
	clock clock.Clock
	dial  VolumeAgentDialer
	files VolumeFileDialer    // nil leaves ListFiles/ReadFile Unimplemented (T-77)
	snap  *SnapshotDispatcher // nil until the cluster is unsealed with a backup config
	log   *slog.Logger
}

// NewVolumeServer builds the volume service. dial removes the docker volume on
// its node when the volume is deleted (best effort; nil skips that cleanup).
func NewVolumeServer(store *state.Store, raft Applier, dial VolumeAgentDialer, clk clock.Clock, log *slog.Logger) *VolumeServer {
	if clk == nil {
		clk = clock.Real{}
	}
	if log == nil {
		log = slog.Default()
	}
	return &VolumeServer{store: store, raft: raft, clock: clk, dial: dial, log: log}
}

// WithSnapshots attaches the snapshot dispatcher (enables Snapshot/Restore).
func (s *VolumeServer) WithSnapshots(d *SnapshotDispatcher) *VolumeServer {
	s.snap = d
	return s
}

// CreateVolume creates a named volume pinned to a node. When node_id is empty
// the least-used ALIVE worker is chosen. Names are unique within (project, env).
func (s *VolumeServer) CreateVolume(ctx context.Context, req *zatterav1.CreateVolumeRequest) (*zatterav1.Volume, error) {
	if !validDNSName(req.GetName()) {
		return nil, status.Error(codes.InvalidArgument, "name must be DNS-safe: [a-z0-9-], 1-40 chars")
	}
	env, ok := s.store.Environment(req.GetEnvironmentId())
	if !ok || env.GetProjectId() != req.GetProjectId() {
		return nil, status.Errorf(codes.NotFound, "environment %q not found", req.GetEnvironmentId())
	}
	if _, exists := s.store.VolumeByName(req.GetProjectId(), req.GetEnvironmentId(), req.GetName()); exists {
		return nil, status.Errorf(codes.AlreadyExists, "volume %q already exists in this environment", req.GetName())
	}

	node := req.GetNodeId()
	if node == "" {
		node = leastUsedVolumeNode(s.store)
	} else if _, ok := s.store.Node(node); !ok {
		return nil, status.Errorf(codes.InvalidArgument, "node %q not found", node)
	}
	if node == "" {
		return nil, status.Error(codes.FailedPrecondition, "no schedulable node available for the volume")
	}

	now := timestamppb.New(s.clock.Now())
	v := &zatterav1.Volume{
		Meta:           &zatterav1.Meta{Id: ids.New(), CreatedAt: now, UpdatedAt: now},
		ProjectId:      req.GetProjectId(),
		EnvironmentId:  req.GetEnvironmentId(),
		Name:           req.GetName(),
		NodeId:         node,
		Status:         zatterav1.VolumeStatus_VOLUME_STATUS_ACTIVE,
		SnapshotPolicy: req.GetSnapshotPolicy(),
	}
	if err := s.apply(ctx, &clusterv1.Command{
		Mutation: &clusterv1.Command_PutVolume{PutVolume: &clusterv1.PutVolume{Volume: v}},
	}); err != nil {
		return nil, toStatus(err)
	}
	return v, nil
}

// ListVolumes returns the project's volumes.
func (s *VolumeServer) ListVolumes(_ context.Context, req *zatterav1.ListVolumesRequest) (*zatterav1.ListVolumesResponse, error) {
	return &zatterav1.ListVolumesResponse{Volumes: s.store.ListVolumes(req.GetProjectId())}, nil
}

// SnapshotVolume takes an on-demand snapshot of a volume and returns the
// completed VolumeSnapshot record.
func (s *VolumeServer) SnapshotVolume(ctx context.Context, req *zatterav1.SnapshotVolumeRequest) (*zatterav1.VolumeSnapshot, error) {
	if s.snap == nil {
		return nil, status.Error(codes.FailedPrecondition, "snapshots unavailable: cluster not unsealed or no backup destination")
	}
	v, ok := s.store.Volume(req.GetVolumeId())
	if !ok || v.GetProjectId() != req.GetProjectId() {
		return nil, status.Errorf(codes.NotFound, "volume %q not found", req.GetVolumeId())
	}
	snap, err := s.snap.RunSnapshot(ctx, v)
	if err != nil {
		return nil, toStatus(err)
	}
	return snap, nil
}

// ListSnapshots returns a volume's snapshots (newest first).
func (s *VolumeServer) ListSnapshots(_ context.Context, req *zatterav1.ListSnapshotsRequest) (*zatterav1.ListSnapshotsResponse, error) {
	v, ok := s.store.Volume(req.GetVolumeId())
	if !ok || v.GetProjectId() != req.GetProjectId() {
		return nil, status.Errorf(codes.NotFound, "volume %q not found", req.GetVolumeId())
	}
	return &zatterav1.ListSnapshotsResponse{Snapshots: s.store.ListVolumeSnapshots(req.GetVolumeId())}, nil
}

// RestoreSnapshot restores a snapshot into its volume. It refuses while the
// volume is mounted — the service must be stopped first (single-writer).
func (s *VolumeServer) RestoreSnapshot(ctx context.Context, req *zatterav1.RestoreSnapshotRequest) (*zatterav1.Volume, error) {
	if s.snap == nil {
		return nil, status.Error(codes.FailedPrecondition, "snapshots unavailable: cluster not unsealed or no backup destination")
	}
	v, ok := s.store.Volume(req.GetVolumeId())
	if !ok || v.GetProjectId() != req.GetProjectId() {
		return nil, status.Errorf(codes.NotFound, "volume %q not found", req.GetVolumeId())
	}
	if s.volumeMounted(v) {
		return nil, status.Error(codes.FailedPrecondition,
			"volume is in use; stop the service (scale its environment to 0 replicas) before restoring")
	}
	var snap *zatterav1.VolumeSnapshot
	for _, sn := range s.store.ListVolumeSnapshots(req.GetVolumeId()) {
		if sn.GetMeta().GetId() == req.GetSnapshotId() {
			snap = sn
			break
		}
	}
	if snap == nil || snap.GetStatus() != zatterav1.SnapshotStatus_SNAPSHOT_STATUS_COMPLETE {
		return nil, status.Errorf(codes.NotFound, "no completed snapshot %q for this volume", req.GetSnapshotId())
	}

	if err := s.setVolumeStatus(ctx, v, zatterav1.VolumeStatus_VOLUME_STATUS_RESTORING); err != nil {
		return nil, toStatus(err)
	}
	if err := s.snap.Restore(ctx, v, snap.GetManifestKey()); err != nil {
		_ = s.setVolumeStatus(ctx, v, zatterav1.VolumeStatus_VOLUME_STATUS_ACTIVE)
		return nil, toStatus(err)
	}
	if err := s.setVolumeStatus(ctx, v, zatterav1.VolumeStatus_VOLUME_STATUS_ACTIVE); err != nil {
		return nil, toStatus(err)
	}
	out, _ := s.store.Volume(req.GetVolumeId())
	return out, nil
}

// setVolumeStatus re-reads and updates the volume's status.
func (s *VolumeServer) setVolumeStatus(ctx context.Context, v *zatterav1.Volume, st zatterav1.VolumeStatus) error {
	cur, ok := s.store.Volume(v.GetMeta().GetId())
	if !ok {
		return nil
	}
	cur.Status = st
	cur.GetMeta().UpdatedAt = timestamppb.New(s.clock.Now())
	return s.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_PutVolume{PutVolume: &clusterv1.PutVolume{Volume: cur}}})
}

// DeleteVolume removes a volume. It refuses while the volume is mounted (a live
// fencing lease or a running instance on its node).
func (s *VolumeServer) DeleteVolume(ctx context.Context, req *zatterav1.DeleteVolumeRequest) (*emptypb.Empty, error) {
	v, ok := s.store.Volume(req.GetVolumeId())
	if !ok || v.GetProjectId() != req.GetProjectId() {
		return nil, status.Errorf(codes.NotFound, "volume %q not found", req.GetVolumeId())
	}
	if s.volumeMounted(v) {
		return nil, status.Error(codes.FailedPrecondition, "volume is in use; stop the service before deleting it")
	}
	if err := s.apply(ctx, &clusterv1.Command{
		Mutation: &clusterv1.Command_DeleteVolume{DeleteVolume: &clusterv1.DeleteByID{Id: req.GetVolumeId()}},
	}); err != nil {
		return nil, toStatus(err)
	}
	// Best effort: remove the docker volume on its node. A failure (node down,
	// no runtime) leaves an orphaned volume but never fails the delete.
	s.removeDockerVolume(ctx, v)
	return &emptypb.Empty{}, nil
}

// removeDockerVolume asks the volume's node to delete the underlying docker
// volume. Best effort — logged, never fatal.
func (s *VolumeServer) removeDockerVolume(ctx context.Context, v *zatterav1.Volume) {
	if s.dial == nil || v.GetNodeId() == "" {
		return
	}
	node, ok := s.store.Node(v.GetNodeId())
	if !ok {
		return
	}
	cctx, cancel := context.WithTimeout(ctx, volumeRemoveTimeout)
	defer cancel()
	if err := s.dial.RemoveVolume(cctx, node, v.GetEnvironmentId(), v.GetName()); err != nil {
		s.log.Warn("volume: docker cleanup failed (orphaned on node)", "volume", v.GetMeta().GetId(), "node", v.GetNodeId(), "err", err)
	}
}

// volumeMounted reports whether the volume is currently in use: an unexpired
// fencing lease, or a running (non-job) instance on its pinned node.
func (s *VolumeServer) volumeMounted(v *zatterav1.Volume) bool {
	if l := v.GetLease(); l != nil && l.GetExpiresAt() != nil && s.clock.Now().Before(l.GetExpiresAt().AsTime()) {
		return true
	}
	for _, a := range s.store.ListAssignments(v.GetEnvironmentId()) {
		if a.GetJobId() == "" &&
			a.GetDesired() == zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN &&
			a.GetNodeId() == v.GetNodeId() {
			return true
		}
	}
	return false
}

func (s *VolumeServer) apply(ctx context.Context, cmd *clusterv1.Command) error {
	cmd.RequestId = ids.New()
	id, _ := IdentityFrom(ctx)
	cmd.Actor = "user:" + id.UserID
	cmd.Time = timestamppb.New(s.clock.Now())
	return s.raft.Apply(ctx, cmd)
}

// GRPCVolumeAgentDialer dials a node's AgentLocalService to remove a docker
// volume. Connect supplies the per-node mTLS connection.
type GRPCVolumeAgentDialer struct {
	Connect func(ctx context.Context, node *zatterav1.Node) (*grpc.ClientConn, error)
}

// RemoveVolume runs the RemoveVolume RPC against the node, closing the
// connection after.
func (g GRPCVolumeAgentDialer) RemoveVolume(ctx context.Context, node *zatterav1.Node, envID, volumeName string) error {
	conn, err := g.Connect(ctx, node)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	_, err = clusterv1.NewAgentLocalServiceClient(conn).RemoveVolume(ctx, &clusterv1.AgentRemoveVolumeRequest{
		EnvironmentId: envID,
		VolumeName:    volumeName,
	})
	return err
}

// leastUsedVolumeNode picks the ALIVE schedulable worker hosting the fewest
// volumes (ties broken by node id). Shared shape with the scheduler's picker.
func leastUsedVolumeNode(st *state.Store) string {
	counts := map[string]int{}
	for _, v := range st.ListVolumes("") {
		counts[v.GetNodeId()]++
	}
	best, bestCount := "", 0
	for _, n := range st.ListNodes() {
		id := n.GetMeta().GetId()
		if n.GetStatus() != zatterav1.NodeStatus_NODE_STATUS_ALIVE || !n.GetSchedulable() {
			continue
		}
		c := counts[id]
		if best == "" || c < bestCount || (c == bestCount && id < best) {
			best, bestCount = id, c
		}
	}
	return best
}
