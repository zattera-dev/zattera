package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/daemon/volumes"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

// SnapshotDispatcher runs volume snapshot/restore/prune by dialing the volume's
// node over AgentLocalService with an S3 target built from the (unsealed) backup
// config + cluster data key. It also satisfies scheduler.SnapshotDispatcher.
type SnapshotDispatcher struct {
	store   *state.Store
	raft    Applier
	sealer  secrets.Sealer
	dataKey []byte
	connect func(ctx context.Context, node *zatterav1.Node) (*grpc.ClientConn, error)
	clock   clock.Clock
	log     *slog.Logger
}

// NewSnapshotDispatcher builds the dispatcher. sealer/dataKey come from the
// unsealed keyring; connect dials a node's AgentLocalService.
func NewSnapshotDispatcher(store *state.Store, raft Applier, sealer secrets.Sealer, dataKey []byte, connect func(context.Context, *zatterav1.Node) (*grpc.ClientConn, error), clk clock.Clock, log *slog.Logger) *SnapshotDispatcher {
	if clk == nil {
		clk = clock.Real{}
	}
	if log == nil {
		log = slog.Default()
	}
	return &SnapshotDispatcher{store: store, raft: raft, sealer: sealer, dataKey: dataKey, connect: connect, clock: clk, log: log}
}

// Snapshot (scheduler-facing) fires a snapshot in the background so the leader
// loop never blocks on the upload.
func (d *SnapshotDispatcher) Snapshot(_ context.Context, vol *zatterav1.Volume) error {
	go func() {
		if _, err := d.RunSnapshot(context.Background(), vol); err != nil {
			d.log.Warn("snapshot: run failed", "volume", vol.GetMeta().GetId(), "err", err)
		}
	}()
	return nil
}

// RunSnapshot performs one snapshot synchronously: it records a RUNNING
// snapshot, streams the node-side op, and finalizes the record COMPLETE/FAILED.
func (d *SnapshotDispatcher) RunSnapshot(ctx context.Context, vol *zatterav1.Volume) (*zatterav1.VolumeSnapshot, error) {
	target, err := d.s3Target()
	if err != nil {
		return nil, err
	}
	node, ok := d.store.Node(vol.GetNodeId())
	if !ok {
		return nil, fmt.Errorf("volume node %q not found", vol.GetNodeId())
	}

	snap := &zatterav1.VolumeSnapshot{
		Meta:     &zatterav1.Meta{Id: ids.New(), CreatedAt: timestamppb.New(d.clock.Now())},
		VolumeId: vol.GetMeta().GetId(),
		Status:   zatterav1.SnapshotStatus_SNAPSHOT_STATUS_RUNNING,
	}
	if err := d.putSnapshot(ctx, snap); err != nil {
		return nil, err
	}

	prog, err := d.streamSnapshot(ctx, node, vol, snap, target)
	if err != nil {
		snap.Status = zatterav1.SnapshotStatus_SNAPSHOT_STATUS_FAILED
		snap.Error = err.Error()
		_ = d.putSnapshot(ctx, snap)
		return snap, err
	}
	snap.Status = zatterav1.SnapshotStatus_SNAPSHOT_STATUS_COMPLETE
	snap.ManifestKey = prog.GetManifestKey()
	snap.LogicalSizeBytes = prog.GetBytesTotal()
	snap.UploadedBytes = prog.GetBytesDone()
	if err := d.putSnapshot(ctx, snap); err != nil {
		return nil, err
	}
	return snap, nil
}

// streamSnapshot dials the node and drains the progress stream, returning the
// final "done" message.
func (d *SnapshotDispatcher) streamSnapshot(ctx context.Context, node *zatterav1.Node, volume *zatterav1.Volume, snap *zatterav1.VolumeSnapshot, target *clusterv1.S3Target) (*clusterv1.VolumeOpProgress, error) {
	conn, err := d.connect(ctx, node)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	stream, err := clusterv1.NewAgentLocalServiceClient(conn).SnapshotVolume(ctx, &clusterv1.AgentSnapshotVolumeRequest{
		Volume:         volume,
		Snapshot:       snap,
		PreHookCommand: volume.GetSnapshotPolicy().GetPreHook(),
		S3:             target,
	})
	if err != nil {
		return nil, err
	}
	var last *clusterv1.VolumeOpProgress
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if msg.GetError() != "" {
			return nil, errors.New(msg.GetError())
		}
		last = msg
	}
	if last == nil || last.GetPhase() != "done" {
		return nil, errors.New("snapshot stream ended without a done message")
	}
	return last, nil
}

// Restore streams a restore of the snapshot into the volume's node.
func (d *SnapshotDispatcher) Restore(ctx context.Context, vol *zatterav1.Volume, manifestKey string) error {
	target, err := d.s3Target()
	if err != nil {
		return err
	}
	node, ok := d.store.Node(vol.GetNodeId())
	if !ok {
		return fmt.Errorf("volume node %q not found", vol.GetNodeId())
	}
	conn, err := d.connect(ctx, node)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	stream, err := clusterv1.NewAgentLocalServiceClient(conn).RestoreVolume(ctx, &clusterv1.AgentRestoreVolumeRequest{
		Volume: vol, ManifestKey: manifestKey, S3: target,
	})
	if err != nil {
		return err
	}
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if msg.GetError() != "" {
			return errors.New(msg.GetError())
		}
	}
}

// Prune (scheduler-facing) deletes the dead snapshots' manifest objects and
// garbage-collects orphaned chunks against S3.
func (d *SnapshotDispatcher) Prune(ctx context.Context, _ *zatterav1.Volume, deadSnapshotIDs []string) error {
	eng, store, err := d.engine()
	if err != nil {
		return err
	}
	for _, id := range deadSnapshotIDs {
		if err := store.Delete(ctx, "manifests/"+id); err != nil {
			d.log.Warn("snapshot prune: delete manifest", "id", id, "err", err)
		}
	}
	_, err = eng.Prune(ctx)
	return err
}

// s3Target unseals the backup config + attaches the data key for a node op.
func (d *SnapshotDispatcher) s3Target() (*clusterv1.S3Target, error) {
	if len(d.dataKey) == 0 || d.sealer == nil {
		return nil, errors.New("cluster is not unsealed; cannot snapshot")
	}
	bc, ok := d.store.BackupConfig()
	if !ok || bc.GetS3Bucket() == "" {
		return nil, errors.New("backup destination not configured (set an S3 bucket)")
	}
	ak, err := d.sealer.Open(bc.GetS3AccessKey())
	if err != nil {
		return nil, fmt.Errorf("unseal s3 access key: %w", err)
	}
	sk, err := d.sealer.Open(bc.GetS3SecretKey())
	if err != nil {
		return nil, fmt.Errorf("unseal s3 secret key: %w", err)
	}
	return &clusterv1.S3Target{
		Endpoint:  bc.GetS3Endpoint(),
		Bucket:    bc.GetS3Bucket(),
		Prefix:    bc.GetS3Prefix(),
		Region:    bc.GetS3Region(),
		AccessKey: string(ak),
		SecretKey: string(sk),
		DataKey:   d.dataKey,
	}, nil
}

// engine builds a control-side snapshot engine (for prune, which is pure object
// ops and needs no host path).
func (d *SnapshotDispatcher) engine() (*volumes.Engine, volumes.ObjectStore, error) {
	target, err := d.s3Target()
	if err != nil {
		return nil, nil, err
	}
	host, ssl := parseS3Endpoint(target.GetEndpoint())
	store, err := volumes.NewS3Store(volumes.S3Config{
		Endpoint: host, Region: target.GetRegion(), Bucket: target.GetBucket(), Prefix: target.GetPrefix(),
		AccessKey: target.GetAccessKey(), SecretKey: target.GetSecretKey(), UseSSL: ssl,
	})
	if err != nil {
		return nil, nil, err
	}
	eng, err := volumes.NewEngine(store, target.GetDataKey(), volumes.Options{})
	return eng, store, err
}

func (d *SnapshotDispatcher) putSnapshot(ctx context.Context, snap *zatterav1.VolumeSnapshot) error {
	return d.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_PutVolumeSnapshot{PutVolumeSnapshot: &clusterv1.PutVolumeSnapshot{Snapshot: snap}}})
}

func (d *SnapshotDispatcher) apply(ctx context.Context, cmd *clusterv1.Command) error {
	cmd.RequestId = ids.New()
	cmd.Actor = "system:snapshots"
	cmd.Time = timestamppb.New(d.clock.Now())
	return d.raft.Apply(ctx, cmd)
}

func parseS3Endpoint(ep string) (host string, useSSL bool) {
	switch {
	case len(ep) > 8 && ep[:8] == "https://":
		return ep[8:], true
	case len(ep) > 7 && ep[:7] == "http://":
		return ep[7:], false
	default:
		return ep, true
	}
}

var _ interface {
	Snapshot(context.Context, *zatterav1.Volume) error
	Prune(context.Context, *zatterav1.Volume, []string) error
} = (*SnapshotDispatcher)(nil)
