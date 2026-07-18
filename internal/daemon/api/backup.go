package api

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/backup"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/daemon/volumes"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

// CAMaterial exposes the cluster CA cert + private key so a backup can include
// them (restored nodes keep the same CA, so their certs stay valid). *ca.CA
// satisfies it.
type CAMaterial interface {
	CABundlePEM() []byte
	PrivateKeyPEM() ([]byte, error)
}

// BackupServer implements BackupService: configure the S3 destination, run a
// full backup on demand, and list past backups. Available only on an unsealed
// control node (it needs the data key to seal/unseal credentials + state).
type BackupServer struct {
	zatterav1.UnimplementedBackupServiceServer
	store  *state.Store
	raft   Applier
	sealer secrets.Sealer
	ca     CAMaterial
	clock  clock.Clock
}

// NewBackupServer builds the backup service. sealer/ca may be nil on a sealed
// node; all methods then return FailedPrecondition.
func NewBackupServer(store *state.Store, raft Applier, sealer secrets.Sealer, ca CAMaterial, clk clock.Clock) *BackupServer {
	if clk == nil {
		clk = clock.Real{}
	}
	return &BackupServer{store: store, raft: raft, sealer: sealer, ca: ca, clock: clk}
}

// SetBackupConfig stores the S3 destination, sealing the plaintext credentials
// with the cluster data key.
func (s *BackupServer) SetBackupConfig(ctx context.Context, req *zatterav1.SetBackupConfigRequest) (*emptypb.Empty, error) {
	if s.sealer == nil {
		return nil, status.Error(codes.FailedPrecondition, "cluster is not unsealed")
	}
	cfg := req.GetConfig()
	if cfg == nil {
		cfg = &zatterav1.BackupConfig{}
	}
	cfg = proto.Clone(cfg).(*zatterav1.BackupConfig)
	if cfg.GetS3Bucket() == "" {
		return nil, status.Error(codes.InvalidArgument, "s3_bucket is required")
	}
	if ak := req.GetS3AccessKeyPlain(); ak != "" {
		ev, err := s.sealer.Seal([]byte(ak))
		if err != nil {
			return nil, toStatus(err)
		}
		cfg.S3AccessKey = ev
	}
	if sk := req.GetS3SecretKeyPlain(); sk != "" {
		ev, err := s.sealer.Seal([]byte(sk))
		if err != nil {
			return nil, toStatus(err)
		}
		cfg.S3SecretKey = ev
	}
	if err := s.apply(ctx, &clusterv1.Command{
		Mutation: &clusterv1.Command_PutBackupConfig{PutBackupConfig: &clusterv1.PutBackupConfig{Config: cfg}},
	}); err != nil {
		return nil, toStatus(err)
	}
	return &emptypb.Empty{}, nil
}

// TriggerBackup runs a full backup now (state + CA + key material + volume
// snapshot refs) to the configured destination and records it.
func (s *BackupServer) TriggerBackup(ctx context.Context, _ *zatterav1.TriggerBackupRequest) (*zatterav1.BackupRecord, error) {
	if s.sealer == nil || s.ca == nil {
		return nil, status.Error(codes.FailedPrecondition, "cluster is not unsealed")
	}
	km, ok := s.store.ClusterKeyMaterial()
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "cluster key material unavailable")
	}
	objStore, err := s.backupStore()
	if err != nil {
		return nil, err
	}
	caKey, err := s.ca.PrivateKeyPEM()
	if err != nil {
		return nil, toStatus(err)
	}

	idx, err := backup.Backup(ctx, backup.Input{
		Store:       s.store,
		ObjectStore: objStore,
		Sealer:      s.sealer,
		KeyMaterial: km,
		CACertPEM:   s.ca.CABundlePEM(),
		CAKeyPEM:    caKey,
		Now:         s.clock.Now(),
	})
	rec := &zatterav1.BackupRecord{
		Meta: &zatterav1.Meta{Id: ids.New(), CreatedAt: timestamppb.New(s.clock.Now())},
		Kind: "full",
	}
	if err != nil {
		rec.Status = "failed"
		rec.Error = err.Error()
		_ = s.apply(ctx, recordCmd(rec))
		return nil, toStatus(err)
	}
	rec.Status = "complete"
	rec.ManifestKey = fmt.Sprintf("backups/%d/index.json", idx.TimestampUnix)
	if err := s.apply(ctx, recordCmd(rec)); err != nil {
		return nil, toStatus(err)
	}
	return rec, nil
}

// ListBackups returns past backup records and the current (credential-redacted)
// destination config.
func (s *BackupServer) ListBackups(_ context.Context, _ *emptypb.Empty) (*zatterav1.ListBackupsResponse, error) {
	resp := &zatterav1.ListBackupsResponse{Backups: s.store.ListBackupRecords()}
	if cfg, ok := s.store.BackupConfig(); ok {
		cfg = proto.Clone(cfg).(*zatterav1.BackupConfig)
		cfg.S3AccessKey = nil
		cfg.S3SecretKey = nil
		resp.Config = cfg
	}
	return resp, nil
}

// backupStore builds the S3 object store from the (unsealed) backup config.
func (s *BackupServer) backupStore() (volumes.ObjectStore, error) {
	cfg, ok := s.store.BackupConfig()
	if !ok || cfg.GetS3Bucket() == "" {
		return nil, status.Error(codes.FailedPrecondition, "no backup destination configured (set an S3 bucket)")
	}
	store, err := ObjectStoreFor(cfg, s.sealer)
	if err != nil {
		return nil, toStatus(err)
	}
	return store, nil
}

// ObjectStoreFor builds the S3 client for a backup destination, unsealing its
// credentials. Shared by the backup path and the audit/event archiver (T-92),
// which deliberately reuse one destination and one set of credentials.
func ObjectStoreFor(cfg *zatterav1.BackupConfig, sealer secrets.Sealer) (volumes.ObjectStore, error) {
	if sealer == nil {
		return nil, fmt.Errorf("cluster key not unsealed")
	}
	ak, err := sealer.Open(cfg.GetS3AccessKey())
	if err != nil {
		return nil, fmt.Errorf("unseal s3 access key: %w", err)
	}
	sk, err := sealer.Open(cfg.GetS3SecretKey())
	if err != nil {
		return nil, fmt.Errorf("unseal s3 secret key: %w", err)
	}
	host, ssl := parseS3Endpoint(cfg.GetS3Endpoint())
	return volumes.NewS3Store(volumes.S3Config{
		Endpoint: host, Region: cfg.GetS3Region(), Bucket: cfg.GetS3Bucket(), Prefix: cfg.GetS3Prefix(),
		AccessKey: string(ak), SecretKey: string(sk), UseSSL: ssl,
	})
}

func recordCmd(rec *zatterav1.BackupRecord) *clusterv1.Command {
	return &clusterv1.Command{Mutation: &clusterv1.Command_PutBackupRecord{PutBackupRecord: &clusterv1.PutBackupRecord{Record: rec}}}
}

func (s *BackupServer) apply(ctx context.Context, cmd *clusterv1.Command) error {
	cmd.RequestId = ids.New()
	id, _ := IdentityFrom(ctx)
	cmd.Actor = "user:" + id.UserID
	cmd.Time = timestamppb.New(s.clock.Now())
	return s.raft.Apply(ctx, cmd)
}
