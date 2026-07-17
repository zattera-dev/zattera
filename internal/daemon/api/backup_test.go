package api

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/emptypb"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

type fakeCA struct{}

func (fakeCA) CABundlePEM() []byte            { return []byte("CACERT") }
func (fakeCA) PrivateKeyPEM() ([]byte, error) { return []byte("CAKEY"), nil }

func newBackupHarness(t *testing.T, unsealed bool) (*BackupServer, secrets.Sealer, context.Context) {
	t.Helper()
	rs := raftstore.NewTestStore(t)
	ctx := withIdentity(context.Background(), Identity{UserID: "admin"})
	if !unsealed {
		return NewBackupServer(rs.State(), rs, nil, nil, clock.NewFake()), nil, ctx
	}
	dataKey, _ := secrets.GenerateDataKey()
	sealer, _ := secrets.NewSealer(dataKey, 1)
	// The cluster's key material must exist for TriggerBackup.
	km, _ := secrets.SealDataKey(dataKey, "recovery-pass", 1)
	rs.State().SetClusterKeyMaterial(km)
	return NewBackupServer(rs.State(), rs, sealer, fakeCA{}, clock.NewFake()), sealer, ctx
}

func TestSetBackupConfigSealsCreds(t *testing.T) {
	srv, sealer, ctx := newBackupHarness(t, true)

	_, err := srv.SetBackupConfig(ctx, &zatterav1.SetBackupConfigRequest{
		Config:           &zatterav1.BackupConfig{S3Endpoint: "s3.example.com", S3Bucket: "backups", S3Region: "eu"},
		S3AccessKeyPlain: "AKIA",
		S3SecretKeyPlain: "shh",
	})
	if err != nil {
		t.Fatalf("set config: %v", err)
	}

	cfg, ok := srv.store.BackupConfig()
	if !ok || cfg.GetS3Bucket() != "backups" {
		t.Fatalf("config not stored: %+v", cfg)
	}
	// Credentials are sealed, not plaintext.
	ak, err := sealer.Open(cfg.GetS3AccessKey())
	if err != nil || string(ak) != "AKIA" {
		t.Fatalf("access key not sealed/recoverable: %q err=%v", ak, err)
	}
	sk, _ := sealer.Open(cfg.GetS3SecretKey())
	if string(sk) != "shh" {
		t.Fatalf("secret key = %q, want shh", sk)
	}
}

func TestSetBackupConfigRequiresBucket(t *testing.T) {
	srv, _, ctx := newBackupHarness(t, true)
	_, err := srv.SetBackupConfig(ctx, &zatterav1.SetBackupConfigRequest{Config: &zatterav1.BackupConfig{S3Endpoint: "x"}})
	if statusCode(err) != codes.InvalidArgument {
		t.Fatalf("missing bucket = %v, want InvalidArgument", err)
	}
}

func TestBackupSealedNode(t *testing.T) {
	srv, _, ctx := newBackupHarness(t, false)
	if _, err := srv.SetBackupConfig(ctx, &zatterav1.SetBackupConfigRequest{Config: &zatterav1.BackupConfig{S3Bucket: "b"}}); statusCode(err) != codes.FailedPrecondition {
		t.Fatalf("sealed SetBackupConfig = %v, want FailedPrecondition", err)
	}
	if _, err := srv.TriggerBackup(ctx, &zatterav1.TriggerBackupRequest{}); statusCode(err) != codes.FailedPrecondition {
		t.Fatalf("sealed TriggerBackup = %v, want FailedPrecondition", err)
	}
}

func TestTriggerBackupNeedsDestination(t *testing.T) {
	srv, _, ctx := newBackupHarness(t, true) // unsealed, but no BackupConfig set
	if _, err := srv.TriggerBackup(ctx, &zatterav1.TriggerBackupRequest{}); statusCode(err) != codes.FailedPrecondition {
		t.Fatalf("trigger without a destination = %v, want FailedPrecondition", err)
	}
}

func TestListBackupsRedactsCreds(t *testing.T) {
	srv, _, ctx := newBackupHarness(t, true)
	if _, err := srv.SetBackupConfig(ctx, &zatterav1.SetBackupConfigRequest{
		Config: &zatterav1.BackupConfig{S3Bucket: "backups"}, S3AccessKeyPlain: "AKIA", S3SecretKeyPlain: "shh",
	}); err != nil {
		t.Fatal(err)
	}
	resp, err := srv.ListBackups(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetConfig().GetS3Bucket() != "backups" {
		t.Fatalf("config missing: %+v", resp.GetConfig())
	}
	if resp.GetConfig().GetS3AccessKey() != nil || resp.GetConfig().GetS3SecretKey() != nil {
		t.Fatal("ListBackups must redact credentials")
	}
}
