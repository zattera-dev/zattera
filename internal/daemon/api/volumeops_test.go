package api

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/state"
)

func failConnect(context.Context, *zatterav1.Node) (*grpc.ClientConn, error) {
	return nil, context.Canceled // never reached in these tests
}

func TestSnapshotRPCsRequireDispatcher(t *testing.T) {
	rs := raftstore.NewTestStore(t)
	st := rs.State()
	st.PutApp(&zatterav1.App{Meta: &zatterav1.Meta{Id: "app1"}, ProjectId: "p1", Name: "web"})
	st.PutEnvironment(&zatterav1.Environment{Meta: &zatterav1.Meta{Id: "e1"}, ProjectId: "p1", AppId: "app1", Name: "production"})
	st.PutVolume(&zatterav1.Volume{Meta: &zatterav1.Meta{Id: "v1"}, ProjectId: "p1", EnvironmentId: "e1", Name: "data", NodeId: "n1"})
	srv := NewVolumeServer(st, rs, nil, clock.NewFake(), nil) // no dispatcher
	ctx := withIdentity(context.Background(), Identity{UserID: "u1"})

	if _, err := srv.SnapshotVolume(ctx, &zatterav1.SnapshotVolumeRequest{ProjectId: "p1", VolumeId: "v1"}); statusCode(err) != codes.FailedPrecondition {
		t.Fatalf("snapshot without dispatcher = %v, want FailedPrecondition", err)
	}
	if _, err := srv.RestoreSnapshot(ctx, &zatterav1.RestoreSnapshotRequest{ProjectId: "p1", VolumeId: "v1"}); statusCode(err) != codes.FailedPrecondition {
		t.Fatalf("restore without dispatcher = %v, want FailedPrecondition", err)
	}
}

func TestRestoreRefusesWhileMounted(t *testing.T) {
	rs := raftstore.NewTestStore(t)
	st := rs.State()
	st.PutNode(&zatterav1.Node{Meta: &zatterav1.Meta{Id: "n1"}, Status: zatterav1.NodeStatus_NODE_STATUS_ALIVE, Schedulable: true})
	st.PutEnvironment(&zatterav1.Environment{Meta: &zatterav1.Meta{Id: "e1"}, ProjectId: "p1", AppId: "app1", Name: "production"})
	st.PutVolume(&zatterav1.Volume{Meta: &zatterav1.Meta{Id: "v1"}, ProjectId: "p1", EnvironmentId: "e1", Name: "data", NodeId: "n1"})
	st.PutVolumeSnapshot(&zatterav1.VolumeSnapshot{Meta: &zatterav1.Meta{Id: "snap1"}, VolumeId: "v1", ManifestKey: "snap1", Status: zatterav1.SnapshotStatus_SNAPSHOT_STATUS_COMPLETE})
	// A running instance on the volume's node makes it mounted.
	st.PutAssignment(&zatterav1.Assignment{Meta: &zatterav1.Meta{Id: "a1"}, EnvironmentId: "e1", NodeId: "n1", Desired: zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN})

	disp := NewSnapshotDispatcher(st, rs, nil, []byte("k"), failConnect, clock.NewFake(), nil)
	srv := NewVolumeServer(st, rs, nil, clock.NewFake(), nil).WithSnapshots(disp)
	ctx := withIdentity(context.Background(), Identity{UserID: "u1"})

	_, err := srv.RestoreSnapshot(ctx, &zatterav1.RestoreSnapshotRequest{ProjectId: "p1", VolumeId: "v1", SnapshotId: "snap1"})
	if statusCode(err) != codes.FailedPrecondition {
		t.Fatalf("restore while mounted = %v, want FailedPrecondition", err)
	}
}

func TestSnapshotDispatcherS3Target(t *testing.T) {
	st := state.New()
	dataKey, _ := secrets.GenerateDataKey()
	sealer, _ := secrets.NewSealer(dataKey, 1)
	ak, _ := sealer.Seal([]byte("AKIA"))
	sk, _ := sealer.Seal([]byte("secret"))
	st.SetBackupConfig(&zatterav1.BackupConfig{
		S3Endpoint: "s3.example.com", S3Bucket: "backups", S3Prefix: "z/", S3Region: "eu",
		S3AccessKey: ak, S3SecretKey: sk,
	})

	d := NewSnapshotDispatcher(st, nil, sealer, dataKey, failConnect, clock.NewFake(), nil)
	target, err := d.s3Target()
	if err != nil {
		t.Fatalf("s3Target: %v", err)
	}
	if target.GetAccessKey() != "AKIA" || target.GetSecretKey() != "secret" {
		t.Fatalf("creds not unsealed: %+v", target)
	}
	if string(target.GetDataKey()) != string(dataKey) || target.GetBucket() != "backups" {
		t.Fatalf("target = %+v", target)
	}

	// No backup config → a clear error.
	d2 := NewSnapshotDispatcher(state.New(), nil, sealer, dataKey, failConnect, clock.NewFake(), nil)
	if _, err := d2.s3Target(); err == nil {
		t.Fatal("expected an error without a backup destination")
	}
}
