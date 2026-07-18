//go:build integration

package integration

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"google.golang.org/protobuf/types/known/timestamppb"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/api"
	"github.com/zattera-dev/zattera/internal/daemon/backup"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/daemon/volumes"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

const drPass = "correct horse battery staple"

// TestDisasterRecovery backs up a seeded cluster (state + a real volume
// snapshot) to MinIO, restores into a fresh data dir, and asserts the restored
// state matches and the volume snapshot is still restorable.
func TestDisasterRecovery(t *testing.T) {
	RequireDocker(t)
	endpoint := startMinIO(t)
	ctx := context.Background()

	cli, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(minioAccess, minioSecret, "")})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.MakeBucket(ctx, minioBucket, minio.MakeBucketOptions{}); err != nil {
		t.Fatalf("make bucket: %v", err)
	}

	store, err := volumes.NewS3Store(volumes.S3Config{
		Endpoint: endpoint, Bucket: minioBucket, Prefix: "dr/", AccessKey: minioAccess, SecretKey: minioSecret,
	})
	if err != nil {
		t.Fatal(err)
	}
	dataKey, _ := secrets.GenerateDataKey()
	kr, _ := secrets.NewKeyring(dataKey, 1)
	vault, _ := secrets.NewUnsealedVault(kr)

	// A real volume snapshot in the object store (so "restorable" is meaningful).
	eng, _ := volumes.NewEngine(store, dataKey, volumes.Options{AverageSize: 8 << 10})
	volDir := t.TempDir()
	writeFile(t, volDir, "pg/base.bin", bigData(128<<10, 3))
	if _, err := eng.Snapshot(ctx, volDir, "snapA", time.Now().Unix(), nil); err != nil {
		t.Fatalf("volume snapshot: %v", err)
	}

	// Seed cluster state into a raft store (the BackupService applies through it).
	rs := raftstore.NewTestStore(t)
	st := rs.State()
	km, err := secrets.SealDataKey(dataKey, drPass, 1)
	if err != nil {
		t.Fatal(err)
	}
	st.SetClusterKeyMaterial(km)
	st.PutProject(&zatterav1.Project{Meta: &zatterav1.Meta{Id: "p1"}, Name: "web"})
	st.PutApp(&zatterav1.App{Meta: &zatterav1.Meta{Id: "app1"}, ProjectId: "p1", Name: "api"})
	st.PutEnvironment(&zatterav1.Environment{Meta: &zatterav1.Meta{Id: "e1"}, ProjectId: "p1", AppId: "app1", Name: "production"})
	st.PutNode(&zatterav1.Node{Meta: &zatterav1.Meta{Id: "old-node"}, MeshIp: "10.90.0.5", Status: zatterav1.NodeStatus_NODE_STATUS_ALIVE})
	st.PutVolume(&zatterav1.Volume{Meta: &zatterav1.Meta{Id: "v1"}, ProjectId: "p1", EnvironmentId: "e1", Name: "data", NodeId: "old-node"})
	st.PutVolumeSnapshot(&zatterav1.VolumeSnapshot{
		Meta:     &zatterav1.Meta{Id: "snapA", CreatedAt: timestamppb.New(time.Unix(1000, 0))},
		VolumeId: "v1", ManifestKey: "snapA", Status: zatterav1.SnapshotStatus_SNAPSHOT_STATUS_COMPLETE,
	})

	// Configure the destination and run a full backup through the wired service.
	srv := api.NewBackupServer(st, rs, vault, drCA{}, clock.Real{})
	if _, err := srv.SetBackupConfig(ctx, &zatterav1.SetBackupConfigRequest{
		Config:           &zatterav1.BackupConfig{S3Endpoint: "http://" + endpoint, S3Bucket: minioBucket, S3Prefix: "dr/", S3Region: "us-east-1"},
		S3AccessKeyPlain: minioAccess,
		S3SecretKeyPlain: minioSecret,
	}); err != nil {
		t.Fatalf("set backup config: %v", err)
	}
	if _, err := srv.TriggerBackup(ctx, &zatterav1.TriggerBackupRequest{Kind: "full"}); err != nil {
		t.Fatalf("trigger backup: %v", err)
	}

	// Restore into a fresh data dir.
	dataDir := t.TempDir()
	if _, err := backup.Restore(ctx, backup.RestoreInput{
		ObjectStore: store, Passphrase: drPass, DataDir: dataDir, NodeID: ids.New(),
	}); err != nil {
		t.Fatalf("restore: %v", err)
	}

	// Reopen the restored raft store and assert state equality.
	assertRestoredState(t, dataDir)

	// Verify decodes the backup index; it references the volume's snapshot, whose
	// manifest is present. (The byte-identical chunk restore is covered by
	// TestSnapshotMinIO.)
	idx, _, err := backup.Verify(ctx, store, drPass)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if len(idx.Volumes) != 1 || idx.Volumes[0].ManifestKey != "snapA" {
		t.Fatalf("index volume ref wrong: %+v", idx.Volumes)
	}
	if ok, err := store.Has(ctx, "manifests/"+idx.Volumes[0].ManifestKey); err != nil || !ok {
		t.Fatalf("referenced snapshot manifest missing from the store (ok=%v err=%v)", ok, err)
	}
}

// drCA is a stand-in CA whose PEMs are backed up and restored verbatim.
type drCA struct{}

func (drCA) CABundlePEM() []byte            { return []byte("CACERT") }
func (drCA) PrivateKeyPEM() ([]byte, error) { return []byte("CAKEY"), nil }

func assertRestoredState(t *testing.T, dataDir string) {
	t.Helper()
	st := state.New()
	rs, err := raftstore.New(raftstore.Config{
		NodeID: ids.New(), DataDir: dataDir + "/raft", BindAddr: freeAddr(t), Bootstrap: false,
	}, st)
	if err != nil {
		t.Fatalf("reopen raft: %v", err)
	}
	defer func() { _ = rs.Shutdown() }()
	// Give the FSM a moment to restore from the on-disk snapshot.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := rs.State().Project("p1"); ok {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	s := rs.State()
	if _, ok := s.Project("p1"); !ok {
		t.Fatal("project not restored")
	}
	if a, ok := s.App("app1"); !ok || a.GetName() != "api" {
		t.Fatal("app not restored")
	}
	if _, ok := s.Environment("e1"); !ok {
		t.Fatal("environment not restored")
	}
	// The old node is preserved but marked DOWN, mesh IP intact.
	n, ok := s.Node("old-node")
	if !ok || n.GetStatus() != zatterav1.NodeStatus_NODE_STATUS_DOWN || n.GetMeshIp() != "10.90.0.5" {
		t.Fatalf("old node not marked DOWN with preserved mesh IP: %+v", n)
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}
