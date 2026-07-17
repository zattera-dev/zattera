package backup

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/daemon/volumes"
	"github.com/zattera-dev/zattera/internal/state"
)

func seedState(t *testing.T) *state.Store {
	t.Helper()
	st := state.New()
	st.SetOrg(&zatterav1.Org{Meta: &zatterav1.Meta{Id: "org1"}, Name: "acme"})
	st.PutProject(&zatterav1.Project{Meta: &zatterav1.Meta{Id: "p1"}, Name: "web"})
	st.PutApp(&zatterav1.App{Meta: &zatterav1.Meta{Id: "app1"}, ProjectId: "p1", Name: "api"})
	st.PutEnvironment(&zatterav1.Environment{Meta: &zatterav1.Meta{Id: "e1"}, ProjectId: "p1", AppId: "app1", Name: "production"})
	st.PutNode(&zatterav1.Node{Meta: &zatterav1.Meta{Id: "n1"}, MeshIp: "10.90.0.1", Status: zatterav1.NodeStatus_NODE_STATUS_ALIVE})
	st.PutVolume(&zatterav1.Volume{Meta: &zatterav1.Meta{Id: "v1"}, ProjectId: "p1", EnvironmentId: "e1", Name: "data", NodeId: "n1"})
	st.PutVolumeSnapshot(&zatterav1.VolumeSnapshot{
		Meta:     &zatterav1.Meta{Id: "snap1", CreatedAt: timestamppb.New(time.Unix(1000, 0))},
		VolumeId: "v1", ManifestKey: "snap1", Status: zatterav1.SnapshotStatus_SNAPSHOT_STATUS_COMPLETE,
	})
	return st
}

func backupInput(t *testing.T, st *state.Store, store volumes.ObjectStore, pass string) Input {
	t.Helper()
	dataKey, _ := secrets.GenerateDataKey()
	sealer, err := secrets.NewSealer(dataKey, 3)
	if err != nil {
		t.Fatal(err)
	}
	km, err := secrets.SealDataKey(dataKey, pass, 3)
	if err != nil {
		t.Fatal(err)
	}
	return Input{
		Store: st, ObjectStore: store, Sealer: sealer, KeyMaterial: km,
		CACertPEM: []byte("CERT"), CAKeyPEM: []byte("KEY"), Now: time.Unix(2000, 0),
	}
}

func TestBackupVerifyRoundTrip(t *testing.T) {
	ctx := context.Background()
	st := seedState(t)
	store := volumes.NewMemStore()
	in := backupInput(t, st, store, "correct horse battery staple")

	idx, err := Backup(ctx, in)
	if err != nil {
		t.Fatalf("backup: %v", err)
	}
	if len(idx.Volumes) != 1 || idx.Volumes[0].ManifestKey != "snap1" {
		t.Fatalf("index volume refs wrong: %+v", idx.Volumes)
	}

	// Verify decrypts the state with the passphrase and rebuilds it.
	gotIdx, restored, err := Verify(ctx, store, "correct horse battery staple")
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if gotIdx.TimestampUnix != idx.TimestampUnix {
		t.Fatalf("index mismatch: %d vs %d", gotIdx.TimestampUnix, idx.TimestampUnix)
	}
	// State equality on the important objects.
	if _, ok := restored.Project("p1"); !ok {
		t.Fatal("project not restored")
	}
	if a, ok := restored.App("app1"); !ok || a.GetName() != "api" {
		t.Fatal("app not restored")
	}
	if _, ok := restored.Environment("e1"); !ok {
		t.Fatal("environment not restored")
	}
	if v, ok := restored.Volume("v1"); !ok || v.GetNodeId() != "n1" {
		t.Fatal("volume not restored")
	}
}

func TestVerifyWrongPassphraseFails(t *testing.T) {
	ctx := context.Background()
	store := volumes.NewMemStore()
	if _, err := Backup(ctx, backupInput(t, seedState(t), store, "right-pass")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Verify(ctx, store, "wrong-pass"); err == nil {
		t.Fatal("verify with the wrong passphrase must fail")
	}
}

func TestBackupRequiresKeyMaterial(t *testing.T) {
	in := backupInput(t, seedState(t), volumes.NewMemStore(), "pass")
	in.KeyMaterial = nil
	if _, err := Backup(context.Background(), in); err == nil {
		t.Fatal("backup without cluster key material must fail")
	}
}
