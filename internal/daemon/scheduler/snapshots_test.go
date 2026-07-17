package scheduler

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

// fakeDispatcher records Snapshot/Prune calls.
type fakeDispatcher struct {
	snaps  []string   // volume ids snapshotted
	pruned [][]string // dead snapshot id batches pruned
}

func (f *fakeDispatcher) Snapshot(_ context.Context, v *zatterav1.Volume) error {
	f.snaps = append(f.snaps, v.GetMeta().GetId())
	return nil
}

func (f *fakeDispatcher) Prune(_ context.Context, _ *zatterav1.Volume, dead []string) error {
	f.pruned = append(f.pruned, dead)
	return nil
}

func newSnapRig(t *testing.T) (*SnapshotScheduler, *raftstore.Store, *clock.Fake, *fakeDispatcher) {
	t.Helper()
	rs := raftstore.NewTestStore(t)
	clk := clock.NewFake()
	disp := &fakeDispatcher{}
	return NewSnapshotScheduler(rs, disp, clk, nil), rs, clk, disp
}

func putSnapVolume(st *state.Store, id, schedule string, keepLast uint32, createdAt time.Time) {
	st.PutVolume(&zatterav1.Volume{
		Meta:           &zatterav1.Meta{Id: id, CreatedAt: timestamppb.New(createdAt)},
		ProjectId:      "p1",
		EnvironmentId:  "e1",
		Name:           "data",
		NodeId:         "n1",
		Status:         zatterav1.VolumeStatus_VOLUME_STATUS_ACTIVE,
		SnapshotPolicy: &zatterav1.SnapshotPolicy{Schedule: schedule, KeepLast: keepLast},
	})
}

func putSnapshot(st *state.Store, volID string, createdAt time.Time) string {
	id := ids.New()
	st.PutVolumeSnapshot(&zatterav1.VolumeSnapshot{
		Meta:     &zatterav1.Meta{Id: id, CreatedAt: timestamppb.New(createdAt)},
		VolumeId: volID,
		Status:   zatterav1.SnapshotStatus_SNAPSHOT_STATUS_COMPLETE,
	})
	return id
}

func mustEvalSnap(t *testing.T, s *SnapshotScheduler) {
	t.Helper()
	if err := s.evaluate(context.Background()); err != nil {
		t.Fatalf("evaluate: %v", err)
	}
}

func TestSnapshotSchedule(t *testing.T) {
	t.Run("fires once per due cron slot", func(t *testing.T) {
		s, rs, clk, disp := newSnapRig(t)
		st := rs.State()
		// Hourly snapshots; volume created at the fake epoch.
		putSnapVolume(st, "vol1", "0 * * * *", 7, clk.Now())

		// Before the first slot: no snapshot.
		mustEvalSnap(t, s)
		if len(disp.snaps) != 0 {
			t.Fatalf("snapshot fired before any slot was due: %v", disp.snaps)
		}

		// Advance past the top of the next hour → one snapshot, and only one even
		// if we evaluate repeatedly within the same slot.
		clk.Advance(65 * time.Minute)
		mustEvalSnap(t, s)
		mustEvalSnap(t, s)
		if len(disp.snaps) != 1 {
			t.Fatalf("want exactly 1 snapshot for one due slot, got %d", len(disp.snaps))
		}

		// A completed snapshot advances the baseline; the next hour fires again.
		putSnapshot(st, "vol1", clk.Now())
		clk.Advance(60 * time.Minute)
		mustEvalSnap(t, s)
		if len(disp.snaps) != 2 {
			t.Fatalf("want 2 snapshots after the second slot, got %d", len(disp.snaps))
		}
	})

	t.Run("manual-only policy never fires", func(t *testing.T) {
		s, rs, clk, disp := newSnapRig(t)
		st := rs.State()
		putSnapVolume(st, "vol1", "", 7, clk.Now()) // no schedule
		clk.Advance(48 * time.Hour)
		mustEvalSnap(t, s)
		if len(disp.snaps) != 0 {
			t.Fatalf("manual-only policy should not auto-snapshot, got %d", len(disp.snaps))
		}
	})

	t.Run("keep_last prunes the oldest beyond the limit", func(t *testing.T) {
		s, rs, clk, disp := newSnapRig(t)
		st := rs.State()
		putSnapVolume(st, "vol1", "", 2, clk.Now()) // keep_last = 2, manual

		// Four completed snapshots at increasing times.
		var ids4 []string
		base := clk.Now()
		for i := 0; i < 4; i++ {
			ids4 = append(ids4, putSnapshot(st, "vol1", base.Add(time.Duration(i)*time.Hour)))
		}

		mustEvalSnap(t, s)

		// Only the two newest survive.
		remaining := st.ListVolumeSnapshots("vol1")
		if len(remaining) != 2 {
			t.Fatalf("keep_last=2 should leave 2 snapshots, got %d", len(remaining))
		}
		surviving := map[string]bool{}
		for _, snap := range remaining {
			surviving[snap.GetMeta().GetId()] = true
		}
		if !surviving[ids4[2]] || !surviving[ids4[3]] {
			t.Fatalf("the two newest should survive; remaining=%v", surviving)
		}
		// Prune was asked to GC exactly the two oldest.
		if len(disp.pruned) != 1 || len(disp.pruned[0]) != 2 {
			t.Fatalf("prune should target the 2 oldest, got %v", disp.pruned)
		}
	})

	t.Run("default keep_last is 7", func(t *testing.T) {
		s, rs, clk, disp := newSnapRig(t)
		st := rs.State()
		putSnapVolume(st, "vol1", "", 0, clk.Now()) // 0 → default 7
		base := clk.Now()
		for i := 0; i < 9; i++ {
			putSnapshot(st, "vol1", base.Add(time.Duration(i)*time.Hour))
		}
		mustEvalSnap(t, s)
		if got := len(st.ListVolumeSnapshots("vol1")); got != 7 {
			t.Fatalf("default keep_last should retain 7, got %d", got)
		}
		if len(disp.pruned) != 1 || len(disp.pruned[0]) != 2 {
			t.Fatalf("should prune the 2 oldest, got %v", disp.pruned)
		}
	})
}
