package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"time"

	cron "github.com/robfig/cron/v3"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/leaderrunner"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

const (
	// snapshotTick drives the schedule re-check; cron granularity is a minute so
	// a sub-minute tick reliably catches each slot.
	snapshotTick = 30 * time.Second
	// defaultKeepLast is the retained snapshot count when a policy omits it.
	defaultKeepLast = 7
)

// SnapshotDispatcher performs the node-side snapshot work the leader schedules:
// Snapshot dispatches a snapshot of the volume to its node; Prune deletes the
// given snapshots' manifest objects and garbage-collects orphaned chunks.
type SnapshotDispatcher interface {
	Snapshot(ctx context.Context, vol *zatterav1.Volume) error
	Prune(ctx context.Context, vol *zatterav1.Volume, deadSnapshotIDs []string) error
}

// SnapshotScheduler fires scheduled volume snapshots (SnapshotPolicy.schedule)
// on the leader and enforces keep_last retention.
type SnapshotScheduler struct {
	store *raftstore.Store
	disp  SnapshotDispatcher
	clock clock.Clock
	log   *slog.Logger

	// lastFire is the last scheduled slot fired per volume, in memory — it stops
	// a re-fire while a snapshot's record has not yet landed. Reset per term.
	lastFire map[string]time.Time
}

// NewSnapshotScheduler builds the scheduler.
func NewSnapshotScheduler(store *raftstore.Store, disp SnapshotDispatcher, clk clock.Clock, log *slog.Logger) *SnapshotScheduler {
	if log == nil {
		log = slog.Default()
	}
	if clk == nil {
		clk = clock.Real{}
	}
	return &SnapshotScheduler{store: store, disp: disp, clock: clk, log: log, lastFire: map[string]time.Time{}}
}

// Run evaluates while this node leads.
func (s *SnapshotScheduler) Run(ctx context.Context) {
	leaderrunner.Run(ctx, s.store, s.clock, s.leaderLoop)
}

func (s *SnapshotScheduler) leaderLoop(ctx context.Context) {
	s.lastFire = map[string]time.Time{}
	sub := s.store.State().Watch(state.KindVolume, state.KindVolumeSnapshot)
	defer sub.Close()
	tick := s.clock.NewTicker(snapshotTick)
	defer tick.Stop()
	for {
		if err := s.evaluate(ctx); errors.Is(err, raftstore.ErrNotLeader) {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-s.store.LeaderCh():
			if !s.store.IsLeader() {
				return
			}
		case <-sub.Notify():
			sub.Drain()
		case <-tick.C():
		}
	}
}

func (s *SnapshotScheduler) evaluate(ctx context.Context) error {
	if !s.store.IsLeader() {
		return raftstore.ErrNotLeader
	}
	st := s.store.State()
	now := s.clock.Now()
	for _, v := range st.ListVolumes("") {
		if v.GetSnapshotPolicy().GetSchedule() != "" {
			if err := s.maybeSnapshot(ctx, st, v, now); err != nil {
				return err
			}
		}
		if err := s.enforceKeepLast(ctx, st, v); err != nil {
			return err
		}
	}
	return nil
}

// maybeSnapshot triggers a snapshot when the cron schedule's most recent slot is
// due and has not been fired yet.
func (s *SnapshotScheduler) maybeSnapshot(ctx context.Context, st *state.Store, v *zatterav1.Volume, now time.Time) error {
	sched, err := cron.ParseStandard(v.GetSnapshotPolicy().GetSchedule())
	if err != nil {
		s.log.Warn("snapshot: bad schedule", "volume", v.GetMeta().GetId(), "schedule", v.GetSnapshotPolicy().GetSchedule(), "err", err)
		return nil
	}
	due := sched.Next(s.baseline(st, v))
	if due.After(now) {
		return nil // next slot is in the future
	}
	if id := v.GetMeta().GetId(); !due.After(s.lastFire[id]) {
		return nil // this slot already fired
	}
	s.lastFire[v.GetMeta().GetId()] = due
	if err := s.disp.Snapshot(ctx, v); err != nil {
		s.log.Warn("snapshot: dispatch failed", "volume", v.GetMeta().GetId(), "err", err)
	}
	return nil
}

// baseline is the reference time the next scheduled slot is computed from: the
// most recent snapshot, else the volume's creation time.
func (s *SnapshotScheduler) baseline(st *state.Store, v *zatterav1.Volume) time.Time {
	base := v.GetMeta().GetCreatedAt().AsTime()
	for _, snap := range st.ListVolumeSnapshots(v.GetMeta().GetId()) {
		if t := snap.GetMeta().GetCreatedAt().AsTime(); t.After(base) {
			base = t
		}
	}
	return base
}

// enforceKeepLast deletes the oldest completed snapshots beyond keep_last and
// prunes their chunks.
func (s *SnapshotScheduler) enforceKeepLast(ctx context.Context, st *state.Store, v *zatterav1.Volume) error {
	keep := int(v.GetSnapshotPolicy().GetKeepLast())
	if keep <= 0 {
		keep = defaultKeepLast
	}
	var complete []*zatterav1.VolumeSnapshot
	for _, snap := range st.ListVolumeSnapshots(v.GetMeta().GetId()) {
		if snap.GetStatus() == zatterav1.SnapshotStatus_SNAPSHOT_STATUS_COMPLETE {
			complete = append(complete, snap)
		}
	}
	if len(complete) <= keep {
		return nil
	}
	// Newest first; drop everything past keep.
	sort.Slice(complete, func(i, j int) bool {
		return complete[i].GetMeta().GetCreatedAt().AsTime().After(complete[j].GetMeta().GetCreatedAt().AsTime())
	})
	dead := complete[keep:]
	deadIDs := make([]string, 0, len(dead))
	for _, snap := range dead {
		deadIDs = append(deadIDs, snap.GetMeta().GetId())
		if err := s.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_DeleteVolumeSnapshot{
			DeleteVolumeSnapshot: &clusterv1.DeleteByID{Id: snap.GetMeta().GetId()},
		}}); err != nil {
			return err
		}
	}
	if err := s.disp.Prune(ctx, v, deadIDs); err != nil {
		s.log.Warn("snapshot: prune failed", "volume", v.GetMeta().GetId(), "err", err)
	}
	return nil
}

func (s *SnapshotScheduler) apply(ctx context.Context, cmd *clusterv1.Command) error {
	cmd.RequestId = ids.New()
	cmd.Actor = "system:snapshots"
	cmd.Time = timestamppb.New(s.clock.Now())
	err := s.store.Apply(ctx, cmd)
	if errors.Is(err, raftstore.ErrNotLeader) {
		return err
	}
	if err != nil {
		s.log.Warn("snapshot apply failed", "err", err)
	}
	return nil
}
