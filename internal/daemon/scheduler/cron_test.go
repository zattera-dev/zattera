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

// newCronRig builds a leader scheduler on a fake clock whose epoch is the
// clock's start, and seeds a project/app/env with an active release so cron
// runs have an image to source.
func newCronRig(t *testing.T, spec *zatterav1.CronSpec) (*Scheduler, *raftstore.Store, *clock.Fake) {
	t.Helper()
	rs := raftstore.NewTestStore(t)
	clk := clock.NewFake()
	s := New(rs, clk, nil)
	s.resetCron() // epoch = clock start (2030-01-01T00:00:00Z)

	st := rs.State()
	st.PutRelease(&zatterav1.Release{Meta: &zatterav1.Meta{Id: "rel1"}, ProjectId: "p1", AppId: "app1"})
	st.PutEnvironment(&zatterav1.Environment{
		Meta:            &zatterav1.Meta{Id: "e1"},
		ProjectId:       "p1",
		AppId:           "app1",
		Name:            "production",
		ActiveReleaseId: "rel1",
		Service:         &zatterav1.ServiceSpec{Cron: []*zatterav1.CronSpec{spec}},
	})
	return s, rs, clk
}

func cronJobs(st *state.Store, cronName string) []*zatterav1.Job {
	var out []*zatterav1.Job
	for _, j := range st.ListJobs("p1", "e1") {
		if j.GetCronName() == cronName {
			out = append(out, j)
		}
	}
	return out
}

// seedRunningCronJob inserts an already-active run for the cron (as if a prior
// slot placed it and it's still going).
func seedRunningCronJob(st *state.Store, cronName string) string {
	id := ids.New()
	st.PutJob(&zatterav1.Job{
		Meta:          &zatterav1.Meta{Id: id, CreatedAt: timestamppb.Now()},
		ProjectId:     "p1",
		AppId:         "app1",
		EnvironmentId: "e1",
		CronName:      cronName,
		Status:        zatterav1.JobStatus_JOB_STATUS_RUNNING,
	})
	return id
}

// TestCronFiresOnDueSlot: a slot fires exactly once, and does not re-fire until
// the next slot arrives.
func TestCronFiresOnDueSlot(t *testing.T) {
	spec := &zatterav1.CronSpec{Name: "nightly", Schedule: "*/5 * * * *", Command: "echo hi", Concurrency: zatterav1.ConcurrencyPolicy_CONCURRENCY_POLICY_FORBID}
	s, rs, clk := newCronRig(t, spec)
	ctx := context.Background()
	st := rs.State()

	// Before the first slot: nothing fires.
	clk.Advance(3 * time.Minute) // 00:03
	if err := s.reconcileCron(ctx, st); err != nil {
		t.Fatal(err)
	}
	if n := len(cronJobs(st, "nightly")); n != 0 {
		t.Fatalf("fired before first slot: %d jobs", n)
	}

	// Past 00:05 + jitter (< 30s): exactly one job, with the cron fields set.
	clk.Advance(3 * time.Minute) // 00:06
	if err := s.reconcileCron(ctx, st); err != nil {
		t.Fatal(err)
	}
	jobs := cronJobs(st, "nightly")
	if len(jobs) != 1 {
		t.Fatalf("want 1 job after first slot, got %d", len(jobs))
	}
	j := jobs[0]
	if j.GetStatus() != zatterav1.JobStatus_JOB_STATUS_QUEUED || j.GetReleaseId() != "rel1" || j.GetCommand() != "echo hi" {
		t.Fatalf("job fields wrong: %+v", j)
	}

	// Same slot re-evaluated: no duplicate.
	if err := s.reconcileCron(ctx, st); err != nil {
		t.Fatal(err)
	}
	if n := len(cronJobs(st, "nightly")); n != 1 {
		t.Fatalf("duplicate fire within a slot: %d jobs", n)
	}

	// Mark it done, cross the next slot (00:10): a second run fires.
	j.Status = zatterav1.JobStatus_JOB_STATUS_SUCCEEDED
	st.PutJob(j)
	clk.Advance(5 * time.Minute) // 00:11
	if err := s.reconcileCron(ctx, st); err != nil {
		t.Fatal(err)
	}
	if n := len(cronJobs(st, "nightly")); n != 2 {
		t.Fatalf("want 2 jobs after second slot, got %d", n)
	}
}

// TestCronForbidSkipsWhileActive: FORBID does not enqueue while a prior run is
// still active; it resumes once the run is terminal.
func TestCronForbidSkipsWhileActive(t *testing.T) {
	spec := &zatterav1.CronSpec{Name: "sync", Schedule: "*/5 * * * *", Concurrency: zatterav1.ConcurrencyPolicy_CONCURRENCY_POLICY_FORBID}
	s, rs, clk := newCronRig(t, spec)
	ctx := context.Background()
	st := rs.State()

	running := seedRunningCronJob(st, "sync")
	clk.Advance(6 * time.Minute) // past 00:05
	if err := s.reconcileCron(ctx, st); err != nil {
		t.Fatal(err)
	}
	// Only the seeded run exists; the slot was skipped.
	if n := len(cronJobs(st, "sync")); n != 1 {
		t.Fatalf("FORBID enqueued despite active run: %d jobs", n)
	}

	// Finish the active run, cross the next slot: now it fires.
	j, _ := st.Job(running)
	j.Status = zatterav1.JobStatus_JOB_STATUS_SUCCEEDED
	st.PutJob(j)
	clk.Advance(5 * time.Minute) // past 00:10
	if err := s.reconcileCron(ctx, st); err != nil {
		t.Fatal(err)
	}
	if n := len(cronJobs(st, "sync")); n != 2 {
		t.Fatalf("FORBID did not resume after run finished: %d jobs", n)
	}
}

// TestCronReplaceCancelsActive: REPLACE cancels the active run and enqueues a
// fresh one.
func TestCronReplaceCancelsActive(t *testing.T) {
	spec := &zatterav1.CronSpec{Name: "rep", Schedule: "*/5 * * * *", Concurrency: zatterav1.ConcurrencyPolicy_CONCURRENCY_POLICY_REPLACE}
	s, rs, clk := newCronRig(t, spec)
	ctx := context.Background()
	st := rs.State()

	running := seedRunningCronJob(st, "rep")
	clk.Advance(6 * time.Minute)
	if err := s.reconcileCron(ctx, st); err != nil {
		t.Fatal(err)
	}
	prev, _ := st.Job(running)
	if prev.GetStatus() != zatterav1.JobStatus_JOB_STATUS_CANCELED {
		t.Fatalf("REPLACE did not cancel the active run: %v", prev.GetStatus())
	}
	// A new QUEUED run exists alongside the canceled one.
	var queued int
	for _, j := range cronJobs(st, "rep") {
		if j.GetStatus() == zatterav1.JobStatus_JOB_STATUS_QUEUED {
			queued++
		}
	}
	if queued != 1 {
		t.Fatalf("REPLACE did not enqueue exactly one fresh run: %d queued", queued)
	}
}

// TestCronAllowOverlaps: ALLOW enqueues a new run even while one is active.
func TestCronAllowOverlaps(t *testing.T) {
	spec := &zatterav1.CronSpec{Name: "ovl", Schedule: "*/5 * * * *", Concurrency: zatterav1.ConcurrencyPolicy_CONCURRENCY_POLICY_ALLOW}
	s, rs, clk := newCronRig(t, spec)
	ctx := context.Background()
	st := rs.State()

	seedRunningCronJob(st, "ovl")
	clk.Advance(6 * time.Minute)
	if err := s.reconcileCron(ctx, st); err != nil {
		t.Fatal(err)
	}
	if n := len(cronJobs(st, "ovl")); n != 2 {
		t.Fatalf("ALLOW should overlap: want 2 jobs, got %d", n)
	}
}

// TestCronMissedSlotsSkippedOnFailover: a slot that elapsed before this node
// became leader (epoch) is not replayed; only slots after the epoch fire.
func TestCronMissedSlotsSkippedOnFailover(t *testing.T) {
	spec := &zatterav1.CronSpec{Name: "fo", Schedule: "*/5 * * * *", Concurrency: zatterav1.ConcurrencyPolicy_CONCURRENCY_POLICY_FORBID}
	s, rs, clk := newCronRig(t, spec)
	ctx := context.Background()
	st := rs.State()

	// Simulate becoming leader at 00:07 — the 00:05 slot was missed.
	clk.Advance(7 * time.Minute)
	s.resetCron() // epoch = 00:07

	// At 00:08 nothing fires: 00:05 is behind the epoch, next slot is 00:10.
	clk.Advance(1 * time.Minute)
	if err := s.reconcileCron(ctx, st); err != nil {
		t.Fatal(err)
	}
	if n := len(cronJobs(st, "fo")); n != 0 {
		t.Fatalf("missed slot replayed after failover: %d jobs", n)
	}

	// Crossing 00:10 (the first post-epoch slot) fires once.
	clk.Advance(3 * time.Minute) // 00:11
	if err := s.reconcileCron(ctx, st); err != nil {
		t.Fatal(err)
	}
	if n := len(cronJobs(st, "fo")); n != 1 {
		t.Fatalf("first post-epoch slot did not fire: %d jobs", n)
	}
}

// TestCronJitterDeterministic: jitter is stable per (env, name), bounded by the
// window, and varies across names.
func TestCronJitterDeterministic(t *testing.T) {
	a1 := cronJitter("e1", "nightly")
	a2 := cronJitter("e1", "nightly")
	if a1 != a2 {
		t.Fatalf("jitter not deterministic: %v vs %v", a1, a2)
	}
	if a1 < 0 || a1 >= cronJitterWindow {
		t.Fatalf("jitter out of window: %v", a1)
	}
	// Different spec name in the same env should (for these fixtures) differ,
	// proving the name is part of the hash.
	if cronJitter("e1", "nightly") == cronJitter("e1", "hourly") {
		t.Fatal("jitter did not vary across cron names")
	}
}
