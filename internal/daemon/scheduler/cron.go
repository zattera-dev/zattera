package scheduler

import (
	"context"
	"hash/fnv"
	"time"

	cron "github.com/robfig/cron/v3"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

// cronJitterWindow bounds the deterministic per-spec jitter, spreading fires of
// distinct crons that share a slot so they don't all place at once.
const cronJitterWindow = 30 * time.Second

// resetCron reinitializes the cron firing state for a fresh leader term. The
// epoch is the moment this node became leader: scheduled slots earlier than it
// (missed while another node led, or while there was no leader) are skipped
// rather than replayed — a cron guarantees "at most once per slot", not
// catch-up. next-run is always computed forward from the epoch.
func (s *Scheduler) resetCron() {
	s.cronLast = map[string]time.Time{}
	s.cronEpoch = s.clock.Now()
}

// reconcileCron fires each environment's due CronSpecs as one-shot Jobs,
// honoring the spec's ConcurrencyPolicy. Leader-only; part of evaluate().
func (s *Scheduler) reconcileCron(ctx context.Context, st *state.Store) error {
	if s.cronLast == nil {
		s.resetCron()
	}
	now := s.clock.Now()
	for _, env := range st.ListEnvironments("", "") {
		for _, spec := range env.GetService().GetCron() {
			if err := s.maybeFireCron(ctx, st, env, spec, now); err != nil {
				return err
			}
		}
	}
	return nil
}

// maybeFireCron fires a spec when its most recent scheduled slot (plus jitter)
// is due and has not been fired this term.
func (s *Scheduler) maybeFireCron(ctx context.Context, st *state.Store, env *zatterav1.Environment, spec *zatterav1.CronSpec, now time.Time) error {
	if spec.GetName() == "" || spec.GetSchedule() == "" {
		return nil
	}
	sched, err := cron.ParseStandard(spec.GetSchedule())
	if err != nil {
		s.log.Warn("cron: bad schedule", "env", env.GetMeta().GetId(), "cron", spec.GetName(), "schedule", spec.GetSchedule(), "err", err)
		return nil
	}

	key := env.GetMeta().GetId() + "\x00" + spec.GetName()
	baseline := s.cronEpoch
	if last, ok := s.cronLast[key]; ok && last.After(baseline) {
		baseline = last
	}
	due := sched.Next(baseline)
	if due.Add(cronJitter(env.GetMeta().GetId(), spec.GetName())).After(now) {
		return nil // this slot's (jittered) fire time is still in the future
	}
	if last, ok := s.cronLast[key]; ok && !due.After(last) {
		return nil // already fired this slot
	}
	s.cronLast[key] = due
	return s.fireCron(ctx, st, env, spec)
}

// fireCron applies the ConcurrencyPolicy then (if allowed) enqueues the run.
func (s *Scheduler) fireCron(ctx context.Context, st *state.Store, env *zatterav1.Environment, spec *zatterav1.CronSpec) error {
	if env.GetActiveReleaseId() == "" {
		s.log.Warn("cron: skip, env has no active release", "env", env.GetMeta().GetId(), "cron", spec.GetName())
		return nil
	}
	active := activeCronJobs(st, env, spec.GetName())
	switch spec.GetConcurrency() {
	case zatterav1.ConcurrencyPolicy_CONCURRENCY_POLICY_ALLOW:
		// Overlap permitted: fall through and enqueue alongside active runs.
	case zatterav1.ConcurrencyPolicy_CONCURRENCY_POLICY_REPLACE:
		for _, j := range active {
			if err := s.cancelCronJob(ctx, st, j); err != nil {
				return err
			}
		}
	default: // FORBID (and UNSPECIFIED): skip while a run is still active.
		if len(active) > 0 {
			s.log.Info("cron: skip, prior run still active (forbid)", "env", env.GetMeta().GetId(), "cron", spec.GetName())
			return nil
		}
	}
	return s.enqueueCronJob(ctx, env, spec)
}

// enqueueCronJob creates a QUEUED Job tagged with the cron name; reconcileJobs
// then places and drives it like any one-shot run.
func (s *Scheduler) enqueueCronJob(ctx context.Context, env *zatterav1.Environment, spec *zatterav1.CronSpec) error {
	now := timestamppb.New(s.clock.Now())
	job := &zatterav1.Job{
		Meta:          &zatterav1.Meta{Id: ids.New(), CreatedAt: now, UpdatedAt: now},
		ProjectId:     env.GetProjectId(),
		AppId:         env.GetAppId(),
		EnvironmentId: env.GetMeta().GetId(),
		ReleaseId:     env.GetActiveReleaseId(),
		Command:       spec.GetCommand(),
		CronName:      spec.GetName(),
		Status:        zatterav1.JobStatus_JOB_STATUS_QUEUED,
		MaxRetries:    spec.GetMaxRetries(),
	}
	s.log.Info("cron: enqueued run", "env", env.GetMeta().GetId(), "cron", spec.GetName(), "job", job.GetMeta().GetId())
	return s.apply(ctx, &clusterv1.Command{
		Mutation: &clusterv1.Command_PutJob{PutJob: &clusterv1.PutJob{Job: job}},
	})
}

// cancelCronJob marks an active cron run CANCELED and stops its assignment;
// reconcileJobs reaps the assignment on a later pass (REPLACE policy).
func (s *Scheduler) cancelCronJob(ctx context.Context, st *state.Store, job *zatterav1.Job) error {
	if a, ok := s.jobAssignment(st, job); ok && a.GetDesired() != zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_STOP {
		a.Desired = zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_STOP
		if err := s.apply(ctx, &clusterv1.Command{
			Mutation: &clusterv1.Command_PutAssignments{PutAssignments: &clusterv1.PutAssignments{Assignments: []*zatterav1.Assignment{a}}},
		}); err != nil {
			return err
		}
	}
	job.Status = zatterav1.JobStatus_JOB_STATUS_CANCELED
	job.FinishedAt = timestamppb.New(s.clock.Now())
	return s.putJob(ctx, job)
}

// activeCronJobs returns this cron's non-terminal runs in the environment.
func activeCronJobs(st *state.Store, env *zatterav1.Environment, cronName string) []*zatterav1.Job {
	var out []*zatterav1.Job
	for _, j := range st.ListJobs(env.GetProjectId(), env.GetMeta().GetId()) {
		if j.GetCronName() == cronName && !jobTerminal(j.GetStatus()) {
			out = append(out, j)
		}
	}
	return out
}

// jobTerminal reports whether a job status is finished.
func jobTerminal(s zatterav1.JobStatus) bool {
	switch s {
	case zatterav1.JobStatus_JOB_STATUS_SUCCEEDED,
		zatterav1.JobStatus_JOB_STATUS_FAILED,
		zatterav1.JobStatus_JOB_STATUS_CANCELED:
		return true
	default:
		return false
	}
}

// cronJitter is a deterministic per-(env,spec) delay in [0, cronJitterWindow):
// stable across leader terms and processes, so replays compute the same fire
// time, yet distinct across crons so they don't stampede a shared slot.
func cronJitter(envID, name string) time.Duration {
	h := fnv.New32a()
	_, _ = h.Write([]byte(envID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(name))
	return time.Duration(h.Sum32()%uint32(cronJitterWindow/time.Second)) * time.Second
}
