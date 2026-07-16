package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/leaderrunner"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
)

const (
	// retentionKeep is how many recent releases per environment survive GC.
	retentionKeep = 10
	// retentionInterval is the leader sweep cadence.
	retentionInterval = time.Hour
	// tarballMaxAge is how long an uploaded source tarball is retained.
	tarballMaxAge = 24 * time.Hour
)

// RegistrySweeper untags an image and reclaims blobs it solely referenced. The
// daemon wires this to the local registry (the sweep runs on the control node
// that hosts the blobs, never over the mesh).
type RegistrySweeper interface {
	UntagAndSweep(repo, tag string) error
}

// Retention prunes old releases (and their registry images) plus stale source
// tarballs on the leader.
type Retention struct {
	store      *raftstore.Store
	clk        clock.Clock
	log        *slog.Logger
	sweeper    RegistrySweeper // nil = state-only retention
	uploadsDir string          // "" = skip tarball cleanup
}

// NewRetention constructs the retention controller.
func NewRetention(store *raftstore.Store, clk clock.Clock, sweeper RegistrySweeper, uploadsDir string, log *slog.Logger) *Retention {
	if log == nil {
		log = slog.Default()
	}
	if clk == nil {
		clk = clock.Real{}
	}
	return &Retention{store: store, clk: clk, log: log, sweeper: sweeper, uploadsDir: uploadsDir}
}

// Run sweeps hourly while this node leads.
func (r *Retention) Run(ctx context.Context) {
	leaderrunner.Run(ctx, r.store, r.clk, r.leaderLoop)
}

// leaderLoop sweeps once immediately, then on every interval, until leadership
// is lost or ctx ends.
func (r *Retention) leaderLoop(ctx context.Context) {
	for {
		if err := r.sweep(ctx); err != nil && !errors.Is(err, raftstore.ErrNotLeader) {
			r.log.Warn("retention sweep failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-r.store.LeaderCh():
			if !r.store.IsLeader() {
				return
			}
		case <-r.clk.After(retentionInterval):
		}
	}
}

// sweep runs one retention pass (exported behaviour for tests).
func (r *Retention) sweep(ctx context.Context) error {
	if !r.store.IsLeader() {
		return raftstore.ErrNotLeader
	}
	st := r.store.State()

	// Group releases by environment.
	byEnv := map[string]bool{}
	for _, rel := range st.ListReleases("") {
		byEnv[rel.GetEnvironmentId()] = true
	}
	for envID := range byEnv {
		if err := r.sweepEnv(ctx, envID); err != nil {
			return err
		}
	}
	r.sweepTarballs()
	return nil
}

func (r *Retention) sweepEnv(ctx context.Context, envID string) error {
	st := r.store.State()
	env, ok := st.Environment(envID)
	if !ok {
		return nil // env deleted; leave its releases for env-teardown to reap
	}
	rels := st.ListReleases(envID) // newest version first

	keep := map[string]bool{}
	keep[env.GetActiveReleaseId()] = true
	keep[previousReleaseID(rels, env.GetActiveReleaseId())] = true
	for i, rel := range rels {
		if i < retentionKeep {
			keep[rel.GetMeta().GetId()] = true
		}
	}
	// Anything referenced by an in-flight or draining deployment stays.
	for _, d := range st.ListDeployments(envID) {
		if !isTerminalPhase(d.GetPhase()) {
			keep[d.GetReleaseId()] = true
			keep[d.GetPreviousReleaseId()] = true
		}
	}

	for _, rel := range rels {
		id := rel.GetMeta().GetId()
		if keep[id] {
			continue
		}
		// Reclaim the image first (best-effort), then delete the release.
		if r.sweeper != nil && rel.GetSource().GetBuildId() != "" {
			repo := rel.GetProjectId() + "/" + rel.GetAppId()
			if err := r.sweeper.UntagAndSweep(repo, rel.GetSource().GetBuildId()); err != nil {
				r.log.Warn("registry sweep failed", "release", id, "err", err)
			}
		}
		if err := r.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_DeleteRelease{DeleteRelease: &clusterv1.DeleteByID{Id: id}}}); err != nil {
			return err
		}
		r.log.Info("release pruned", "env", envID, "release", id, "version", rel.GetVersion())
	}
	return nil
}

// sweepTarballs deletes uploaded source tarballs older than tarballMaxAge.
// File mtimes are real wall-clock times, so this uses time.Now (not the
// injected clock) for the age comparison.
func (r *Retention) sweepTarballs() {
	if r.uploadsDir == "" {
		return
	}
	entries, err := os.ReadDir(r.uploadsDir)
	if err != nil {
		return
	}
	cutoff := time.Now().Add(-tarballMaxAge)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil || !info.ModTime().Before(cutoff) {
			continue
		}
		_ = os.Remove(filepath.Join(r.uploadsDir, e.Name()))
	}
}

func (r *Retention) apply(ctx context.Context, cmd *clusterv1.Command) error {
	cmd.RequestId = ids.New()
	cmd.Actor = "system:retention"
	cmd.Time = timestamppb.New(r.clk.Now())
	return r.store.Apply(ctx, cmd)
}

// previousReleaseID returns the id of the highest-version release below the
// active one (the rollback-window target that must survive GC). rels is sorted
// newest-version-first.
func previousReleaseID(rels []*zatterav1.Release, activeID string) string {
	var activeVer uint64
	found := false
	for _, rel := range rels {
		if rel.GetMeta().GetId() == activeID {
			activeVer = rel.GetVersion()
			found = true
			break
		}
	}
	if !found {
		return ""
	}
	for _, rel := range rels { // descending, so the first below active is the previous
		if rel.GetVersion() < activeVer {
			return rel.GetMeta().GetId()
		}
	}
	return ""
}
