package scheduler

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/state"
)

type fakeSweeper struct {
	mu    sync.Mutex
	swept []string // "repo:tag"
}

func (f *fakeSweeper) UntagAndSweep(repo, tag string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.swept = append(f.swept, repo+":"+tag)
	return nil
}

func seedReleases(st *state.Store, envID string, n int) []string {
	ids := make([]string, n)
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("rel-%d", i)
		ids[i-1] = id
		st.PutRelease(&zatterav1.Release{
			Meta:          &zatterav1.Meta{Id: id},
			EnvironmentId: envID,
			ProjectId:     "proj",
			AppId:         "app",
			Version:       uint64(i),
			Source:        &zatterav1.ReleaseSource{BuildId: fmt.Sprintf("build-%d", i)},
		})
	}
	return ids
}

func TestRetentionKeepsRecentActiveAndReferenced(t *testing.T) {
	rs := raftstore.NewTestStore(t)
	st := rs.State()

	// 15 releases v1..v15; v15 is active.
	seedReleases(st, envID, 15)
	st.PutEnvironment(&zatterav1.Environment{
		Meta: &zatterav1.Meta{Id: envID}, Name: "production", ActiveReleaseId: "rel-15",
	})
	// An in-flight deployment pins an otherwise-old release (v2).
	st.PutDeployment(&zatterav1.Deployment{
		Meta: &zatterav1.Meta{Id: "d-inflight"}, EnvironmentId: envID,
		ReleaseId: "rel-2", Phase: zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_HEALTHCHECKING,
	})

	sweeper := &fakeSweeper{}
	r := NewRetention(rs, clock.NewFake(), sweeper, "", nil)
	if err := r.sweep(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Kept: last 10 (v6..v15) + the referenced v2. Deleted: v1, v3, v4, v5.
	kept := map[string]bool{}
	for _, rel := range st.ListReleases(envID) {
		kept[rel.GetMeta().GetId()] = true
	}
	for i := 6; i <= 15; i++ {
		if !kept[fmt.Sprintf("rel-%d", i)] {
			t.Errorf("rel-%d (recent) should be kept", i)
		}
	}
	if !kept["rel-2"] {
		t.Error("rel-2 (referenced by in-flight deployment) should be kept")
	}
	for _, del := range []string{"rel-1", "rel-3", "rel-4", "rel-5"} {
		if kept[del] {
			t.Errorf("%s should have been pruned", del)
		}
	}

	// The registry image of each pruned release was swept.
	wantSwept := map[string]bool{
		"proj/app:build-1": true, "proj/app:build-3": true,
		"proj/app:build-4": true, "proj/app:build-5": true,
	}
	if len(sweeper.swept) != len(wantSwept) {
		t.Fatalf("swept = %v, want %v", sweeper.swept, wantSwept)
	}
	for _, s := range sweeper.swept {
		if !wantSwept[s] {
			t.Errorf("unexpected sweep %q", s)
		}
	}
}

func TestRetentionKeepsPreviousRelease(t *testing.T) {
	rs := raftstore.NewTestStore(t)
	st := rs.State()
	// Only 12 releases; active is v11 (not the newest), so v10 is the previous.
	seedReleases(st, envID, 12)
	st.PutEnvironment(&zatterav1.Environment{
		Meta: &zatterav1.Meta{Id: envID}, Name: "production", ActiveReleaseId: "rel-11",
	})

	r := NewRetention(rs, clock.NewFake(), &fakeSweeper{}, "", nil)
	if err := r.sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	kept := map[string]bool{}
	for _, rel := range st.ListReleases(envID) {
		kept[rel.GetMeta().GetId()] = true
	}
	// v3..v12 are the last 10; v1, v2 pruned; active v11 and previous v10 are in
	// the last-10 window anyway — assert the window plus that v1/v2 are gone.
	if kept["rel-1"] || kept["rel-2"] {
		t.Error("v1/v2 should be pruned")
	}
	if !kept["rel-11"] || !kept["rel-10"] {
		t.Error("active and previous release must be kept")
	}
}

func TestRetentionTarballCleanup(t *testing.T) {
	rs := raftstore.NewTestStore(t)
	uploads := t.TempDir()

	old := filepath.Join(uploads, "oldhash")
	fresh := filepath.Join(uploads, "freshhash")
	if err := os.WriteFile(old, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fresh, []byte("y"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Backdate the old tarball beyond the retention age.
	past := time.Now().Add(-25 * time.Hour)
	if err := os.Chtimes(old, past, past); err != nil {
		t.Fatal(err)
	}

	r := NewRetention(rs, clock.NewFake(), nil, uploads, nil)
	if err := r.sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Error("stale tarball should have been deleted")
	}
	if _, err := os.Stat(fresh); err != nil {
		t.Error("fresh tarball must be kept")
	}
}
