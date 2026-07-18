package api

import (
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/archive"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/daemon/volumes"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
)

// archiveHarness builds an Auditor whose ring holds `live` entries and whose
// archive holds `archived`, with one entry deliberately present in both.
func archiveHarness(t *testing.T) (*Auditor, *volumes.MemStore, secrets.Sealer, time.Time) {
	t.Helper()
	rs := raftstore.NewTestStore(t)
	st := rs.State()
	key, _ := secrets.GenerateDataKey()
	sealer, _ := secrets.NewSealer(key, 1)
	store := volumes.NewMemStore()

	a := NewAuditor(st, rs, nil, 0)
	a.SetArchive(func() (*archive.Reader, bool) { return archive.NewReader(store, sealer), true })
	return a, store, sealer, time.Now()
}

func auditAt(at time.Time, method string) *zatterav1.AuditEntry {
	return &zatterav1.AuditEntry{
		Meta:   &zatterav1.Meta{Id: ids.New(), CreatedAt: timestamppb.New(at)},
		Method: method, Outcome: "ok",
	}
}

func putArchive(t *testing.T, store *volumes.MemStore, sealer secrets.Sealer, key string, recs ...proto.Message) {
	t.Helper()
	blob, err := archive.Encode(sealer, recs)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := store.Put(context.Background(), key, blob); err != nil {
		t.Fatalf("put: %v", err)
	}
}

// TestQueryAuditIncludeArchive covers the merged read path: archived entries
// extend the result past what the ring still holds, an entry present in both
// appears once, ordering stays newest-first, and the caller is told how many
// entries came from the archive.
func TestQueryAuditIncludeArchive(t *testing.T) {
	a, store, sealer, now := archiveHarness(t)

	// Ring: two recent entries.
	recent := auditAt(now.Add(-time.Minute), "/zattera.v1.AppService/CreateApp")
	overlap := auditAt(now.Add(-2*time.Minute), "/zattera.v1.AppService/SetEnvVars")
	a.store.AppendAudit([]*zatterav1.AuditEntry{overlap, recent})

	// Archive: one old entry the ring no longer has, plus a copy of `overlap`.
	old := auditAt(now.Add(-90*24*time.Hour), "/zattera.v1.AppService/DeleteApp")
	startMs := old.GetMeta().GetCreatedAt().AsTime().UnixMilli()
	endMs := overlap.GetMeta().GetCreatedAt().AsTime().UnixMilli()
	putArchive(t, store, sealer,
		fmt.Sprintf("audit/2026-04-21/%d-%d-%s.ndjson.gz.enc", startMs, endMs, ids.New()),
		old, overlap)

	ctx := context.Background()

	t.Run("without the flag the archive is not read", func(t *testing.T) {
		resp, err := a.QueryAudit(ctx, &zatterav1.QueryAuditRequest{})
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.GetEntries()) != 2 {
			t.Fatalf("ring-only query returned %d entries, want 2", len(resp.GetEntries()))
		}
		if resp.GetFromArchive() != 0 {
			t.Errorf("from_archive = %d without --archive", resp.GetFromArchive())
		}
	})

	t.Run("merged", func(t *testing.T) {
		resp, err := a.QueryAudit(ctx, &zatterav1.QueryAuditRequest{IncludeArchive: true})
		if err != nil {
			t.Fatal(err)
		}
		entries := resp.GetEntries()
		if len(entries) != 3 {
			t.Fatalf("merged query returned %d entries, want 3", len(entries))
		}
		if resp.GetFromArchive() != 1 {
			t.Errorf("from_archive = %d, want 1", resp.GetFromArchive())
		}
		// The entry in both must appear exactly once.
		var overlaps int
		for _, e := range entries {
			if e.GetMeta().GetId() == overlap.GetMeta().GetId() {
				overlaps++
			}
		}
		if overlaps != 1 {
			t.Errorf("overlapping entry appeared %d times", overlaps)
		}
		// Newest first across the seam.
		for i := 1; i < len(entries); i++ {
			prev := entries[i-1].GetMeta().GetCreatedAt().AsTime()
			cur := entries[i].GetMeta().GetCreatedAt().AsTime()
			if cur.After(prev) {
				t.Fatalf("merged result is not newest-first at %d", i)
			}
		}
	})

	t.Run("filters apply to archived entries too", func(t *testing.T) {
		resp, err := a.QueryAudit(ctx, &zatterav1.QueryAuditRequest{
			IncludeArchive: true,
			MethodPrefix:   "/zattera.v1.AppService/DeleteApp",
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.GetEntries()) != 1 || resp.GetFromArchive() != 1 {
			t.Fatalf("filtered archive query = %d entries (%d archived)", len(resp.GetEntries()), resp.GetFromArchive())
		}
	})

	t.Run("limit is applied after the merge", func(t *testing.T) {
		resp, err := a.QueryAudit(ctx, &zatterav1.QueryAuditRequest{IncludeArchive: true, Limit: 2})
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.GetEntries()) != 2 {
			t.Fatalf("limit=2 returned %d entries", len(resp.GetEntries()))
		}
		// The two newest are both from the ring, so nothing archived survives.
		if resp.GetFromArchive() != 0 {
			t.Errorf("from_archive = %d, want 0 (the newest 2 are live)", resp.GetFromArchive())
		}
	})
}

// TestIncludeArchiveWithoutArchiveConfigured checks the degraded path: asking
// for the archive when none is configured returns the ring rather than failing,
// so the flag is safe to pass unconditionally.
func TestIncludeArchiveWithoutArchiveConfigured(t *testing.T) {
	rs := raftstore.NewTestStore(t)
	a := NewAuditor(rs.State(), rs, nil, 0)
	a.store.AppendAudit([]*zatterav1.AuditEntry{auditAt(time.Now(), "m")})

	resp, err := a.QueryAudit(context.Background(), &zatterav1.QueryAuditRequest{IncludeArchive: true})
	if err != nil {
		t.Fatalf("include_archive with no archive configured: %v", err)
	}
	if len(resp.GetEntries()) != 1 || resp.GetFromArchive() != 0 {
		t.Fatalf("got %d entries (%d archived)", len(resp.GetEntries()), resp.GetFromArchive())
	}
}
