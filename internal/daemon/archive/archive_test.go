package archive

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/daemon/volumes"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
)

// --- fakes ---

// fakeSource is an in-memory stand-in for the state store: two append-ordered
// rings plus the KV the cursor lives in.
type fakeSource struct {
	mu     sync.Mutex
	audit  []*zatterav1.AuditEntry
	events []*zatterav1.Event
	kv     map[string][]byte
}

func newFakeSource() *fakeSource { return &fakeSource{kv: map[string][]byte{}} }

func (f *fakeSource) AuditSince(sinceMs int64) []*zatterav1.AuditEntry {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*zatterav1.AuditEntry
	for _, e := range f.audit {
		if e.GetMeta().GetCreatedAt().AsTime().UnixMilli() >= sinceMs {
			out = append(out, e)
		}
	}
	return out
}

func (f *fakeSource) EventsSince(sinceMs int64) []*zatterav1.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*zatterav1.Event
	for _, e := range f.events {
		if e.GetMeta().GetCreatedAt().AsTime().UnixMilli() >= sinceMs {
			out = append(out, e)
		}
	}
	return out
}

func (f *fakeSource) KV(key string) ([]byte, int64, int64, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.kv[key]
	return v, 1, 0, ok
}

// PutCursor doubles as the CursorStore.
func (f *fakeSource) PutCursor(_ context.Context, key string, value []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.kv[key] = value
	return nil
}

func (f *fakeSource) addAudit(at time.Time, method string) *zatterav1.AuditEntry {
	f.mu.Lock()
	defer f.mu.Unlock()
	e := &zatterav1.AuditEntry{
		Meta:   &zatterav1.Meta{Id: ids.New(), CreatedAt: timestamppb.New(at)},
		Method: method, Outcome: "ok",
	}
	f.audit = append(f.audit, e)
	return e
}

func (f *fakeSource) addEvent(at time.Time, kind string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, &zatterav1.Event{
		Meta: &zatterav1.Meta{Id: ids.New(), CreatedAt: timestamppb.New(at)},
		Kind: kind, Severity: "info", Message: kind,
	})
}

func testSealer(t *testing.T) secrets.Sealer {
	t.Helper()
	key, err := secrets.GenerateDataKey()
	if err != nil {
		t.Fatal(err)
	}
	sealer, err := secrets.NewSealer(key, 1)
	if err != nil {
		t.Fatal(err)
	}
	return sealer
}

func fixture(t *testing.T) (*Archiver, *fakeSource, *volumes.MemStore, secrets.Sealer, *clock.Fake) {
	t.Helper()
	src := newFakeSource()
	store := volumes.NewMemStore()
	sealer := testSealer(t)
	clk := clock.NewFake()
	a := New(src, src, sealer, func() (volumes.ObjectStore, bool) { return store, true }, clk, nil)
	return a, src, store, sealer, clk
}

// --- tests ---

// TestArchiveRoundTrip writes both streams out and reads them back through the
// reader, which is the whole point: an archived entry must survive encoding,
// compression, sealing and the object layout.
func TestArchiveRoundTrip(t *testing.T) {
	a, src, store, sealer, clk := fixture(t)
	base := clk.Now()
	src.addAudit(base.Add(-time.Hour), "/zattera.v1.AppService/CreateApp")
	src.addAudit(base.Add(-30*time.Minute), "/zattera.v1.AppService/DeleteApp")
	src.addEvent(base.Add(-time.Hour), "deploy.succeeded")

	// All three are older than settleLag, so the first sweep takes them.
	if n := a.Sweep(context.Background()); n != 3 {
		t.Fatalf("swept %d records, want 3", n)
	}
	if store.Len() != 2 {
		t.Fatalf("wrote %d objects, want one per stream", store.Len())
	}

	r := NewReader(store, sealer)
	entries, err := r.Audit(context.Background(), 0, 0, 100, nil)
	if err != nil {
		t.Fatalf("read audit: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("read back %d audit entries, want 2", len(entries))
	}
	// Newest first.
	if !strings.HasSuffix(entries[0].GetMethod(), "DeleteApp") {
		t.Errorf("archive not newest-first: %s", entries[0].GetMethod())
	}
	if entries[0].GetOutcome() != "ok" {
		t.Errorf("field lost in round trip: %+v", entries[0])
	}

	events, err := r.Events(context.Background(), 0, 0, 100, nil)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	if len(events) != 1 || events[0].GetKind() != "deploy.succeeded" {
		t.Fatalf("read back events = %+v", events)
	}
}

// TestArchiveCursorResumes proves the sweeper neither loses nor duplicates
// records across sweeps — the property the whole cursor design exists for.
func TestArchiveCursorResumes(t *testing.T) {
	a, src, store, sealer, clk := fixture(t)
	base := clk.Now()
	for i := 0; i < 5; i++ {
		src.addAudit(base.Add(-time.Duration(10-i)*time.Minute), "m1")
	}
	if n := a.Sweep(context.Background()); n != 5 {
		t.Fatalf("first sweep = %d, want 5", n)
	}
	// A sweep with nothing new must write nothing at all.
	if n := a.Sweep(context.Background()); n != 0 {
		t.Fatalf("second sweep = %d, want 0 (cursor did not hold)", n)
	}
	if store.Len() != 1 {
		t.Fatalf("wrote %d objects, want 1", store.Len())
	}

	// New entries after the cursor are picked up.
	clk.Advance(time.Hour)
	src.addAudit(clk.Now().Add(-10*time.Minute), "m2")
	if n := a.Sweep(context.Background()); n != 1 {
		t.Fatalf("third sweep = %d, want 1", n)
	}

	r := NewReader(store, sealer)
	entries, err := r.Audit(context.Background(), 0, 0, 100, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 6 {
		t.Fatalf("archive holds %d entries, want 6 with no duplicates", len(entries))
	}
	seen := map[string]bool{}
	for _, e := range entries {
		if seen[e.GetMeta().GetId()] {
			t.Fatalf("duplicate entry %s in archive", e.GetMeta().GetId())
		}
		seen[e.GetMeta().GetId()] = true
	}
}

// TestArchiveSameMillisecondBoundary is the case a bare timestamp watermark
// gets wrong: several entries sharing the cursor's exact millisecond, with more
// arriving at that same millisecond before the next sweep.
func TestArchiveSameMillisecondBoundary(t *testing.T) {
	a, src, store, sealer, clk := fixture(t)
	at := clk.Now().Add(-10 * time.Minute)
	first := src.addAudit(at, "a")
	src.addAudit(at, "b")
	if n := a.Sweep(context.Background()); n != 2 {
		t.Fatalf("first sweep = %d, want 2", n)
	}

	// A third entry lands at the very same millisecond.
	src.addAudit(at, "c")
	if n := a.Sweep(context.Background()); n != 1 {
		t.Fatalf("boundary sweep = %d, want exactly the new entry", n)
	}

	r := NewReader(store, sealer)
	entries, err := r.Audit(context.Background(), 0, 0, 100, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("archive holds %d entries, want 3 (no loss, no duplication)", len(entries))
	}
	var count int
	for _, e := range entries {
		if e.GetMeta().GetId() == first.GetMeta().GetId() {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("boundary entry archived %d times, want 1", count)
	}
}

// TestArchiveSettleLag checks that entries too recent to have settled are left
// for the next sweep rather than archived behind a cursor that would then skip
// their late-arriving siblings.
func TestArchiveSettleLag(t *testing.T) {
	a, src, store, _, clk := fixture(t)
	src.addAudit(clk.Now(), "fresh")
	if n := a.Sweep(context.Background()); n != 0 {
		t.Fatalf("swept %d unsettled records, want 0", n)
	}
	if store.Len() != 0 {
		t.Fatalf("wrote an object for unsettled records")
	}

	clk.Advance(settleLag + time.Minute)
	if n := a.Sweep(context.Background()); n != 1 {
		t.Fatalf("swept %d after settling, want 1", n)
	}
}

// TestArchiveDisabled covers the off states: no destination means the sweep is
// a silent no-op, not an error.
func TestArchiveDisabled(t *testing.T) {
	src := newFakeSource()
	src.addAudit(time.Now().Add(-time.Hour), "m")
	a := New(src, src, testSealer(t), func() (volumes.ObjectStore, bool) { return nil, false }, clock.NewFake(), nil)
	if n := a.Sweep(context.Background()); n != 0 {
		t.Fatalf("swept %d with archiving off", n)
	}
}

// TestArchiveReaderSkipsNonOverlapping proves the key layout does its job: a
// narrow time window must not fetch objects outside it.
func TestArchiveReaderSkipsNonOverlapping(t *testing.T) {
	a, src, store, sealer, clk := fixture(t)
	base := clk.Now()

	// Three sweeps, an hour apart, produce three objects.
	for i := 0; i < 3; i++ {
		src.addAudit(clk.Now().Add(-10*time.Minute), "m")
		if n := a.Sweep(context.Background()); n != 1 {
			t.Fatalf("sweep %d wrote %d records", i, n)
		}
		clk.Advance(time.Hour)
	}
	if store.Len() != 3 {
		t.Fatalf("expected 3 objects, got %d", store.Len())
	}

	counting := &countingStore{ObjectStore: store}
	r := NewReader(counting, sealer)
	// A window starting after the first object's range: it must be skipped
	// entirely, while the two later objects are fetched.
	sinceMs := base.Add(30 * time.Minute).UnixMilli()
	entries, err := r.Audit(context.Background(), sinceMs, 0, 100, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("window returned %d entries, want the 2 within it", len(entries))
	}
	if counting.gets > 2 {
		t.Errorf("fetched %d objects for a 2-object window — key ranges are not being used to skip", counting.gets)
	}
}

// countingStore records how many objects a read actually pulled.
type countingStore struct {
	volumes.ObjectStore
	gets int
}

func (c *countingStore) Get(ctx context.Context, key string) ([]byte, error) {
	c.gets++
	return c.ObjectStore.Get(ctx, key)
}

// TestArchiveWrongKeyFails ensures archived data is genuinely encrypted: a
// different cluster key must not be able to read it.
func TestArchiveWrongKeyFails(t *testing.T) {
	a, src, store, _, clk := fixture(t)
	src.addAudit(clk.Now().Add(-time.Hour), "m")
	if n := a.Sweep(context.Background()); n != 1 {
		t.Fatalf("sweep = %d", n)
	}
	if _, err := NewReader(store, testSealer(t)).Audit(context.Background(), 0, 0, 100, nil); err == nil {
		t.Fatal("archive was readable with a different cluster key")
	}
}
