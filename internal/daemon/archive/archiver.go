package archive

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/protobuf/proto"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/daemon/volumes"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// KV keys holding each stream's resume cursor.
const (
	kvCursorPrefix = "archive/cursor/"
)

// settleLag is how far behind "now" the sweeper stays. Audit entries are
// batched through raft every 2s and events are written by several loops, so
// anything younger than this may still be in flight; archiving it risks
// writing an object and then finding an older entry lands behind the cursor.
const settleLag = 2 * time.Minute

// maxBatch caps how many records go into one object, so a cluster that has
// been offline for a week does not try to seal one enormous blob in memory.
const maxBatch = 5000

// Source is the state the archiver reads. *state.Store satisfies it.
type Source interface {
	AuditSince(sinceMs int64) []*zatterav1.AuditEntry
	EventsSince(sinceMs int64) []*zatterav1.Event
	KV(key string) ([]byte, int64, int64, bool)
}

// CursorStore persists the resume cursor through raft.
type CursorStore interface {
	PutCursor(ctx context.Context, key string, value []byte) error
}

// Archiver copies the audit and event rings out to object storage.
type Archiver struct {
	src     Source
	cursors CursorStore
	sealer  secrets.Sealer
	clk     clock.Clock
	log     *slog.Logger

	// dest resolves the destination at sweep time: the backup config can
	// change (or archiving can be switched off) without a restart. Returning
	// a nil store means "archiving is not configured", not an error.
	dest func() (volumes.ObjectStore, bool)
}

// New builds an archiver.
func New(src Source, cursors CursorStore, sealer secrets.Sealer, dest func() (volumes.ObjectStore, bool), clk clock.Clock, log *slog.Logger) *Archiver {
	if clk == nil {
		clk = clock.Real{}
	}
	if log == nil {
		log = slog.Default()
	}
	return &Archiver{src: src, cursors: cursors, sealer: sealer, dest: dest, clk: clk, log: log}
}

// Sweep archives everything settled since the last cursor for both streams.
// It returns the number of records written.
func (a *Archiver) Sweep(ctx context.Context) int {
	store, ok := a.dest()
	if !ok || store == nil {
		return 0 // archiving off or no destination configured
	}
	cutoff := a.clk.Now().Add(-settleLag).UnixMilli()

	n := 0
	for _, s := range []Stream{StreamAudit, StreamEvents} {
		written, err := a.sweepStream(ctx, store, s, cutoff)
		if err != nil {
			a.log.Warn("archive sweep failed", "stream", s, "err", err)
			continue
		}
		n += written
	}
	return n
}

func (a *Archiver) sweepStream(ctx context.Context, store volumes.ObjectStore, stream Stream, cutoffMs int64) (int, error) {
	key := kvCursorPrefix + string(stream)
	raw, _, _, _ := a.src.KV(key)
	cursor := parseCursor(raw)
	seen := cursor.seen()

	records, stamps := a.pending(stream, cursor.Ms, cutoffMs, seen)
	if len(records) == 0 {
		return 0, nil
	}

	// Append order is only approximately chronological (ids are minted before
	// the raft round trip), so take the real bounds — a key whose range does
	// not cover its contents would make the reader skip the object.
	startMs, endMs := stamps[0].ms, stamps[0].ms
	for _, st := range stamps[1:] {
		if st.ms < startMs {
			startMs = st.ms
		}
		if st.ms > endMs {
			endMs = st.ms
		}
	}
	objKey := objectKey(stream, startMs, endMs)
	blob, err := Encode(a.sealer, records)
	if err != nil {
		return 0, err
	}
	if err := store.Put(ctx, objKey, blob); err != nil {
		return 0, fmt.Errorf("archive: put %s: %w", objKey, err)
	}
	// Advance the cursor only after the object is durable. A crash between the
	// two re-archives the same records; the reader dedupes by id, so a
	// duplicate object is harmless where a lost one would not be.
	if err := a.cursors.PutCursor(ctx, key, cursorJSON(cursor.advance(stamps))); err != nil {
		return 0, fmt.Errorf("archive: save cursor: %w", err)
	}
	a.log.Info("archived", "stream", stream, "records", len(records), "key", objKey, "bytes", len(blob))
	return len(records), nil
}

// pending collects the settled, not-yet-archived records of one stream in
// chronological order, capped at maxBatch.
func (a *Archiver) pending(stream Stream, sinceMs, cutoffMs int64, seen map[string]bool) ([]proto.Message, []stamped) {
	var records []proto.Message
	var stamps []stamped
	add := func(id string, ms int64, m proto.Message) bool {
		if ms > cutoffMs || seen[id] {
			return true // not settled yet, or already archived
		}
		records = append(records, m)
		stamps = append(stamps, stamped{id: id, ms: ms})
		return len(records) < maxBatch
	}
	if stream == StreamAudit {
		for _, e := range a.src.AuditSince(sinceMs) {
			if !add(e.GetMeta().GetId(), e.GetMeta().GetCreatedAt().AsTime().UnixMilli(), e) {
				break
			}
		}
	} else {
		for _, e := range a.src.EventsSince(sinceMs) {
			if !add(e.GetMeta().GetId(), e.GetMeta().GetCreatedAt().AsTime().UnixMilli(), e) {
				break
			}
		}
	}
	return records, stamps
}
