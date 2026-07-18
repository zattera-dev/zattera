package archive

import (
	"context"
	"sort"

	"google.golang.org/protobuf/encoding/protojson"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/daemon/volumes"
)

// unmarshal tolerates fields written by a newer server than the one reading.
var unmarshal = protojson.UnmarshalOptions{DiscardUnknown: true}

// Reader fetches archived records back out of object storage.
type Reader struct {
	store  volumes.ObjectStore
	sealer secrets.Sealer
}

// NewReader builds a reader over a destination.
func NewReader(store volumes.ObjectStore, sealer secrets.Sealer) *Reader {
	return &Reader{store: store, sealer: sealer}
}

// Audit returns archived audit entries overlapping [sinceMs, untilMs] that
// satisfy keep, newest first, at most limit of them. untilMs <= 0 is
// open-ended. Objects whose key range misses the window are never fetched.
func (r *Reader) Audit(ctx context.Context, sinceMs, untilMs int64, limit int, keep func(*zatterav1.AuditEntry) bool) ([]*zatterav1.AuditEntry, error) {
	var out []*zatterav1.AuditEntry
	err := r.scan(ctx, StreamAudit, sinceMs, untilMs, limit, func(line []byte) (bool, error) {
		var e zatterav1.AuditEntry
		if err := unmarshal.Unmarshal(line, &e); err != nil {
			return false, err
		}
		ms := e.GetMeta().GetCreatedAt().AsTime().UnixMilli()
		if ms < sinceMs || (untilMs > 0 && ms > untilMs) {
			return false, nil
		}
		if keep != nil && !keep(&e) {
			return false, nil
		}
		out = append(out, &e)
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	sortAuditNewestFirst(out)
	return capSlice(out, limit), nil
}

// Events is Audit for the event stream.
func (r *Reader) Events(ctx context.Context, sinceMs, untilMs int64, limit int, keep func(*zatterav1.Event) bool) ([]*zatterav1.Event, error) {
	var out []*zatterav1.Event
	err := r.scan(ctx, StreamEvents, sinceMs, untilMs, limit, func(line []byte) (bool, error) {
		var e zatterav1.Event
		if err := unmarshal.Unmarshal(line, &e); err != nil {
			return false, err
		}
		ms := e.GetMeta().GetCreatedAt().AsTime().UnixMilli()
		if ms < sinceMs || (untilMs > 0 && ms > untilMs) {
			return false, nil
		}
		if keep != nil && !keep(&e) {
			return false, nil
		}
		out = append(out, &e)
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	sortEventsNewestFirst(out)
	return capSlice(out, limit), nil
}

// scan walks matching objects newest-first and feeds each line to emit, which
// reports whether the record was kept. It stops once limit records are kept.
//
// Newest-first matters: a 90-day query with a limit of 100 should read one
// recent object, not every object since the beginning of the archive.
func (r *Reader) scan(ctx context.Context, stream Stream, sinceMs, untilMs int64, limit int, emit func([]byte) (bool, error)) error {
	keys, err := listOverlapping(ctx, r.store, stream, sinceMs, untilMs)
	if err != nil {
		return err
	}
	kept := 0
	for i := len(keys) - 1; i >= 0; i-- {
		if limit > 0 && kept >= limit {
			return nil
		}
		blob, err := r.store.Get(ctx, keys[i])
		if err != nil {
			return err
		}
		derr := decode(r.sealer, blob, func(line []byte) error {
			ok, err := emit(line)
			if err != nil {
				return err
			}
			if ok {
				kept++
			}
			return nil
		})
		if derr != nil {
			return derr
		}
	}
	return nil
}

func capSlice[T any](s []T, limit int) []T {
	if limit > 0 && len(s) > limit {
		return s[:limit]
	}
	return s
}

func sortAuditNewestFirst(s []*zatterav1.AuditEntry) {
	sort.SliceStable(s, func(i, j int) bool {
		return s[i].GetMeta().GetCreatedAt().AsTime().After(s[j].GetMeta().GetCreatedAt().AsTime())
	})
}

func sortEventsNewestFirst(s []*zatterav1.Event) {
	sort.SliceStable(s, func(i, j int) bool {
		return s[i].GetMeta().GetCreatedAt().AsTime().After(s[j].GetMeta().GetCreatedAt().AsTime())
	})
}
