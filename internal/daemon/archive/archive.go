// Package archive gives audit entries and events durability beyond their
// in-state rings (T-92).
//
// Both histories live in capped rings inside replicated state (50k audit
// entries, 10k events), so on a busy cluster they age out in days — fine for
// "what just happened", useless for "who deleted this app in April". When
// backup.archive is on, a leader-gated sweeper continuously copies both
// streams to the cluster's existing S3 destination as gzipped NDJSON, sealed
// with the cluster data key exactly like a backup, and the query RPCs can read
// them back and merge with the ring.
//
// Objects are laid out so a time-range query can skip most of them without a
// download:
//
//	audit/<YYYY-MM-DD>/<startMs>-<endMs>-<ulid>.ndjson.gz.enc
//	events/<YYYY-MM-DD>/<startMs>-<endMs>-<ulid>.ndjson.gz.enc
//
// Nothing here ever deletes an object: the archive is the durable copy, and
// how long it is kept is the bucket's lifecycle policy to decide.
package archive

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/daemon/volumes"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
)

// Stream names the two archived histories; it is also the object key prefix.
type Stream string

const (
	StreamAudit  Stream = "audit"
	StreamEvents Stream = "events"
)

// objectSuffix is appended to every archived object. The chain reads
// outside-in: newline-delimited JSON, gzipped, then sealed.
const objectSuffix = ".ndjson.gz.enc"

// Cursor is the archiver's resume point for one stream, persisted in the
// replicated KV.
//
// It is a millisecond watermark plus the ids already written at exactly that
// millisecond. A bare watermark cannot be both complete and duplicate-free:
// exclusive (> ms) drops entries sharing the boundary millisecond, inclusive
// (>= ms) re-writes them. Audit ids are minted before the raft round trip so
// they are not a strict watermark either, hence time plus an id set.
type Cursor struct {
	Ms  int64    `json:"ms"`
	IDs []string `json:"ids,omitempty"`
}

func (c Cursor) seen() map[string]bool {
	m := make(map[string]bool, len(c.IDs))
	for _, id := range c.IDs {
		m[id] = true
	}
	return m
}

// advance folds a batch of (id, ms) pairs into the cursor.
func (c Cursor) advance(written []stamped) Cursor {
	next := c
	for _, w := range written {
		switch {
		case w.ms > next.Ms:
			next.Ms, next.IDs = w.ms, []string{w.id}
		case w.ms == next.Ms:
			next.IDs = append(next.IDs, w.id)
		}
	}
	return next
}

type stamped struct {
	id string
	ms int64
}

// Encode marshals records to sealed, gzipped NDJSON.
func Encode(sealer secrets.Sealer, records []proto.Message) ([]byte, error) {
	var raw bytes.Buffer
	zw := gzip.NewWriter(&raw)
	// protojson keeps the archive self-describing and greppable once decrypted,
	// and tolerates proto field additions on the way back in.
	marshal := protojson.MarshalOptions{}
	for _, rec := range records {
		line, err := marshal.Marshal(rec)
		if err != nil {
			return nil, fmt.Errorf("archive: marshal record: %w", err)
		}
		if _, err := zw.Write(append(line, '\n')); err != nil {
			return nil, err
		}
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	sealed, err := sealer.Seal(raw.Bytes())
	if err != nil {
		return nil, fmt.Errorf("archive: seal: %w", err)
	}
	blob, err := proto.Marshal(sealed)
	if err != nil {
		return nil, fmt.Errorf("archive: marshal sealed blob: %w", err)
	}
	return blob, nil
}

// decode reverses Encode, calling emit for each JSON line.
func decode(sealer secrets.Sealer, blob []byte, emit func([]byte) error) error {
	var enc zatterav1.EncryptedValue
	if err := proto.Unmarshal(blob, &enc); err != nil {
		return fmt.Errorf("archive: unmarshal sealed blob: %w", err)
	}
	raw, err := sealer.Open(&enc)
	if err != nil {
		return fmt.Errorf("archive: open: %w", err)
	}
	zr, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("archive: gunzip: %w", err)
	}
	defer func() { _ = zr.Close() }()
	plain, err := io.ReadAll(zr)
	if err != nil {
		return fmt.Errorf("archive: read: %w", err)
	}
	for _, line := range bytes.Split(plain, []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		if err := emit(line); err != nil {
			return err
		}
	}
	return nil
}

// objectKey builds the key for a batch spanning [startMs, endMs].
func objectKey(stream Stream, startMs, endMs int64) string {
	day := time.UnixMilli(startMs).UTC().Format("2006-01-02")
	return path.Join(string(stream), day, fmt.Sprintf("%d-%d-%s%s", startMs, endMs, ids.New(), objectSuffix))
}

// parseKeyRange recovers the [startMs, endMs] a key covers so a reader can skip
// non-overlapping objects without fetching them. ok is false for anything that
// does not look like one of our objects.
func parseKeyRange(key string) (startMs, endMs int64, ok bool) {
	base := path.Base(key)
	if !strings.HasSuffix(base, objectSuffix) {
		return 0, 0, false
	}
	parts := strings.SplitN(strings.TrimSuffix(base, objectSuffix), "-", 3)
	if len(parts) < 2 {
		return 0, 0, false
	}
	start, err1 := strconv.ParseInt(parts[0], 10, 64)
	end, err2 := strconv.ParseInt(parts[1], 10, 64)
	if err1 != nil || err2 != nil {
		return 0, 0, false
	}
	return start, end, true
}

// listOverlapping returns the keys of objects whose range intersects
// [sinceMs, untilMs] (untilMs <= 0 means open-ended), oldest first.
func listOverlapping(ctx context.Context, store volumes.ObjectStore, stream Stream, sinceMs, untilMs int64) ([]string, error) {
	keys, err := store.List(ctx, string(stream)+"/")
	if err != nil {
		return nil, err
	}
	var out []string
	for _, k := range keys {
		start, end, ok := parseKeyRange(k)
		if !ok {
			continue
		}
		if end < sinceMs {
			continue
		}
		if untilMs > 0 && start > untilMs {
			continue
		}
		out = append(out, k)
	}
	// The start is an unpadded decimal, so lexical key order is not time order.
	sort.Slice(out, func(i, j int) bool {
		si, _, _ := parseKeyRange(out[i])
		sj, _, _ := parseKeyRange(out[j])
		return si < sj
	})
	return out, nil
}

// cursorJSON round-trips a Cursor through the KV.
func cursorJSON(c Cursor) []byte {
	b, _ := json.Marshal(c)
	return b
}

func parseCursor(b []byte) Cursor {
	var c Cursor
	if len(b) == 0 {
		return c
	}
	_ = json.Unmarshal(b, &c)
	return c
}
