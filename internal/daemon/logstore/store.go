// Package logstore defines the per-node log storage contract (spec §3.10).
// The segmented, zstd-compressed implementation is task T-50; the fake lives
// in testutil.
package logstore

import (
	"context"
	"time"
)

// StreamID identifies one log stream. For runtime logs use the assignment id
// prefixed "instance/"; for build logs "build/<build-id>"; for job runs
// "job/<job-id>".
type StreamID string

// Entry is one log line with its origin metadata.
type Entry struct {
	Time   time.Time
	Stderr bool
	Line   string
}

// Query selects entries from one or more streams.
type Query struct {
	Streams []StreamID
	Since   time.Time
	Until   time.Time // zero = now
	Limit   int       // 0 = default 1000
}

// Store is the per-node log store. Append is called by the agent's tailers;
// Query/Follow serve the AgentLocalService fan-out.
type Store interface {
	// Append adds entries to a stream (creating it on first write). Entries
	// must be in non-decreasing time order per call.
	Append(stream StreamID, entries []Entry) error
	// Query returns matching entries in time order.
	Query(ctx context.Context, q Query) ([]Entry, error)
	// Follow returns history matching q, then keeps streaming live entries
	// until ctx is canceled. The channel is closed on ctx cancellation.
	Follow(ctx context.Context, q Query) (<-chan Entry, error)
	// DeleteStream drops a stream's segments (retention, app deletion).
	DeleteStream(stream StreamID) error
	Close() error
}
