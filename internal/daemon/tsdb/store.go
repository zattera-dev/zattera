// Package tsdb defines the embedded time-series store contract (spec §3.10):
// per-series ring buffers, 15s raw resolution for 24h + 5m downsampled for
// 30d. Implementation is task T-59; the fake lives in testutil.
package tsdb

import "time"

// SeriesKey identifies one series.
type SeriesKey struct {
	// Metric name: "cpu_percent", "memory_bytes", "rps", ...
	Metric string
	// Scope: "node", "instance", "env", "app".
	Scope string
	// ID of the scoped object.
	ScopeID string
}

// Point is one sample.
type Point struct {
	Time  time.Time
	Value float64
}

// Resolutions stored by the ring TSDB.
const (
	RawStep        = 15 * time.Second
	RawRetention   = 24 * time.Hour
	DownStep       = 5 * time.Minute
	DownRetention  = 30 * 24 * time.Hour
)

// Store is the per-node TSDB.
type Store interface {
	// Record adds a sample (typically every 15s per series). Out-of-order
	// samples older than the current slot are dropped.
	Record(key SeriesKey, p Point)
	// Query returns points in [since, until] at the best-fitting resolution
	// for step (RawStep or DownStep).
	Query(key SeriesKey, since, until time.Time, step time.Duration) []Point
	// Keys lists series matching a scope filter ("" matches all).
	Keys(scope, scopeID string) []SeriesKey
	// Flush persists ring state to disk (called periodically and on stop).
	Flush() error
	Close() error
}
