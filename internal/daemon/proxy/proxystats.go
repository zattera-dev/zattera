package proxy

import (
	"sort"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// latencyRing bounds retained per-env latency samples for percentile estimates.
const latencyRing = 512

// Stats accumulates per-environment proxy metrics. The agent heartbeat reads a
// Snapshot into ProxySample (T-14).
type Stats struct {
	clk clock.Clock

	mu   sync.Mutex
	envs map[string]*envStat
}

type envStat struct {
	inflight int64
	requests int64 // cumulative
	errors   int64 // cumulative
	lastReq  time.Time
	lat      []float64 // recent latencies (ms), bounded

	lastSnapTime time.Time
	lastSnapReqs int64
}

// NewStats constructs the metrics accumulator.
func NewStats(clk clock.Clock) *Stats {
	if clk == nil {
		clk = clock.Real{}
	}
	return &Stats{clk: clk, envs: map[string]*envStat{}}
}

func (s *Stats) get(env string) *envStat {
	es, ok := s.envs[env]
	if !ok {
		es = &envStat{lastSnapTime: s.clk.Now()}
		s.envs[env] = es
	}
	return es
}

// begin records the start of a request for env.
func (s *Stats) begin(env string) {
	s.mu.Lock()
	es := s.get(env)
	es.inflight++
	es.requests++
	es.lastReq = s.clk.Now()
	s.mu.Unlock()
}

// end records the completion of a request, its latency and error state.
func (s *Stats) end(env string, latencyMs float64, isErr bool) {
	s.mu.Lock()
	es := s.get(env)
	es.inflight--
	if isErr {
		es.errors++
	}
	es.lat = append(es.lat, latencyMs)
	if len(es.lat) > latencyRing {
		es.lat = es.lat[len(es.lat)-latencyRing:]
	}
	s.mu.Unlock()
}

// Inflight returns the current in-flight count for an env (test/introspection).
func (s *Stats) Inflight(env string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if es, ok := s.envs[env]; ok {
		return es.inflight
	}
	return 0
}

// Snapshot returns the current per-env ProxySample and resets the rps window.
func (s *Stats) Snapshot() map[string]*clusterv1.ProxySample {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clk.Now()
	out := make(map[string]*clusterv1.ProxySample, len(s.envs))
	for env, es := range s.envs {
		p50, p99 := percentiles(es.lat)
		var rps float64
		if dt := now.Sub(es.lastSnapTime).Seconds(); dt > 0 {
			rps = float64(es.requests-es.lastSnapReqs) / dt
		}
		var errRate float64
		if es.requests > 0 {
			errRate = float64(es.errors) / float64(es.requests)
		}
		sample := &clusterv1.ProxySample{
			Inflight:     uint32(max64(es.inflight, 0)),
			Rps:          rps,
			ErrorRate:    errRate,
			LatencyP50Ms: p50,
			LatencyP99Ms: p99,
		}
		if !es.lastReq.IsZero() {
			sample.LastRequestAt = timestamppb.New(es.lastReq)
		}
		out[env] = sample
		es.lastSnapTime = now
		es.lastSnapReqs = es.requests
	}
	return out
}

func percentiles(lat []float64) (p50, p99 float64) {
	if len(lat) == 0 {
		return 0, 0
	}
	sorted := append([]float64(nil), lat...)
	sort.Float64s(sorted)
	return sorted[len(sorted)*50/100], sorted[min(len(sorted)*99/100, len(sorted)-1)]
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
