package api

import (
	"context"
	"log/slog"
	"sort"
	"time"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/livestate"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/state"
)

// statsNodeTimeout bounds a per-node historical query so a dead node can't hang
// the fan-out.
const statsNodeTimeout = 3 * time.Second

// StatsDialer queries one node's AgentLocalService.Stats (over the mesh in
// production; a fake in tests). nil disables the historical path.
type StatsDialer interface {
	Stats(ctx context.Context, node *zatterav1.Node, q *zatterav1.StatsQuery) (*zatterav1.StatsResponse, error)
}

// Node-level metrics served from the latest heartbeat.
var nodeMetrics = []string{"cpu_percent", "memory_bytes", "memory_percent", "disk_bytes", "disk_percent"}

// Environment-level metrics served from proxy samples.
var envMetrics = []string{"rps", "inflight", "error_rate", "latency_p50_ms", "latency_p99_ms"}

// MetricsServer implements MetricsService.Stats. v1 (T-51) serves ONLY current
// values from livestate — one Point per Series, stamped now — so the CLI/API
// shape matches the historical TSDB that lands in T-59/T-60.
type MetricsServer struct {
	zatterav1.UnimplementedMetricsServiceServer
	store *state.Store
	live  *livestate.Registry
	clk   clock.Clock
	dial  StatsDialer
	log   *slog.Logger
}

// NewMetricsServer builds the metrics service. dial enables the historical
// (TSDB) path — a query with a `since` bound fans out to the nodes' local ring
// TSDBs (T-60); a nil dial keeps only the live path. clk/log default when nil.
func NewMetricsServer(store *state.Store, live *livestate.Registry, dial StatsDialer, clk clock.Clock, log *slog.Logger) *MetricsServer {
	if clk == nil {
		clk = clock.Real{}
	}
	if log == nil {
		log = slog.Default()
	}
	return &MetricsServer{store: store, live: live, clk: clk, dial: dial, log: log}
}

// Stats returns single-point series scoped to a node, environment, app, or the
// whole cluster (empty scope → all nodes).
func (s *MetricsServer) Stats(ctx context.Context, q *zatterav1.StatsQuery) (*zatterav1.StatsResponse, error) {
	// A query with a time range reads history from the nodes' ring TSDBs; without
	// one (or without a configured dialer) it serves current values from
	// livestate — same series shape either way.
	if q.GetSince() != nil && s.dial != nil {
		return s.statsHistory(ctx, q)
	}

	nowMs := s.clk.Now().UnixMilli()
	want := metricFilter(q.GetMetrics())

	var series []*zatterav1.Series
	switch {
	case q.GetEnvironmentId() != "":
		series = s.envSeries(map[string]bool{q.GetEnvironmentId(): true}, "", nowMs, want)
	case q.GetAppId() != "":
		envIDs, appID := s.appEnvironments(q.GetAppId())
		series = s.envSeries(envIDs, appID, nowMs, want)
	case q.GetNodeId() != "":
		if ns, ok := s.live.Get(q.GetNodeId()); ok {
			series = s.nodeSeries([]livestate.NodeState{ns}, nowMs, want)
		}
	default:
		series = s.nodeSeries(s.live.Snapshot(), nowMs, want)
	}
	return &zatterav1.StatsResponse{Series: series}, nil
}

// nodeSeries builds per-node single-point series (one per node × metric).
func (s *MetricsServer) nodeSeries(nodes []livestate.NodeState, nowMs int64, want map[string]bool) []*zatterav1.Series {
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].NodeID < nodes[j].NodeID })
	var out []*zatterav1.Series
	for _, ns := range nodes {
		hb := ns.Heartbeat
		if hb == nil {
			continue
		}
		labels := map[string]string{"node": ns.NodeID}
		add := func(metric string, value float64) {
			if want[metric] {
				out = append(out, point(metric, labels, nowMs, value))
			}
		}
		add("cpu_percent", hb.GetCpuPercent())
		add("memory_bytes", float64(hb.GetMemoryUsedBytes()))
		add("memory_percent", pct(hb.GetMemoryUsedBytes(), hb.GetMemoryTotalBytes()))
		add("disk_bytes", float64(hb.GetDiskUsedBytes()))
		add("disk_percent", pct(hb.GetDiskUsedBytes(), hb.GetDiskTotalBytes()))
	}
	return out
}

// envSeries aggregates proxy samples for the given environments across every
// node reporting them. rps/inflight sum; rates/latencies average across nodes.
func (s *MetricsServer) envSeries(envIDs map[string]bool, appID string, nowMs int64, want map[string]bool) []*zatterav1.Series {
	type agg struct {
		rps, inflight           float64
		errRate, p50, p99       float64
		rateSamples, latSamples int
	}
	byEnv := map[string]*agg{}
	for _, ns := range s.live.Snapshot() {
		if ns.Heartbeat == nil {
			continue
		}
		for envID, ps := range ns.Heartbeat.GetProxy() {
			if !envIDs[envID] {
				continue
			}
			a := byEnv[envID]
			if a == nil {
				a = &agg{}
				byEnv[envID] = a
			}
			a.rps += ps.GetRps()
			a.inflight += float64(ps.GetInflight())
			a.errRate += ps.GetErrorRate()
			a.p50 += ps.GetLatencyP50Ms()
			a.p99 += ps.GetLatencyP99Ms()
			a.rateSamples++
			a.latSamples++
		}
	}

	envIDList := make([]string, 0, len(byEnv))
	for id := range byEnv {
		envIDList = append(envIDList, id)
	}
	sort.Strings(envIDList)

	var out []*zatterav1.Series
	for _, envID := range envIDList {
		a := byEnv[envID]
		labels := map[string]string{"env": envID}
		if appID != "" {
			labels["app"] = appID
		}
		add := func(metric string, value float64) {
			if want[metric] {
				out = append(out, point(metric, labels, nowMs, value))
			}
		}
		add("rps", a.rps)
		add("inflight", a.inflight)
		add("error_rate", avg(a.errRate, a.rateSamples))
		add("latency_p50_ms", avg(a.p50, a.latSamples))
		add("latency_p99_ms", avg(a.p99, a.latSamples))
	}
	return out
}

// appEnvironments resolves an app id (or name, via any project) to its
// environment ids.
func (s *MetricsServer) appEnvironments(appRef string) (map[string]bool, string) {
	app, ok := s.store.App(appRef)
	if !ok {
		return map[string]bool{}, appRef
	}
	appID := app.GetMeta().GetId()
	envIDs := map[string]bool{}
	for _, env := range s.store.ListEnvironments(app.GetProjectId(), appID) {
		envIDs[env.GetMeta().GetId()] = true
	}
	return envIDs, appID
}

// metricFilter returns the set of metrics to emit; empty request → the union of
// the default node + env sets (each scope only emits the ones it knows).
func metricFilter(requested []string) map[string]bool {
	want := map[string]bool{}
	if len(requested) == 0 {
		for _, m := range nodeMetrics {
			want[m] = true
		}
		for _, m := range envMetrics {
			want[m] = true
		}
		return want
	}
	for _, m := range requested {
		want[m] = true
	}
	return want
}

func point(metric string, labels map[string]string, tMs int64, value float64) *zatterav1.Series {
	return &zatterav1.Series{
		Metric: metric,
		Labels: labels,
		Points: []*zatterav1.Point{{TimeUnixMs: tMs, Value: value}},
	}
}

func pct(used, total uint64) float64 {
	if total == 0 {
		return 0
	}
	return float64(used) / float64(total) * 100
}

func avg(sum float64, n int) float64 {
	if n == 0 {
		return 0
	}
	return sum / float64(n)
}
