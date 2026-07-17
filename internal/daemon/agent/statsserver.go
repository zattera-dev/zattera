package agent

import (
	"context"
	"sort"
	"time"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/tsdb"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// defaultStatsWindow is the range served when a query omits `since`.
const defaultStatsWindow = time.Hour

// StatsServer serves AgentLocalService.Stats from this node's local ring TSDB
// (T-60). The control plane fans a StatsQuery out to each relevant node and
// merges the returned series; this side just applies the scope/metric/time
// filter against local series.
type StatsServer struct {
	store  tsdb.Store
	clk    clock.Clock
	nodeID string
}

// NewStatsServer builds the agent-side historical stats server.
func NewStatsServer(store tsdb.Store, nodeID string, clk clock.Clock) *StatsServer {
	if clk == nil {
		clk = clock.Real{}
	}
	return &StatsServer{store: store, clk: clk, nodeID: nodeID}
}

// Stats returns local series matching the query's scope, metric filter and time
// range at the resolution nearest the requested step.
func (s *StatsServer) Stats(_ context.Context, q *zatterav1.StatsQuery) (*zatterav1.StatsResponse, error) {
	until := s.clk.Now()
	if q.GetUntil() != nil {
		until = q.GetUntil().AsTime()
	}
	since := until.Add(-defaultStatsWindow)
	if q.GetSince() != nil {
		since = q.GetSince().AsTime()
	}
	step := tsdb.RawStep
	if secs := q.GetStepSeconds(); secs > 0 {
		step = time.Duration(secs) * time.Second
	}
	want := statsMetricFilter(q.GetMetrics())

	var out []*zatterav1.Series
	for _, key := range s.store.Keys("", "") {
		if want != nil && !want[key.Metric] {
			continue
		}
		if !s.scopeMatches(key, q) {
			continue
		}
		pts := s.store.Query(key, since, until, step)
		if len(pts) == 0 {
			continue
		}
		points := make([]*zatterav1.Point, 0, len(pts))
		for _, p := range pts {
			points = append(points, &zatterav1.Point{TimeUnixMs: p.Time.UnixMilli(), Value: p.Value})
		}
		out = append(out, &zatterav1.Series{
			Metric: key.Metric,
			Labels: map[string]string{key.Scope: key.ScopeID},
			Points: points,
		})
	}
	sortSeries(out)
	return &zatterav1.StatsResponse{Series: out}, nil
}

// scopeMatches decides whether a local series belongs to the query's scope. The
// agent cannot resolve app→env or instance→env (that is control state), so for
// an env/app/cluster query it returns the candidate series (env proxy series and
// all instance series) and lets control filter and aggregate.
func (s *StatsServer) scopeMatches(key tsdb.SeriesKey, q *zatterav1.StatsQuery) bool {
	switch {
	case q.GetNodeId() != "":
		return key.Scope == scopeNode && key.ScopeID == q.GetNodeId()
	case q.GetEnvironmentId() != "" || q.GetAppId() != "":
		return key.Scope == scopeEnv || key.Scope == scopeInstance
	default: // cluster scope
		return key.Scope == scopeNode
	}
}

// statsMetricFilter returns the set of metrics to emit, or nil for "all".
func statsMetricFilter(metrics []string) map[string]bool {
	if len(metrics) == 0 {
		return nil
	}
	want := make(map[string]bool, len(metrics))
	for _, m := range metrics {
		want[m] = true
	}
	return want
}

// sortSeries orders series by metric then scope id for deterministic output.
func sortSeries(series []*zatterav1.Series) {
	sort.Slice(series, func(i, j int) bool {
		if series[i].GetMetric() != series[j].GetMetric() {
			return series[i].GetMetric() < series[j].GetMetric()
		}
		return seriesScopeID(series[i]) < seriesScopeID(series[j])
	})
}

func seriesScopeID(s *zatterav1.Series) string {
	for _, v := range s.GetLabels() {
		return v
	}
	return ""
}
