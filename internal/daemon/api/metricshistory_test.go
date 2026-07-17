package api

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/agent"
	"github.com/zattera-dev/zattera/internal/daemon/livestate"
	"github.com/zattera-dev/zattera/internal/daemon/tsdb"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/state"
)

// fakeStatsDialer routes a fan-out query to per-node agent StatsServers backed by
// real ring TSDBs — exercising the agent scope filter and the control merge.
type fakeStatsDialer struct{ servers map[string]*agent.StatsServer }

func (f fakeStatsDialer) Stats(ctx context.Context, node *zatterav1.Node, q *zatterav1.StatsQuery) (*zatterav1.StatsResponse, error) {
	srv, ok := f.servers[node.GetMeta().GetId()]
	if !ok {
		return &zatterav1.StatsResponse{}, nil
	}
	return srv.Stats(ctx, q)
}

func recordSeries(store tsdb.Store, metric, scope, id string, at []time.Time, vals []float64) {
	for i := range at {
		store.Record(tsdb.SeriesKey{Metric: metric, Scope: scope, ScopeID: id}, tsdb.Point{Time: at[i], Value: vals[i]})
	}
}

// histPoints returns the values of the series matching metric+label, ordered by
// time; fails if the series is absent.
func histPoints(t *testing.T, resp *zatterav1.StatsResponse, metric, labelKey, labelVal string) []float64 {
	t.Helper()
	for _, s := range resp.GetSeries() {
		if s.GetMetric() != metric || s.GetLabels()[labelKey] != labelVal {
			continue
		}
		out := make([]float64, 0, len(s.GetPoints()))
		for _, p := range s.GetPoints() {
			out = append(out, p.GetValue())
		}
		return out
	}
	t.Fatalf("series %s{%s=%s} not found in %d series", metric, labelKey, labelVal, len(resp.GetSeries()))
	return nil
}

func hasSeries(resp *zatterav1.StatsResponse, metric, labelKey, labelVal string) bool {
	for _, s := range resp.GetSeries() {
		if s.GetMetric() == metric && s.GetLabels()[labelKey] == labelVal {
			return true
		}
	}
	return false
}

func TestStatsHistory(t *testing.T) {
	clk := clock.NewFake()
	now := clk.Now()
	t0, t1 := now.Add(-30*time.Second), now.Add(-15*time.Second)
	at := []time.Time{t0, t1}

	// Per-node TSDBs. n1 is control+worker (runs ingress → env proxy series);
	// n2 is a worker.
	s1 := tsdb.Open(tsdb.Config{})
	s2 := tsdb.Open(tsdb.Config{})
	defer s1.Close()
	defer s2.Close()

	recordSeries(s1, "cpu_percent", "node", "n1", at, []float64{25, 30})
	recordSeries(s1, "rps", "env", "env1", at, []float64{10, 12})
	recordSeries(s1, "cpu_percent", "instance", "inst-a", at, []float64{20, 22})
	recordSeries(s1, "memory_bytes", "instance", "inst-a", at, []float64{100, 110})

	recordSeries(s2, "cpu_percent", "node", "n2", at, []float64{50, 55})
	recordSeries(s2, "cpu_percent", "instance", "inst-b", at, []float64{40, 42})
	recordSeries(s2, "memory_bytes", "instance", "inst-b", at, []float64{200, 210})
	recordSeries(s2, "cpu_percent", "instance", "inst-c", at, []float64{90, 92}) // env2

	dial := fakeStatsDialer{servers: map[string]*agent.StatsServer{
		"n1": agent.NewStatsServer(s1, "n1", clk),
		"n2": agent.NewStatsServer(s2, "n2", clk),
	}}

	st := state.New()
	st.PutNode(&zatterav1.Node{Meta: &zatterav1.Meta{Id: "n1"}})
	st.PutNode(&zatterav1.Node{Meta: &zatterav1.Meta{Id: "n2"}})
	st.PutApp(&zatterav1.App{Meta: &zatterav1.Meta{Id: "app1"}, Name: "web", ProjectId: "p1"})
	st.PutEnvironment(&zatterav1.Environment{Meta: &zatterav1.Meta{Id: "env1"}, AppId: "app1", ProjectId: "p1", Name: "production"})
	st.PutEnvironment(&zatterav1.Environment{Meta: &zatterav1.Meta{Id: "env2"}, AppId: "app1", ProjectId: "p1", Name: "staging"})
	st.PutAssignment(&zatterav1.Assignment{Meta: &zatterav1.Meta{Id: "inst-a"}, EnvironmentId: "env1", AppId: "app1", ProjectId: "p1", NodeId: "n1"})
	st.PutAssignment(&zatterav1.Assignment{Meta: &zatterav1.Meta{Id: "inst-b"}, EnvironmentId: "env1", AppId: "app1", ProjectId: "p1", NodeId: "n2"})
	st.PutAssignment(&zatterav1.Assignment{Meta: &zatterav1.Meta{Id: "inst-c"}, EnvironmentId: "env2", AppId: "app1", ProjectId: "p1", NodeId: "n2"})

	s := NewMetricsServer(st, livestate.New(clk), dial, clk, nil)
	ctx := context.Background()
	since := timestamppb.New(now.Add(-time.Hour))

	t.Run("node scope passthrough", func(t *testing.T) {
		resp, err := s.Stats(ctx, &zatterav1.StatsQuery{NodeId: "n1", Since: since})
		if err != nil {
			t.Fatal(err)
		}
		if got := histPoints(t, resp, "cpu_percent", "node", "n1"); len(got) != 2 || got[0] != 25 || got[1] != 30 {
			t.Fatalf("node cpu = %v, want [25 30]", got)
		}
		// A node-scope query must not leak env/instance series.
		if hasSeries(resp, "rps", "env", "env1") {
			t.Fatal("node scope leaked env series")
		}
	})

	t.Run("cluster scope spans nodes", func(t *testing.T) {
		resp, err := s.Stats(ctx, &zatterav1.StatsQuery{Since: since})
		if err != nil {
			t.Fatal(err)
		}
		if !hasSeries(resp, "cpu_percent", "node", "n1") || !hasSeries(resp, "cpu_percent", "node", "n2") {
			t.Fatalf("cluster scope missing a node: %d series", len(resp.GetSeries()))
		}
	})

	t.Run("env scope aggregates", func(t *testing.T) {
		resp, err := s.Stats(ctx, &zatterav1.StatsQuery{EnvironmentId: "env1", Since: since})
		if err != nil {
			t.Fatal(err)
		}
		// rps sums across nodes (only n1 reports env1) → unchanged.
		if got := histPoints(t, resp, "rps", "env", "env1"); len(got) != 2 || got[0] != 10 || got[1] != 12 {
			t.Fatalf("env rps = %v, want [10 12]", got)
		}
		// cpu averages across the env's instances (inst-a, inst-b).
		if got := histPoints(t, resp, "cpu_percent", "env", "env1"); len(got) != 2 || got[0] != 30 || got[1] != 32 {
			t.Fatalf("env cpu = %v, want [30 32]", got)
		}
		// memory sums across the env's instances.
		if got := histPoints(t, resp, "memory_bytes", "env", "env1"); len(got) != 2 || got[0] != 300 || got[1] != 320 {
			t.Fatalf("env mem = %v, want [300 320]", got)
		}
		// inst-c belongs to env2 and must not contribute.
		if got := histPoints(t, resp, "cpu_percent", "env", "env1"); got[0] == 90 {
			t.Fatal("env1 cpu wrongly included inst-c")
		}
	})

	t.Run("app scope spans its envs", func(t *testing.T) {
		resp, err := s.Stats(ctx, &zatterav1.StatsQuery{AppId: "app1", Since: since})
		if err != nil {
			t.Fatal(err)
		}
		if !hasSeries(resp, "cpu_percent", "env", "env1") || !hasSeries(resp, "cpu_percent", "env", "env2") {
			t.Fatalf("app scope missing an env: %d series", len(resp.GetSeries()))
		}
		// env2 cpu is inst-c alone.
		if got := histPoints(t, resp, "cpu_percent", "env", "env2"); len(got) != 2 || got[0] != 90 {
			t.Fatalf("env2 cpu = %v, want [90 92]", got)
		}
		// app label is stamped on the aggregated series.
		for _, ser := range resp.GetSeries() {
			if ser.GetLabels()["app"] != "app1" {
				t.Fatalf("series %s missing app label: %v", ser.GetMetric(), ser.GetLabels())
			}
		}
	})

	t.Run("no since falls back to live", func(t *testing.T) {
		// Without a since bound the historical path is skipped; live has no data
		// here, so the response is empty rather than an error.
		resp, err := s.Stats(ctx, &zatterav1.StatsQuery{NodeId: "n1"})
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.GetSeries()) != 0 {
			t.Fatalf("expected live fallback (empty), got %d series", len(resp.GetSeries()))
		}
	})
}
