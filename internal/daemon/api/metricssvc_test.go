package api

import (
	"context"
	"testing"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/livestate"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/state"
)

// value pulls the single point of the series matching metric+scope label.
func value(t *testing.T, resp *zatterav1.StatsResponse, metric, labelKey, labelVal string) (float64, bool) {
	t.Helper()
	for _, s := range resp.GetSeries() {
		if s.GetMetric() != metric || s.GetLabels()[labelKey] != labelVal {
			continue
		}
		pts := s.GetPoints()
		if len(pts) != 1 {
			t.Fatalf("series %s/%s should have exactly one point, got %d", metric, labelVal, len(pts))
		}
		return pts[0].GetValue(), true
	}
	return 0, false
}

func TestStatsLive(t *testing.T) {
	clk := clock.NewFake()
	st := state.New()
	live := livestate.New(clk)

	// Two nodes with heartbeats; env "env1" runs on both (proxy samples sum).
	live.Heartbeat("n1", &clusterv1.Heartbeat{
		CpuPercent: 25, MemoryUsedBytes: 512 << 20, MemoryTotalBytes: 1024 << 20,
		DiskUsedBytes: 10 << 30, DiskTotalBytes: 100 << 30,
		Proxy: map[string]*clusterv1.ProxySample{
			"env1": {Rps: 10, Inflight: 3, ErrorRate: 0.02, LatencyP50Ms: 5, LatencyP99Ms: 20},
		},
	})
	live.Heartbeat("n2", &clusterv1.Heartbeat{
		CpuPercent: 75, MemoryUsedBytes: 256 << 20, MemoryTotalBytes: 1024 << 20,
		Proxy: map[string]*clusterv1.ProxySample{
			"env1": {Rps: 6, Inflight: 1, ErrorRate: 0.04, LatencyP50Ms: 7, LatencyP99Ms: 30},
		},
	})

	st.PutApp(&zatterav1.App{Meta: &zatterav1.Meta{Id: "app1"}, Name: "web", ProjectId: "p1"})
	st.PutEnvironment(&zatterav1.Environment{Meta: &zatterav1.Meta{Id: "env1"}, AppId: "app1", ProjectId: "p1", Name: "production"})

	s := NewMetricsServer(st, live, nil, clk, nil)
	ctx := context.Background()

	// Cluster (empty scope) → per-node metrics.
	all, err := s.Stats(ctx, &zatterav1.StatsQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := value(t, all, "cpu_percent", "node", "n1"); !ok || v != 25 {
		t.Fatalf("n1 cpu = %v ok=%v", v, ok)
	}
	if v, ok := value(t, all, "memory_percent", "node", "n1"); !ok || v != 50 {
		t.Fatalf("n1 mem%% = %v ok=%v, want 50", v, ok)
	}
	if v, ok := value(t, all, "cpu_percent", "node", "n2"); !ok || v != 75 {
		t.Fatalf("n2 cpu = %v ok=%v", v, ok)
	}
	// All points stamped at the fake clock's now.
	nowMs := clk.Now().UnixMilli()
	for _, ser := range all.GetSeries() {
		if ser.GetPoints()[0].GetTimeUnixMs() != nowMs {
			t.Fatalf("point not stamped at now: %d != %d", ser.GetPoints()[0].GetTimeUnixMs(), nowMs)
		}
	}

	// Single-node scope.
	one, err := s.Stats(ctx, &zatterav1.StatsQuery{NodeId: "n2"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := value(t, one, "cpu_percent", "node", "n1"); ok {
		t.Fatal("node scope must not include other nodes")
	}
	if v, ok := value(t, one, "disk_bytes", "node", "n2"); !ok || v != 0 {
		t.Fatalf("n2 disk = %v ok=%v (no disk reported → 0)", v, ok)
	}

	// Environment scope → summed rps/inflight, averaged rates.
	env, err := s.Stats(ctx, &zatterav1.StatsQuery{EnvironmentId: "env1"})
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := value(t, env, "rps", "env", "env1"); v != 16 {
		t.Fatalf("env rps = %v, want 16 (10+6)", v)
	}
	if v, _ := value(t, env, "inflight", "env", "env1"); v != 4 {
		t.Fatalf("env inflight = %v, want 4 (3+1)", v)
	}
	if v, _ := value(t, env, "latency_p99_ms", "env", "env1"); v != 25 {
		t.Fatalf("env p99 = %v, want 25 (avg 20,30)", v)
	}

	// App scope → same env samples, labeled with the app id.
	app, err := s.Stats(ctx, &zatterav1.StatsQuery{AppId: "app1"})
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := value(t, app, "rps", "env", "env1"); !ok || v != 16 {
		t.Fatalf("app-scoped env rps = %v ok=%v", v, ok)
	}
	found := false
	for _, ser := range app.GetSeries() {
		if ser.GetLabels()["app"] == "app1" {
			found = true
		}
	}
	if !found {
		t.Fatal("app scope should label series with the app id")
	}

	// Metric filter restricts the emitted set.
	filtered, err := s.Stats(ctx, &zatterav1.StatsQuery{Metrics: []string{"cpu_percent"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, ser := range filtered.GetSeries() {
		if ser.GetMetric() != "cpu_percent" {
			t.Fatalf("filter leaked metric %q", ser.GetMetric())
		}
	}
}
