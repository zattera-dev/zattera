package agent

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/tsdb"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

func seedStore(t *testing.T, clk *clock.Fake) tsdb.Store {
	t.Helper()
	s := tsdb.Open(tsdb.Config{})
	t.Cleanup(func() { _ = s.Close() })
	now := clk.Now()
	rec := func(metric, scope, id string) {
		s.Record(tsdb.SeriesKey{Metric: metric, Scope: scope, ScopeID: id}, tsdb.Point{Time: now.Add(-15 * time.Second), Value: 1})
		s.Record(tsdb.SeriesKey{Metric: metric, Scope: scope, ScopeID: id}, tsdb.Point{Time: now, Value: 2})
	}
	rec("cpu_percent", "node", "n1")
	rec("rps", "env", "env1")
	rec("cpu_percent", "instance", "inst-a")
	return s
}

func TestStatsServerNodeScope(t *testing.T) {
	clk := clock.NewFake()
	srv := NewStatsServer(seedStore(t, clk), "n1", clk)
	since := timestamppb.New(clk.Now().Add(-time.Hour))

	resp, err := srv.Stats(context.Background(), &zatterav1.StatsQuery{NodeId: "n1", Since: since})
	if err != nil {
		t.Fatal(err)
	}
	// Node scope returns only the node series with both points.
	if len(resp.GetSeries()) != 1 {
		t.Fatalf("want 1 series, got %d", len(resp.GetSeries()))
	}
	ser := resp.GetSeries()[0]
	if ser.GetMetric() != "cpu_percent" || ser.GetLabels()["node"] != "n1" {
		t.Fatalf("unexpected series %s %v", ser.GetMetric(), ser.GetLabels())
	}
	if len(ser.GetPoints()) != 2 {
		t.Fatalf("want 2 points, got %d", len(ser.GetPoints()))
	}
}

func TestStatsServerEnvScopeReturnsEnvAndInstances(t *testing.T) {
	clk := clock.NewFake()
	srv := NewStatsServer(seedStore(t, clk), "n1", clk)
	since := timestamppb.New(clk.Now().Add(-time.Hour))

	resp, err := srv.Stats(context.Background(), &zatterav1.StatsQuery{EnvironmentId: "env1", Since: since})
	if err != nil {
		t.Fatal(err)
	}
	var env, inst, node int
	for _, s := range resp.GetSeries() {
		switch {
		case s.GetLabels()["env"] != "":
			env++
		case s.GetLabels()["instance"] != "":
			inst++
		case s.GetLabels()["node"] != "":
			node++
		}
	}
	if env != 1 || inst != 1 {
		t.Fatalf("env scope: want env=1 inst=1, got env=%d inst=%d", env, inst)
	}
	if node != 0 {
		t.Fatalf("env scope leaked %d node series", node)
	}
}

func TestStatsServerMetricFilter(t *testing.T) {
	clk := clock.NewFake()
	srv := NewStatsServer(seedStore(t, clk), "n1", clk)
	since := timestamppb.New(clk.Now().Add(-time.Hour))

	resp, err := srv.Stats(context.Background(), &zatterav1.StatsQuery{
		EnvironmentId: "env1", Metrics: []string{"rps"}, Since: since,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range resp.GetSeries() {
		if s.GetMetric() != "rps" {
			t.Fatalf("metric filter leaked %s", s.GetMetric())
		}
	}
	if len(resp.GetSeries()) != 1 {
		t.Fatalf("want 1 rps series, got %d", len(resp.GetSeries()))
	}
}
