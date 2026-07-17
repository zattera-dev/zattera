package api

import (
	"context"
	"sort"
	"sync"

	"google.golang.org/grpc"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

// statsHistory serves a time-ranged query from the nodes' ring TSDBs. Node and
// cluster scopes concatenate each node's series; env and app scopes aggregate
// per-env proxy series and per-instance resource series into env-level series.
func (s *MetricsServer) statsHistory(ctx context.Context, q *zatterav1.StatsQuery) (*zatterav1.StatsResponse, error) {
	switch {
	case q.GetNodeId() != "":
		node, ok := s.store.Node(q.GetNodeId())
		if !ok {
			return &zatterav1.StatsResponse{}, nil
		}
		return &zatterav1.StatsResponse{Series: s.fanOut(ctx, []*zatterav1.Node{node}, q)}, nil
	case q.GetAppId() != "":
		envIDs, appID := s.appEnvironments(q.GetAppId())
		return &zatterav1.StatsResponse{Series: s.envHistory(ctx, q, envIDs, appID)}, nil
	case q.GetEnvironmentId() != "":
		return &zatterav1.StatsResponse{Series: s.envHistory(ctx, q, map[string]bool{q.GetEnvironmentId(): true}, "")}, nil
	default: // cluster: every node's node-scope series
		return &zatterav1.StatsResponse{Series: s.fanOut(ctx, s.store.ListNodes(), q)}, nil
	}
}

// fanOut queries each node's local TSDB concurrently and returns the flattened,
// sorted series. A node that errors is logged and skipped (partial results).
func (s *MetricsServer) fanOut(ctx context.Context, nodes []*zatterav1.Node, q *zatterav1.StatsQuery) []*zatterav1.Series {
	var mu sync.Mutex
	var all []*zatterav1.Series
	var wg sync.WaitGroup
	for _, node := range nodes {
		wg.Add(1)
		go func(n *zatterav1.Node) {
			defer wg.Done()
			nctx, cancel := context.WithTimeout(ctx, statsNodeTimeout)
			defer cancel()
			resp, err := s.dial.Stats(nctx, n, q)
			if err != nil {
				s.log.Warn("stats fan-out: node query failed", "node", n.GetMeta().GetId(), "err", err)
				return
			}
			mu.Lock()
			all = append(all, resp.GetSeries()...)
			mu.Unlock()
		}(node)
	}
	wg.Wait()
	sortSeries(all)
	return all
}

// envHistory fans out to every node and folds the returned env proxy series and
// per-instance resource series into env-level series. rps/inflight/memory/net
// sum across contributors; cpu/rates/latencies average. Instance→env mapping
// comes from state (the agent can't resolve it).
func (s *MetricsServer) envHistory(ctx context.Context, q *zatterav1.StatsQuery, envIDs map[string]bool, appID string) []*zatterav1.Series {
	// instance (assignment) id → env id, for the target envs only.
	instanceEnv := map[string]string{}
	for envID := range envIDs {
		for _, a := range s.store.ListAssignments(envID) {
			instanceEnv[a.GetMeta().GetId()] = envID
		}
	}

	raw := s.fanOut(ctx, s.store.ListNodes(), q)

	// (env, metric) → timeMs → running aggregate.
	type ekey struct{ env, metric string }
	aggs := map[ekey]map[int64]*aggPoint{}
	add := func(env, metric string, tMs int64, v float64) {
		k := ekey{env, metric}
		byTime := aggs[k]
		if byTime == nil {
			byTime = map[int64]*aggPoint{}
			aggs[k] = byTime
		}
		p := byTime[tMs]
		if p == nil {
			p = &aggPoint{}
			byTime[tMs] = p
		}
		p.sum += v
		p.count++
	}

	for _, ser := range raw {
		labels := ser.GetLabels()
		var env string
		switch {
		case labels["env"] != "" && envIDs[labels["env"]]:
			env = labels["env"]
		case labels["instance"] != "":
			e, ok := instanceEnv[labels["instance"]]
			if !ok {
				continue // instance belongs to another env
			}
			env = e
		default:
			continue
		}
		for _, p := range ser.GetPoints() {
			add(env, ser.GetMetric(), p.GetTimeUnixMs(), p.GetValue())
		}
	}

	// Materialize sorted series per (env, metric).
	keys := make([]ekey, 0, len(aggs))
	for k := range aggs {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].metric != keys[j].metric {
			return keys[i].metric < keys[j].metric
		}
		return keys[i].env < keys[j].env
	})

	out := make([]*zatterav1.Series, 0, len(keys))
	for _, k := range keys {
		byTime := aggs[k]
		times := make([]int64, 0, len(byTime))
		for t := range byTime {
			times = append(times, t)
		}
		sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
		points := make([]*zatterav1.Point, 0, len(times))
		for _, t := range times {
			points = append(points, &zatterav1.Point{TimeUnixMs: t, Value: byTime[t].reduce(k.metric)})
		}
		labels := map[string]string{"env": k.env}
		if appID != "" {
			labels["app"] = appID
		}
		out = append(out, &zatterav1.Series{Metric: k.metric, Labels: labels, Points: points})
	}
	return out
}

// aggPoint accumulates contributions to one (metric, timestamp) cell.
type aggPoint struct {
	sum   float64
	count int
}

// reduce collapses the cell: additive metrics sum, everything else averages.
func (a *aggPoint) reduce(metric string) float64 {
	if sumMetrics[metric] {
		return a.sum
	}
	if a.count == 0 {
		return 0
	}
	return a.sum / float64(a.count)
}

// sumMetrics are aggregated by summing across contributors; all others average.
var sumMetrics = map[string]bool{
	"rps":          true,
	"inflight":     true,
	"memory_bytes": true,
	"net_rx_bytes": true,
	"net_tx_bytes": true,
}

// sortSeries orders series by metric then scope-id for deterministic output.
func sortSeries(series []*zatterav1.Series) {
	sort.Slice(series, func(i, j int) bool {
		if series[i].GetMetric() != series[j].GetMetric() {
			return series[i].GetMetric() < series[j].GetMetric()
		}
		return seriesScopeID(series[i]) < seriesScopeID(series[j])
	})
}

// seriesScopeID returns a stable id for sorting: the node/instance/env/app label.
func seriesScopeID(s *zatterav1.Series) string {
	l := s.GetLabels()
	for _, k := range []string{"node", "instance", "env", "app"} {
		if v := l[k]; v != "" {
			return v
		}
	}
	return ""
}

// GRPCStatsDialer is the production StatsDialer: it dials a node's
// AgentLocalService over the mesh with node mTLS via Connect.
type GRPCStatsDialer struct {
	Connect func(ctx context.Context, node *zatterav1.Node) (*grpc.ClientConn, error)
}

// Stats runs a unary Stats RPC against the node, closing the connection after.
func (g GRPCStatsDialer) Stats(ctx context.Context, node *zatterav1.Node, q *zatterav1.StatsQuery) (*zatterav1.StatsResponse, error) {
	conn, err := g.Connect(ctx, node)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	return clusterv1.NewAgentLocalServiceClient(conn).Stats(ctx, q)
}
