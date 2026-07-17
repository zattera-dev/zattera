package agent

import (
	"context"
	"log/slog"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
	gnet "github.com/shirou/gopsutil/v3/net"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	"github.com/zattera-dev/zattera/internal/daemon/tsdb"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// metricsInterval is the sampler cadence (spec §3.10: 15s raw resolution).
const metricsInterval = 15 * time.Second

// Scope strings for tsdb.SeriesKey.
const (
	scopeNode     = "node"
	scopeInstance = "instance"
	scopeEnv      = "env"
)

// NodeMetrics is a point-in-time node resource sample recorded to the TSDB.
// Network counters are cumulative byte totals (rates are derived downstream).
type NodeMetrics struct {
	CPUPercent float64
	MemUsed    float64
	MemTotal   float64
	DiskUsed   float64
	DiskTotal  float64
	NetRxBytes float64
	NetTxBytes float64
}

// NodeMetricsFunc returns the current node sample. Production uses gopsutil.
type NodeMetricsFunc func() NodeMetrics

// InstanceMetrics is one instance's resource sample keyed by its instance id
// (the assignment id).
type InstanceMetrics struct {
	InstanceID  string
	CPUPercent  float64
	MemoryBytes float64
	NetRxBytes  float64
	NetTxBytes  float64
}

// InstanceMetricsFunc returns per-instance samples for every instance running
// on this node.
type InstanceMetricsFunc func(ctx context.Context) []InstanceMetrics

// ProxyMetricsFunc returns per-environment proxy samples (nil when this node
// runs no proxy).
type ProxyMetricsFunc func() map[string]*clusterv1.ProxySample

// metricsSampler records node, instance and proxy metrics into a tsdb.Store on a
// fixed cadence. Any provider may be nil; that dimension is simply skipped.
type metricsSampler struct {
	store     tsdb.Store
	clk       clock.Clock
	log       *slog.Logger
	nodeID    string
	interval  time.Duration
	node      NodeMetricsFunc
	instances InstanceMetricsFunc
	proxy     ProxyMetricsFunc
	// publish, when set, receives the latest per-instance and per-env samples
	// each tick so the heartbeat can attach them to livestate (T-61). The
	// sampler is the SOLE caller of proxy() — which resets the RPS window — so
	// the heartbeat reads the published copy instead of sampling again.
	publish func(map[string]*clusterv1.InstanceSample, map[string]*clusterv1.ProxySample)
}

// Run samples until ctx is canceled. It takes one sample immediately so a
// short-lived process still records a point.
func (m *metricsSampler) Run(ctx context.Context) {
	m.sample(ctx)
	tick := m.clk.NewTicker(m.interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C():
			m.sample(ctx)
		}
	}
}

func (m *metricsSampler) sample(ctx context.Context) {
	now := m.clk.Now()
	rec := func(metric, scope, id string, v float64) {
		m.store.Record(tsdb.SeriesKey{Metric: metric, Scope: scope, ScopeID: id}, tsdb.Point{Time: now, Value: v})
	}

	if m.node != nil {
		n := m.node()
		rec("cpu_percent", scopeNode, m.nodeID, n.CPUPercent)
		rec("memory_used_bytes", scopeNode, m.nodeID, n.MemUsed)
		rec("memory_total_bytes", scopeNode, m.nodeID, n.MemTotal)
		rec("disk_used_bytes", scopeNode, m.nodeID, n.DiskUsed)
		rec("disk_total_bytes", scopeNode, m.nodeID, n.DiskTotal)
		rec("net_rx_bytes", scopeNode, m.nodeID, n.NetRxBytes)
		rec("net_tx_bytes", scopeNode, m.nodeID, n.NetTxBytes)
	}

	var instSamples map[string]*clusterv1.InstanceSample
	if m.instances != nil {
		instSamples = map[string]*clusterv1.InstanceSample{}
		for _, in := range m.instances(ctx) {
			if in.InstanceID == "" {
				continue
			}
			rec("cpu_percent", scopeInstance, in.InstanceID, in.CPUPercent)
			rec("memory_bytes", scopeInstance, in.InstanceID, in.MemoryBytes)
			rec("net_rx_bytes", scopeInstance, in.InstanceID, in.NetRxBytes)
			rec("net_tx_bytes", scopeInstance, in.InstanceID, in.NetTxBytes)
			instSamples[in.InstanceID] = &clusterv1.InstanceSample{
				CpuPercent:  in.CPUPercent,
				MemoryBytes: uint64(in.MemoryBytes),
				NetRxBytes:  uint64(in.NetRxBytes),
				NetTxBytes:  uint64(in.NetTxBytes),
			}
		}
	}

	var proxySamples map[string]*clusterv1.ProxySample
	if m.proxy != nil {
		proxySamples = m.proxy()
		for env, p := range proxySamples {
			if env == "" || p == nil {
				continue
			}
			rec("rps", scopeEnv, env, p.GetRps())
			rec("latency_p50_ms", scopeEnv, env, p.GetLatencyP50Ms())
			rec("latency_p99_ms", scopeEnv, env, p.GetLatencyP99Ms())
			rec("error_rate", scopeEnv, env, p.GetErrorRate())
			rec("inflight", scopeEnv, env, float64(p.GetInflight()))
		}
	}

	if m.publish != nil {
		m.publish(instSamples, proxySamples)
	}
}

// gopsutilNodeMetrics builds a NodeMetricsFunc probing host CPU/mem/disk/net.
// Any failing probe degrades that dimension to zero rather than failing the
// whole sample.
func gopsutilNodeMetrics(diskPath string, log *slog.Logger) NodeMetricsFunc {
	return func() NodeMetrics {
		var n NodeMetrics
		if pct, err := cpu.Percent(0, false); err == nil && len(pct) > 0 {
			n.CPUPercent = pct[0]
		} else if err != nil {
			log.Debug("metrics: cpu sample failed", "err", err)
		}
		if vm, err := mem.VirtualMemory(); err == nil {
			n.MemUsed = float64(vm.Used)
			n.MemTotal = float64(vm.Total)
		} else {
			log.Debug("metrics: memory sample failed", "err", err)
		}
		if du, err := disk.Usage(diskPath); err == nil {
			n.DiskUsed = float64(du.Used)
			n.DiskTotal = float64(du.Total)
		} else {
			log.Debug("metrics: disk sample failed", "err", err, "path", diskPath)
		}
		if io, err := gnet.IOCounters(false); err == nil && len(io) > 0 {
			n.NetRxBytes = float64(io[0].BytesRecv)
			n.NetTxBytes = float64(io[0].BytesSent)
		} else if err != nil {
			log.Debug("metrics: net sample failed", "err", err)
		}
		return n
	}
}
