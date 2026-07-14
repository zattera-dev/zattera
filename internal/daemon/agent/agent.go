// Package agent is the node-side runtime that keeps a node in sync with the
// control plane. It maintains a single AgentSync bidi stream: a hello on every
// (re)connect, periodic heartbeats carrying live host metrics, and applies the
// full AssignmentSet the control plane pushes. Reconciling those assignments
// against the local container runtime is the executor's job (T-15); this
// skeleton establishes the stream, heartbeats, and reconnection.
package agent

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/runtime"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

const (
	defaultHeartbeatInterval = 10 * time.Second
	minBackoff               = 1 * time.Second
	maxBackoff               = 30 * time.Second
)

// HostSample is a point-in-time snapshot of node resource usage carried by a
// heartbeat.
type HostSample struct {
	CPUPercent       float64
	MemoryUsedBytes  uint64
	MemoryTotalBytes uint64
	DiskUsedBytes    uint64
	DiskTotalBytes   uint64
}

// SampleFunc returns the current host sample. Production uses gopsutil; tests
// inject a deterministic stub.
type SampleFunc func() HostSample

// Conn is a control-plane connection paired with a closer. The agent redials on
// every (re)connect and closes the connection when a session ends.
type Conn struct {
	grpc.ClientConnInterface
	Close func() error
}

// Config configures an Agent.
type Config struct {
	NodeID  string
	Version string
	Clock   clock.Clock
	Logger  *slog.Logger

	// Runtime is reconciled against pushed assignments by the executor (T-15).
	// When nil the agent maintains the stream/heartbeats but runs no executor
	// (e.g. a control-only node without Docker).
	Runtime runtime.ContainerRuntime
	// HostIP is where the executor publishes container ports (mesh IP, or
	// 127.0.0.1 in single-node/dev).
	HostIP string
	// RegistryAuth pulls private images (join credential, T-17); nil for public.
	RegistryAuth *runtime.RegistryAuth

	// Dial opens a fresh connection to a control node's AgentSyncService. The
	// agent calls it on every (re)connect and closes the returned Conn when the
	// session ends.
	Dial func(ctx context.Context) (*Conn, error)

	// Sample returns host metrics for heartbeats. Defaults to a gopsutil probe.
	Sample SampleFunc
	// DiskPath is the filesystem the default sampler probes for disk usage.
	DiskPath string

	// HeartbeatInterval overrides the 10s default (tests shorten it via a fake
	// clock).
	HeartbeatInterval time.Duration

	// OnAssignments is invoked with every AssignmentSet the control plane
	// pushes. The executor (T-15) reconciles Docker to it; the skeleton records
	// the version and forwards the set here.
	OnAssignments func(*clusterv1.AssignmentSet)
}

// Agent maintains the node's AgentSync stream to the control plane.
type Agent struct {
	cfg   Config
	clock clock.Clock
	log   *slog.Logger

	// executor reconciles pushed assignments against the local runtime (nil
	// when Config.Runtime is nil).
	executor *Executor
	// statusCh carries executor status batches to the current sync stream; it
	// persists across reconnects and buffers while disconnected.
	statusCh chan *clusterv1.StatusBatch

	// assignmentVersion is the highest AssignmentSet version applied so far;
	// echoed in the hello so control can skip a redundant resend.
	mu                sync.Mutex
	assignmentVersion uint64
}

// New builds an Agent, filling defaults.
func New(cfg Config) *Agent {
	if cfg.Clock == nil {
		cfg.Clock = clock.Real{}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = defaultHeartbeatInterval
	}
	if cfg.DiskPath == "" {
		cfg.DiskPath = "/"
	}
	if cfg.Sample == nil {
		cfg.Sample = gopsutilSampler(cfg.DiskPath, cfg.Logger)
	}
	a := &Agent{cfg: cfg, clock: cfg.Clock, log: cfg.Logger, statusCh: make(chan *clusterv1.StatusBatch, 64)}
	if cfg.Runtime != nil {
		a.executor = NewExecutor(ExecutorConfig{
			NodeID:       cfg.NodeID,
			HostIP:       cfg.HostIP,
			Runtime:      cfg.Runtime,
			Clock:        cfg.Clock,
			Logger:       cfg.Logger,
			Report:       a.reportStatus,
			RegistryAuth: cfg.RegistryAuth,
		})
	}
	return a
}

// Executor returns the node's executor (nil when the agent runs without a
// container runtime), exposing its assignment view for log stream resolution.
func (a *Agent) Executor() *Executor { return a.executor }

// reportStatus enqueues an executor status batch for the sync stream. It never
// blocks: if the buffer is full (long disconnect) the oldest is dropped —
// control re-derives truth from the next full reconcile.
func (a *Agent) reportStatus(observed map[string]*zatterav1.AssignmentObserved) {
	if len(observed) == 0 {
		return
	}
	batch := &clusterv1.StatusBatch{Observed: observed}
	select {
	case a.statusCh <- batch:
	default:
		a.log.Warn("agent status buffer full; dropping batch", "node", a.cfg.NodeID)
	}
}

// Run maintains the sync stream until ctx is canceled, reconnecting with
// exponential backoff + jitter after any session ends.
func (a *Agent) Run(ctx context.Context) error {
	if a.executor != nil {
		go a.executor.Run(ctx)
	}
	backoff := minBackoff
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		start := a.clock.Now()
		if err := a.session(ctx); err != nil && ctx.Err() == nil {
			a.log.Warn("agent sync session ended", "node", a.cfg.NodeID, "err", err)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// A long-lived session resets the backoff; a fast failure grows it.
		if a.clock.Now().Sub(start) >= maxBackoff {
			backoff = minBackoff
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-a.clock.After(withJitter(backoff)):
		}
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

// version returns the highest assignment-set version applied so far.
func (a *Agent) version() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.assignmentVersion
}

// applyAssignments records the version and forwards the set to the executor.
func (a *Agent) applyAssignments(set *clusterv1.AssignmentSet) {
	a.mu.Lock()
	if v := set.GetVersion(); v > a.assignmentVersion {
		a.assignmentVersion = v
	}
	a.mu.Unlock()
	a.log.Info("assignment set received", "node", a.cfg.NodeID,
		"version", set.GetVersion(), "count", len(set.GetAssignments()))
	if a.executor != nil {
		a.executor.Submit(set)
	}
	if a.cfg.OnAssignments != nil {
		a.cfg.OnAssignments(set)
	}
}

// withJitter adds up to ±25% jitter so reconnecting nodes don't thunder.
func withJitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	// Deterministic-enough jitter without importing math/rand into hot paths:
	// spread by the low bits of the current nanosecond.
	spread := d / 4
	j := time.Duration(time.Now().UnixNano()%int64(2*spread+1)) - spread
	return d + j
}
