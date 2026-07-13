package api

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/livestate"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

const (
	livenessTick        = 5 * time.Second
	heartbeatDeadline   = 30 * time.Second
	leaderGracePeriod   = 45 * time.Second
	heartbeatFlushEvery = 60 * time.Second
)

// LivenessMonitor is a leader-only loop that turns livestate heartbeats into
// durable node status: nodes with a stale (or missing) heartbeat go DOWN, a
// fresh heartbeat brings a DOWN node back ALIVE. Transitions apply only on
// change; last_heartbeat_at is batched. A newly elected leader gets a grace
// window before demoting nodes it simply hasn't heard from yet.
type LivenessMonitor struct {
	store       *state.Store
	raft        Applier
	live        *livestate.Registry
	clock       clock.Clock
	log         *slog.Logger
	localNodeID string

	// leaderSince marks when this node last acquired leadership (grace anchor).
	leaderSince time.Time
	// lastFlush throttles last_heartbeat_at persistence per node.
	lastFlush map[string]time.Time
}

// NewLivenessMonitor builds the monitor.
func NewLivenessMonitor(store *state.Store, raft Applier, live *livestate.Registry, clk clock.Clock, localNodeID string, log *slog.Logger) *LivenessMonitor {
	if log == nil {
		log = slog.Default()
	}
	return &LivenessMonitor{
		store:       store,
		raft:        raft,
		live:        live,
		clock:       clk,
		log:         log,
		localNodeID: localNodeID,
		lastFlush:   map[string]time.Time{},
	}
}

// Run evaluates liveness every 5s until ctx is canceled. The grace window is
// anchored at Run start (leadership acquisition).
func (m *LivenessMonitor) Run(ctx context.Context) {
	m.leaderSince = m.clock.Now()
	tick := m.clock.NewTicker(livenessTick)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C():
			m.evaluate(ctx)
		}
	}
}

// evaluate reconciles each remote node's durable status against its livestate
// heartbeat. Leader-only; a lost leadership mid-pass stops the pass.
func (m *LivenessMonitor) evaluate(ctx context.Context) {
	if !m.raft.IsLeader() {
		return
	}
	now := m.clock.Now()
	inGrace := now.Sub(m.leaderSince) < leaderGracePeriod

	for _, n := range m.store.ListNodes() {
		id := n.GetMeta().GetId()
		if id == m.localNodeID {
			continue // never mark ourselves down
		}
		ns, ok := m.live.Get(id)
		fresh := ok && !ns.LastHeartbeat.IsZero() && now.Sub(ns.LastHeartbeat) <= heartbeatDeadline

		// During the post-election grace window, don't demote a node we simply
		// haven't heard from yet (livestate is rebuilt from scratch on election).
		if inGrace && !fresh && n.GetStatus() != zatterav1.NodeStatus_NODE_STATUS_DOWN {
			continue
		}

		desired := zatterav1.NodeStatus_NODE_STATUS_DOWN
		if fresh {
			desired = zatterav1.NodeStatus_NODE_STATUS_ALIVE
		}
		statusChanged := n.GetStatus() != desired
		flushHB := fresh && now.Sub(m.lastFlush[id]) >= heartbeatFlushEvery
		if !statusChanged && !flushHB {
			continue
		}

		var hbAt *timestamppb.Timestamp
		if fresh {
			hbAt = timestamppb.New(ns.LastHeartbeat)
		}
		cmd := &clusterv1.Command{
			RequestId: ids.New(),
			Actor:     "system:liveness",
			Time:      timestamppb.Now(),
			Mutation: &clusterv1.Command_SetNodeStatus{SetNodeStatus: &clusterv1.SetNodeStatus{
				NodeId:          id,
				Status:          desired,
				LastHeartbeatAt: hbAt,
			}},
		}
		if err := m.raft.Apply(ctx, cmd); err != nil {
			if errors.Is(err, raftstore.ErrNotLeader) {
				return // leadership lost; stop cleanly
			}
			m.log.Warn("liveness: set node status failed", "node", id, "err", err)
			continue
		}
		if statusChanged {
			m.log.Info("node liveness changed", "node", id, "status", desired.String())
		}
		if flushHB {
			m.lastFlush[id] = now
		}
	}
}
