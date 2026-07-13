package raftstore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"google.golang.org/protobuf/proto"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	"github.com/zattera-dev/zattera/internal/state"
)

// ErrNotLeader is returned by Apply on followers. The API layer's
// leader-forward interceptor retries against LeaderAddr.
var ErrNotLeader = errors.New("raftstore: not the leader")

const (
	applyTimeout      = 10 * time.Second
	snapshotThreshold = 8192
	snapshotInterval  = 10 * time.Minute
)

// Config for a raft store node.
type Config struct {
	// NodeID is this node's stable ULID (also the raft server id).
	NodeID string
	// DataDir holds raft.db and snapshots/ (e.g. /var/lib/zattera/raft).
	DataDir string
	// Transport to use. Nil = TCP transport bound to BindAddr/AdvertiseAddr.
	Transport raft.Transport
	// BindAddr like "10.90.0.1:7480" (mesh IP; 127.0.0.1 in single-node).
	BindAddr string
	// AdvertiseAddr defaults to BindAddr.
	AdvertiseAddr string
	// Bootstrap starts a fresh single-node cluster if no prior state exists.
	Bootstrap bool
	// BootstrapServers, when non-empty and Bootstrap is true, bootstraps with
	// this full member set instead of just this node (simcluster / 3-node
	// first boot). Every listed server must bootstrap with the same set.
	BootstrapServers []raft.Server
	// Inmem replaces bolt/file stores with in-memory ones (tests only).
	Inmem bool
	Logger *slog.Logger
}

// Store owns the raft node and the FSM. It is the only writer to the
// underlying state.Store.
type Store struct {
	raft  *raft.Raft
	fsm   *FSM
	state *state.Store
	log   *slog.Logger

	trans raft.Transport
}

// New creates (and possibly bootstraps) the raft node.
func New(cfg Config, st *state.Store) (*Store, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	fsm := NewFSM(st, logger)

	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(cfg.NodeID)
	rc.SnapshotThreshold = snapshotThreshold
	rc.SnapshotInterval = snapshotInterval
	rc.LogOutput = os.Stderr // TODO(T-04): bridge hashicorp logs into slog

	var (
		logStore  raft.LogStore
		stable    raft.StableStore
		snaps     raft.SnapshotStore
		transport = cfg.Transport
		err       error
	)
	if cfg.Inmem {
		inmem := raft.NewInmemStore()
		logStore, stable = inmem, inmem
		snaps = raft.NewInmemSnapshotStore()
	} else {
		if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
			return nil, fmt.Errorf("raftstore: data dir: %w", err)
		}
		bolt, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft.db"))
		if err != nil {
			return nil, fmt.Errorf("raftstore: bolt store: %w", err)
		}
		logStore, stable = bolt, bolt
		snaps, err = raft.NewFileSnapshotStore(cfg.DataDir, 2, os.Stderr)
		if err != nil {
			return nil, fmt.Errorf("raftstore: snapshot store: %w", err)
		}
	}
	if transport == nil {
		advertise := cfg.AdvertiseAddr
		if advertise == "" {
			advertise = cfg.BindAddr
		}
		addr, err := net.ResolveTCPAddr("tcp", advertise)
		if err != nil {
			return nil, fmt.Errorf("raftstore: advertise addr: %w", err)
		}
		transport, err = raft.NewTCPTransport(cfg.BindAddr, addr, 3, 10*time.Second, os.Stderr)
		if err != nil {
			return nil, fmt.Errorf("raftstore: tcp transport: %w", err)
		}
	}

	r, err := raft.NewRaft(rc, fsm, logStore, stable, snaps, transport)
	if err != nil {
		return nil, fmt.Errorf("raftstore: new raft: %w", err)
	}

	if cfg.Bootstrap {
		hasState, err := raft.HasExistingState(logStore, stable, snaps)
		if err != nil {
			return nil, err
		}
		if !hasState {
			servers := cfg.BootstrapServers
			if len(servers) == 0 {
				servers = []raft.Server{{ID: rc.LocalID, Address: transport.LocalAddr()}}
			}
			f := r.BootstrapCluster(raft.Configuration{Servers: servers})
			if err := f.Error(); err != nil {
				return nil, fmt.Errorf("raftstore: bootstrap: %w", err)
			}
		}
	}

	return &Store{raft: r, fsm: fsm, state: st, log: logger, trans: transport}, nil
}

// State returns the shared state store (read-side).
func (s *Store) State() *state.Store { return s.state }

// Raft exposes the raw raft node (membership changes, stats).
func (s *Store) Raft() *raft.Raft { return s.raft }

// IsLeader reports whether this node currently leads.
func (s *Store) IsLeader() bool { return s.raft.State() == raft.Leader }

// LeaderAddr returns the current leader's transport address and server id
// ("" if unknown, e.g. during elections).
func (s *Store) LeaderAddr() (addr string, id string) {
	a, i := s.raft.LeaderWithID()
	return string(a), string(i)
}

// LeaderCh exposes raft leadership change notifications.
func (s *Store) LeaderCh() <-chan bool { return s.raft.LeaderCh() }

// Apply proposes a command and waits for it to be applied to the FSM.
// Returns ErrNotLeader on followers. Business errors from the apply handler
// (e.g. state.ErrKVConflict) are returned as-is.
func (s *Store) Apply(ctx context.Context, cmd *clusterv1.Command) error {
	if s.raft.State() != raft.Leader {
		return ErrNotLeader
	}
	data, err := proto.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("raftstore: marshal command: %w", err)
	}
	timeout := applyTimeout
	if dl, ok := ctx.Deadline(); ok {
		if until := time.Until(dl); until < timeout {
			timeout = until
		}
	}
	f := s.raft.Apply(data, timeout)
	if err := f.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
			return ErrNotLeader
		}
		return fmt.Errorf("raftstore: apply: %w", err)
	}
	res, _ := f.Response().(*ApplyResult)
	if res == nil {
		return nil
	}
	return res.Err
}

// Barrier blocks until all preceding log entries are applied on the leader —
// use before linearizable reads that must observe prior writes.
func (s *Store) Barrier(ctx context.Context) error {
	timeout := applyTimeout
	if dl, ok := ctx.Deadline(); ok {
		if until := time.Until(dl); until < timeout {
			timeout = until
		}
	}
	if err := s.raft.Barrier(timeout).Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return ErrNotLeader
		}
		return err
	}
	return nil
}

// WaitForLeader blocks until a leader is known or ctx expires. Convenience
// for startup and tests.
func (s *Store) WaitForLeader(ctx context.Context) error {
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		if addr, _ := s.raft.LeaderWithID(); addr != "" {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("raftstore: no leader elected: %w", ctx.Err())
		case <-tick.C:
		}
	}
}

// AddVoter joins another control node to the raft cluster (leader only).
func (s *Store) AddVoter(nodeID, addr string) error {
	return s.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, applyTimeout).Error()
}

// RemoveServer removes a control node from the raft cluster (leader only).
func (s *Store) RemoveServer(nodeID string) error {
	return s.raft.RemoveServer(raft.ServerID(nodeID), 0, applyTimeout).Error()
}

// Shutdown stops the raft node gracefully.
func (s *Store) Shutdown() error {
	return s.raft.Shutdown().Error()
}
