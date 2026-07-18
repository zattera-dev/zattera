package agent_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/agent"
	"github.com/zattera-dev/zattera/internal/daemon/api"
	"github.com/zattera-dev/zattera/internal/daemon/livestate"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
)

const testNodeID = "node-test-1"

// TestAgentSync exercises the full agent↔control loop end to end over an
// in-process gRPC server: hello registers the node, heartbeats reach livestate,
// an assignment change is pushed as a new AssignmentSet, and a forced
// disconnect reconnects and resyncs.
func TestAgentSync(t *testing.T) {
	rig := newRig(t)
	defer rig.stop()

	// --- hello registers the node in livestate ---
	waitFor(t, 3*time.Second, func() bool {
		ns, ok := rig.live.Get(testNodeID)
		return ok && ns.Connected
	}, "node to register")

	// --- heartbeat lands in livestate ---
	advanceUntil(t, rig.agentClk, 10*time.Second, 3*time.Second, func() bool {
		ns, _ := rig.live.Get(testNodeID)
		return ns.Heartbeat.GetCpuPercent() == 12.5
	}, "heartbeat to land")

	// --- an assignment change pushes a new AssignmentSet ---
	rig.apply(t, putAssignment("assign-1", "env-1"))
	advanceUntil(t, rig.serverClk, 200*time.Millisecond, 3*time.Second, func() bool {
		return rig.recorder.has("assign-1")
	}, "first assignment pushed")

	// --- disconnect + reconnect resyncs ---
	rig.closeCurrentConn()
	waitFor(t, 3*time.Second, func() bool {
		ns, _ := rig.live.Get(testNodeID)
		return !ns.Connected
	}, "node to disconnect")

	// A new assignment applied while down must arrive after reconnect.
	rig.apply(t, putAssignment("assign-2", "env-1"))
	// Drive the agent's reconnect backoff timer, then the server debounce.
	advanceUntil(t, rig.agentClk, 5*time.Second, 5*time.Second, func() bool {
		ns, _ := rig.live.Get(testNodeID)
		return ns.Connected && rig.dialCount() >= 2
	}, "agent to reconnect")
	advanceUntil(t, rig.serverClk, 200*time.Millisecond, 3*time.Second, func() bool {
		return rig.recorder.has("assign-2")
	}, "second assignment resynced")
}

// --- test rig -------------------------------------------------------------

type rig struct {
	live      *livestate.Registry
	rs        *raftstore.Store
	agentClk  *clock.Fake
	serverClk *clock.Fake
	recorder  *recorder

	grpcSrv *grpc.Server
	cancel  context.CancelFunc

	mu    sync.Mutex
	conns []*grpc.ClientConn
}

func newRig(t *testing.T) *rig {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	rs := raftstore.NewTestStore(t)
	r := &rig{
		live:      livestate.New(clock.NewFake()),
		rs:        rs,
		agentClk:  clock.NewFake(),
		serverClk: clock.NewFake(),
		recorder:  &recorder{},
	}

	// Control-side server (no auth interceptor → handler trusts hello.node_id).
	r.grpcSrv = grpc.NewServer()
	clusterv1.RegisterAgentSyncServiceServer(r.grpcSrv, api.NewSyncServer(rs.State(), rs, r.live, r.serverClk, log, secrets.NewVault()))
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = r.grpcSrv.Serve(lis) }()
	addr := lis.Addr().String()

	// Agent dialing the server; each Dial makes a fresh, tracked connection.
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	ag := agent.New(agent.Config{
		NodeID:            testNodeID,
		Version:           "test",
		Clock:             r.agentClk,
		Logger:            log,
		HeartbeatInterval: 10 * time.Second,
		Sample: func() agent.HostSample {
			return agent.HostSample{CPUPercent: 12.5, MemoryUsedBytes: 100, MemoryTotalBytes: 1000, DiskUsedBytes: 5, DiskTotalBytes: 50}
		},
		Dial: func(context.Context) (*agent.Conn, error) {
			cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return nil, err
			}
			r.mu.Lock()
			r.conns = append(r.conns, cc)
			r.mu.Unlock()
			return &agent.Conn{ClientConnInterface: cc, Close: cc.Close}, nil
		},
		OnAssignments: r.recorder.add,
	})
	go func() { _ = ag.Run(ctx) }()
	return r
}

func (r *rig) stop() {
	r.cancel()
	r.grpcSrv.Stop()
}

func (r *rig) apply(t *testing.T, cmd *clusterv1.Command) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.rs.Apply(ctx, cmd); err != nil {
		t.Fatalf("apply: %v", err)
	}
}

func (r *rig) dialCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.conns)
}

func (r *rig) closeCurrentConn() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n := len(r.conns); n > 0 {
		_ = r.conns[n-1].Close()
	}
}

// recorder captures every AssignmentSet the agent applies.
type recorder struct {
	mu   sync.Mutex
	sets []*clusterv1.AssignmentSet
}

func (rec *recorder) add(set *clusterv1.AssignmentSet) {
	rec.mu.Lock()
	rec.sets = append(rec.sets, set)
	rec.mu.Unlock()
}

func (rec *recorder) has(assignID string) bool {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	for _, set := range rec.sets {
		for _, a := range set.GetAssignments() {
			if a.GetMeta().GetId() == assignID {
				return true
			}
		}
	}
	return false
}

func putAssignment(assignID, envID string) *clusterv1.Command {
	return &clusterv1.Command{
		RequestId: ids.New(),
		Actor:     "test",
		Time:      timestamppb.Now(),
		Mutation: &clusterv1.Command_PutAssignments{PutAssignments: &clusterv1.PutAssignments{
			Assignments: []*zatterav1.Assignment{{
				Meta:          &zatterav1.Meta{Id: assignID},
				NodeId:        testNodeID,
				EnvironmentId: envID,
				Desired:       zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN,
				ConfigHash:    "hash-" + assignID,
			}},
		}},
	}
}

// --- polling helpers ------------------------------------------------------

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("timeout waiting for %s", what)
	}
}

func advanceUntil(t *testing.T, clk *clock.Fake, step, timeout time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		clk.Advance(step)
		time.Sleep(2 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("timeout advancing clock for %s", what)
	}
}
