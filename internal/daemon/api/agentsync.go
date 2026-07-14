package api

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/livestate"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

const (
	// defaultAssignmentDebounce coalesces a burst of assignment changes into a
	// single full-set push per node.
	defaultAssignmentDebounce = 200 * time.Millisecond
	// defaultStatusFlush caps how often a node's observed-status batch is
	// committed to durable state.
	defaultStatusFlush = 2 * time.Second
)

// SyncServer implements AgentSyncService. Agents dial in and open a bidi
// stream; the server registers the node in livestate, pushes its full
// AssignmentSet on connect and whenever assignments change, records heartbeats,
// and folds reported status batches back into durable state.
type SyncServer struct {
	clusterv1.UnimplementedAgentSyncServiceServer

	store   *state.Store
	applier Applier
	live    *livestate.Registry
	clock   clock.Clock
	log     *slog.Logger
	// sealer decrypts env vars for the per-assignment runtime payload. May be
	// nil before the cluster key is unsealed (env is then omitted).
	sealer secrets.Sealer

	assignmentDebounce time.Duration
	statusFlush        time.Duration
}

// NewSyncServer builds the control-side AgentSync handler. applier commits
// status batches (SetAssignmentsObserved) through raft; sealer decrypts env
// vars pushed to agents (may be nil).
func NewSyncServer(store *state.Store, applier Applier, live *livestate.Registry, clk clock.Clock, log *slog.Logger, sealer secrets.Sealer) *SyncServer {
	if log == nil {
		log = slog.Default()
	}
	if clk == nil {
		clk = clock.Real{}
	}
	return &SyncServer{
		store:              store,
		applier:            applier,
		live:               live,
		clock:              clk,
		log:                log,
		sealer:             sealer,
		assignmentDebounce: defaultAssignmentDebounce,
		statusFlush:        defaultStatusFlush,
	}
}

// Sync runs one agent connection: hello handshake, then concurrent assignment
// pushes, status flushing, and heartbeat/status receipt until the stream ends.
func (s *SyncServer) Sync(stream clusterv1.AgentSyncService_SyncServer) error {
	ctx := stream.Context()

	// Every (re)connect opens with a hello.
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	hello := first.GetHello()
	if hello == nil {
		return status.Error(codes.InvalidArgument, "first agent message must be a hello")
	}
	nodeID, err := s.resolveNodeID(ctx, hello)
	if err != nil {
		return err
	}

	release := s.live.Connect(nodeID)
	defer release()
	s.log.Info("agent connected", "node", nodeID, "version", hello.GetBinaryVersion(),
		"assignment_version", hello.GetAssignmentVersion())
	defer s.log.Info("agent disconnected", "node", nodeID)

	// The stream has a single writer (the sender goroutine); the receive loop
	// below never calls Send. Deregister everything when either side ends.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	statusBuf := &statusBuffer{m: map[string]*zatterav1.AssignmentObserved{}}

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.runSender(runCtx, stream, nodeID, hello.GetAssignmentVersion())
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.runStatusFlusher(runCtx, nodeID, statusBuf)
	}()

	recvErr := s.recvLoop(stream, nodeID, statusBuf)
	cancel()
	wg.Wait()
	// Best-effort final flush of anything buffered before we forget the node.
	s.flushStatus(context.Background(), nodeID, statusBuf)
	return recvErr
}

// recvLoop reads heartbeats and status batches until the stream errors.
func (s *SyncServer) recvLoop(stream clusterv1.AgentSyncService_SyncServer, nodeID string, buf *statusBuffer) error {
	for {
		msg, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || status.Code(err) == codes.Canceled {
				return nil
			}
			return err
		}
		switch b := msg.Body.(type) {
		case *clusterv1.AgentMessage_Heartbeat:
			s.live.Heartbeat(nodeID, b.Heartbeat)
		case *clusterv1.AgentMessage_Status:
			buf.add(b.Status.GetObserved())
		case *clusterv1.AgentMessage_Ack:
			// Informational: the agent confirms an assignment version. No-op.
		case *clusterv1.AgentMessage_Hello:
			// A duplicate hello mid-stream is ignored; identity is fixed at open.
		}
	}
}

// runSender pushes the full AssignmentSet for this node: once on connect
// (unless the agent already holds the current version) and, debounced, whenever
// assignments change.
func (s *SyncServer) runSender(ctx context.Context, stream clusterv1.AgentSyncService_SyncServer, nodeID string, knownVersion uint64) {
	sub := s.store.Watch(state.KindAssignment)
	defer sub.Close()

	// Version-skip: a reconnect where nothing changed carries the same version,
	// so skip the redundant initial resend.
	if s.store.Version() != knownVersion {
		if err := s.sendAssignments(stream, nodeID); err != nil {
			return
		}
	}

	var timer <-chan time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-sub.Notify():
			sub.Drain()
			if timer == nil {
				timer = s.clock.After(s.assignmentDebounce)
			}
		case <-timer:
			timer = nil
			if err := s.sendAssignments(stream, nodeID); err != nil {
				return
			}
		}
	}
}

// sendAssignments transmits this node's full desired set (idempotent; no
// deltas), enriched with the per-assignment runtime payload (release image +
// frozen spec + decrypted env) the agent needs to materialize containers. The
// version is the store's global mutation counter.
func (s *SyncServer) sendAssignments(stream clusterv1.AgentSyncService_SyncServer, nodeID string) error {
	assignments := s.store.ListAssignmentsByNode(nodeID)
	set := &clusterv1.AssignmentSet{
		Version:     s.store.Version(),
		Assignments: assignments,
		Runtime:     map[string]*clusterv1.AssignmentRuntime{},
	}
	for _, a := range assignments {
		if rt := s.buildRuntime(a); rt != nil {
			set.Runtime[a.GetMeta().GetId()] = rt
		}
	}
	return stream.Send(&clusterv1.ControlMessage{
		Body: &clusterv1.ControlMessage_Assignments{Assignments: set},
	})
}

// buildRuntime resolves the assignment's release into an image + frozen spec
// and decrypts the environment's env vars. Returns nil when the release is
// unknown (the agent then reports the assignment FAILED). Env is included only
// when a sealer is available; secrets never persist in Raft, only in this frame.
func (s *SyncServer) buildRuntime(a *zatterav1.Assignment) *clusterv1.AssignmentRuntime {
	rel, ok := s.store.Release(a.GetReleaseId())
	if !ok {
		return nil
	}
	rt := &clusterv1.AssignmentRuntime{
		ImageRef: rel.GetImageRef(),
		Spec:     rel.GetService(),
	}
	// Per-(project,env,node) bridge subnet (T-46): the scheduler allocates it;
	// the agent attaches the container and points its DNS at the gateway.
	if subnet, ok := s.store.NetworkAllocation(a.GetProjectId(), a.GetEnvironmentId(), a.GetNodeId()); ok {
		rt.SubnetCidr = subnet
	}
	// Env vars: user-set (decrypted) values first, then platform-injected vars
	// (T-50). ZATTERA_ENV/ZATTERA_APP are authoritative identity and override
	// any user value; PORT defaults to the first port but respects a user
	// override.
	env := map[string]string{}
	if s.sealer != nil {
		for k, ev := range s.store.EnvVars(a.GetEnvironmentId()) {
			pt, err := s.sealer.Open(ev)
			if err != nil {
				s.log.Warn("agentsync: env var decrypt failed", "env", a.GetEnvironmentId(), "key", k, "err", err)
				continue
			}
			env[k] = string(pt)
		}
	}
	s.injectPlatformEnv(env, a, rel)
	if len(env) > 0 {
		rt.Env = env
	}
	return rt
}

// injectPlatformEnv layers the platform-provided variables onto env: PORT (the
// first container port, unless the user set one), ZATTERA_ENV and ZATTERA_APP.
func (s *SyncServer) injectPlatformEnv(env map[string]string, a *zatterav1.Assignment, rel *zatterav1.Release) {
	if ports := rel.GetService().GetPorts(); len(ports) > 0 && ports[0].GetContainerPort() > 0 {
		if _, ok := env["PORT"]; !ok {
			env["PORT"] = strconv.Itoa(int(ports[0].GetContainerPort()))
		}
	}
	if envRec, ok := s.store.Environment(a.GetEnvironmentId()); ok && envRec.GetName() != "" {
		env["ZATTERA_ENV"] = envRec.GetName()
	}
	if app, ok := s.store.App(a.GetAppId()); ok && app.GetName() != "" {
		env["ZATTERA_APP"] = app.GetName()
	}
}

// runStatusFlusher commits buffered observed-status batches at most once per
// statusFlush interval.
func (s *SyncServer) runStatusFlusher(ctx context.Context, nodeID string, buf *statusBuffer) {
	tick := s.clock.NewTicker(s.statusFlush)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C():
			s.flushStatus(ctx, nodeID, buf)
		}
	}
}

// flushStatus applies the buffered observed batch (if any) as a single
// SetAssignmentsObserved command.
func (s *SyncServer) flushStatus(ctx context.Context, nodeID string, buf *statusBuffer) {
	observed := buf.drain()
	if len(observed) == 0 {
		return
	}
	cmd := &clusterv1.Command{
		RequestId: ids.New(),
		Actor:     "node:" + nodeID,
		Time:      timestamppb.Now(),
		Mutation: &clusterv1.Command_SetAssignmentsObserved{
			SetAssignmentsObserved: &clusterv1.SetAssignmentsObserved{
				NodeId:   nodeID,
				Observed: observed,
			},
		},
	}
	if err := applyAnywhere(ctx, s.applier, cmd, s.log); err != nil {
		s.log.Warn("apply status batch failed", "node", nodeID, "count", len(observed), "err", err)
	}
}

// resolveNodeID trusts the mTLS node identity when present (production, enforced
// by the auth interceptor), cross-checking it against the hello's claim. Over a
// loopback dial with no client cert (single-node/dev) it trusts the hello.
func (s *SyncServer) resolveNodeID(ctx context.Context, hello *clusterv1.AgentHello) (string, error) {
	claimed := hello.GetNodeId()
	if id, ok := IdentityFrom(ctx); ok && id.NodeID != "" {
		if claimed != "" && claimed != id.NodeID {
			return "", status.Errorf(codes.PermissionDenied,
				"hello node_id %q does not match certificate identity %q", claimed, id.NodeID)
		}
		return id.NodeID, nil
	}
	if claimed == "" {
		return "", status.Error(codes.InvalidArgument, "hello node_id is required")
	}
	return claimed, nil
}

// statusBuffer accumulates the latest observed status per assignment id between
// flushes. Later reports overwrite earlier ones.
type statusBuffer struct {
	mu sync.Mutex
	m  map[string]*zatterav1.AssignmentObserved
}

func (b *statusBuffer) add(observed map[string]*zatterav1.AssignmentObserved) {
	if len(observed) == 0 {
		return
	}
	b.mu.Lock()
	for id, o := range observed {
		b.m[id] = o
	}
	b.mu.Unlock()
}

func (b *statusBuffer) drain() map[string]*zatterav1.AssignmentObserved {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.m) == 0 {
		return nil
	}
	out := b.m
	b.m = map[string]*zatterav1.AssignmentObserved{}
	return out
}

// applyAnywhere commits cmd through raft. Agents may connect to ANY control
// node, so a stream served by a follower must still reach the leader. Until
// command-level follower→leader forwarding lands (multi-control, M2) a follower
// drops the command with a warning rather than failing the stream — in the
// single-control Phase 2 topology the stream's node IS the leader.
func applyAnywhere(ctx context.Context, applier Applier, cmd *clusterv1.Command, log *slog.Logger) error {
	err := applier.Apply(ctx, cmd)
	if errors.Is(err, raftstore.ErrNotLeader) {
		log.Warn("dropping command on follower (no command forwarding yet)", "actor", cmd.GetActor())
		return nil
	}
	return err
}
