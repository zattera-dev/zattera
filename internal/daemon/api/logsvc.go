package api

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/state"
)

const (
	// logNodeTimeout bounds a per-node query so a dead node can't hang the fan-out.
	logNodeTimeout = 3 * time.Second
	// logReorderWindow is how long follow buffers lines to reorder across nodes.
	logReorderWindow = 500 * time.Millisecond
	defaultLogLimit  = 1000
)

// LogStream is the receive side of an AgentLocalService.QueryLogs stream.
type LogStream interface {
	Recv() (*zatterav1.LogLine, error)
}

// LogDialer opens a QueryLogs stream to a node's AgentLocalService (over the
// mesh in production; a fake in tests).
type LogDialer interface {
	QueryLogs(ctx context.Context, node *zatterav1.Node, q *zatterav1.LogQuery) (LogStream, error)
}

// LogServer implements LogService.Query: it resolves the selector to the nodes
// running matching instances/builds, fans QueryLogs out to each, and merges the
// results by timestamp (enriching with app/env names from state).
type LogServer struct {
	zatterav1.UnimplementedLogServiceServer
	store *state.Store
	dial  LogDialer
	clk   clock.Clock
	log   *slog.Logger
}

// NewLogServer builds the log service.
func NewLogServer(store *state.Store, dial LogDialer, clk clock.Clock, log *slog.Logger) *LogServer {
	if log == nil {
		log = slog.Default()
	}
	if clk == nil {
		clk = clock.Real{}
	}
	return &LogServer{store: store, dial: dial, clk: clk, log: log}
}

// Query streams merged log lines for a selector.
func (s *LogServer) Query(q *zatterav1.LogQuery, stream grpc.ServerStreamingServer[zatterav1.LogLine]) error {
	ctx := stream.Context()
	// Normalize the selector's project reference to its canonical id: clients
	// pass the project name (e.g. --project smoke) but assignments — and the
	// per-node stream resolver — match on the id. This same q is forwarded to
	// each agent, so resolving it here fixes both the node fan-out and the
	// agent-side match.
	s.resolveSelectorProject(q.GetSelector())
	nodes := s.resolveNodes(q.GetSelector())
	if len(nodes) == 0 {
		return nil
	}
	if q.GetFollow() {
		return s.follow(ctx, nodes, q, stream)
	}
	return s.collect(ctx, nodes, q, stream)
}

// collect drains every node (bounded), merges by time, applies since/until +
// limit, and sends. A node that errors yields a warning and partial results.
func (s *LogServer) collect(ctx context.Context, nodes []*zatterav1.Node, q *zatterav1.LogQuery, stream grpc.ServerStreamingServer[zatterav1.LogLine]) error {
	var mu sync.Mutex
	var lines []*zatterav1.LogLine
	var wg sync.WaitGroup
	for _, node := range nodes {
		wg.Add(1)
		go func(n *zatterav1.Node) {
			defer wg.Done()
			nctx, cancel := context.WithTimeout(ctx, logNodeTimeout)
			defer cancel()
			st, err := s.dial.QueryLogs(nctx, n, q)
			if err != nil {
				s.log.Warn("log node unreachable", "node", n.GetMeta().GetId(), "err", err)
				return
			}
			for {
				l, err := st.Recv()
				if err != nil {
					return
				}
				mu.Lock()
				lines = append(lines, l)
				mu.Unlock()
			}
		}(node)
	}
	wg.Wait()

	lines = filterByTime(lines, q)
	sort.SliceStable(lines, func(i, j int) bool { return lineTime(lines[i]).Before(lineTime(lines[j])) })
	limit := int(q.GetLimit())
	if limit == 0 {
		limit = defaultLogLimit
	}
	if len(lines) > limit {
		lines = lines[len(lines)-limit:] // most recent
	}
	for _, l := range lines {
		s.enrich(l)
		if err := stream.Send(l); err != nil {
			return err
		}
	}
	return nil
}

// follow keeps every node stream open and emits lines through a small reorder
// window so cross-node ordering is approximately correct.
func (s *LogServer) follow(ctx context.Context, nodes []*zatterav1.Node, q *zatterav1.LogQuery, stream grpc.ServerStreamingServer[zatterav1.LogLine]) error {
	incoming := make(chan *zatterav1.LogLine, 256)
	var wg sync.WaitGroup
	for _, node := range nodes {
		wg.Add(1)
		go func(n *zatterav1.Node) {
			defer wg.Done()
			st, err := s.dial.QueryLogs(ctx, n, q)
			if err != nil {
				s.log.Warn("log node unreachable", "node", n.GetMeta().GetId(), "err", err)
				return
			}
			for {
				l, err := st.Recv()
				if err != nil {
					return
				}
				select {
				case incoming <- l:
				case <-ctx.Done():
					return
				}
			}
		}(node)
	}
	go func() { wg.Wait(); close(incoming) }()

	var buf []*zatterav1.LogLine
	tick := s.clk.NewTicker(logReorderWindow / 2)
	defer tick.Stop()
	flush := func(all bool) error {
		sort.SliceStable(buf, func(i, j int) bool { return lineTime(buf[i]).Before(lineTime(buf[j])) })
		cutoff := s.clk.Now().Add(-logReorderWindow)
		i := 0
		for ; i < len(buf); i++ {
			if !all && lineTime(buf[i]).After(cutoff) {
				break
			}
			s.enrich(buf[i])
			if err := stream.Send(buf[i]); err != nil {
				return err
			}
		}
		buf = buf[i:]
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case l, ok := <-incoming:
			if !ok {
				return flush(true)
			}
			buf = append(buf, l)
		case <-tick.C():
			if err := flush(false); err != nil {
				return err
			}
		}
	}
}

// resolveSelectorProject rewrites a project name in the selector to its id, in
// place. A no-op when the ref is empty or already a known id.
func (s *LogServer) resolveSelectorProject(sel *zatterav1.LogSelector) {
	ref := sel.GetProjectId()
	if ref == "" {
		return
	}
	if _, ok := s.store.Project(ref); ok {
		return // already an id
	}
	if p, ok := s.store.ProjectByName(ref); ok {
		sel.ProjectId = p.GetMeta().GetId()
	}
}

// resolveNodes maps a selector to the nodes that hold matching logs.
func (s *LogServer) resolveNodes(sel *zatterav1.LogSelector) []*zatterav1.Node {
	ids := map[string]bool{}

	// Build logs: an explicit build, or the build behind a deployment.
	buildID := sel.GetBuildId()
	if sel.GetDeploymentId() != "" {
		if d, ok := s.store.Deployment(sel.GetDeploymentId()); ok && d.GetBuildId() != "" {
			buildID = d.GetBuildId()
		}
	}
	if buildID != "" {
		if b, ok := s.store.Build(buildID); ok && b.GetNodeId() != "" {
			ids[b.GetNodeId()] = true
		}
	}

	// Runtime logs: assignments matching the selector (skip for a pure build query).
	if sel.GetBuildId() == "" || hasRuntimeSelector(sel) {
		for _, a := range s.store.ListAssignments(sel.GetEnvironmentId()) {
			if matchesSelector(a, sel) && a.GetNodeId() != "" {
				ids[a.GetNodeId()] = true
			}
		}
	}

	nodes := make([]*zatterav1.Node, 0, len(ids))
	for id := range ids {
		if n, ok := s.store.Node(id); ok {
			nodes = append(nodes, n)
		}
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].GetMeta().GetId() < nodes[j].GetMeta().GetId() })
	return nodes
}

func hasRuntimeSelector(sel *zatterav1.LogSelector) bool {
	return sel.GetEnvironmentId() != "" || sel.GetAppId() != "" || sel.GetInstanceId() != "" ||
		sel.GetDeploymentId() != "" || sel.GetJobId() != ""
}

func matchesSelector(a *zatterav1.Assignment, sel *zatterav1.LogSelector) bool {
	if sel.GetProjectId() != "" && a.GetProjectId() != sel.GetProjectId() {
		return false
	}
	if sel.GetAppId() != "" && a.GetAppId() != sel.GetAppId() {
		return false
	}
	if sel.GetInstanceId() != "" && a.GetMeta().GetId() != sel.GetInstanceId() {
		return false
	}
	if sel.GetDeploymentId() != "" && a.GetDeploymentId() != sel.GetDeploymentId() {
		return false
	}
	if sel.GetJobId() != "" && a.GetJobId() != sel.GetJobId() {
		return false
	}
	return true
}

// enrich fills app/env names from state when the node did not.
func (s *LogServer) enrich(l *zatterav1.LogLine) {
	if l.GetAppName() != "" && l.GetEnvironmentName() != "" {
		return
	}
	a, ok := s.store.Assignment(l.GetInstanceId())
	if !ok {
		return
	}
	if l.GetAppName() == "" {
		if app, ok := s.store.App(a.GetAppId()); ok {
			l.AppName = app.GetName()
		}
	}
	if l.GetEnvironmentName() == "" {
		if env, ok := s.store.Environment(a.GetEnvironmentId()); ok {
			l.EnvironmentName = env.GetName()
		}
	}
}

func filterByTime(lines []*zatterav1.LogLine, q *zatterav1.LogQuery) []*zatterav1.LogLine {
	since := q.GetSince().AsTime()
	until := q.GetUntil().AsTime()
	hasSince := q.GetSince() != nil
	hasUntil := q.GetUntil() != nil
	out := lines[:0]
	for _, l := range lines {
		t := lineTime(l)
		if hasSince && t.Before(since) {
			continue
		}
		if hasUntil && t.After(until) {
			continue
		}
		out = append(out, l)
	}
	return out
}

func lineTime(l *zatterav1.LogLine) time.Time { return l.GetTime().AsTime() }

// GRPCLogDialer is the production LogDialer: it dials a node's AgentLocalService
// over the mesh with node mTLS. Connect supplies the per-node client connection;
// the daemon owns the transport details.
type GRPCLogDialer struct {
	Connect func(ctx context.Context, node *zatterav1.Node) (*grpc.ClientConn, error)
}

// QueryLogs opens the stream, keeping the connection alive for its lifetime.
func (g GRPCLogDialer) QueryLogs(ctx context.Context, node *zatterav1.Node, q *zatterav1.LogQuery) (LogStream, error) {
	conn, err := g.Connect(ctx, node)
	if err != nil {
		return nil, err
	}
	stream, err := clusterv1.NewAgentLocalServiceClient(conn).QueryLogs(ctx, q)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &grpcLogStream{stream: stream, conn: conn}, nil
}

type grpcLogStream struct {
	stream grpc.ServerStreamingClient[zatterav1.LogLine]
	conn   *grpc.ClientConn
}

func (l *grpcLogStream) Recv() (*zatterav1.LogLine, error) {
	line, err := l.stream.Recv()
	if err != nil {
		_ = l.conn.Close()
	}
	return line, err
}
