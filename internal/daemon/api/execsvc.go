package api

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sort"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/state"
)

// AgentExecStream is the control→agent Exec bidi stream (implemented by the
// generated gRPC client; faked in tests).
type AgentExecStream interface {
	Send(*clusterv1.AgentExecInput) error
	Recv() (*clusterv1.AgentExecOutput, error)
	CloseSend() error
}

// AgentProxyStream is the control→agent ProxyTCP bidi stream.
type AgentProxyStream interface {
	Send(*clusterv1.TCPChunk) error
	Recv() (*clusterv1.TCPChunk, error)
	CloseSend() error
}

// ExecDialer opens AgentLocalService streams to a node over the mesh. The
// production implementation dials node mTLS; tests inject a fake.
type ExecDialer interface {
	Exec(ctx context.Context, node *zatterav1.Node) (AgentExecStream, error)
	ProxyTCP(ctx context.Context, node *zatterav1.Node) (AgentProxyStream, error)
	Top(ctx context.Context, node *zatterav1.Node, containerID string) (*clusterv1.AgentTopResponse, error)
}

// ExecServer implements ExecService: it resolves an instance to its node and
// relays the user's interactive stream (exec / port-forward / top) to that
// node's AgentLocalService over the mesh (T-49). The relay is a pure byte pump
// with a goroutine per direction; stream close propagates both ways.
type ExecServer struct {
	zatterav1.UnimplementedExecServiceServer
	store *state.Store
	dial  ExecDialer
	log   *slog.Logger
}

// NewExecServer builds the exec service.
func NewExecServer(store *state.Store, dial ExecDialer, log *slog.Logger) *ExecServer {
	if log == nil {
		log = slog.Default()
	}
	return &ExecServer{store: store, dial: dial, log: log}
}

// Exec relays a bidirectional exec/attach session to the instance's node.
func (s *ExecServer) Exec(stream grpc.BidiStreamingServer[zatterav1.ExecInput, zatterav1.ExecOutput]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	start := first.GetStart()
	if start == nil || start.GetInstanceId() == "" {
		return status.Error(codes.InvalidArgument, "first Exec message must set start.instance_id")
	}
	node, containerID, err := s.resolveInstance(start.GetProjectId(), start.GetInstanceId())
	if err != nil {
		return err
	}

	ctx := stream.Context()
	agentStream, err := s.dial.Exec(ctx, node)
	if err != nil {
		return status.Errorf(codes.Unavailable, "reach node: %v", err)
	}

	// Open the agent-side exec with the resolved container + terminal params.
	if err := agentStream.Send(&clusterv1.AgentExecInput{
		Start: &clusterv1.AgentExecStart{
			ContainerId: containerID,
			Command:     start.GetCommand(),
			Tty:         start.GetTty(),
			InitialSize: agentSize(start.GetInitialSize()),
		},
		Stdin: first.GetStdin(),
	}); err != nil {
		return status.Errorf(codes.Unavailable, "start exec: %v", err)
	}

	// user → agent
	go func() {
		for {
			in, err := stream.Recv()
			if err != nil {
				_ = agentStream.CloseSend()
				return
			}
			fwd := &clusterv1.AgentExecInput{Stdin: in.GetStdin()}
			if r := in.GetResize(); r != nil {
				fwd.Resize = &clusterv1.AgentTerminalSize{Cols: r.GetCols(), Rows: r.GetRows()}
			}
			if err := agentStream.Send(fwd); err != nil {
				return
			}
		}
	}()

	// agent → user (carries the exit code; drives completion)
	for {
		out, err := agentStream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return status.Errorf(codes.Unavailable, "exec stream: %v", err)
		}
		if err := stream.Send(&zatterav1.ExecOutput{
			Stdout:   out.GetStdout(),
			Stderr:   out.GetStderr(),
			Exited:   out.GetExited(),
			ExitCode: out.GetExitCode(),
		}); err != nil {
			return err
		}
		if out.GetExited() {
			return nil
		}
	}
}

// PortForward tunnels one TCP connection to a healthy replica of the app.
func (s *ExecServer) PortForward(stream grpc.BidiStreamingServer[zatterav1.PortForwardInput, zatterav1.PortForwardOutput]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	start := first.GetStart()
	if start == nil {
		return status.Error(codes.InvalidArgument, "first PortForward message must set start")
	}
	node, dialAddr, err := s.resolvePortTarget(start)
	if err != nil {
		return err
	}

	ctx := stream.Context()
	agentStream, err := s.dial.ProxyTCP(ctx, node)
	if err != nil {
		return status.Errorf(codes.Unavailable, "reach node: %v", err)
	}
	// First chunk carries the dial target (+ any early bytes).
	if err := agentStream.Send(&clusterv1.TCPChunk{DialAddr: dialAddr, Data: first.GetData()}); err != nil {
		return status.Errorf(codes.Unavailable, "open tunnel: %v", err)
	}

	// user → agent
	go func() {
		for {
			in, err := stream.Recv()
			if err != nil {
				_ = agentStream.CloseSend()
				return
			}
			if len(in.GetData()) == 0 {
				continue
			}
			if err := agentStream.Send(&clusterv1.TCPChunk{Data: in.GetData()}); err != nil {
				return
			}
		}
	}()

	// agent → user
	for {
		chunk, err := agentStream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return status.Errorf(codes.Unavailable, "tunnel: %v", err)
		}
		if len(chunk.GetData()) == 0 {
			continue
		}
		if err := stream.Send(&zatterav1.PortForwardOutput{Data: chunk.GetData()}); err != nil {
			return err
		}
	}
}

// Top returns a process listing from inside the instance's container.
func (s *ExecServer) Top(ctx context.Context, req *zatterav1.TopRequest) (*zatterav1.TopResponse, error) {
	node, containerID, err := s.resolveInstance(req.GetProjectId(), req.GetInstanceId())
	if err != nil {
		return nil, err
	}
	at, err := s.dial.Top(ctx, node, containerID)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "top: %v", err)
	}
	out := &zatterav1.TopResponse{Titles: at.GetTitles()}
	for _, p := range at.GetProcesses() {
		out.Processes = append(out.Processes, &zatterav1.TopProcess{Fields: p.GetFields()})
	}
	return out, nil
}

// resolveInstance maps an instance (assignment) id to its node + container,
// verifying it belongs to the requested project and is actually running.
func (s *ExecServer) resolveInstance(projectID, instanceID string) (*zatterav1.Node, string, error) {
	a, ok := s.store.Assignment(instanceID)
	if !ok {
		return nil, "", status.Error(codes.NotFound, "instance not found")
	}
	if projectID != "" && a.GetProjectId() != projectID && !projectMatches(s.store, projectID, a.GetProjectId()) {
		return nil, "", status.Error(codes.NotFound, "instance not found")
	}
	containerID := a.GetObserved().GetContainerId()
	if containerID == "" {
		return nil, "", status.Error(codes.FailedPrecondition, "instance is not running")
	}
	node, ok := s.store.Node(a.GetNodeId())
	if !ok {
		return nil, "", status.Error(codes.Unavailable, "instance's node is unknown")
	}
	return node, containerID, nil
}

// resolvePortTarget picks a healthy replica of the target app/env and computes
// the agent-side dial address (node mesh IP + the port's mesh host binding).
func (s *ExecServer) resolvePortTarget(start *zatterav1.PortForwardStart) (*zatterav1.Node, string, error) {
	candidates := make([]*zatterav1.Assignment, 0)
	for _, a := range s.store.ListAssignments(start.GetEnvironmentId()) {
		if start.GetAppId() != "" && a.GetAppId() != start.GetAppId() {
			continue
		}
		if a.GetDesired() != zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN {
			continue
		}
		if a.GetObserved().GetState() != zatterav1.InstanceState_INSTANCE_STATE_HEALTHY {
			continue
		}
		candidates = append(candidates, a)
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].GetMeta().GetId() < candidates[j].GetMeta().GetId()
	})

	for _, a := range candidates {
		node, ok := s.store.Node(a.GetNodeId())
		if !ok || node.GetMeshIp() == "" {
			continue
		}
		hostPort, ok := resolveMeshPort(a, start.GetPortName())
		if !ok {
			continue
		}
		return node, net.JoinHostPort(node.GetMeshIp(), strconv.Itoa(int(hostPort))), nil
	}
	return nil, "", status.Error(codes.Unavailable, "no healthy instance with the requested port")
}

// resolveMeshPort returns the node-side host port bound for portName (or the
// sole binding when portName is empty).
func resolveMeshPort(a *zatterav1.Assignment, portName string) (uint32, bool) {
	binds := a.GetMeshPortBindings()
	if len(binds) == 0 {
		binds = a.GetObserved().GetMeshPortBindings()
	}
	if len(binds) == 0 {
		return 0, false
	}
	if portName != "" {
		p, ok := binds[portName]
		return p, ok
	}
	if len(binds) == 1 {
		for _, p := range binds {
			return p, true
		}
	}
	// Ambiguous: prefer a conventional "http" binding.
	if p, ok := binds["http"]; ok {
		return p, true
	}
	return 0, false
}

// projectMatches resolves a project name/id and checks it against the canonical id.
func projectMatches(store *state.Store, projectRef, canonicalID string) bool {
	if p, ok := store.Project(projectRef); ok {
		return p.GetMeta().GetId() == canonicalID
	}
	if p, ok := store.ProjectByName(projectRef); ok {
		return p.GetMeta().GetId() == canonicalID
	}
	return false
}

func agentSize(s *zatterav1.TerminalSize) *clusterv1.AgentTerminalSize {
	if s == nil {
		return nil
	}
	return &clusterv1.AgentTerminalSize{Cols: s.GetCols(), Rows: s.GetRows()}
}

// GRPCExecDialer is the production ExecDialer: it dials a node's
// AgentLocalService over the mesh with node mTLS. Connect supplies the per-node
// client connection; the daemon owns the transport details.
type GRPCExecDialer struct {
	Connect func(ctx context.Context, node *zatterav1.Node) (*grpc.ClientConn, error)
}

// Exec opens a bidi Exec stream, closing the connection when it ends.
func (g GRPCExecDialer) Exec(ctx context.Context, node *zatterav1.Node) (AgentExecStream, error) {
	conn, err := g.Connect(ctx, node)
	if err != nil {
		return nil, err
	}
	st, err := clusterv1.NewAgentLocalServiceClient(conn).Exec(ctx)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &connExecStream{AgentExecStream: st, conn: conn}, nil
}

// ProxyTCP opens a bidi ProxyTCP stream, closing the connection when it ends.
func (g GRPCExecDialer) ProxyTCP(ctx context.Context, node *zatterav1.Node) (AgentProxyStream, error) {
	conn, err := g.Connect(ctx, node)
	if err != nil {
		return nil, err
	}
	st, err := clusterv1.NewAgentLocalServiceClient(conn).ProxyTCP(ctx)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &connProxyStream{AgentProxyStream: st, conn: conn}, nil
}

// Top is a unary call to the node's AgentLocalService.
func (g GRPCExecDialer) Top(ctx context.Context, node *zatterav1.Node, containerID string) (*clusterv1.AgentTopResponse, error) {
	conn, err := g.Connect(ctx, node)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	return clusterv1.NewAgentLocalServiceClient(conn).Top(ctx, &clusterv1.AgentTopRequest{ContainerId: containerID})
}

type connExecStream struct {
	AgentExecStream
	conn *grpc.ClientConn
}

func (c *connExecStream) Recv() (*clusterv1.AgentExecOutput, error) {
	out, err := c.AgentExecStream.Recv()
	if err != nil {
		_ = c.conn.Close()
	}
	return out, err
}

type connProxyStream struct {
	AgentProxyStream
	conn *grpc.ClientConn
}

func (c *connProxyStream) Recv() (*clusterv1.TCPChunk, error) {
	out, err := c.AgentProxyStream.Recv()
	if err != nil {
		_ = c.conn.Close()
	}
	return out, err
}
