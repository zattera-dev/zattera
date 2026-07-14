package api

import (
	"context"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/agent"
	crt "github.com/zattera-dev/zattera/internal/daemon/runtime"
	"github.com/zattera-dev/zattera/internal/state"
	"github.com/zattera-dev/zattera/internal/testutil/fakeruntime"
)

// startGRPC serves register(s) on a loopback listener and returns a dial addr.
func startGRPC(t *testing.T, register func(*grpc.Server)) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	register(srv)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// dialInsecure connects to an in-process gRPC server.
func dialInsecure(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// execHarness wires: fakeruntime → agent ExecServer (gRPC) → control ExecServer
// (gRPC over a GRPCExecDialer pointing at the agent) → ExecServiceClient.
type execHarness struct {
	rt     *fakeruntime.Fake
	store  *state.Store
	client zatterav1.ExecServiceClient
}

func newExecHarness(t *testing.T, rt *fakeruntime.Fake) *execHarness {
	t.Helper()
	// Agent-local server (the "node").
	agentAddr := startGRPC(t, func(s *grpc.Server) {
		clusterv1.RegisterAgentLocalServiceServer(s, agent.NewExecServer(rt, nil))
	})

	// Control state: one node whose mesh IP is loopback, one healthy instance.
	st := state.New()
	dialer := GRPCExecDialer{Connect: func(_ context.Context, _ *zatterav1.Node) (*grpc.ClientConn, error) {
		return grpc.NewClient(agentAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}}
	ctrlAddr := startGRPC(t, func(s *grpc.Server) {
		zatterav1.RegisterExecServiceServer(s, NewExecServer(st, dialer, nil))
	})

	return &execHarness{rt: rt, store: st, client: zatterav1.NewExecServiceClient(dialInsecure(t, ctrlAddr))}
}

// seedInstance registers a node + running assignment in the control store.
func (h *execHarness) seedInstance(instanceID, containerID string, meshIP string, ports map[string]uint32) {
	h.store.PutNode(&zatterav1.Node{Meta: &zatterav1.Meta{Id: "n1"}, MeshIp: meshIP})
	h.store.PutAssignment(&zatterav1.Assignment{
		Meta:             &zatterav1.Meta{Id: instanceID},
		NodeId:           "n1",
		ProjectId:        "p1",
		AppId:            "a1",
		EnvironmentId:    "e1",
		Desired:          zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN,
		MeshPortBindings: ports,
		Observed: &zatterav1.AssignmentObserved{
			State:       zatterav1.InstanceState_INSTANCE_STATE_HEALTHY,
			ContainerId: containerID,
		},
	})
}

func TestExecEchoAndExitCode(t *testing.T) {
	rt := fakeruntime.New()
	// The fake container must exist and be running for Exec to proceed.
	id, _ := rt.CreateContainer(context.Background(), crt.ContainerSpec{Image: "img"})
	_ = rt.StartContainer(context.Background(), id)
	// Echo stdin→stdout, then exit 7.
	rt.Hooks.Exec = func(_ string, _ crt.ExecSpec, stdin io.Reader, stdout, _ io.Writer, _ <-chan crt.TermSize) (int, error) {
		_, _ = io.Copy(stdout, stdin)
		return 7, nil
	}

	h := newExecHarness(t, rt)
	h.seedInstance("inst-1", id, "127.0.0.1", nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := h.client.Exec(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&zatterav1.ExecInput{Start: &zatterav1.ExecStart{
		ProjectId: "p1", InstanceId: "inst-1", Command: []string{"cat"},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&zatterav1.ExecInput{Stdin: []byte("hello ")}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&zatterav1.ExecInput{Stdin: []byte("world")}); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}

	var got []byte
	var exit int32
	exited := false
	for {
		out, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, out.GetStdout()...)
		if out.GetExited() {
			exit = out.GetExitCode()
			exited = true
		}
	}
	if string(got) != "hello world" {
		t.Fatalf("echo = %q, want %q", got, "hello world")
	}
	if !exited || exit != 7 {
		t.Fatalf("exit = %d exited=%v, want 7/true", exit, exited)
	}
}

func TestExecInstanceNotRunning(t *testing.T) {
	h := newExecHarness(t, fakeruntime.New())
	// Assignment with no container id → FailedPrecondition.
	h.store.PutNode(&zatterav1.Node{Meta: &zatterav1.Meta{Id: "n1"}, MeshIp: "127.0.0.1"})
	h.store.PutAssignment(&zatterav1.Assignment{
		Meta: &zatterav1.Meta{Id: "inst-x"}, NodeId: "n1", ProjectId: "p1",
		Observed: &zatterav1.AssignmentObserved{State: zatterav1.InstanceState_INSTANCE_STATE_PENDING},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := h.client.Exec(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = stream.Send(&zatterav1.ExecInput{Start: &zatterav1.ExecStart{ProjectId: "p1", InstanceId: "inst-x"}})
	if _, err := stream.Recv(); err == nil {
		t.Fatal("expected error for a non-running instance")
	}
}

func TestExecTop(t *testing.T) {
	rt := fakeruntime.New()
	id, _ := rt.CreateContainer(context.Background(), crt.ContainerSpec{Image: "img"})
	_ = rt.StartContainer(context.Background(), id)

	h := newExecHarness(t, rt)
	h.seedInstance("inst-1", id, "127.0.0.1", nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := h.client.Top(ctx, &zatterav1.TopRequest{ProjectId: "p1", InstanceId: "inst-1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetTitles()) == 0 || len(resp.GetProcesses()) == 0 {
		t.Fatalf("empty top: %+v", resp)
	}
}

func TestExecPortForwardRoundTrip(t *testing.T) {
	// A local TCP echo stands in for the container's port; the agent's ProxyTCP
	// dials it via the dial_addr the control plane computes (meshIP:hostPort).
	echo, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = echo.Close() })
	go func() {
		for {
			c, err := echo.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) { defer func() { _ = c.Close() }(); _, _ = io.Copy(c, c) }(c)
		}
	}()
	_, portStr, _ := net.SplitHostPort(echo.Addr().String())
	p, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatal(err)
	}
	port := uint32(p)

	rt := fakeruntime.New()
	id, _ := rt.CreateContainer(context.Background(), crt.ContainerSpec{Image: "img"})
	_ = rt.StartContainer(context.Background(), id)

	h := newExecHarness(t, rt)
	h.seedInstance("inst-1", id, "127.0.0.1", map[string]uint32{"http": port})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := h.client.PortForward(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&zatterav1.PortForwardInput{Start: &zatterav1.PortForwardStart{
		ProjectId: "p1", AppId: "a1", EnvironmentId: "e1", PortName: "http",
	}}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&zatterav1.PortForwardInput{Data: []byte("roundtrip")}); err != nil {
		t.Fatal(err)
	}

	got := make([]byte, 0, 9)
	for len(got) < len("roundtrip") {
		out, err := stream.Recv()
		if err != nil {
			t.Fatalf("recv: %v (got %q)", err, got)
		}
		got = append(got, out.GetData()...)
	}
	if string(got) != "roundtrip" {
		t.Fatalf("port-forward echo = %q", got)
	}
}
