package agent

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	crt "github.com/zattera-dev/zattera/internal/daemon/runtime"
)

// proxyDialTimeout bounds the ProxyTCP dial to the container/mesh target.
const proxyDialTimeout = 5 * time.Second

// ExecServer implements the interactive methods of AgentLocalService (T-49):
// Exec (bidi TTY), Top (process snapshot) and ProxyTCP (port-forward leg). It
// runs on every node against the local container runtime; the control plane's
// ExecService relays user streams to it over the mesh.
//
// It embeds UnimplementedAgentLocalServiceServer so it can stand alone in tests;
// the production node composes these methods with the build/log/volume methods
// into one AgentLocalService when the :8444 mTLS listener is wired.
type ExecServer struct {
	clusterv1.UnimplementedAgentLocalServiceServer
	rt  crt.ContainerRuntime
	log *slog.Logger
	// dial opens the ProxyTCP upstream (injectable for tests). Defaults to a
	// bounded TCP dial.
	dial func(ctx context.Context, addr string) (net.Conn, error)
}

// NewExecServer builds the exec server over a container runtime.
func NewExecServer(rt crt.ContainerRuntime, log *slog.Logger) *ExecServer {
	if log == nil {
		log = slog.Default()
	}
	return &ExecServer{
		rt:  rt,
		log: log,
		dial: func(ctx context.Context, addr string) (net.Conn, error) {
			d := net.Dialer{Timeout: proxyDialTimeout}
			return d.DialContext(ctx, "tcp", addr)
		},
	}
}

// Exec runs a command inside a running container and relays its TTY/pipes over
// the bidi stream. The first client message must carry AgentExecStart.
func (s *ExecServer) Exec(stream grpc.BidiStreamingServer[clusterv1.AgentExecInput, clusterv1.AgentExecOutput]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	start := first.GetStart()
	if start == nil || start.GetContainerId() == "" {
		return status.Error(codes.InvalidArgument, "first Exec message must set start.container_id")
	}
	cmd := start.GetCommand()
	if len(cmd) == 0 {
		cmd = []string{"/bin/sh"}
	}

	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	// stdin: pump client bytes into the exec via an in-memory pipe.
	stdinR, stdinW := io.Pipe()
	resize := make(chan crt.TermSize, 1)
	if sz := start.GetInitialSize(); sz != nil && (sz.GetCols() > 0 || sz.GetRows() > 0) {
		resize <- crt.TermSize{Cols: sz.GetCols(), Rows: sz.GetRows()}
	}

	// Serialize Send: stdout and stderr writers both send on the one stream.
	var sendMu sync.Mutex
	send := func(o *clusterv1.AgentExecOutput) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return stream.Send(o)
	}
	stdout := writerFunc(func(p []byte) (int, error) {
		if err := send(&clusterv1.AgentExecOutput{Stdout: append([]byte(nil), p...)}); err != nil {
			return 0, err
		}
		return len(p), nil
	})
	stderr := writerFunc(func(p []byte) (int, error) {
		if err := send(&clusterv1.AgentExecOutput{Stderr: append([]byte(nil), p...)}); err != nil {
			return 0, err
		}
		return len(p), nil
	})

	// Reader goroutine: subsequent messages carry stdin bytes and resize events.
	go func() {
		defer func() { _ = stdinW.Close() }()
		for {
			in, err := stream.Recv()
			if err != nil {
				cancel() // client closed / errored: unblock the exec
				return
			}
			if r := in.GetResize(); r != nil {
				select {
				case resize <- crt.TermSize{Cols: r.GetCols(), Rows: r.GetRows()}:
				case <-ctx.Done():
					return
				default: // drop stale resize rather than block
				}
			}
			if len(in.GetStdin()) > 0 {
				if _, err := stdinW.Write(in.GetStdin()); err != nil {
					return
				}
			}
		}
	}()

	spec := crt.ExecSpec{Command: cmd, TTY: start.GetTty()}
	code, err := s.rt.Exec(ctx, start.GetContainerId(), spec, stdinR, stdout, stderr, resize)
	_ = stdinR.Close()
	if err != nil && ctx.Err() == nil {
		s.log.Warn("exec failed", "container", short(start.GetContainerId()), "err", err)
		return send(&clusterv1.AgentExecOutput{Exited: true, ExitCode: int32(code), Stderr: []byte(err.Error() + "\n")})
	}
	return send(&clusterv1.AgentExecOutput{Exited: true, ExitCode: int32(code)})
}

// Top returns a one-shot process listing from inside the container.
func (s *ExecServer) Top(ctx context.Context, req *clusterv1.AgentTopRequest) (*clusterv1.AgentTopResponse, error) {
	if req.GetContainerId() == "" {
		return nil, status.Error(codes.InvalidArgument, "container_id required")
	}
	titles, rows, err := s.rt.Top(ctx, req.GetContainerId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "top: %v", err)
	}
	out := &clusterv1.AgentTopResponse{Titles: titles}
	for _, r := range rows {
		out.Processes = append(out.Processes, &clusterv1.AgentTopProcess{Fields: r})
	}
	return out, nil
}

// ProxyTCP splices the bidi stream to a TCP target. The first chunk carries the
// dial address ("containerIP:port" or "meshIP:port"); its data (if any) is
// forwarded after the dial.
func (s *ExecServer) ProxyTCP(stream grpc.BidiStreamingServer[clusterv1.TCPChunk, clusterv1.TCPChunk]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if first.GetDialAddr() == "" {
		return status.Error(codes.InvalidArgument, "first ProxyTCP chunk must set dial_addr")
	}
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	conn, err := s.dial(ctx, first.GetDialAddr())
	if err != nil {
		return status.Errorf(codes.Unavailable, "dial %s: %v", first.GetDialAddr(), err)
	}
	defer func() { _ = conn.Close() }()

	if len(first.GetData()) > 0 {
		if _, err := conn.Write(first.GetData()); err != nil {
			return nil
		}
	}

	done := make(chan struct{}, 2)
	// upstream → stream
	go func() {
		buf := make([]byte, 32*1024)
		for {
			n, err := conn.Read(buf)
			if n > 0 {
				if serr := stream.Send(&clusterv1.TCPChunk{Data: append([]byte(nil), buf[:n]...)}); serr != nil {
					break
				}
			}
			if err != nil {
				break
			}
		}
		done <- struct{}{}
	}()
	// stream → upstream
	go func() {
		for {
			in, err := stream.Recv()
			if err != nil {
				break
			}
			if len(in.GetData()) > 0 {
				if _, werr := conn.Write(in.GetData()); werr != nil {
					break
				}
			}
		}
		if cw, ok := conn.(interface{ CloseWrite() error }); ok {
			_ = cw.CloseWrite()
		}
		done <- struct{}{}
	}()
	<-done
	cancel()
	<-done
	return nil
}

// writerFunc adapts a function to io.Writer.
type writerFunc func([]byte) (int, error)

func (w writerFunc) Write(p []byte) (int, error) { return w(p) }
