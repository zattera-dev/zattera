// Package api hosts the public control-plane API. One TLS listener serves both
// gRPC (HTTP/2, application/grpc) and the grpc-gateway REST mux on the same
// port; content-type routing splits the two. The listener requests — but does
// not require — client certs, so token-bearing CLIs and mTLS nodes share it.
//
// Services are wired incrementally through Options: a nil service is simply not
// registered, letting later tasks (T-04 auth, T-05 projects, …) land one at a
// time.
package api

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/ca"
)

// maxRecvMsgSize gives headroom over the 1MB source-tarball chunks.
const maxRecvMsgSize = 64 << 20

// Options configures the API server. A nil service field is not registered.
type Options struct {
	CA     *ca.CA
	Listen string // e.g. ":8443"
	Logger *slog.Logger

	// Server certificate SANs. Must include 127.0.0.1 (the gateway dials the
	// public port over loopback) and localhost.
	DNSNames []string
	IPs      []net.IP

	// Public services (nil = not registered).
	AuthService    zatterav1.AuthServiceServer
	ProjectService zatterav1.ProjectServiceServer
	AppService     zatterav1.AppServiceServer
	DeployService  zatterav1.DeployServiceServer
	NodeService    zatterav1.NodeServiceServer
	StateService   zatterav1.StateServiceServer
	AuditService   zatterav1.AuditServiceServer
	LogService     zatterav1.LogServiceServer
	DomainService  zatterav1.DomainServiceServer
	ExecService    zatterav1.ExecServiceServer

	// Node↔control services (mTLS node identity, no REST gateway).
	AgentSyncService clusterv1.AgentSyncServiceServer
	// JoinService is token-authenticated (no mTLS), no REST gateway.
	JoinService clusterv1.JoinServiceServer
	// MeshService distributes WireGuard peer sets (mTLS node identity).
	MeshService clusterv1.MeshServiceServer
	// RouteService streams route snapshots to node proxies (mTLS node identity).
	RouteService clusterv1.RouteServiceServer

	// GitHubWebhook, if set, is mounted as a raw HTTP handler at
	// /v1/github/webhook (signature-authenticated, not part of the gRPC policy).
	GitHubWebhook http.Handler

	// Interceptors run in the given order (auth → rbac → audit → leader-forward
	// per later tasks). Health checks bypass them via a method skip inside each.
	UnaryInterceptors  []grpc.UnaryServerInterceptor
	StreamInterceptors []grpc.StreamServerInterceptor
}

// Server owns the TLS listener, the gRPC server and the REST gateway.
type Server struct {
	opts     Options
	log      *slog.Logger
	grpc     *grpc.Server
	http     *http.Server
	health   *health.Server
	lis      net.Listener
	endpoint string // loopback dial target for the gateway (127.0.0.1:port)
}

// New binds the listener and wires the gRPC server, health service and REST
// gateway. Call Serve to start accepting.
func New(opts Options) (*Server, error) {
	if opts.CA == nil {
		return nil, fmt.Errorf("api: CA is required")
	}
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}

	lis, err := net.Listen("tcp", opts.Listen)
	if err != nil {
		return nil, fmt.Errorf("api: listen %s: %w", opts.Listen, err)
	}
	endpoint := loopbackEndpoint(lis.Addr())

	// gRPC server.
	grpcSrv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(opts.UnaryInterceptors...),
		grpc.ChainStreamInterceptor(opts.StreamInterceptors...),
		grpc.MaxRecvMsgSize(maxRecvMsgSize),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	registerGRPC(grpcSrv, opts)

	hs := health.NewServer()
	healthpb.RegisterHealthServer(grpcSrv, hs)
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	// Fail closed: every registered method must have an auth policy entry.
	if err := ValidateMethodTable(grpcSrv.GetServiceInfo()); err != nil {
		_ = lis.Close()
		return nil, err
	}

	// REST gateway dialing the same port over loopback with CA-trusted TLS.
	gwMux := runtime.NewServeMux()
	gwCreds := credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    opts.CA.Pool(),
		ServerName: "127.0.0.1",
	})
	dialOpts := []grpc.DialOption{grpc.WithTransportCredentials(gwCreds)}
	if err := registerGateway(context.Background(), gwMux, endpoint, dialOpts, opts); err != nil {
		_ = lis.Close()
		return nil, err
	}
	if err := gwMux.HandlePath(http.MethodGet, "/healthz", healthzHandler); err != nil {
		_ = lis.Close()
		return nil, fmt.Errorf("api: register /healthz: %w", err)
	}

	tlsCfg, err := opts.CA.ServerTLSConfig(opts.DNSNames, opts.IPs)
	if err != nil {
		_ = lis.Close()
		return nil, err
	}
	tlsCfg.NextProtos = []string{"h2", "http/1.1"}

	s := &Server{opts: opts, log: log, grpc: grpcSrv, health: hs, lis: lis, endpoint: endpoint}
	s.http = &http.Server{
		Handler:   s.routeHandler(grpcSrv, gwMux),
		TLSConfig: tlsCfg,
	}
	return s, nil
}

// routeHandler splits gRPC (HTTP/2 + application/grpc) from REST on one port.
func (s *Server) routeHandler(grpcSrv *grpc.Server, gw http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			grpcSrv.ServeHTTP(w, r)
			return
		}
		// GitHub push-to-deploy webhook (T-37): a raw HTTP route, authenticated
		// by its HMAC signature rather than the gRPC auth chain.
		if s.opts.GitHubWebhook != nil && r.URL.Path == "/v1/github/webhook" {
			s.opts.GitHubWebhook.ServeHTTP(w, r)
			return
		}
		gw.ServeHTTP(w, r)
	})
}

// Addr returns the bound listen address.
func (s *Server) Addr() net.Addr { return s.lis.Addr() }

// Endpoint returns the loopback dial target (127.0.0.1:port).
func (s *Server) Endpoint() string { return s.endpoint }

// Health exposes the grpc health server (liveness wiring can flip statuses).
func (s *Server) Health() *health.Server { return s.health }

// Serve blocks serving TLS until ctx is canceled, then shuts down gracefully.
func (s *Server) Serve(ctx context.Context) error {
	errc := make(chan error, 1)
	go func() {
		// Certs come from TLSConfig.Certificates; empty file args are fine.
		err := s.http.ServeTLS(s.lis, "", "")
		if err == http.ErrServerClosed {
			err = nil
		}
		errc <- err
	}()
	s.log.Info("api server listening", "addr", s.lis.Addr().String())

	select {
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.http.Shutdown(shutCtx)
		s.grpc.GracefulStop()
		return nil
	case err := <-errc:
		return err
	}
}

func healthzHandler(w http.ResponseWriter, _ *http.Request, _ map[string]string) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// registerGRPC registers every non-nil service on the gRPC server.
func registerGRPC(s *grpc.Server, opts Options) {
	if opts.AuthService != nil {
		zatterav1.RegisterAuthServiceServer(s, opts.AuthService)
	}
	if opts.ProjectService != nil {
		zatterav1.RegisterProjectServiceServer(s, opts.ProjectService)
	}
	if opts.AppService != nil {
		zatterav1.RegisterAppServiceServer(s, opts.AppService)
	}
	if opts.DeployService != nil {
		zatterav1.RegisterDeployServiceServer(s, opts.DeployService)
	}
	if opts.NodeService != nil {
		zatterav1.RegisterNodeServiceServer(s, opts.NodeService)
	}
	if opts.StateService != nil {
		zatterav1.RegisterStateServiceServer(s, opts.StateService)
	}
	if opts.AuditService != nil {
		zatterav1.RegisterAuditServiceServer(s, opts.AuditService)
	}
	if opts.LogService != nil {
		zatterav1.RegisterLogServiceServer(s, opts.LogService)
	}
	if opts.DomainService != nil {
		zatterav1.RegisterDomainServiceServer(s, opts.DomainService)
	}
	// ExecService is gRPC-only (bidi streams; no REST gateway).
	if opts.ExecService != nil {
		zatterav1.RegisterExecServiceServer(s, opts.ExecService)
	}
	if opts.AgentSyncService != nil {
		clusterv1.RegisterAgentSyncServiceServer(s, opts.AgentSyncService)
	}
	if opts.JoinService != nil {
		clusterv1.RegisterJoinServiceServer(s, opts.JoinService)
	}
	if opts.MeshService != nil {
		clusterv1.RegisterMeshServiceServer(s, opts.MeshService)
	}
	if opts.RouteService != nil {
		clusterv1.RegisterRouteServiceServer(s, opts.RouteService)
	}
}

// registerGateway registers REST handlers for the services that expose one.
func registerGateway(ctx context.Context, mux *runtime.ServeMux, endpoint string, dialOpts []grpc.DialOption, opts Options) error {
	if opts.AuthService != nil {
		if err := zatterav1.RegisterAuthServiceHandlerFromEndpoint(ctx, mux, endpoint, dialOpts); err != nil {
			return fmt.Errorf("api: gateway auth: %w", err)
		}
	}
	if opts.ProjectService != nil {
		if err := zatterav1.RegisterProjectServiceHandlerFromEndpoint(ctx, mux, endpoint, dialOpts); err != nil {
			return fmt.Errorf("api: gateway project: %w", err)
		}
	}
	if opts.AppService != nil {
		if err := zatterav1.RegisterAppServiceHandlerFromEndpoint(ctx, mux, endpoint, dialOpts); err != nil {
			return fmt.Errorf("api: gateway app: %w", err)
		}
	}
	if opts.DeployService != nil {
		if err := zatterav1.RegisterDeployServiceHandlerFromEndpoint(ctx, mux, endpoint, dialOpts); err != nil {
			return fmt.Errorf("api: gateway deploy: %w", err)
		}
	}
	if opts.DomainService != nil {
		if err := zatterav1.RegisterDomainServiceHandlerFromEndpoint(ctx, mux, endpoint, dialOpts); err != nil {
			return fmt.Errorf("api: gateway domain: %w", err)
		}
	}
	if opts.NodeService != nil {
		if err := zatterav1.RegisterNodeServiceHandlerFromEndpoint(ctx, mux, endpoint, dialOpts); err != nil {
			return fmt.Errorf("api: gateway node: %w", err)
		}
	}
	if opts.AuditService != nil {
		if err := zatterav1.RegisterAuditServiceHandlerFromEndpoint(ctx, mux, endpoint, dialOpts); err != nil {
			return fmt.Errorf("api: gateway audit: %w", err)
		}
	}
	return nil
}

// loopbackEndpoint maps a bound listen addr to a 127.0.0.1:port dial target
// for the gateway's loopback dial.
func loopbackEndpoint(addr net.Addr) string {
	_, port, err := net.SplitHostPort(addr.String())
	if err != nil {
		return addr.String()
	}
	return net.JoinHostPort("127.0.0.1", port)
}
