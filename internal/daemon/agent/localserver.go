package agent

import (
	"context"

	"google.golang.org/grpc"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

// LocalServer composes the node-local AgentLocalService methods that the control
// plane dials over the mesh (T-54): builds (BuildServer) and interactive
// exec/top/port-forward (ExecServer). QueryLogs/Stats/volume methods remain
// Unimplemented until their agent-side backends land.
//
// A single gRPC server can register only one AgentLocalServiceServer, so this
// type forwards each method to the right sub-server instead of embedding both.
type LocalServer struct {
	clusterv1.UnimplementedAgentLocalServiceServer
	build *BuildServer
	exec  *ExecServer
	logs  *LogServer
	stats *StatsServer
}

// NewLocalServer builds the composite. Any sub-server may be nil (e.g. a
// non-builder node passes build=nil); the corresponding methods then report
// Unimplemented via the embedded base.
func NewLocalServer(build *BuildServer, exec *ExecServer, logs *LogServer, stats *StatsServer) *LocalServer {
	return &LocalServer{build: build, exec: exec, logs: logs, stats: stats}
}

// QueryLogs dispatches to the log sub-server.
func (s *LocalServer) QueryLogs(q *zatterav1.LogQuery, stream grpc.ServerStreamingServer[zatterav1.LogLine]) error {
	if s.logs == nil {
		return s.UnimplementedAgentLocalServiceServer.QueryLogs(q, stream)
	}
	return s.logs.QueryLogs(q, stream)
}

// RunBuild dispatches to the build sub-server.
func (s *LocalServer) RunBuild(req *clusterv1.RunBuildRequest, stream grpc.ServerStreamingServer[clusterv1.BuildEvent]) error {
	if s.build == nil {
		return s.UnimplementedAgentLocalServiceServer.RunBuild(req, stream)
	}
	return s.build.RunBuild(req, stream)
}

// CancelBuild dispatches to the build sub-server.
func (s *LocalServer) CancelBuild(ctx context.Context, req *clusterv1.CancelBuildRequest) (*clusterv1.CancelBuildResponse, error) {
	if s.build == nil {
		return s.UnimplementedAgentLocalServiceServer.CancelBuild(ctx, req)
	}
	return s.build.CancelBuild(ctx, req)
}

// Exec dispatches to the exec sub-server.
func (s *LocalServer) Exec(stream grpc.BidiStreamingServer[clusterv1.AgentExecInput, clusterv1.AgentExecOutput]) error {
	if s.exec == nil {
		return s.UnimplementedAgentLocalServiceServer.Exec(stream)
	}
	return s.exec.Exec(stream)
}

// Stats dispatches to the stats sub-server (local ring TSDB, T-60).
func (s *LocalServer) Stats(ctx context.Context, q *zatterav1.StatsQuery) (*zatterav1.StatsResponse, error) {
	if s.stats == nil {
		return s.UnimplementedAgentLocalServiceServer.Stats(ctx, q)
	}
	return s.stats.Stats(ctx, q)
}

// Top dispatches to the exec sub-server.
func (s *LocalServer) Top(ctx context.Context, req *clusterv1.AgentTopRequest) (*clusterv1.AgentTopResponse, error) {
	if s.exec == nil {
		return s.UnimplementedAgentLocalServiceServer.Top(ctx, req)
	}
	return s.exec.Top(ctx, req)
}

// ProxyTCP dispatches to the exec sub-server.
func (s *LocalServer) ProxyTCP(stream grpc.BidiStreamingServer[clusterv1.TCPChunk, clusterv1.TCPChunk]) error {
	if s.exec == nil {
		return s.UnimplementedAgentLocalServiceServer.ProxyTCP(stream)
	}
	return s.exec.ProxyTCP(stream)
}
