package daemon

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	"github.com/zattera-dev/zattera/internal/daemon/agent"
	"github.com/zattera-dev/zattera/internal/daemon/intdns"
	"github.com/zattera-dev/zattera/internal/daemon/proxy"
)

// intdnsReconcileInterval re-syncs the resolver's per-network scopes with the
// executor's active assignments (bridges come and go with deployments).
const intdnsReconcileInterval = 3 * time.Second

// startInternalMesh runs the per-node internal service mesh (F26): the VIP proxy
// binds each service VIP and L4-load-balances it across the service's replicas
// on every node, and the internal DNS resolver answers <svc>.internal on each
// bridge gateway. Both read the node's live route snapshot; the resolver's
// per-network scopes come from the executor's active assignments. Runs on every
// worker-capable node so a container on any node can reach <svc>.internal.
func startInternalMesh(ctx context.Context, source proxy.RouteSource, exec *agent.Executor, log *slog.Logger) {
	go intdns.NewVIPProxy(source, log).Run(ctx)

	resolver := intdns.New(source, nil, log)
	go func() {
		defer resolver.Close()
		t := time.NewTicker(intdnsReconcileInterval)
		defer t.Stop()
		for {
			resolver.Reconcile(intdnsScopes(exec))
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
	log.Info("internal service mesh started (VIP proxy + internal DNS)")
}

func intdnsScopes(exec *agent.Executor) []intdns.NetworkScope {
	ns := exec.NetworkScopes()
	out := make([]intdns.NetworkScope, len(ns))
	for i, s := range ns {
		out[i] = intdns.NetworkScope{Gateway: s.Gateway, ProjectID: s.ProjectID, EnvID: s.EnvID}
	}
	return out
}

// grpcRouteDialer opens a WatchRoutes stream to the control plane over node mTLS.
// It is the RouteSource transport for worker nodes; control nodes read routes
// in-process from the RouteBuilder. It picks the next control endpoint on every
// (re)connect so the route stream fails over when a control node dies (T-55c).
type grpcRouteDialer struct {
	ce     *controlEndpoints
	nodeID string
}

func (d grpcRouteDialer) WatchRoutes(ctx context.Context, haveVersion uint64) (proxy.RouteStream, error) {
	addr, creds := d.ce.pick()
	if addr == "" {
		return nil, fmt.Errorf("daemon: no control endpoint available")
	}
	conn, err := grpc.NewClient(addr, controlDialOpts(creds)...)
	if err != nil {
		return nil, err
	}
	stream, err := clusterv1.NewRouteServiceClient(conn).WatchRoutes(ctx, &clusterv1.WatchRoutesRequest{
		NodeId:      d.nodeID,
		HaveVersion: haveVersion,
	})
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &routeStream{stream: stream, conn: conn}, nil
}

// routeStream adapts the gRPC stream to proxy.RouteStream, closing the
// connection when the stream ends.
type routeStream struct {
	stream interface {
		Recv() (*clusterv1.RouteSnapshot, error)
	}
	conn *grpc.ClientConn
}

func (r *routeStream) Recv() (*clusterv1.RouteSnapshot, error) {
	snap, err := r.stream.Recv()
	if err != nil {
		_ = r.conn.Close()
	}
	return snap, err
}
