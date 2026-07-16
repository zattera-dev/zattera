package api

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

const (
	// natKeepaliveSeconds keeps a NAT'd worker's hole to the hub (or a punched
	// worker↔worker path) open.
	natKeepaliveSeconds = 25
	// endpointTTL expires a disco-observed reflexive endpoint (livestate).
	endpointTTL = 5 * time.Minute
	// endpointFoldInterval batches observed endpoints into durable state.
	endpointFoldInterval = 30 * time.Second
)

// MeshServer implements MeshService: WatchPeers streams each node its full
// WireGuard peer set (hub-and-spoke Phase A + worker↔worker Phase B);
// ReportObservedEndpoint records disco-observed reflexive endpoints (T-20).
type MeshServer struct {
	clusterv1.UnimplementedMeshServiceServer
	store    *state.Store
	raft     Applier
	clock    clock.Clock
	log      *slog.Logger
	debounce time.Duration

	mu        sync.Mutex
	endpoints map[string]observedEndpoint // node id → latest disco observation

	// punch tracks live PunchStreams for control-coordinated hole punching (T-57).
	punch *punchRegistry
}

type observedEndpoint struct {
	addr string
	at   time.Time
}

// NewMeshServer builds the mesh peer-distribution service. raft may be nil when
// only the peer builder is exercised (no endpoint folding).
func NewMeshServer(store *state.Store, raft Applier, clk clock.Clock, log *slog.Logger) *MeshServer {
	if log == nil {
		log = slog.Default()
	}
	if clk == nil {
		clk = clock.Real{}
	}
	return &MeshServer{
		store:     store,
		raft:      raft,
		clock:     clk,
		log:       log,
		debounce:  defaultAssignmentDebounce,
		endpoints: map[string]observedEndpoint{},
		punch:     newPunchRegistry(),
	}
}

// WatchPeers streams the calling node its full PeerSet, resending (debounced) on
// every node change.
func (s *MeshServer) WatchPeers(req *clusterv1.WatchPeersRequest, stream clusterv1.MeshService_WatchPeersServer) error {
	nodeID, err := s.callerNodeID(stream.Context(), req.GetNodeId())
	if err != nil {
		return err
	}

	sub := s.store.Watch(state.KindNode)
	defer sub.Close()

	if err := stream.Send(s.buildPeerSet(nodeID)); err != nil {
		return err
	}

	var timer <-chan time.Time
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case <-sub.Notify():
			sub.Drain()
			if timer == nil {
				timer = s.clock.After(s.debounce)
			}
		case <-timer:
			timer = nil
			if err := stream.Send(s.buildPeerSet(nodeID)); err != nil {
				return err
			}
		}
	}
}

// callerNodeID prefers the mTLS node identity, cross-checking the request's
// claimed id. Over a no-cert loopback dial it trusts the request.
func (s *MeshServer) callerNodeID(ctx context.Context, claimed string) (string, error) {
	if id, ok := IdentityFrom(ctx); ok && id.NodeID != "" {
		if claimed != "" && claimed != id.NodeID {
			return "", status.Errorf(codes.PermissionDenied, "node_id %q does not match certificate identity %q", claimed, id.NodeID)
		}
		return id.NodeID, nil
	}
	if claimed == "" {
		return "", status.Error(codes.InvalidArgument, "node_id is required")
	}
	return claimed, nil
}

// buildPeerSet computes the WireGuard peers for one node (Phase A hub-and-spoke):
//   - a worker gets ONLY the control nodes, each routing the whole mesh
//     (allowed_ips = 10.90.0.0/16), hub_and_spoke=true, with a 25s keepalive
//     when the worker itself is NAT'd (no public endpoint) so it keeps the hole
//     to the hub open.
//   - a control gets EVERY other mesh node with a /32 allowed IP. It never sets
//     a keepalive and never dials a NAT'd worker (empty endpoint) — the hub
//     waits for the worker to connect.
func (s *MeshServer) buildPeerSet(selfID string) *clusterv1.PeerSet {
	nodes := s.store.ListNodes()
	var self *zatterav1.Node
	for _, n := range nodes {
		if n.GetMeta().GetId() == selfID {
			self = n
			break
		}
	}
	ps := &clusterv1.PeerSet{Version: s.store.Version()}
	if self == nil {
		return ps
	}
	selfControl := hasControlRole(self.GetRoles())
	selfHasEndpoint := len(self.GetPublicEndpoints()) > 0
	ps.HubAndSpoke = !selfControl

	for _, n := range nodes {
		if n.GetMeta().GetId() == selfID {
			continue
		}
		if n.GetWireguardPublicKey() == "" || n.GetMeshIp() == "" {
			continue // not mesh-ready yet
		}
		isControl := hasControlRole(n.GetRoles())
		peer := &clusterv1.Peer{
			NodeId:             n.GetMeta().GetId(),
			WireguardPublicKey: n.GetWireguardPublicKey(),
			MeshIp:             n.GetMeshIp(),
			Endpoints:          n.GetPublicEndpoints(),
			IsControl:          isControl,
		}
		switch {
		case selfControl:
			// Control sees every node with a /32.
			peer.AllowedIps = []string{n.GetMeshIp() + "/32"}
		case isControl:
			// Worker → control: the hub route for the whole mesh.
			peer.AllowedIps = []string{meshCIDR}
			if !selfHasEndpoint {
				peer.PersistentKeepaliveSeconds = natKeepaliveSeconds
			}
		default:
			// Worker → worker (Phase B): a direct /32 path only when BOTH sides
			// have a known endpoint. The control peers keep the /16, so WG's
			// most-specific match uses this /32 and the hub catches the rest.
			if !selfHasEndpoint || len(n.GetPublicEndpoints()) == 0 {
				continue
			}
			peer.AllowedIps = []string{n.GetMeshIp() + "/32"}
			peer.PersistentKeepaliveSeconds = natKeepaliveSeconds
		}
		ps.Peers = append(ps.Peers, peer)
	}
	sort.Slice(ps.Peers, func(i, j int) bool { return ps.Peers[i].GetNodeId() < ps.Peers[j].GetNodeId() })
	return ps
}

// ReportObservedEndpoint records a node's own disco-observed reflexive endpoint.
// Only self-reports are accepted (the mTLS identity must match the claimed node).
func (s *MeshServer) ReportObservedEndpoint(ctx context.Context, req *clusterv1.ReportObservedEndpointRequest) (*clusterv1.ReportObservedEndpointResponse, error) {
	caller, err := s.callerNodeID(ctx, req.GetNodeId())
	if err != nil {
		return nil, err
	}
	if req.GetObservedEndpoint() == "" {
		return &clusterv1.ReportObservedEndpointResponse{Accepted: false}, nil
	}
	s.mu.Lock()
	s.endpoints[caller] = observedEndpoint{addr: req.GetObservedEndpoint(), at: s.clock.Now()}
	s.mu.Unlock()
	return &clusterv1.ReportObservedEndpointResponse{Accepted: true}, nil
}

// RunEndpointFolder periodically folds fresh disco-observed endpoints into the
// nodes' durable public_endpoints (leader only), so the peer builder can hand
// out direct worker↔worker paths. Blocks until ctx is done.
func (s *MeshServer) RunEndpointFolder(ctx context.Context) {
	tick := s.clock.NewTicker(endpointFoldInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C():
			s.foldEndpoints(ctx)
		}
	}
}

func (s *MeshServer) foldEndpoints(ctx context.Context) {
	if s.raft == nil || !s.raft.IsLeader() {
		return
	}
	now := s.clock.Now()
	s.mu.Lock()
	fresh := make(map[string]string, len(s.endpoints))
	for id, e := range s.endpoints {
		if now.Sub(e.at) <= endpointTTL {
			fresh[id] = e.addr
		} else {
			delete(s.endpoints, id)
		}
	}
	s.mu.Unlock()

	for id, addr := range fresh {
		n, ok := s.store.Node(id)
		if !ok || containsStr(n.GetPublicEndpoints(), addr) {
			continue
		}
		n.PublicEndpoints = append(n.GetPublicEndpoints(), addr)
		n.GetMeta().UpdatedAt = timestamppb.New(now)
		cmd := &clusterv1.Command{
			RequestId: ids.New(),
			Actor:     "system:disco",
			Time:      timestamppb.Now(),
			Mutation:  &clusterv1.Command_PutNode{PutNode: &clusterv1.PutNode{Node: n}},
		}
		if err := s.raft.Apply(ctx, cmd); err != nil {
			s.log.Warn("disco: fold endpoint failed", "node", id, "err", err)
		}
	}
}

func containsStr(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}
