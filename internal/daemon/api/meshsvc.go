package api

import (
	"context"
	"log/slog"
	"net/netip"
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

// MeshsockLabel marks a node that runs the meshsock datapath (T-57): such
// nodes are paired directly by the peer builder regardless of public
// reachability, because meshsock handles NAT traversal (punch + relay).
const MeshsockLabel = "zattera.dev/meshsock"

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

// buildPeerSet computes the WireGuard peers for one node:
//   - a worker routes the whole mesh (allowed_ips = 10.90.0.0/16) through ONE
//     active control hub (see activeHubID) and holds a direct /32 to every other
//     control node, so their raft/API IPs stay reachable and their tunnels stay
//     warm for instant failover. When the active hub is marked DOWN, this
//     recomputes and WatchPeers pushes the /16 onto the next live hub (T-55c). A
//     NAT'd worker (no public endpoint) keeps a 25s keepalive to every hub so the
//     standby holes stay open.
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
	ps := &clusterv1.PeerSet{Version: s.store.Version(), LeaderNodeId: leaderNodeID(s.raft)}
	if self == nil {
		return ps
	}
	selfControl := hasControlRole(self.GetRoles())
	selfHasEndpoint := len(self.GetPublicEndpoints()) > 0
	selfMeshsock := self.GetLabels()[MeshsockLabel] == "true"
	ps.HubAndSpoke = !selfControl
	hubID := activeHubID(nodes) // the control node a worker routes the /16 through

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
			RelayCapable:       isControl, // control nodes run the DERP-lite relay
		}
		switch {
		case selfControl:
			// Control sees every node with a /32.
			peer.AllowedIps = []string{n.GetMeshIp() + "/32"}
		case isControl:
			// Worker → control. Exactly one control node — the active hub — carries
			// the whole-mesh /16; the rest get a direct /32 (a warm standby route
			// that also keeps their own IP reachable). WireGuard can only route a
			// prefix to one peer, so this is what makes multi-control failover work.
			if n.GetMeta().GetId() == hubID {
				peer.AllowedIps = []string{meshCIDR}
			} else {
				peer.AllowedIps = []string{n.GetMeshIp() + "/32"}
			}
			if !selfHasEndpoint {
				peer.PersistentKeepaliveSeconds = natKeepaliveSeconds
			}
		case selfMeshsock && n.GetLabels()[MeshsockLabel] == "true":
			// Worker → worker over meshsock (T-57): pair directly even with no
			// public endpoint — the bind punches / relays to reach it. The /32
			// beats the hub /16 so meshsock owns worker↔worker traffic.
			peer.AllowedIps = []string{n.GetMeshIp() + "/32"}
			peer.PersistentKeepaliveSeconds = natKeepaliveSeconds
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

// leaderNodeID returns the current raft leader's node id via the optional
// LeaderAddr method (raftstore.Store implements it), or "" when unknown / not
// exposed (unit tests pass a bare Applier or nil).
func leaderNodeID(a Applier) string {
	if lr, ok := a.(interface{ LeaderAddr() (string, string) }); ok {
		_, id := lr.LeaderAddr()
		return id
	}
	return ""
}

// activeHubID picks the control node a worker routes the whole mesh (/16)
// through: the ALIVE, mesh-ready control node with the LOWEST mesh IP. Every
// worker computes the same hub deterministically, and because it is health-
// driven the choice climbs to the next live control node the moment the active
// one is marked DOWN (gossip/heartbeat → SetNodeStatus → a WatchPeers re-push),
// which is what makes multi-control hub failover work. Selecting by mesh IP
// keeps the bootstrap node (10.90.0.1) the default hub — matching the historical
// single-hub behaviour — with failover climbing to .2, .3, … Falls back to the
// lowest mesh IP among all controls when none are ALIVE (a maybe-dead default
// route beats none). Returns "" when no control node is mesh-ready.
func activeHubID(nodes []*zatterav1.Node) string {
	activeID, activeIP := "", netip.Addr{}
	fallbackID, fallbackIP := "", netip.Addr{}
	lower := func(a, b netip.Addr) bool { return !b.IsValid() || a.Compare(b) < 0 }
	for _, n := range nodes {
		if !hasControlRole(n.GetRoles()) || n.GetWireguardPublicKey() == "" || n.GetMeshIp() == "" {
			continue
		}
		ip, err := netip.ParseAddr(n.GetMeshIp())
		if err != nil {
			continue
		}
		id := n.GetMeta().GetId()
		if lower(ip, fallbackIP) {
			fallbackID, fallbackIP = id, ip
		}
		if n.GetStatus() == zatterav1.NodeStatus_NODE_STATUS_ALIVE && lower(ip, activeIP) {
			activeID, activeIP = id, ip
		}
	}
	if activeID != "" {
		return activeID
	}
	return fallbackID
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
