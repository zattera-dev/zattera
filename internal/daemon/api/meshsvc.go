package api

import (
	"context"
	"log/slog"
	"sort"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/state"
)

// natKeepaliveSeconds keeps a NAT'd worker's hole to the hub open.
const natKeepaliveSeconds = 25

// MeshServer implements MeshService.WatchPeers: it streams each node the full
// WireGuard peer set it should install, rebuilding on any node change
// (hub-and-spoke, Phase A). ReportObservedEndpoint (disco) lands in T-20.
type MeshServer struct {
	clusterv1.UnimplementedMeshServiceServer
	store    *state.Store
	clock    clock.Clock
	log      *slog.Logger
	debounce time.Duration
}

// NewMeshServer builds the mesh peer-distribution service.
func NewMeshServer(store *state.Store, clk clock.Clock, log *slog.Logger) *MeshServer {
	if log == nil {
		log = slog.Default()
	}
	if clk == nil {
		clk = clock.Real{}
	}
	return &MeshServer{store: store, clock: clk, log: log, debounce: defaultAssignmentDebounce}
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
	selfNATd := len(self.GetPublicEndpoints()) == 0
	ps.HubAndSpoke = !selfControl

	for _, n := range nodes {
		if n.GetMeta().GetId() == selfID {
			continue
		}
		if n.GetWireguardPublicKey() == "" || n.GetMeshIp() == "" {
			continue // not mesh-ready yet
		}
		isControl := hasControlRole(n.GetRoles())
		if !selfControl && !isControl {
			continue // workers peer only with control nodes
		}
		peer := &clusterv1.Peer{
			NodeId:             n.GetMeta().GetId(),
			WireguardPublicKey: n.GetWireguardPublicKey(),
			MeshIp:             n.GetMeshIp(),
			Endpoints:          n.GetPublicEndpoints(),
			IsControl:          isControl,
		}
		if selfControl {
			peer.AllowedIps = []string{n.GetMeshIp() + "/32"}
		} else {
			peer.AllowedIps = []string{meshCIDR}
			if selfNATd {
				peer.PersistentKeepaliveSeconds = natKeepaliveSeconds
			}
		}
		ps.Peers = append(ps.Peers, peer)
	}
	sort.Slice(ps.Peers, func(i, j int) bool { return ps.Peers[i].GetNodeId() < ps.Peers[j].GetNodeId() })
	return ps
}
