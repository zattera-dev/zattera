package api

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
)

// punchLeadTime is how far in the future a coordinated punch is scheduled — long
// enough for the PunchCommand to reach the target and both sides to arm, short
// enough that NAT mappings stay open.
const punchLeadTime = 500 * time.Millisecond

// punchRegistry tracks the live PunchStreams (one per meshsock-capable node) so
// RequestPunch can push a "punch now" command to a target.
type punchRegistry struct {
	mu      sync.Mutex
	streams map[string]chan *clusterv1.PunchCommand // node id → its command queue
}

func newPunchRegistry() *punchRegistry {
	return &punchRegistry{streams: map[string]chan *clusterv1.PunchCommand{}}
}

// register adds a node's command queue, replacing any prior stream (a
// reconnect supersedes the old one). Returns the queue and an unregister func.
func (r *punchRegistry) register(nodeID string) (chan *clusterv1.PunchCommand, func()) {
	ch := make(chan *clusterv1.PunchCommand, 16)
	r.mu.Lock()
	if old := r.streams[nodeID]; old != nil {
		close(old)
	}
	r.streams[nodeID] = ch
	r.mu.Unlock()
	return ch, func() {
		r.mu.Lock()
		if r.streams[nodeID] == ch {
			delete(r.streams, nodeID)
		}
		r.mu.Unlock()
	}
}

// push queues a command for nodeID; ok=false when it has no live stream.
func (r *punchRegistry) push(nodeID string, cmd *clusterv1.PunchCommand) (ok bool) {
	r.mu.Lock()
	ch := r.streams[nodeID]
	r.mu.Unlock()
	if ch == nil {
		return false
	}
	select {
	case ch <- cmd:
		return true
	default:
		return false // queue full: caller treats as uncoordinated
	}
}

// PunchStream keeps a node registered as punch-capable and forwards
// PunchCommands to it until the stream closes. (T-57)
func (s *MeshServer) PunchStream(req *clusterv1.PunchStreamRequest, stream clusterv1.MeshService_PunchStreamServer) error {
	nodeID, err := s.callerNodeID(stream.Context(), req.GetNodeId())
	if err != nil {
		return err
	}
	ch, unregister := s.punch.register(nodeID)
	defer unregister()
	s.log.Info("punch stream opened", "node", nodeID)
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case cmd, ok := <-ch:
			if !ok {
				return nil // superseded by a newer stream
			}
			if err := stream.Send(cmd); err != nil {
				return err
			}
		}
	}
}

// RequestPunch coordinates a simultaneous open between the caller and a target:
// it pushes a PunchCommand to the target (over its PunchStream) and returns the
// target's endpoints + a shared punch time to the caller. coordinated=false when
// the target is not punch-capable (no live stream). (T-57)
func (s *MeshServer) RequestPunch(ctx context.Context, req *clusterv1.RequestPunchRequest) (*clusterv1.RequestPunchResponse, error) {
	caller, err := s.callerNodeID(ctx, req.GetNodeId())
	if err != nil {
		return nil, err
	}
	target := req.GetTargetNodeId()
	if target == "" || target == caller {
		return nil, status.Error(codes.InvalidArgument, "a distinct target_node_id is required")
	}

	callerEps := s.punchEndpoints(caller)
	targetEps := s.punchEndpoints(target)
	if len(targetEps) == 0 {
		return &clusterv1.RequestPunchResponse{Coordinated: false}, nil
	}

	at := timestamppb.New(s.clock.Now().Add(punchLeadTime))
	pushed := s.punch.push(target, &clusterv1.PunchCommand{
		PeerNodeId:    caller,
		PeerEndpoints: callerEps,
		PunchAt:       at,
	})
	if !pushed {
		return &clusterv1.RequestPunchResponse{Coordinated: false}, nil
	}
	return &clusterv1.RequestPunchResponse{
		Coordinated:     true,
		TargetEndpoints: targetEps,
		PunchAt:         at,
	}, nil
}

// punchEndpoints returns a node's punch candidate addresses: its durable public
// endpoints plus any fresh disco-observed reflexive address.
func (s *MeshServer) punchEndpoints(nodeID string) []string {
	var eps []string
	if n, ok := s.store.Node(nodeID); ok {
		eps = append(eps, n.GetPublicEndpoints()...)
	}
	s.mu.Lock()
	if e, ok := s.endpoints[nodeID]; ok && s.clock.Now().Sub(e.at) <= endpointTTL {
		eps = appendUnique(eps, e.addr)
	}
	s.mu.Unlock()
	return eps
}

func appendUnique(s []string, v string) []string {
	for _, x := range s {
		if x == v {
			return s
		}
	}
	return append(s, v)
}
