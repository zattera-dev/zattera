package api

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/ca"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

// joinTokenPrefix and separator: K10<ca-hash-hex>::<secret>. The CA hash pins
// the cluster CA so a joining node can verify the control plane it dials before
// sending its CSR (k3s-style, T-17).
const (
	joinTokenPrefix = "K10"
	joinTokenSep    = "::"
)

// NodeServer implements zatterav1.NodeServiceServer.
type NodeServer struct {
	zatterav1.UnimplementedNodeServiceServer
	store *state.Store
	raft  Applier
	clock clock.Clock
	ca    *ca.CA
}

// NewNodeServer builds the node service.
func NewNodeServer(store *state.Store, raft Applier, clk clock.Clock, authority *ca.CA) *NodeServer {
	return &NodeServer{store: store, raft: raft, clock: clk, ca: authority}
}

// ListNodes returns all registered nodes.
func (s *NodeServer) ListNodes(_ context.Context, _ *emptypb.Empty) (*zatterav1.ListNodesResponse, error) {
	return &zatterav1.ListNodesResponse{Nodes: s.store.ListNodes()}, nil
}

// GetNode returns one node by id.
func (s *NodeServer) GetNode(_ context.Context, req *zatterav1.GetNodeRequest) (*zatterav1.Node, error) {
	n, ok := s.store.Node(req.GetNodeId())
	if !ok {
		return nil, status.Error(codes.NotFound, "node not found")
	}
	return n, nil
}

// CreateJoinToken mints a single-use (by default) join token pinned to the
// cluster CA. The plaintext is returned once; only its hash is stored.
func (s *NodeServer) CreateJoinToken(ctx context.Context, req *zatterav1.CreateJoinTokenRequest) (*zatterav1.CreateJoinTokenResponse, error) {
	secret, err := randomBase62(32)
	if err != nil {
		return nil, status.Error(codes.Internal, "token generation failed")
	}
	tokenStr := joinTokenPrefix + s.caHashHex() + joinTokenSep + secret

	roles := req.GetRoles()
	if len(roles) == 0 {
		roles = []zatterav1.NodeRole{zatterav1.NodeRole_NODE_ROLE_WORKER}
	}
	id, _ := IdentityFrom(ctx)
	now := s.clock.Now()
	jt := &zatterav1.JoinToken{
		Meta:            newMeta(ids.New(), now),
		SecretHash:      HashToken(secret),
		SingleUse:       req.GetSingleUse(),
		Roles:           roles,
		CreatedByUserId: id.UserID,
	}
	if d := req.GetTtl().AsDuration(); d > 0 {
		jt.ExpiresAt = timestamppb.New(now.Add(d))
	}
	if err := s.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_PutJoinToken{PutJoinToken: &clusterv1.PutJoinToken{Token: jt}}}); err != nil {
		return nil, toStatus(err)
	}
	return &zatterav1.CreateJoinTokenResponse{Token: tokenStr, Info: redactJoinToken(jt)}, nil
}

// SetNodeLabels updates a node's labels and schedulable flag.
func (s *NodeServer) SetNodeLabels(ctx context.Context, req *zatterav1.SetNodeLabelsRequest) (*zatterav1.Node, error) {
	n, ok := s.store.Node(req.GetNodeId())
	if !ok {
		return nil, status.Error(codes.NotFound, "node not found")
	}
	n = clone(n)
	if req.GetLabels() != nil {
		n.Labels = req.GetLabels()
	}
	n.Schedulable = req.GetSchedulable()
	n.GetMeta().UpdatedAt = timestamppb.New(s.clock.Now())
	if err := s.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_PutNode{PutNode: &clusterv1.PutNode{Node: n}}}); err != nil {
		return nil, toStatus(err)
	}
	return n, nil
}

// DrainNode marks a node DRAINING and unschedulable; the scheduler then
// migrates its instances away and marks it DRAINED (T-29).
func (s *NodeServer) DrainNode(ctx context.Context, req *zatterav1.DrainNodeRequest) (*zatterav1.Node, error) {
	if _, ok := s.store.Node(req.GetNodeId()); !ok {
		return nil, status.Error(codes.NotFound, "node not found")
	}
	if err := s.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_SetNodeStatus{SetNodeStatus: &clusterv1.SetNodeStatus{
		NodeId:         req.GetNodeId(),
		Status:         zatterav1.NodeStatus_NODE_STATUS_DRAINING,
		SchedulableSet: true,
		Schedulable:    false,
	}}}); err != nil {
		return nil, toStatus(err)
	}
	n, _ := s.store.Node(req.GetNodeId())
	return n, nil
}

// RemoveNode deletes a drained node (or any node with force), reaping its
// assignments and, for a control node, removing it from the raft cluster. It
// refuses to remove the last control node without force.
func (s *NodeServer) RemoveNode(ctx context.Context, req *zatterav1.RemoveNodeRequest) (*emptypb.Empty, error) {
	n, ok := s.store.Node(req.GetNodeId())
	if !ok {
		return nil, status.Error(codes.NotFound, "node not found")
	}
	if !req.GetForce() && n.GetStatus() != zatterav1.NodeStatus_NODE_STATUS_DRAINED {
		return nil, status.Error(codes.FailedPrecondition, "node is not drained; drain it first or pass force")
	}
	isControl := false
	for _, r := range n.GetRoles() {
		if r == zatterav1.NodeRole_NODE_ROLE_CONTROL {
			isControl = true
		}
	}
	if isControl && !req.GetForce() && s.controlNodeCount() <= 1 {
		return nil, status.Error(codes.FailedPrecondition, "refusing to remove the last control node without force")
	}

	// A control node leaves the raft quorum FIRST: if this fails we return an
	// error with the node record intact (retryable) rather than orphaning a raft
	// voter that no longer maps to a node. RemoveServer is idempotent, so a retry
	// after a partial removal is safe. Only *raftstore.Store implements it; a
	// nil/forwarding Applier skips (control HA disabled).
	if isControl {
		if m, ok := s.raft.(interface{ RemoveServer(string) error }); ok {
			if err := m.RemoveServer(req.GetNodeId()); err != nil {
				return nil, status.Errorf(codes.Unavailable, "remove control node from raft quorum: %v", err)
			}
		}
	}

	// Reap the node's assignments so the scheduler re-places their replicas.
	var reap []string
	for _, a := range s.store.ListAssignmentsByNode(req.GetNodeId()) {
		reap = append(reap, a.GetMeta().GetId())
	}
	if len(reap) > 0 {
		if err := s.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_DeleteAssignments{DeleteAssignments: &clusterv1.DeleteAssignments{AssignmentIds: reap}}}); err != nil {
			return nil, toStatus(err)
		}
	}
	if err := s.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_DeleteNode{DeleteNode: &clusterv1.DeleteByID{Id: req.GetNodeId()}}}); err != nil {
		return nil, toStatus(err)
	}
	return &emptypb.Empty{}, nil
}

// controlNodeCount counts registered control nodes.
func (s *NodeServer) controlNodeCount() int {
	n := 0
	for _, node := range s.store.ListNodes() {
		for _, r := range node.GetRoles() {
			if r == zatterav1.NodeRole_NODE_ROLE_CONTROL {
				n++
			}
		}
	}
	return n
}

// caHashHex is the hex SHA-256 over the CA certificate DER bytes.
func (s *NodeServer) caHashHex() string {
	sum := sha256.Sum256(s.ca.Certificate().Raw)
	return hex.EncodeToString(sum[:])
}

func (s *NodeServer) apply(ctx context.Context, cmd *clusterv1.Command) error {
	id, _ := IdentityFrom(ctx)
	cmd.RequestId = ids.New()
	cmd.Actor = id.Actor()
	cmd.Time = timestamppb.Now()
	return s.raft.Apply(ctx, cmd)
}

func redactJoinToken(t *zatterav1.JoinToken) *zatterav1.JoinToken {
	c := clone(t)
	c.SecretHash = ""
	return c
}

// randomBase62 returns n random bytes encoded base62.
func randomBase62(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base62Encode(b), nil
}
