package api

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/ca"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

// meshIPRetries bounds the re-scan loop when a concurrent join races for the
// same mesh IP (Join runs on the leader, so this is defence in depth).
const meshIPRetries = 8

// JoinConfig carries the control-plane facts a joining node needs to come up.
type JoinConfig struct {
	MeshEnabled bool
	// ControlGRPCAddr is where the joined node opens its AgentSync stream (the
	// control node's mesh IP:port, or 127.0.0.1:port with mesh disabled).
	ControlGRPCAddr string
	// RegistryAddr is the embedded registry endpoint for image pulls.
	RegistryAddr string
	// RaftPort is the port control nodes bind for the raft transport (e.g.
	// "7480"). A joining control node's raft address is meshIP:RaftPort. Empty
	// disables control-node raft handover.
	RaftPort string
}

// JoinServer implements JoinService: token-authenticated node enrollment. It is
// reachable over TLS without mTLS — the join token IS the credential, verified
// in the handler.
type JoinServer struct {
	clusterv1.UnimplementedJoinServiceServer
	store   *state.Store
	raft    Applier
	clock   clock.Clock
	ca      *ca.CA
	keyring *secrets.Keyring
	cfg     JoinConfig
	log     *slog.Logger
}

// NewJoinServer builds the join service. keyring is the cluster data key (may be
// nil on a node that never bootstrapped/unsealed); it is handed to joining
// control nodes so they come up already unsealed (T-55).
func NewJoinServer(store *state.Store, raft Applier, clk clock.Clock, authority *ca.CA, keyring *secrets.Keyring, cfg JoinConfig, log *slog.Logger) *JoinServer {
	if log == nil {
		log = slog.Default()
	}
	return &JoinServer{store: store, raft: raft, clock: clk, ca: authority, keyring: keyring, cfg: cfg, log: log}
}

// Join enrolls a node: it verifies + consumes the token, allocates a mesh IP,
// signs the node's CSR, registers the node, and issues a registry credential.
func (s *JoinServer) Join(ctx context.Context, req *clusterv1.JoinRequest) (*clusterv1.JoinResponse, error) {
	tok, err := s.verifyToken(req.GetTokenSecret())
	if err != nil {
		return nil, err
	}
	if len(req.GetCsrPem()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "a certificate signing request is required")
	}

	// Consume the token first: its apply handler rejects a double-use of a
	// single-use token, closing the replay window before we mint anything.
	if err := s.apply(ctx, &clusterv1.Command{
		Mutation: &clusterv1.Command_ConsumeJoinToken{ConsumeJoinToken: &clusterv1.ConsumeJoinToken{TokenId: tok.GetMeta().GetId()}},
	}); err != nil {
		return nil, status.Error(codes.PermissionDenied, "join token could not be consumed (already used?)")
	}

	roles := tok.GetRoles()
	if len(roles) == 0 {
		roles = []zatterav1.NodeRole{zatterav1.NodeRole_NODE_ROLE_WORKER}
	}

	// A restarting node re-enrolls under its existing id: resume that record
	// (keeping its mesh IP and roles) instead of registering a duplicate.
	nodeID := ids.New()
	if reqID := req.GetExistingNodeId(); reqID != "" {
		if existing, ok := s.store.Node(reqID); ok {
			nodeID = reqID
			roles = existing.GetRoles()
			s.log.Info("node re-joining under existing identity", "node", nodeID)
		}
	}

	meshIP, err := s.registerNode(ctx, nodeID, roles, req)
	if err != nil {
		return nil, err
	}

	var meshParsed net.IP
	if meshIP != "" {
		meshParsed = net.ParseIP(meshIP)
	}
	certPEM, err := s.ca.SignCSR(req.GetCsrPem(), nodeID, meshParsed, ca.NodeCertTTL)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "sign csr: %v", err)
	}

	regUser := "node-" + nodeID
	regPass, err := randomBase62(24)
	if err != nil {
		return nil, status.Error(codes.Internal, "registry credential generation failed")
	}
	if err := s.apply(ctx, &clusterv1.Command{
		Mutation: &clusterv1.Command_PutKv{PutKv: &clusterv1.PutKV{
			Key:             "registry/creds/" + nodeID,
			Value:           []byte(HashToken(regPass)),
			ExpectedVersion: -1,
		}},
	}); err != nil {
		return nil, toStatus(err)
	}

	resp := &clusterv1.JoinResponse{
		NodeId:           nodeID,
		MeshIp:           meshIP,
		CaCertPem:        s.ca.CABundlePEM(),
		NodeCertPem:      certPEM,
		InitialPeers:     s.buildInitialPeers(),
		ControlGrpcAddr:  s.cfg.ControlGRPCAddr,
		RegistryAddr:     s.cfg.RegistryAddr,
		RegistryUsername: regUser,
		RegistryPassword: regPass,
		MeshEnabled:      s.cfg.MeshEnabled,
		Roles:            roles,
	}

	// Control-node handover (T-55): enroll the node in raft and hand it the
	// cluster secrets so it comes up as a fully autonomous control node. Requires
	// the mesh (raft binds the node's mesh IP) — a control token joined
	// mesh-disabled falls back to worker-only.
	if hasControlRole(roles) {
		if err := s.handoverControl(nodeID, meshIP, resp); err != nil {
			return nil, err
		}
	}

	s.log.Info("node joined", "node", nodeID, "mesh_ip", meshIP, "roles", roles)
	return resp, nil
}

// handoverControl fills the response's cluster-secret handover fields for a
// joining control node: the data key, the root CA private key, and the raft
// address it must bind — so it can come up fully autonomous and unsealed
// (spec §2.10). It requires the mesh (raft binds the node's mesh IP); a control
// token joined mesh-disabled falls back to worker-only.
//
// Raft enrollment (AddVoter) is intentionally NOT performed here: the leader
// must not add a voter before that node's raft transport is listening, or an
// unreachable voter stalls commits. Enrollment is triggered by the node once it
// is up (see raftEnroll / T-55b) — a hook that lands with the daemon
// join-as-control bring-up, which depends on multi-control mesh addressing.
func (s *JoinServer) handoverControl(nodeID, meshIP string, resp *clusterv1.JoinResponse) error {
	if !s.cfg.MeshEnabled || meshIP == "" || s.cfg.RaftPort == "" {
		s.log.Warn("control-role join without a mesh; node will run worker-only (control HA needs the mesh)", "node", nodeID)
		return nil
	}
	caKeyPEM, err := s.ca.PrivateKeyPEM()
	if err != nil {
		return status.Errorf(codes.Internal, "export ca key: %v", err)
	}
	if s.keyring == nil {
		// Without the data key the new control node cannot decrypt secrets; this
		// means the handling node itself is not unsealed — a config/ordering bug.
		return status.Error(codes.FailedPrecondition, "cluster is not unsealed; cannot hand a data key to a joining control node")
	}
	resp.DataKey = s.keyring.DataKey()
	resp.DataKeyVersion = s.keyring.KeyVersion()
	resp.CaKeyPem = caKeyPEM
	resp.RaftBindAddr = net.JoinHostPort(meshIP, s.cfg.RaftPort)
	return nil
}

// verifyToken matches the presented secret against an unexpired, unused join
// token. Hash equality is not secret, so no constant-time compare is needed.
func (s *JoinServer) verifyToken(secret string) (*zatterav1.JoinToken, error) {
	if secret == "" {
		return nil, status.Error(codes.PermissionDenied, "a join token is required")
	}
	hash := HashToken(secret)
	now := s.clock.Now()
	for _, t := range s.store.ListJoinTokens() {
		if t.GetSecretHash() != hash {
			continue
		}
		if t.GetSingleUse() && t.GetUsed() {
			return nil, status.Error(codes.PermissionDenied, "join token already used")
		}
		if exp := t.GetExpiresAt(); exp != nil && now.After(exp.AsTime()) {
			return nil, status.Error(codes.PermissionDenied, "join token expired")
		}
		return t, nil
	}
	return nil, status.Error(codes.PermissionDenied, "invalid join token")
}

// registerNode allocates a mesh IP (when the mesh is enabled) and records the
// node, re-scanning on the (leader-serialized, thus rare) IP collision.
func (s *JoinServer) registerNode(ctx context.Context, nodeID string, roles []zatterav1.NodeRole, req *clusterv1.JoinRequest) (string, error) {
	labels := map[string]string{}
	for k, v := range req.GetDetectedLabels() {
		labels[k] = v
	}
	if req.GetOsArch() != "" {
		labels["zattera.dev/os-arch"] = req.GetOsArch()
	}

	isControl := hasControlRole(roles)
	now := timestamppb.New(s.clock.Now())

	// Resume: a returning node keeps its existing mesh IP and creation time; no
	// new allocation, no duplicate record.
	if existing, ok := s.store.Node(nodeID); ok {
		node := nodeRecord(nodeID, roles, req, labels, existing.GetMeshIp(), existing.GetMeta().GetCreatedAt(), now)
		if err := s.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_PutNode{PutNode: &clusterv1.PutNode{Node: node}}}); err != nil {
			return "", toStatus(err)
		}
		return existing.GetMeshIp(), nil
	}

	for attempt := 0; attempt < meshIPRetries; attempt++ {
		meshIP := ""
		if s.cfg.MeshEnabled {
			ip, err := allocateMeshIP(s.store.ListNodes(), isControl)
			if err != nil {
				return "", status.Error(codes.ResourceExhausted, err.Error())
			}
			meshIP = ip
		}
		node := nodeRecord(nodeID, roles, req, labels, meshIP, now, now)
		if err := s.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_PutNode{PutNode: &clusterv1.PutNode{Node: node}}}); err != nil {
			return "", toStatus(err)
		}
		if meshIP == "" || !meshIPTaken(s.store.ListNodes(), nodeID, meshIP) {
			return meshIP, nil
		}
		// Another node grabbed this IP between our scan and apply; try again.
		s.log.Warn("mesh ip collision on join; retrying", "node", nodeID, "mesh_ip", meshIP)
	}
	return "", status.Error(codes.Aborted, "could not allocate a unique mesh IP; retry")
}

// nodeRecord builds the node state entry for a (re)joining node.
func nodeRecord(nodeID string, roles []zatterav1.NodeRole, req *clusterv1.JoinRequest, labels map[string]string, meshIP string, createdAt, updatedAt *timestamppb.Timestamp) *zatterav1.Node {
	return &zatterav1.Node{
		Meta:               &zatterav1.Meta{Id: nodeID, CreatedAt: createdAt, UpdatedAt: updatedAt},
		Name:               req.GetNodeName(),
		Roles:              roles,
		Labels:             labels,
		MeshIp:             meshIP,
		WireguardPublicKey: req.GetWireguardPublicKey(),
		PublicEndpoints:    req.GetPublicEndpoints(),
		Capacity:           req.GetCapacity(),
		CapacityDiskMb:     req.GetCapacityDiskMb(),
		Status:             zatterav1.NodeStatus_NODE_STATUS_ALIVE,
		Schedulable:        true,
		BinaryVersion:      req.GetBinaryVersion(),
		OsArch:             req.GetOsArch(),
	}
}

// buildInitialPeers returns the control-node peers a joining worker should dial
// (hub-and-spoke). Empty when the mesh is disabled; T-19 refines distribution.
func (s *JoinServer) buildInitialPeers() *clusterv1.PeerSet {
	if !s.cfg.MeshEnabled {
		return nil
	}
	var peers []*clusterv1.Peer
	for _, n := range s.store.ListNodes() {
		if !hasControlRole(n.GetRoles()) || n.GetWireguardPublicKey() == "" {
			continue
		}
		peers = append(peers, &clusterv1.Peer{
			NodeId:                     n.GetMeta().GetId(),
			WireguardPublicKey:         n.GetWireguardPublicKey(),
			MeshIp:                     n.GetMeshIp(),
			Endpoints:                  n.GetPublicEndpoints(),
			PersistentKeepaliveSeconds: 25,
			IsControl:                  true,
			AllowedIps:                 []string{meshCIDR},
		})
	}
	return &clusterv1.PeerSet{Version: s.store.Version(), Peers: peers, HubAndSpoke: true}
}

func (s *JoinServer) apply(ctx context.Context, cmd *clusterv1.Command) error {
	cmd.RequestId = ids.New()
	cmd.Actor = "system:join"
	cmd.Time = timestamppb.Now()
	return s.raft.Apply(ctx, cmd)
}

// --- mesh IP allocation ---------------------------------------------------

const meshCIDR = "10.90.0.0/16"

// allocateMeshIP returns the next free 10.90.0.0/16 address: control nodes take
// low 10.90.0.x addresses, workers climb from 10.90.1.1. Deleted nodes' IPs are
// not reused yet (tombstones land in M2).
func allocateMeshIP(nodes []*zatterav1.Node, isControl bool) (string, error) {
	used := map[string]bool{}
	for _, n := range nodes {
		if ip := n.GetMeshIp(); ip != "" {
			used[ip] = true
		}
	}
	if isControl {
		for h := 2; h <= 254; h++ {
			ip := fmt.Sprintf("10.90.0.%d", h)
			if !used[ip] {
				return ip, nil
			}
		}
		return "", fmt.Errorf("mesh: no free control address in %s", meshCIDR)
	}
	for b := 1; b <= 255; b++ {
		for c := 1; c <= 254; c++ {
			ip := fmt.Sprintf("10.90.%d.%d", b, c)
			if !used[ip] {
				return ip, nil
			}
		}
	}
	return "", fmt.Errorf("mesh: no free worker address in %s", meshCIDR)
}

// meshIPTaken reports whether a node other than self already holds meshIP.
func meshIPTaken(nodes []*zatterav1.Node, selfID, meshIP string) bool {
	for _, n := range nodes {
		if n.GetMeta().GetId() != selfID && n.GetMeshIp() == meshIP {
			return true
		}
	}
	return false
}

// hasControlRole reports whether the node carries the control-plane role.
func hasControlRole(roles []zatterav1.NodeRole) bool {
	for _, r := range roles {
		if r == zatterav1.NodeRole_NODE_ROLE_CONTROL {
			return true
		}
	}
	return false
}
