package api

import (
	"context"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/state"
)

// KeyServer implements KeyService: it hands the cluster data key to a control
// node that restarted sealed (T-112), so a reboot does not leave a node unable
// to touch secrets until an operator intervenes.
//
// Trust model: the transport already proves the caller holds a cluster-signed
// node certificate (reqNode in the policy table). That alone is not enough —
// every worker has one too — so this additionally requires the calling node to
// carry the control role in replicated state. Handing the key only to nodes
// that were already entitled to it at join time keeps the blast radius of a
// stolen worker certificate unchanged.
type KeyServer struct {
	clusterv1.UnimplementedKeyServiceServer
	store *state.Store
	vault *secrets.Vault
	log   *slog.Logger
}

// NewKeyServer builds the key handover service.
func NewKeyServer(store *state.Store, vault *secrets.Vault, log *slog.Logger) *KeyServer {
	if log == nil {
		log = slog.Default()
	}
	return &KeyServer{store: store, vault: vault, log: log}
}

// FetchDataKey returns the cluster data key to an enrolled control node.
func (s *KeyServer) FetchDataKey(ctx context.Context, _ *clusterv1.FetchDataKeyRequest) (*clusterv1.FetchDataKeyResponse, error) {
	nodeID := nodeIDFromPeer(ctx)
	if nodeID == "" {
		return nil, status.Error(codes.PermissionDenied, "node identity required")
	}
	n, found := s.store.Node(nodeID)
	if !found {
		return nil, status.Error(codes.PermissionDenied, "unknown node")
	}
	if !hasControlRole(n.GetRoles()) {
		// A worker's certificate must not be a path to the cluster data key.
		s.log.Warn("data key requested by a non-control node", "node", nodeID)
		return nil, status.Error(codes.PermissionDenied, "node does not hold the control role")
	}
	if !s.vault.Unsealed() {
		return nil, status.Error(codes.FailedPrecondition, "this node is sealed and cannot hand out the data key")
	}
	s.log.Info("handed the cluster data key to a restarting control node", "node", nodeID)
	return &clusterv1.FetchDataKeyResponse{
		DataKey:        s.vault.DataKey(),
		DataKeyVersion: s.vault.KeyVersion(),
	}, nil
}
