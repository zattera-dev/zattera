package mesh

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	"google.golang.org/grpc"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

const (
	peerSyncMinBackoff = 1 * time.Second
	peerSyncMaxBackoff = 30 * time.Second
)

// PeerSyncConfig configures the node-side peer synchronizer.
type PeerSyncConfig struct {
	NodeID  string
	Manager Manager
	Clock   clock.Clock
	Logger  *slog.Logger
	// Dial opens a connection to a control node's MeshService. Called on every
	// (re)connect; the returned closer is invoked when the stream ends.
	Dial func(ctx context.Context) (grpc.ClientConnInterface, func() error, error)
	// OnPeers, when set, is called with every PeerSet received (before it is
	// applied). A worker uses it to keep its control-node failover set current.
	OnPeers func(*clusterv1.PeerSet)
}

// RunPeerSync keeps a WatchPeers stream open and applies every pushed PeerSet to
// the mesh device, reconnecting with backoff. Blocks until ctx is canceled.
func RunPeerSync(ctx context.Context, cfg PeerSyncConfig) error {
	clk := cfg.Clock
	if clk == nil {
		clk = clock.Real{}
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}

	backoff := peerSyncMinBackoff
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		start := clk.Now()
		if err := watchOnce(ctx, cfg, log); err != nil && ctx.Err() == nil {
			log.Warn("peersync stream ended", "node", cfg.NodeID, "err", err)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if clk.Now().Sub(start) >= peerSyncMaxBackoff {
			backoff = peerSyncMinBackoff
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-clk.After(backoff):
		}
		if backoff < peerSyncMaxBackoff {
			backoff *= 2
			if backoff > peerSyncMaxBackoff {
				backoff = peerSyncMaxBackoff
			}
		}
	}
}

func watchOnce(ctx context.Context, cfg PeerSyncConfig, log *slog.Logger) error {
	conn, closer, err := cfg.Dial(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = closer() }()

	stream, err := clusterv1.NewMeshServiceClient(conn).WatchPeers(ctx, &clusterv1.WatchPeersRequest{NodeId: cfg.NodeID})
	if err != nil {
		return err
	}
	for {
		ps, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || ctx.Err() != nil {
				return nil
			}
			return err
		}
		if cfg.OnPeers != nil {
			cfg.OnPeers(ps)
		}
		if err := cfg.Manager.ApplyPeers(ctx, ps); err != nil {
			log.Warn("peersync apply failed", "node", cfg.NodeID, "err", err)
		}
	}
}
