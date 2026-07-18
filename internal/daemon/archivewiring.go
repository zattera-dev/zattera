package daemon

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	"github.com/zattera-dev/zattera/internal/daemon/api"
	"github.com/zattera-dev/zattera/internal/daemon/archive"
	"github.com/zattera-dev/zattera/internal/daemon/leaderrunner"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/daemon/volumes"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

// archiveSweepTick is how often the leader flushes settled audit entries and
// events to object storage. The rings hold tens of thousands of records, so
// five minutes is far inside the window where anything could age out.
const archiveSweepTick = 5 * time.Minute

// archiveDest resolves the archive destination from the cluster's backup
// config. It returns false when archiving is switched off, no bucket is set,
// or the node is sealed — all normal states, not errors.
func archiveDest(rs *raftstore.Store, sealer secrets.Sealer) func() (volumes.ObjectStore, bool) {
	return func() (volumes.ObjectStore, bool) {
		if sealer == nil {
			return nil, false
		}
		cfg, ok := rs.State().BackupConfig()
		if !ok || !cfg.GetArchive() || cfg.GetS3Bucket() == "" {
			return nil, false
		}
		store, err := api.ObjectStoreFor(cfg, sealer)
		if err != nil {
			return nil, false
		}
		return store, true
	}
}

// archiveCursors persists the archiver's resume cursors through raft.
type archiveCursors struct {
	rs  *raftstore.Store
	clk clock.Clock
}

func (c archiveCursors) PutCursor(ctx context.Context, key string, value []byte) error {
	return c.rs.Apply(ctx, &clusterv1.Command{
		RequestId: ids.New(),
		Actor:     "system:archive",
		Time:      timestamppb.New(c.clk.Now()),
		Mutation: &clusterv1.Command_PutKv{PutKv: &clusterv1.PutKV{
			Key: key, Value: value, ExpectedVersion: -1,
		}},
	})
}

// runArchiver sweeps audit entries and events out to object storage on the
// leader (T-92). Reading them back is a query-time concern; see
// Auditor.SetArchive.
func runArchiver(ctx context.Context, rs *raftstore.Store, auditor *api.Auditor, sealer secrets.Sealer, clk clock.Clock, log *slog.Logger) {
	dest := archiveDest(rs, sealer)

	// The query path reads the archive from any control node, leader or not.
	auditor.SetArchive(func() (*archive.Reader, bool) {
		store, ok := dest()
		if !ok {
			return nil, false
		}
		return archive.NewReader(store, sealer), true
	})

	arch := archive.New(rs.State(), archiveCursors{rs: rs, clk: clk}, sealer, dest, clk, log)
	leaderrunner.Run(ctx, rs, clk, func(ctx context.Context) {
		tick := clk.NewTicker(archiveSweepTick)
		defer tick.Stop()
		for {
			arch.Sweep(ctx)
			select {
			case <-ctx.Done():
				return
			case <-rs.LeaderCh():
				if !rs.IsLeader() {
					return
				}
			case <-tick.C():
			}
		}
	})
}

// The state store is the archiver's Source; assert it here so a signature drift
// fails at compile time rather than at the first sweep.
var _ archive.Source = (*state.Store)(nil)
