package daemon

import (
	"context"
	"time"

	"github.com/zattera-dev/zattera/internal/daemon/github"
	"github.com/zattera-dev/zattera/internal/daemon/leaderrunner"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// previewJanitorTick is how often the leader looks for expired preview
// environments. TTLs are days, so an hourly sweep is plenty.
const previewJanitorTick = time.Hour

// runPreviewJanitor deletes expired preview environments on the leader (T-75).
// Deleting the environment is enough: the scheduler's orphan reconciler stops
// and removes its containers, and the route builder drops its hostname.
func runPreviewJanitor(ctx context.Context, rs *raftstore.Store, previews *github.Previews, clk clock.Clock) {
	leaderrunner.Run(ctx, rs, clk, func(ctx context.Context) {
		tick := clk.NewTicker(previewJanitorTick)
		defer tick.Stop()
		for {
			previews.SweepExpired(ctx)
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
