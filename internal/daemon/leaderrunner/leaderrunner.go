// Package leaderrunner factors the "run this loop only while we hold raft
// leadership" boilerplate shared by every leader-gated control-plane loop
// (scheduler, orchestrator, dispatcher, janitors). Each such loop used to
// reimplement the same wait-for-leadership/re-check dance; Run centralizes it so
// leadership transitions are handled one way.
package leaderrunner

import (
	"context"
	"time"

	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// Store is the slice of the raft store a leader-gated loop needs: whether this
// node currently leads, and a channel that fires on every leadership change.
type Store interface {
	IsLeader() bool
	LeaderCh() <-chan bool
}

// retryBackoff is how long Run waits before re-checking leadership when no
// LeaderCh signal arrives (defence against a missed edge).
const retryBackoff = time.Second

// Run invokes leaderLoop whenever this node holds leadership and blocks until
// ctx is canceled. leaderLoop is expected to return promptly once it observes
// leadership loss (via the store's LeaderCh or an ErrNotLeader apply); Run then
// waits for the next election and re-invokes it. leaderLoop receives ctx
// directly, so a daemon shutdown unwinds it too.
func Run(ctx context.Context, store Store, clk clock.Clock, leaderLoop func(context.Context)) {
	if clk == nil {
		clk = clock.Real{}
	}
	for {
		if ctx.Err() != nil {
			return
		}
		if !store.IsLeader() {
			select {
			case <-ctx.Done():
				return
			case <-store.LeaderCh(): // leadership changed; re-check
			case <-clk.After(retryBackoff):
			}
			continue
		}
		leaderLoop(ctx)
	}
}
