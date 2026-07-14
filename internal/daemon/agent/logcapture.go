package agent

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/zattera-dev/zattera/internal/daemon/logstore"
	"github.com/zattera-dev/zattera/internal/daemon/runtime"
)

// LogCapture follows the logs of every managed service container and appends
// them to the node-local logstore keyed by assignment id (T-54). The control
// plane reads them back via AgentLocalService.QueryLogs.
type LogCapture struct {
	rt    runtime.ContainerRuntime
	store *logstore.Segmented
	log   *slog.Logger

	mu        sync.Mutex
	following map[string]context.CancelFunc // assignment id → stop
}

// NewLogCapture builds a capture over a runtime + logstore.
func NewLogCapture(rt runtime.ContainerRuntime, store *logstore.Segmented, log *slog.Logger) *LogCapture {
	if log == nil {
		log = slog.Default()
	}
	return &LogCapture{rt: rt, store: store, log: log, following: map[string]context.CancelFunc{}}
}

// Run polls the owned service containers and keeps one follower per assignment,
// starting followers for new containers and stopping them for gone ones.
func (c *LogCapture) Run(ctx context.Context) {
	tick := time.NewTicker(3 * time.Second)
	defer tick.Stop()
	for {
		c.sync(ctx)
		select {
		case <-ctx.Done():
			c.stopAll()
			return
		case <-tick.C:
		}
	}
}

func (c *LogCapture) sync(ctx context.Context) {
	infos, err := c.rt.ListContainers(ctx, map[string]string{
		runtime.ManagedLabel: "true",
		runtime.LabelRole:    "service",
	})
	if err != nil {
		return
	}
	live := map[string]string{} // assignment id → container id
	for _, in := range infos {
		if aid := in.Labels[runtime.LabelAssignmentID]; aid != "" && in.Running {
			live[aid] = in.ID
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	// Start followers for new containers.
	for aid, cid := range live {
		if _, ok := c.following[aid]; ok {
			continue
		}
		fctx, cancel := context.WithCancel(ctx)
		c.following[aid] = cancel
		go c.follow(fctx, aid, cid)
	}
	// Stop followers whose container is gone.
	for aid, cancel := range c.following {
		if _, ok := live[aid]; !ok {
			cancel()
			delete(c.following, aid)
		}
	}
}

// follow streams one container's logs into the logstore until ctx is canceled.
func (c *LogCapture) follow(ctx context.Context, assignID, containerID string) {
	ch, err := c.rt.Logs(ctx, containerID, runtime.LogsOptions{Follow: true})
	if err != nil {
		return
	}
	stream := logstore.StreamID(assignID)
	for {
		select {
		case <-ctx.Done():
			return
		case entry, ok := <-ch:
			if !ok {
				return
			}
			_ = c.store.Append(stream, []logstore.Entry{{
				Time: entry.Time, Stderr: entry.Stderr, Line: entry.Line,
			}})
		}
	}
}

func (c *LogCapture) stopAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for aid, cancel := range c.following {
		cancel()
		delete(c.following, aid)
	}
}
