// Package proxy hosts the embedded L7/L4 proxy (tasks T-41..T-48). This file
// freezes the RouteSource contract between the route-table client and the
// proxy/DNS/VIP consumers.
package proxy

import (
	"context"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
)

// RouteSource provides the current route table and change notifications.
// Implementations: the RouteStream gRPC client with disk cache (T-42), a
// static source for tests, and a direct in-process source on control nodes.
type RouteSource interface {
	// Current returns the latest snapshot (never nil; may be an empty
	// snapshot with Version 0 before the first sync).
	Current() *clusterv1.RouteSnapshot
	// Updates returns a channel receiving each new snapshot. The channel is
	// closed when ctx is canceled. Slow consumers may miss intermediate
	// versions but always eventually receive the latest.
	Updates(ctx context.Context) <-chan *clusterv1.RouteSnapshot
}

// StaticRouteSource serves a fixed snapshot (tests, and the bootstrap window
// before the first stream sync).
type StaticRouteSource struct {
	Snapshot *clusterv1.RouteSnapshot
}

func (s *StaticRouteSource) Current() *clusterv1.RouteSnapshot {
	if s.Snapshot == nil {
		return &clusterv1.RouteSnapshot{}
	}
	return s.Snapshot
}

func (s *StaticRouteSource) Updates(ctx context.Context) <-chan *clusterv1.RouteSnapshot {
	ch := make(chan *clusterv1.RouteSnapshot, 1)
	ch <- s.Current()
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch
}
