package proxy

import (
	"sync"
	"sync/atomic"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
)

// balancer load-balances requests across a route's endpoints using
// power-of-two-choices (P2C) over live in-flight counters, preferring a
// node-local endpoint when the two candidates are equally loaded.
type balancer struct {
	local string // this node's id

	mu       sync.Mutex
	inflight map[string]*int64
}

func newBalancer(local string) *balancer {
	return &balancer{local: local, inflight: map[string]*int64{}}
}

func (b *balancer) counter(addr string) *int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	c, ok := b.inflight[addr]
	if !ok {
		c = new(int64)
		b.inflight[addr] = c
	}
	return c
}

func (b *balancer) load(addr string) int64 { return atomic.LoadInt64(b.counter(addr)) }

// acquire increments an endpoint's in-flight counter and returns a release func
// (call in a defer so a panic can't leak the counter).
func (b *balancer) acquire(addr string) func() {
	c := b.counter(addr)
	atomic.AddInt64(c, 1)
	return func() { atomic.AddInt64(c, -1) }
}

// pick chooses a healthy endpoint via P2C. rnd(n) returns a value in [0,n);
// it is injected for deterministic tests. Returns nil when none are healthy.
func (b *balancer) pick(eps []*clusterv1.Endpoint, rnd func(n int) int) *clusterv1.Endpoint {
	var healthy []*clusterv1.Endpoint
	for _, e := range eps {
		if e.GetHealthy() {
			healthy = append(healthy, e)
		}
	}
	switch len(healthy) {
	case 0:
		return nil
	case 1:
		return healthy[0]
	}
	i := rnd(len(healthy))
	j := rnd(len(healthy) - 1)
	if j >= i { // make the two picks distinct
		j++
	}
	a, c := healthy[i], healthy[j]
	la, lc := b.load(a.GetAddr()), b.load(c.GetAddr())
	if la != lc {
		if la < lc {
			return a
		}
		return c
	}
	// Tie on load → prefer the node-local endpoint.
	if a.GetNodeId() == b.local && c.GetNodeId() != b.local {
		return a
	}
	if c.GetNodeId() == b.local && a.GetNodeId() != b.local {
		return c
	}
	return a
}
