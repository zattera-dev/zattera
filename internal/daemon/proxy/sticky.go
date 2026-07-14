package proxy

import (
	"hash/fnv"
	"strconv"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
)

// stickyCookie is the affinity cookie name.
const stickyCookie = "zt_sticky"

// stickyID returns an opaque, stable id for an endpoint used as the sticky
// cookie value. It is derived from the endpoint's assignment id (stable across
// snapshots and restarts of the same replica), falling back to its address. The
// value is a hash so the cookie never leaks internal topology.
func stickyID(ep *clusterv1.Endpoint) string {
	key := ep.GetAssignmentId()
	if key == "" {
		key = ep.GetAddr()
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return strconv.FormatUint(uint64(h.Sum32()), 36)
}

// healthyByStickyID returns the current healthy endpoint matching a sticky
// cookie value, or nil if none does (drained/removed/unhealthy → re-pin).
func healthyByStickyID(eps []*clusterv1.Endpoint, id string) *clusterv1.Endpoint {
	if id == "" {
		return nil
	}
	for _, e := range eps {
		if e.GetHealthy() && stickyID(e) == id {
			return e
		}
	}
	return nil
}
