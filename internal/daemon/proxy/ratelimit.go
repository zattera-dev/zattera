package proxy

import (
	"net"
	"sync"
	"time"

	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// maxRateLimitKeys bounds the limiter's memory. Each tracked client costs a
// map entry plus a bucket (~64B), so the cap is ~2MB of state per node. When
// it is exceeded the limiter first drops idle buckets, then evicts arbitrary
// ones — a client whose bucket is evicted simply starts full, which errs
// toward letting traffic through rather than falsely throttling it.
const maxRateLimitKeys = 32768

// rateLimitIdle is how long a bucket must go untouched to be sweepable. A
// bucket refills to full within burst/rps seconds, so anything idle this long
// is indistinguishable from a fresh one and safe to drop.
const rateLimitIdle = 2 * time.Minute

// bucket is a token bucket refilled lazily on access.
type bucket struct {
	tokens float64
	last   time.Time
}

// rateLimiter holds per-key token buckets. It is node-local: no state is
// shared with other ingress nodes (see RateLimit in service.proto for what
// that means for the cluster-wide ceiling).
type rateLimiter struct {
	clk clock.Clock

	mu      sync.Mutex
	buckets map[string]*bucket
}

func newRateLimiter(clk clock.Clock) *rateLimiter {
	if clk == nil {
		clk = clock.Real{}
	}
	return &rateLimiter{clk: clk, buckets: map[string]*bucket{}}
}

// allow consumes one token for key and reports whether the request may
// proceed. rps must be > 0; burst <= 0 is treated as rps.
func (l *rateLimiter) allow(key string, rps, burst float64) bool {
	if rps <= 0 {
		return true
	}
	if burst <= 0 {
		burst = rps
	}
	now := l.clk.Now()

	l.mu.Lock()
	defer l.mu.Unlock()

	b, ok := l.buckets[key]
	if !ok {
		if len(l.buckets) >= maxRateLimitKeys {
			l.evictLocked(now)
		}
		// A new client starts with a full bucket: the first burst requests are
		// free, and steady-state settles at rps.
		b = &bucket{tokens: burst, last: now}
		l.buckets[key] = b
	} else if elapsed := now.Sub(b.last); elapsed > 0 {
		b.tokens += elapsed.Seconds() * rps
		if b.tokens > burst {
			b.tokens = burst
		}
	}
	b.last = now

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// evictLocked frees space in the bucket map. It sweeps idle buckets first and
// falls back to evicting arbitrary ones (Go randomizes map iteration order) if
// the sweep did not free enough. Callers must hold l.mu.
func (l *rateLimiter) evictLocked(now time.Time) {
	for k, b := range l.buckets {
		if now.Sub(b.last) >= rateLimitIdle {
			delete(l.buckets, k)
		}
	}
	// Still full: every bucket is active. Drop an eighth of them so the map
	// keeps accepting new clients instead of wedging at the cap.
	if len(l.buckets) >= maxRateLimitKeys {
		drop := maxRateLimitKeys / 8
		for k := range l.buckets {
			if drop == 0 {
				break
			}
			delete(l.buckets, k)
			drop--
		}
	}
}

// clientIP extracts the bare IP from a RemoteAddr, tolerating an absent port.
// The value is the proxy's own view of the peer, never a client-supplied
// header — X-Forwarded-For is trivially spoofable and would let any caller
// mint unlimited rate-limit identities.
func clientIP(remoteAddr string) string {
	if host, _, err := net.SplitHostPort(remoteAddr); err == nil {
		return host
	}
	return remoteAddr
}
