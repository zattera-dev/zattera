package proxy

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

func TestRateLimiterBurstThenRefill(t *testing.T) {
	clk := clock.NewFake()
	l := newRateLimiter(clk)

	// A fresh key starts with a full bucket: burst requests pass, the next
	// one is shed.
	for i := 0; i < 3; i++ {
		if !l.allow("k", 2, 3) {
			t.Fatalf("request %d denied while bucket should still hold tokens", i)
		}
	}
	if l.allow("k", 2, 3) {
		t.Fatal("request allowed after the burst was exhausted")
	}

	// At 2 rps, half a second buys exactly one token.
	clk.Advance(500 * time.Millisecond)
	if !l.allow("k", 2, 3) {
		t.Fatal("request denied after the bucket refilled by one token")
	}
	if l.allow("k", 2, 3) {
		t.Fatal("request allowed with an empty bucket")
	}
}

func TestRateLimiterRefillCapsAtBurst(t *testing.T) {
	clk := clock.NewFake()
	l := newRateLimiter(clk)

	if !l.allow("k", 10, 10) {
		t.Fatal("first request denied")
	}
	// A long idle period must not accumulate credit beyond burst.
	clk.Advance(time.Hour)
	for i := 0; i < 10; i++ {
		if !l.allow("k", 10, 10) {
			t.Fatalf("request %d denied while the bucket should be full", i)
		}
	}
	if l.allow("k", 10, 10) {
		t.Fatal("bucket accumulated more than burst tokens over an idle hour")
	}
}

func TestRateLimiterKeysAreIndependent(t *testing.T) {
	l := newRateLimiter(clock.NewFake())
	if !l.allow("a", 1, 1) {
		t.Fatal("first request for a denied")
	}
	if l.allow("a", 1, 1) {
		t.Fatal("second request for a should be shed")
	}
	if !l.allow("b", 1, 1) {
		t.Fatal("b was throttled by a's bucket")
	}
}

func TestRateLimiterDisabledWhenRPSZero(t *testing.T) {
	l := newRateLimiter(clock.NewFake())
	for i := 0; i < 100; i++ {
		if !l.allow("k", 0, 0) {
			t.Fatal("rps=0 must not limit anything")
		}
	}
	if len(l.buckets) != 0 {
		t.Fatalf("disabled limiter allocated %d buckets", len(l.buckets))
	}
}

func TestRateLimiterBurstDefaultsToRPS(t *testing.T) {
	l := newRateLimiter(clock.NewFake())
	for i := 0; i < 5; i++ {
		if !l.allow("k", 5, 0) {
			t.Fatalf("request %d denied; burst should default to rps=5", i)
		}
	}
	if l.allow("k", 5, 0) {
		t.Fatal("request allowed past the defaulted burst")
	}
}

func TestRateLimiterEvictsWhenFull(t *testing.T) {
	clk := clock.NewFake()
	l := newRateLimiter(clk)
	for i := 0; i < maxRateLimitKeys; i++ {
		l.allow(fmt.Sprintf("k%d", i), 1, 1)
	}
	if len(l.buckets) != maxRateLimitKeys {
		t.Fatalf("buckets = %d, want %d", len(l.buckets), maxRateLimitKeys)
	}
	// Every bucket is idle past the sweep threshold, so the sweep alone frees
	// the map and the new key still lands.
	clk.Advance(rateLimitIdle + time.Second)
	l.allow("fresh", 1, 1)
	if len(l.buckets) > maxRateLimitKeys {
		t.Fatalf("buckets = %d exceeds the cap", len(l.buckets))
	}
	if _, ok := l.buckets["fresh"]; !ok {
		t.Fatal("new key was not tracked after eviction")
	}
}

func TestRateLimiterEvictsWhenAllBucketsActive(t *testing.T) {
	l := newRateLimiter(clock.NewFake())
	// No time advances, so nothing is sweepable and the arbitrary-eviction
	// fallback has to keep the map bounded.
	for i := 0; i < maxRateLimitKeys+100; i++ {
		l.allow(fmt.Sprintf("k%d", i), 1, 1)
	}
	if len(l.buckets) > maxRateLimitKeys {
		t.Fatalf("buckets = %d exceeds the cap with all buckets active", len(l.buckets))
	}
}

func TestClientIP(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"192.0.2.1:1234", "192.0.2.1"},
		{"192.0.2.1", "192.0.2.1"},
		{"[2001:db8::1]:443", "2001:db8::1"},
	} {
		if got := clientIP(tc.in); got != tc.want {
			t.Errorf("clientIP(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestL7RateLimit429 exercises the full proxy path: the route's limit sheds
// excess requests with 429 + Retry-After and never reaches the backend.
func TestL7RateLimit429(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = io.WriteString(w, "ok")
	}))
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")

	route := &clusterv1.HTTPRoute{
		Hostname:      "app.example.com",
		EnvironmentId: "e1",
		Endpoints:     []*clusterv1.Endpoint{endpoint(addr, "n1")},
		RateLimit:     &zatterav1.RateLimit{RequestsPerSecond: 1, Burst: 2},
	}
	p := staticL7(t, "n1", route)

	for i := 0; i < 2; i++ {
		if rec := doReq(t, p, "GET", "http://app.example.com/", nil); rec.Code != http.StatusOK {
			t.Fatalf("request %d = %d, want 200", i, rec.Code)
		}
	}
	rec := doReq(t, p, "GET", "http://app.example.com/", nil)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("shed request = %d, want 429", rec.Code)
	}
	if got := rec.Header().Get("Retry-After"); got == "" {
		t.Error("429 response is missing Retry-After")
	}
	if got := hits.Load(); got != 2 {
		t.Fatalf("backend saw %d requests, want 2 (shed requests must not reach it)", got)
	}
}

// TestL7RateLimitOffByDefault guards the default: a route with no RateLimit
// set must never throttle.
func TestL7RateLimitOffByDefault(t *testing.T) {
	addr, _ := backend(t, "ok")
	route := &clusterv1.HTTPRoute{
		Hostname:      "app.example.com",
		EnvironmentId: "e1",
		Endpoints:     []*clusterv1.Endpoint{endpoint(addr, "n1")},
	}
	p := staticL7(t, "n1", route)
	for i := 0; i < 50; i++ {
		if rec := doReq(t, p, "GET", "http://app.example.com/", nil); rec.Code != http.StatusOK {
			t.Fatalf("request %d = %d with no rate limit configured", i, rec.Code)
		}
	}
}

// TestL7RateLimitPerEnvironment checks that two routes sharing a client IP but
// pointing at different environments do not share a bucket.
func TestL7RateLimitPerEnvironment(t *testing.T) {
	aAddr, _ := backend(t, "A")
	bAddr, _ := backend(t, "B")
	rl := &zatterav1.RateLimit{RequestsPerSecond: 1, Burst: 1}
	p := staticL7(t, "n1",
		&clusterv1.HTTPRoute{Hostname: "a.example.com", EnvironmentId: "e1",
			Endpoints: []*clusterv1.Endpoint{endpoint(aAddr, "n1")}, RateLimit: rl},
		&clusterv1.HTTPRoute{Hostname: "b.example.com", EnvironmentId: "e2",
			Endpoints: []*clusterv1.Endpoint{endpoint(bAddr, "n1")}, RateLimit: rl},
	)

	if rec := doReq(t, p, "GET", "http://a.example.com/", nil); rec.Code != http.StatusOK {
		t.Fatalf("a first = %d", rec.Code)
	}
	if rec := doReq(t, p, "GET", "http://a.example.com/", nil); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("a second = %d, want 429", rec.Code)
	}
	// e2 has its own bucket even though the client IP is identical.
	if rec := doReq(t, p, "GET", "http://b.example.com/", nil); rec.Code != http.StatusOK {
		t.Fatalf("b was throttled by a's bucket: %d", rec.Code)
	}
}
