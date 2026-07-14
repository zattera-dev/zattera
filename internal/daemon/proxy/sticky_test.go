package proxy

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

// stickyEndpoint is like endpoint but carries an assignment id (sticky key).
func stickyEndpoint(addr, node, asg string) *clusterv1.Endpoint {
	return &clusterv1.Endpoint{Addr: addr, NodeId: node, AssignmentId: asg, Healthy: true}
}

// reqTLS builds an HTTPS request (no redirect) carrying the given cookies.
func reqTLS(host string, cookies ...*http.Cookie) *http.Request {
	req := httptest.NewRequest("GET", "http://"+host+"/", nil)
	req.TLS = &tls.ConnectionState{}
	for _, c := range cookies {
		req.AddCookie(c)
	}
	return req
}

func serve(p *L7, req *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)
	return rec
}

func stickyCookieFrom(rec *httptest.ResponseRecorder) *http.Cookie {
	for _, c := range rec.Result().Cookies() {
		if c.Name == stickyCookie {
			return c
		}
	}
	return nil
}

func TestStickyPinsToOneBackend(t *testing.T) {
	a, _ := backend(t, "a")
	b, _ := backend(t, "b")
	route := &clusterv1.HTTPRoute{
		Hostname:   "h",
		Middleware: &zatterav1.Middleware{StickySessions: true},
		Endpoints:  []*clusterv1.Endpoint{stickyEndpoint(a, "n1", "asg-a"), stickyEndpoint(b, "n1", "asg-b")},
	}
	p := staticL7(t, "n1", route)

	// First request has no cookie → gets pinned and a Set-Cookie back.
	first := serve(p, reqTLS("h"))
	ck := stickyCookieFrom(first)
	if ck == nil {
		t.Fatal("sticky route must set the affinity cookie")
	}
	pinned := first.Body.String()

	// Every subsequent request carrying the cookie hits the same backend and
	// re-uses the pin (no new Set-Cookie).
	for i := 0; i < 50; i++ {
		rec := serve(p, reqTLS("h", ck))
		if rec.Body.String() != pinned {
			t.Fatalf("request %d hit %q, want pinned %q", i, rec.Body.String(), pinned)
		}
		if stickyCookieFrom(rec) != nil {
			t.Fatalf("request %d re-set the cookie unnecessarily", i)
		}
	}
}

func TestStickyNonStickySpreads(t *testing.T) {
	a, _ := backend(t, "a")
	b, _ := backend(t, "b")
	route := &clusterv1.HTTPRoute{
		Hostname:  "h",
		Endpoints: []*clusterv1.Endpoint{stickyEndpoint(a, "n1", "asg-a"), stickyEndpoint(b, "n1", "asg-b")},
	}
	p := staticL7(t, "n1", route)

	hits := map[string]int{}
	for i := 0; i < 200; i++ {
		rec := serve(p, reqTLS("h"))
		if stickyCookieFrom(rec) != nil {
			t.Fatal("non-sticky route must not set a cookie")
		}
		hits[rec.Body.String()]++
	}
	if hits["a"] == 0 || hits["b"] == 0 {
		t.Fatalf("non-sticky route did not spread: %v", hits)
	}
}

func TestStickyFailsOverWhenTargetUnhealthy(t *testing.T) {
	a, _ := backend(t, "a")
	b, _ := backend(t, "b")
	// Pin to a; then mark a unhealthy in the snapshot → must fail over to b
	// and re-pin (new cookie).
	src := &StaticRouteSource{Snapshot: &clusterv1.RouteSnapshot{Version: 1, HttpRoutes: []*clusterv1.HTTPRoute{{
		Hostname:   "h",
		Middleware: &zatterav1.Middleware{StickySessions: true},
		Endpoints:  []*clusterv1.Endpoint{stickyEndpoint(a, "n1", "asg-a"), stickyEndpoint(b, "n1", "asg-b")},
	}}}}
	p := NewL7(src, "n1", nil)

	// Cookie pinning to a.
	aCookie := &http.Cookie{Name: stickyCookie, Value: stickyID(stickyEndpoint(a, "n1", "asg-a"))}
	if rec := serve(p, reqTLS("h", aCookie)); rec.Body.String() != "a" {
		t.Fatalf("expected pin to a, got %q", rec.Body.String())
	}

	// Drain a: only b is healthy now.
	src.Snapshot = &clusterv1.RouteSnapshot{Version: 2, HttpRoutes: []*clusterv1.HTTPRoute{{
		Hostname:   "h",
		Middleware: &zatterav1.Middleware{StickySessions: true},
		Endpoints: []*clusterv1.Endpoint{
			{Addr: a, NodeId: "n1", AssignmentId: "asg-a", Healthy: false},
			stickyEndpoint(b, "n1", "asg-b"),
		},
	}}}

	rec := serve(p, reqTLS("h", aCookie))
	if rec.Body.String() != "b" {
		t.Fatalf("failover body = %q, want b", rec.Body.String())
	}
	ck := stickyCookieFrom(rec)
	if ck == nil || ck.Value != stickyID(stickyEndpoint(b, "n1", "asg-b")) {
		t.Fatalf("failover did not re-pin to b: %+v", ck)
	}
}

func TestStickyIDStableAndOpaque(t *testing.T) {
	e := stickyEndpoint("10.90.0.1:30001", "n1", "asg-x")
	id := stickyID(e)
	if id == "" || id == e.GetAddr() || id == e.GetAssignmentId() {
		t.Fatalf("sticky id should be an opaque hash, got %q", id)
	}
	// Stable across calls and independent of addr changes (keyed on assignment).
	moved := stickyEndpoint("10.90.0.2:40002", "n2", "asg-x")
	if stickyID(moved) != id {
		t.Fatalf("sticky id changed when only the addr moved")
	}
}
