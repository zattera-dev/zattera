package proxy

import (
	"compress/gzip"
	"crypto/tls"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"golang.org/x/crypto/bcrypt"
	"golang.org/x/net/websocket"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// backend returns an httptest server echoing a fixed body, plus its host:port.
func backend(t *testing.T, body string) (string, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return strings.TrimPrefix(srv.URL, "http://"), srv
}

func endpoint(addr, node string) *clusterv1.Endpoint {
	return &clusterv1.Endpoint{Addr: addr, NodeId: node, Healthy: true}
}

// staticL7 builds an L7 over a fixed snapshot, with a deterministic rng.
func staticL7(t *testing.T, node string, routes ...*clusterv1.HTTPRoute) *L7 {
	t.Helper()
	src := &StaticRouteSource{Snapshot: &clusterv1.RouteSnapshot{Version: 1, HttpRoutes: routes}}
	p := NewL7(src, node, clock.NewFake())
	return p
}

func doReq(t *testing.T, p *L7, method, rawurl string, hdr http.Header) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, rawurl, nil)
	req.TLS = &tls.ConnectionState{} // simulate the :443 listener (no HTTPS redirect)
	for k, vs := range hdr {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)
	return rec
}

func TestL7HostAndPathRouting(t *testing.T) {
	aAddr, _ := backend(t, "A-root")
	bAddr, _ := backend(t, "B-api")
	routes := []*clusterv1.HTTPRoute{
		{Hostname: "app.example.com", PathPrefix: "", EnvironmentId: "e1", Endpoints: []*clusterv1.Endpoint{endpoint(aAddr, "n1")}},
		{Hostname: "app.example.com", PathPrefix: "/api", EnvironmentId: "e2", Endpoints: []*clusterv1.Endpoint{endpoint(bAddr, "n1")}},
	}
	p := staticL7(t, "n1", routes...)

	if rec := doReq(t, p, "GET", "http://app.example.com/", nil); rec.Body.String() != "A-root" {
		t.Fatalf("root route body = %q", rec.Body.String())
	}
	// Longest-prefix wins for /api/*.
	if rec := doReq(t, p, "GET", "http://app.example.com/api/x", nil); rec.Body.String() != "B-api" {
		t.Fatalf("/api route body = %q", rec.Body.String())
	}
	// Unknown host → 404 JSON.
	if rec := doReq(t, p, "GET", "http://nope.example.com/", nil); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown host status = %d", rec.Code)
	}
}

func TestL7NoHealthyEndpoint502(t *testing.T) {
	route := &clusterv1.HTTPRoute{Hostname: "h", Endpoints: []*clusterv1.Endpoint{{Addr: "127.0.0.1:1", Healthy: false}}}
	p := staticL7(t, "n1", route)
	rec := doReq(t, p, "GET", "http://h/", nil)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "no healthy endpoint") {
		t.Fatalf("body = %q", rec.Body.String())
	}
}

func TestL7HealthFilteringAndBalance(t *testing.T) {
	a, _ := backend(t, "a")
	b, _ := backend(t, "b")
	route := &clusterv1.HTTPRoute{
		Hostname:      "h",
		EnvironmentId: "e1",
		Endpoints: []*clusterv1.Endpoint{
			endpoint(a, "n1"),
			endpoint(b, "n1"),
			{Addr: "127.0.0.1:1", NodeId: "n1", Healthy: false}, // never picked
		},
	}
	p := staticL7(t, "n1", route)

	hits := map[string]int{}
	for i := 0; i < 200; i++ {
		rec := doReq(t, p, "GET", "http://h/", nil)
		hits[rec.Body.String()]++
	}
	if hits["a"] == 0 || hits["b"] == 0 {
		t.Fatalf("P2C did not spread across both healthy backends: %v", hits)
	}
	if _, ok := hits[""]; ok {
		t.Fatal("an unhealthy backend was selected")
	}
}

func TestL7HTTPSRedirect(t *testing.T) {
	route := &clusterv1.HTTPRoute{
		Hostname:   "h",
		Middleware: &zatterav1.Middleware{RedirectHttps: true},
		Endpoints:  []*clusterv1.Endpoint{{Addr: "127.0.0.1:1", Healthy: true}},
	}
	p := staticL7(t, "n1", route)
	req := httptest.NewRequest("GET", "http://h/path?q=1", nil) // plaintext (no req.TLS)
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)
	if rec.Code != http.StatusPermanentRedirect {
		t.Fatalf("status = %d, want 308", rec.Code)
	}
	if loc := rec.Header().Get("Location"); loc != "https://h/path?q=1" {
		t.Fatalf("Location = %q", loc)
	}
}

func TestL7BasicAuth(t *testing.T) {
	addr, _ := backend(t, "secret")
	hash, _ := bcrypt.GenerateFromPassword([]byte("pw"), bcrypt.MinCost)
	route := &clusterv1.HTTPRoute{
		Hostname:   "h",
		Middleware: &zatterav1.Middleware{BasicAuth: &zatterav1.BasicAuth{Username: "u", PasswordHash: string(hash)}},
		Endpoints:  []*clusterv1.Endpoint{endpoint(addr, "n1")},
	}
	p := staticL7(t, "n1", route)

	if rec := doReq(t, p, "GET", "http://h/", nil); rec.Code != http.StatusUnauthorized {
		t.Fatalf("no-auth status = %d, want 401", rec.Code)
	}
	req := httptest.NewRequest("GET", "http://h/", nil)
	req.SetBasicAuth("u", "pw")
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != "secret" {
		t.Fatalf("good-auth = %d %q", rec.Code, rec.Body.String())
	}
}

func TestL7IPAllowlist(t *testing.T) {
	addr, _ := backend(t, "ok")
	route := &clusterv1.HTTPRoute{
		Hostname:   "h",
		Middleware: &zatterav1.Middleware{IpAllowlist: []string{"10.0.0.0/8"}},
		Endpoints:  []*clusterv1.Endpoint{endpoint(addr, "n1")},
	}
	p := staticL7(t, "n1", route)

	// httptest.NewRequest sets RemoteAddr to 192.0.2.1:1234 → blocked.
	if rec := doReq(t, p, "GET", "http://h/", nil); rec.Code != http.StatusForbidden {
		t.Fatalf("blocked status = %d, want 403", rec.Code)
	}
	req := httptest.NewRequest("GET", "http://h/", nil)
	req.RemoteAddr = "10.1.2.3:5678"
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("allowed status = %d, want 200", rec.Code)
	}
}

func TestL7Gzip(t *testing.T) {
	addr, _ := backend(t, strings.Repeat("compress-me ", 100))
	route := &clusterv1.HTTPRoute{
		Hostname:   "h",
		Middleware: &zatterav1.Middleware{Compress: true},
		Endpoints:  []*clusterv1.Endpoint{endpoint(addr, "n1")},
	}
	p := staticL7(t, "n1", route)

	rec := doReq(t, p, "GET", "http://h/", http.Header{"Accept-Encoding": {"gzip"}})
	if rec.Header().Get("Content-Encoding") != "gzip" {
		t.Fatalf("missing gzip encoding: %v", rec.Header())
	}
	gz, err := gzip.NewReader(rec.Body)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(gz)
	if !strings.HasPrefix(string(body), "compress-me") {
		t.Fatalf("gunzipped body wrong: %q", string(body)[:20])
	}
}

func TestL7WebSocketPassthrough(t *testing.T) {
	// A websocket echo backend.
	wsBackend := httptest.NewServer(websocket.Handler(func(c *websocket.Conn) {
		_, _ = io.Copy(c, c)
	}))
	t.Cleanup(wsBackend.Close)
	addr := strings.TrimPrefix(wsBackend.URL, "http://")

	src := &StaticRouteSource{}
	p := NewL7(src, "n1", clock.NewFake())
	front := httptest.NewServer(p)
	t.Cleanup(front.Close)

	// Route on the front server's own host (port stripped, as matchHTTP does).
	host := strings.TrimPrefix(front.URL, "http://")
	hostOnly := stripPort(host)
	src.Snapshot = &clusterv1.RouteSnapshot{Version: 1, HttpRoutes: []*clusterv1.HTTPRoute{
		// Plaintext ws:// dial → disable the HTTPS redirect for this route.
		{Hostname: hostOnly, Middleware: &zatterav1.Middleware{RedirectHttps: false}, Endpoints: []*clusterv1.Endpoint{endpoint(addr, "n1")}},
	}}

	ws, err := websocket.Dial("ws://"+host+"/", "", "http://"+host+"/")
	if err != nil {
		t.Fatalf("ws dial: %v", err)
	}
	defer func() { _ = ws.Close() }()
	if _, err := ws.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(ws, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "ping" {
		t.Fatalf("ws echo = %q", buf)
	}
}

func TestL7InflightDecrements(t *testing.T) {
	addr, _ := backend(t, "x")
	route := &clusterv1.HTTPRoute{Hostname: "h", EnvironmentId: "e1", Endpoints: []*clusterv1.Endpoint{endpoint(addr, "n1")}}
	p := staticL7(t, "n1", route)
	doReq(t, p, "GET", "http://h/", nil)
	if n := p.Stats().Inflight("e1"); n != 0 {
		t.Fatalf("in-flight leaked: %d", n)
	}
}
