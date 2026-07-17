package daemon

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	"github.com/zattera-dev/zattera/internal/daemon/ca"
	"github.com/zattera-dev/zattera/internal/daemon/proxy"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/daemon/tlsmgr"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// TestIngressCertKV exercises the raft-backed cert KV through the certmagic
// storage adapter: store/load/exists/delete and a distributed lock round-trip.
func TestIngressCertKV(t *testing.T) {
	rs := raftstore.NewTestStore(t)
	store := tlsmgr.NewStorage(newRaftCertKV(rs), clock.Real{})
	ctx := context.Background()

	if _, err := store.Load(ctx, "acme/site.crt"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Load of missing key: %v", err)
	}
	if err := store.Store(ctx, "acme/site.crt", []byte("PEM")); err != nil {
		t.Fatalf("Store: %v", err)
	}
	if !store.Exists(ctx, "acme/site.crt") {
		t.Fatal("Exists should be true after Store")
	}
	got, err := store.Load(ctx, "acme/site.crt")
	if err != nil || string(got) != "PEM" {
		t.Fatalf("Load = %q, %v", got, err)
	}
	if err := store.Delete(ctx, "acme/site.crt"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if store.Exists(ctx, "acme/site.crt") {
		t.Fatal("Exists should be false after Delete")
	}

	// Distributed lock (CAS + TTL through the same KV).
	if err := store.Lock(ctx, "issue:site"); err != nil {
		t.Fatalf("Lock: %v", err)
	}
	if err := store.Unlock(ctx, "issue:site"); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
}

// TestIngressRouteCertHosts derives the ACME-eligible hostnames from the route
// snapshot.
func TestIngressRouteCertHosts(t *testing.T) {
	src := &proxy.StaticRouteSource{Snapshot: &clusterv1.RouteSnapshot{
		HttpRoutes: []*clusterv1.HTTPRoute{
			{Hostname: "a.example.com"},
			{Hostname: "b.example.com"},
			{Hostname: "a.example.com"}, // dup
			{Hostname: ""},              // ignored
		},
	}}
	hosts := certHosts{source: src, extra: []string{"api.example.com", "a.example.com"}}.CertHosts()
	if len(hosts) != 3 { // a, b (routes) + api (extra); a dedups
		t.Fatalf("hosts = %v, want 3 unique", hosts)
	}
}

// TestIngressServeRedirect verifies the generic serve path binds both listeners
// and, with redirects enabled (production mode), 308s plaintext requests to
// HTTPS. Uses the dev TLS manager to avoid dialing ACME.
func TestIngressServeRedirect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	src := &proxy.StaticRouteSource{Snapshot: &clusterv1.RouteSnapshot{
		HttpRoutes: []*clusterv1.HTTPRoute{{Hostname: "app.example.com"}},
	}}
	authority, err := ca.LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	tm, err := tlsmgr.New(tlsmgr.Options{Dev: true, CA: authority})
	if err != nil {
		t.Fatal(err)
	}
	httpAddr, httpsAddr := freeAddr(t), freeAddr(t)
	if err := serveIngress(ctx, configForIngress{HTTPListen: httpAddr, HTTPSListen: httpsAddr},
		src, tm, false /* redirect on */, "n1", clock.Real{}, nil, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatalf("serveIngress: %v", err)
	}

	// Plaintext request → 308 to https (production redirect).
	client := &http.Client{
		Timeout:       2 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	var resp *http.Response
	for i := 0; i < 20; i++ {
		req, _ := http.NewRequest(http.MethodGet, "http://"+httpAddr+"/", nil)
		req.Host = "app.example.com"
		if resp, err = client.Do(req); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusPermanentRedirect {
		t.Fatalf("status = %d, want 308 redirect", resp.StatusCode)
	}
	if loc := resp.Header.Get("Location"); loc != "https://app.example.com/" {
		t.Fatalf("Location = %q", loc)
	}
}

// freeAddr returns a currently-free loopback address "127.0.0.1:<port>".
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}
