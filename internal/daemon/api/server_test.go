package api

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/zattera-dev/zattera/internal/daemon/ca"
)

func TestServerDualProtocol(t *testing.T) {
	authority, err := ca.LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Options{
		CA:       authority,
		Listen:   "127.0.0.1:0",
		DNSNames: []string{"localhost"},
		IPs:      []net.IP{net.ParseIP("127.0.0.1")},
	})
	if err != nil {
		t.Fatalf("api.New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = srv.Serve(ctx) }()

	addr := srv.Addr().String()
	caPool := authority.Pool()

	// Wait for the listener to accept.
	waitReady(t, addr, caPool)

	// 1) REST /healthz over HTTPS.
	t.Run("healthz", func(t *testing.T) {
		client := &http.Client{Transport: &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: caPool, ServerName: "127.0.0.1"},
		}}
		resp, err := client.Get("https://" + addr + "/healthz")
		if err != nil {
			t.Fatalf("GET /healthz: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		if string(body) != "ok\n" {
			t.Errorf("body = %q, want %q", body, "ok\n")
		}
	})

	// 2) gRPC health check over the same port.
	t.Run("grpc-health", func(t *testing.T) {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(
			credentials.NewTLS(&tls.Config{RootCAs: caPool, ServerName: "127.0.0.1"}),
		))
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer func() { _ = conn.Close() }()

		hc := healthpb.NewHealthClient(conn)
		cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer ccancel()
		resp, err := hc.Check(cctx, &healthpb.HealthCheckRequest{})
		if err != nil {
			t.Fatalf("health check: %v", err)
		}
		if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
			t.Errorf("status = %v, want SERVING", resp.GetStatus())
		}
	})
}

// waitReady polls the TLS port until it accepts a handshake or times out.
func waitReady(t *testing.T, addr string, caPool *x509.CertPool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := tls.DialWithDialer(
			&net.Dialer{Timeout: 200 * time.Millisecond},
			"tcp", addr,
			&tls.Config{RootCAs: caPool, ServerName: "127.0.0.1"},
		)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("server never became ready")
}
