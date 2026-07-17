package daemon

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net"
	"net/http"
	"time"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	"github.com/zattera-dev/zattera/internal/config"
	"github.com/zattera-dev/zattera/internal/daemon/ca"
	"github.com/zattera-dev/zattera/internal/daemon/proxy"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/daemon/scheduler"
	"github.com/zattera-dev/zattera/internal/daemon/tlsmgr"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// configForIngress is the subset of config the ingress needs.
type configForIngress struct {
	HTTPListen  string
	HTTPSListen string
}

// startDevIngress serves the single-node/dev ingress: the in-process
// RouteBuilder as the source, self-signed dev certs from the cluster CA, and no
// forced HTTPS redirect (dev HTTPS is on a non-standard port). (T-54)
func startDevIngress(ctx context.Context, cfg configForIngress, rb *scheduler.RouteBuilder, authority *ca.CA, nodeID string, clk clock.Clock, statsSink func(*proxy.Stats), log *slog.Logger) error {
	tm, err := tlsmgr.New(tlsmgr.Options{Dev: true, CA: authority, Logger: log})
	if err != nil {
		return err
	}
	return serveIngress(ctx, cfg, routeBuilderSource{rb: rb}, tm, true, nodeID, clk, statsSink, log)
}

// newProdTLSManager builds the single cluster-wide ACME manager shared by the
// production ingress and (optionally) the public API cert. It issues on demand
// for the current route hostnames plus extraHosts (e.g. the API hostname). (T-89)
func newProdTLSManager(rs *raftstore.Store, source proxy.RouteSource, extraHosts []string, acme config.ACMEConfig, clk clock.Clock, log *slog.Logger) (*tlsmgr.Manager, error) {
	return tlsmgr.New(tlsmgr.Options{
		Storage: tlsmgr.NewStorage(newRaftCertKV(rs), clk),
		Hosts:   certHosts{source: source, extra: extraHosts},
		Email:   acme.Email,
		Staging: acme.Staging,
		Logger:  log,
	})
}

// startProdIngress serves the production ingress on :80/:443: routes come from
// source (the in-process RouteBuilder on control nodes, a RouteClient on
// workers), certificates from the shared ACME manager, and plaintext requests
// 308-redirect to HTTPS. It also starts the L4 passthrough proxy. (T-89)
func startProdIngress(ctx context.Context, cfg config.Config, source proxy.RouteSource, tm *tlsmgr.Manager, nodeID string, clk clock.Clock, statsSink func(*proxy.Stats), log *slog.Logger) error {
	// L4 passthrough for public_l4_port routes (T-43).
	l4 := proxy.NewL4(source, nodeID, log)
	go l4.Run(ctx)

	ic := configForIngress{HTTPListen: cfg.Ingress.HTTPListen, HTTPSListen: cfg.Ingress.HTTPSListen}
	return serveIngress(ctx, ic, source, tm, false, nodeID, clk, statsSink, log)
}

// serveIngress binds the HTTP and HTTPS listeners for an L7 proxy over source,
// using tm for TLS. disableRedirect leaves plaintext requests served directly
// (dev) instead of redirecting to HTTPS (prod). The HTTP listener also mounts
// the ACME HTTP-01 solver (a passthrough in dev).
func serveIngress(ctx context.Context, cfg configForIngress, source proxy.RouteSource, tm *tlsmgr.Manager, disableRedirect bool, nodeID string, clk clock.Clock, statsSink func(*proxy.Stats), log *slog.Logger) error {
	l7 := proxy.NewL7(source, nodeID, clk)
	l7.DisableHTTPSRedirect = disableRedirect
	// Expose the proxy's per-env counters to the metrics sampler (T-59/T-60).
	if statsSink != nil {
		statsSink(l7.Stats())
	}

	httpSrv := &http.Server{
		Handler:           tm.HTTP01Handler(l7),
		ReadHeaderTimeout: 30 * time.Second,
	}
	httpsSrv := &http.Server{
		Handler:           l7,
		ReadHeaderTimeout: 30 * time.Second,
	}

	httpLn, err := net.Listen("tcp", cfg.HTTPListen)
	if err != nil {
		return err
	}
	httpsLn, err := net.Listen("tcp", cfg.HTTPSListen)
	if err != nil {
		_ = httpLn.Close()
		return err
	}
	go func() {
		if err := httpSrv.Serve(httpLn); err != nil && err != http.ErrServerClosed {
			log.Warn("ingress http server stopped", "err", err)
		}
	}()
	go func() {
		if err := httpsSrv.Serve(tls.NewListener(httpsLn, tm.GetTLSConfig())); err != nil && err != http.ErrServerClosed {
			log.Warn("ingress https server stopped", "err", err)
		}
	}()
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(sctx)
		_ = httpsSrv.Shutdown(sctx)
	}()
	log.Info("ingress listening", "http", cfg.HTTPListen, "https", cfg.HTTPSListen)
	return nil
}

// certHosts sources the hostnames ACME may issue for: the current route
// snapshot's hostnames plus any static extras (e.g. the API hostname).
// (tlsmgr.CertHostSource)
type certHosts struct {
	source proxy.RouteSource
	extra  []string
}

func (h certHosts) CertHosts() []string {
	seen := map[string]bool{}
	var out []string
	add := func(hn string) {
		if hn != "" && !seen[hn] {
			seen[hn] = true
			out = append(out, hn)
		}
	}
	for _, r := range h.source.Current().GetHttpRoutes() {
		add(r.GetHostname())
	}
	for _, hn := range h.extra {
		add(hn)
	}
	return out
}

// routeBuilderSource adapts the in-process RouteBuilder to proxy.RouteSource.
type routeBuilderSource struct{ rb *scheduler.RouteBuilder }

func (s routeBuilderSource) Current() *clusterv1.RouteSnapshot { return s.rb.Current() }

func (s routeBuilderSource) Updates(ctx context.Context) <-chan *clusterv1.RouteSnapshot {
	id, ch := s.rb.Subscribe()
	out := make(chan *clusterv1.RouteSnapshot)
	go func() {
		defer s.rb.Unsubscribe(id)
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case snap, ok := <-ch:
				if !ok {
					return
				}
				select {
				case out <- snap:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}
