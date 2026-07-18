package daemon

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/config"
	"github.com/zattera-dev/zattera/internal/daemon/api"
	"github.com/zattera-dev/zattera/internal/daemon/ca"
	"github.com/zattera-dev/zattera/internal/daemon/registry"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/state"
)

// startRegistry mounts the embedded OCI registry (T-32) on a control node's
// registry listener (:5000). Control nodes host image blobs locally; the join
// flow points workers at a control node's registry address (T-17). It serves
// TLS with the CA server cert (plain HTTP only when RegistryConfig.InsecureHTTP,
// intended for tests) and reaps stale upload sessions hourly. A start failure
// is logged and non-fatal — the node runs without a local registry.
func startRegistry(ctx context.Context, cfg config.Config, st *state.Store, authority *ca.CA, clk clock.Clock, log *slog.Logger) (*registry.Registry, error) {
	dir := filepath.Join(cfg.DataDir, "registry")
	// Dev registry is loopback + insecure HTTP and accepts anonymous push/pull,
	// so the co-located builder and host Docker need no credentials (T-54).
	var auth registry.Authenticator
	if !cfg.Dev {
		auth = registryAuthenticator(st, clk)
	}
	reg, err := registry.New(dir, clk, auth, log)
	if err != nil {
		return nil, fmt.Errorf("registry: %w", err)
	}

	listen := cfg.Registry.Listen
	if listen == "" {
		listen = ":5000"
	}
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		_ = reg.Close()
		return nil, fmt.Errorf("registry listen %s: %w", listen, err)
	}
	if !cfg.Registry.InsecureHTTP {
		tlsCfg, terr := authority.ServerTLSConfig(serverDNSNames(cfg), serverIPs(controlMeshIP(cfg)))
		if terr != nil {
			_ = ln.Close()
			_ = reg.Close()
			return nil, fmt.Errorf("registry tls: %w", terr)
		}
		ln = tls.NewListener(ln, tlsCfg)
	}

	srv := &http.Server{Handler: reg.Handler(), ReadHeaderTimeout: 30 * time.Second}
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Error("registry server stopped", "err", err)
		}
	}()

	// Upload-session janitor (24h TTL enforced by Reap).
	go func() {
		t := clk.NewTicker(time.Hour)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C():
				reg.ReapUploads()
			}
		}
	}()

	// Graceful shutdown on context cancel.
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
		_ = reg.Close()
	}()

	log.Info("registry started", "listen", listen, "tls", !cfg.Registry.InsecureHTTP)
	return reg, nil
}

// registryAuthenticator verifies registry basic-auth against node registry
// credentials (KV registry/creds/<id>, minted at join in T-17) and user
// personal access tokens (a zpat_… token supplied as the password).
func registryAuthenticator(st *state.Store, clk clock.Clock) registry.Authenticator {
	return registry.AuthFunc(func(user, pass string) bool {
		if pass == "" {
			return false
		}
		// Node credential: username "node-<id>", password checked by hash.
		if id, ok := strings.CutPrefix(user, "node-"); ok && id != "" {
			if v, _, _, ok := st.KV("registry/creds/" + id); ok {
				return string(v) == api.HashToken(pass)
			}
		}
		// User PAT: the token itself is the password.
		if strings.HasPrefix(pass, "zpat_") {
			if tok, ok := st.TokenByHash(api.HashToken(pass)); ok {
				if exp := tok.GetExpiresAt(); exp != nil && clk.Now().After(exp.AsTime()) {
					return false
				}
				return true
			}
		}
		return false
	})
}

// controlRegistryPeers resolves the OTHER control nodes' registry endpoints
// from cluster state on every call, so pull-through follows joins, removals and
// status changes without a restart (T-101). A DOWN node is skipped: fetching
// from it would only burn the per-peer timeout.
func controlRegistryPeers(st *state.Store, selfID string, cfg config.Config) registry.PeerSource {
	_, port, _ := net.SplitHostPort(cfg.Registry.Listen)
	if port == "" {
		port = "5000"
	}
	scheme := "https"
	if cfg.Registry.InsecureHTTP {
		scheme = "http"
	}
	return registry.PeerSourceFunc(func() []registry.Peer {
		var peers []registry.Peer
		for _, n := range st.ListNodes() {
			if n.GetMeta().GetId() == selfID || n.GetMeshIp() == "" {
				continue
			}
			if n.GetStatus() != zatterav1.NodeStatus_NODE_STATUS_ALIVE {
				continue
			}
			isControl := false
			for _, r := range n.GetRoles() {
				if r == zatterav1.NodeRole_NODE_ROLE_CONTROL {
					isControl = true
				}
			}
			if !isControl {
				continue // only control nodes host blobs
			}
			peers = append(peers, registry.Peer{
				NodeID:  n.GetMeta().GetId(),
				BaseURL: scheme + "://" + net.JoinHostPort(n.GetMeshIp(), port),
			})
		}
		return peers
	})
}

// peerRegistryClient dials peer registries trusting the cluster CA — the same
// authority that signs each control node's registry server cert.
func peerRegistryClient(authority *ca.CA) *http.Client {
	return &http.Client{
		Timeout: peerRegistryTimeout,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: authority.Pool(), MinVersion: tls.VersionTLS12},
		},
	}
}

// peerRegistryTimeout bounds one peer exchange from the client side; the
// fetcher applies its own per-attempt and total deadlines on top.
const peerRegistryTimeout = 5 * time.Minute
