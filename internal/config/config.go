// Package config loads and validates the zatterad server configuration.
// Sources, in increasing precedence: built-in defaults → TOML file
// (/etc/zattera/config.toml or --config) → command-line flags (wired by the
// server command). The CLI has its own tiny config (internal/cli).
package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/BurntSushi/toml"
)

// Role names a node can assume.
const (
	RoleControl = "control"
	RoleWorker  = "worker"
)

// Config is the full zatterad configuration.
type Config struct {
	// NodeName defaults to the hostname.
	NodeName string `toml:"node_name"`
	// DataDir holds raft state, registry blobs, logs, TSDB, certs.
	DataDir string `toml:"data_dir"`
	// Roles: control, worker, or both.
	Roles []string `toml:"roles"`
	// Domain is the cluster app domain (apps get <app>-<env>.<domain>).
	Domain string `toml:"domain"`
	// Dev enables single-node developer mode: self-signed TLS, ACME off,
	// mesh off, bootstrap admin token printed at first start.
	Dev bool `toml:"dev"`

	// SealedAtRest keeps the cluster data key off local disk. The node then
	// cannot auto-unseal itself after a restart: it comes up sealed (alerting,
	// env-var writes and backups disabled) until an operator runs
	// `zattera unseal` or it recovers the key from a control peer. Default
	// false, which favours a node coming back working after a reboot; see
	// ADR-0006.
	SealedAtRest bool `toml:"sealed_at_rest"`

	API      APIConfig      `toml:"api"`
	Ingress  IngressConfig  `toml:"ingress"`
	Registry RegistryConfig `toml:"registry"`
	Mesh     MeshConfig     `toml:"mesh"`
	ACME     ACMEConfig     `toml:"acme"`
	Join     JoinConfig     `toml:"join"`
	Raft     RaftConfig     `toml:"raft"`
	Logs     LogsConfig     `toml:"logs"`
	Upgrade  UpgradeConfig  `toml:"upgrade"`
}

type APIConfig struct {
	// Listen for the public gRPC+REST API (TLS).
	Listen string `toml:"listen"`
	// AdvertiseAddr is how other nodes/CLI reach this API (host:port).
	AdvertiseAddr string `toml:"advertise_addr"`
}

type IngressConfig struct {
	HTTPListen  string `toml:"http_listen"`
	HTTPSListen string `toml:"https_listen"`
	// Disabled turns the ingress proxy off on this node.
	Disabled bool `toml:"disabled"`
}

type RegistryConfig struct {
	Listen string `toml:"listen"`
	// InsecureHTTP serves the embedded registry over plain HTTP instead of
	// TLS. Intended for integration tests (and never for production); the CA
	// server cert is used otherwise.
	InsecureHTTP bool `toml:"insecure_http"`
}

type MeshConfig struct {
	// Disabled forces no mesh (implied in single-node dev mode).
	Disabled bool `toml:"disabled"`
	// ListenPort for WireGuard UDP.
	ListenPort uint16 `toml:"listen_port"`
	// Interface name (Linux). Darwin picks utunN automatically.
	Interface string `toml:"interface"`
	// PublicEndpoints this node is reachable at ("ip:port"); autodetected
	// when empty and the node has a public address.
	PublicEndpoints []string `toml:"public_endpoints"`
	// Mode selects the WireGuard datapath (ADR-0003):
	//   "" / "auto"  — kernel WG when available, else userspace; phases A/B
	//                  (hub + direct peering) only.
	//   "meshsock"   — userspace WG with the meshsock bind: UDP hole punching
	//                  (phase C) + TCP relay fallback (phase D). Use on NAT'd
	//                  nodes that need worker↔worker connectivity without a
	//                  routable endpoint.
	Mode string `toml:"mode"`
}

// MeshsockEnabled reports whether this node should use the meshsock datapath.
func (m MeshConfig) MeshsockEnabled() bool { return m.Mode == "meshsock" }

type ACMEConfig struct {
	Email    string `toml:"email"`
	Disabled bool   `toml:"disabled"`
	// Staging uses the Let's Encrypt staging endpoint (dev/tests).
	Staging bool `toml:"staging"`
}

type JoinConfig struct {
	// Addr of any control node's public API ("host:8443").
	Addr string `toml:"addr"`
	// Token is the join token ("K10<ca-hash>::<secret>").
	Token string `toml:"token"`
}

type RaftConfig struct {
	// Listen for raft transport (control nodes; bound to mesh IP or
	// 127.0.0.1 in single-node).
	Listen string `toml:"listen"`
}

// UpgradeConfig bounds self-upgrade (T-95). The control plane chooses the
// asset URL, but a node only downloads from BaseURL — so a control plane that
// is compromised or misconfigured still cannot point nodes at arbitrary code.
//
// Empty means the official release host (upgrade.DefaultBaseURL), NOT
// "unrestricted": a default that silently accepts any URL is the wrong way
// round for a feature that ends in exec. Set base_url = "*" to opt out, e.g.
// for an air-gapped mirror.
type UpgradeConfig struct {
	BaseURL string `toml:"base_url"`
}

type LogsConfig struct {
	// MaxStreamMB caps retained log bytes per stream.
	MaxStreamMB int `toml:"max_stream_mb"`
	// RetentionDays caps log age.
	RetentionDays int `toml:"retention_days"`
}

// Default returns the built-in defaults.
func Default() Config {
	host, _ := os.Hostname()
	return Config{
		NodeName: host,
		DataDir:  "/var/lib/zattera",
		Roles:    []string{RoleControl, RoleWorker},
		API:      APIConfig{Listen: ":8443"},
		Ingress:  IngressConfig{HTTPListen: ":80", HTTPSListen: ":443"},
		Registry: RegistryConfig{Listen: ":5000"},
		Mesh:     MeshConfig{ListenPort: 51820, Interface: "zt0"},
		Raft:     RaftConfig{Listen: ":7480"},
		Logs:     LogsConfig{MaxStreamMB: 100, RetentionDays: 7},
	}
}

// Load reads TOML from path over the defaults. Empty path = defaults only.
func Load(path string) (Config, error) {
	cfg := Default()
	if path == "" {
		return cfg, nil
	}
	meta, err := toml.DecodeFile(path, &cfg)
	if err != nil {
		return cfg, fmt.Errorf("config: %s: %w", path, err)
	}
	if undecoded := meta.Undecoded(); len(undecoded) > 0 {
		keys := make([]string, len(undecoded))
		for i, k := range undecoded {
			keys[i] = k.String()
		}
		return cfg, fmt.Errorf("config: %s: unknown keys: %s", path, strings.Join(keys, ", "))
	}
	return cfg, nil
}

// Validate checks cross-field invariants.
func (c *Config) Validate() error {
	if c.NodeName == "" {
		return fmt.Errorf("config: node_name is required")
	}
	if c.DataDir == "" {
		return fmt.Errorf("config: data_dir is required")
	}
	if len(c.Roles) == 0 {
		return fmt.Errorf("config: at least one role (control|worker) is required")
	}
	for _, r := range c.Roles {
		if r != RoleControl && r != RoleWorker {
			return fmt.Errorf("config: unknown role %q", r)
		}
	}
	if c.Join.Addr != "" && c.Join.Token == "" {
		return fmt.Errorf("config: join.token is required with join.addr")
	}
	if !c.HasRole(RoleControl) && c.Join.Addr == "" {
		return fmt.Errorf("config: worker-only nodes must set join.addr")
	}
	return nil
}

// HasRole reports whether the node has the given role.
func (c *Config) HasRole(role string) bool {
	for _, r := range c.Roles {
		if r == role {
			return true
		}
	}
	return false
}
