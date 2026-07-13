package mesh

import (
	"fmt"
	"strings"

	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
)

// peerConfig is a resolved WireGuard peer (the proto Peer mapped to what the
// device needs). AllowedIPs are CIDR strings; Endpoint is "host:port" or "".
type peerConfig struct {
	publicKey        wgtypes.Key
	endpoint         string
	keepaliveSeconds uint16
	allowedIPs       []string
	remove           bool
}

// deviceConfig is a full device configuration rendered to WireGuard uapi text.
type deviceConfig struct {
	privateKey wgtypes.Key
	listenPort uint16
	// replacePeers emits replace_peers=true so the device's peer set becomes
	// exactly `peers` — this is how we avoid stale AllowedIPs silently stealing
	// routes across reconfigures.
	replacePeers bool
	peers        []peerConfig
}

// buildUAPI renders cfg to the newline-delimited key=value uapi format that
// device.IpcSet consumes. Keys are hex-encoded; the block ends with a blank
// line as the protocol requires.
//
// Determinism note: peers are emitted in the given order and AllowedIPs within
// a peer in slice order — callers pass a stable order so goldens are stable.
func buildUAPI(cfg deviceConfig) string {
	var b strings.Builder
	fmt.Fprintf(&b, "private_key=%s\n", hexKey(cfg.privateKey))
	if cfg.listenPort != 0 {
		fmt.Fprintf(&b, "listen_port=%d\n", cfg.listenPort)
	}
	if cfg.replacePeers {
		b.WriteString("replace_peers=true\n")
	}
	for _, p := range cfg.peers {
		fmt.Fprintf(&b, "public_key=%s\n", hexKey(p.publicKey))
		if p.remove {
			b.WriteString("remove=true\n")
			continue
		}
		if p.endpoint != "" {
			fmt.Fprintf(&b, "endpoint=%s\n", p.endpoint)
		}
		fmt.Fprintf(&b, "persistent_keepalive_interval=%d\n", p.keepaliveSeconds)
		// replace_allowed_ips=true so a peer's AllowedIPs become exactly this
		// set (a shrunk set must not leave a stale wider route behind).
		b.WriteString("replace_allowed_ips=true\n")
		for _, ip := range p.allowedIPs {
			fmt.Fprintf(&b, "allowed_ip=%s\n", ip)
		}
	}
	b.WriteString("\n")
	return b.String()
}
