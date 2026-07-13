//go:build linux

package mesh

import (
	"encoding/hex"
	"fmt"
	"log/slog"
	"net"
	"time"

	"golang.zx2c4.com/wireguard/wgctrl"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
)

// newKernelBackend probes for kernel WireGuard and, when present, creates the
// zt0 link and returns a wgctrl-backed configurer. Returns errKernelUnsupported
// when the kernel module is absent so the caller falls back to userspace.
func newKernelBackend(ifname string, _ uint16, log *slog.Logger) (deviceBackend, string, error) {
	client, err := wgctrl.New()
	if err != nil {
		return nil, "", errKernelUnsupported
	}
	// Create the WG link if it does not exist yet.
	if _, derr := client.Device(ifname); derr != nil {
		if aerr := ipCmd("link", "add", "dev", ifname, "type", "wireguard"); aerr != nil {
			// If the module simply isn't loaded, fall back to userspace.
			_ = client.Close()
			return nil, "", errKernelUnsupported
		}
	}
	return &kernelBackend{client: client, name: ifname, log: log}, ifname, nil
}

type kernelBackend struct {
	client *wgctrl.Client
	name   string
	log    *slog.Logger
}

func (b *kernelBackend) apply(cfg deviceConfig) error {
	priv := cfg.privateKey
	wcfg := wgtypes.Config{PrivateKey: &priv, ReplacePeers: cfg.replacePeers}
	if cfg.listenPort != 0 {
		p := int(cfg.listenPort)
		wcfg.ListenPort = &p
	}
	for _, pc := range cfg.peers {
		peer := wgtypes.PeerConfig{
			PublicKey:         pc.publicKey,
			Remove:            pc.remove,
			ReplaceAllowedIPs: true,
		}
		if pc.keepaliveSeconds > 0 {
			d := time.Duration(pc.keepaliveSeconds) * time.Second
			peer.PersistentKeepaliveInterval = &d
		}
		if pc.endpoint != "" {
			if ep, err := net.ResolveUDPAddr("udp", pc.endpoint); err == nil {
				peer.Endpoint = ep
			}
		}
		for _, cidr := range pc.allowedIPs {
			if _, ipnet, err := net.ParseCIDR(cidr); err == nil {
				peer.AllowedIPs = append(peer.AllowedIPs, *ipnet)
			}
		}
		wcfg.Peers = append(wcfg.Peers, peer)
	}
	if err := b.client.ConfigureDevice(b.name, wcfg); err != nil {
		return fmt.Errorf("mesh: wgctrl configure %s: %w", b.name, err)
	}
	return nil
}

func (b *kernelBackend) handshakes() (map[string]int64, error) {
	dev, err := b.client.Device(b.name)
	if err != nil {
		return nil, fmt.Errorf("mesh: wgctrl device %s: %w", b.name, err)
	}
	out := make(map[string]int64, len(dev.Peers))
	for _, p := range dev.Peers {
		var sec int64
		if !p.LastHandshakeTime.IsZero() {
			sec = p.LastHandshakeTime.Unix()
		}
		out[hex.EncodeToString(p.PublicKey[:])] = sec
	}
	return out, nil
}

func (b *kernelBackend) close() error {
	_ = ipCmd("link", "del", "dev", b.name)
	return b.client.Close()
}
