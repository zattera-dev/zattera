// Package mesh owns the WireGuard overlay (ADR-0003). The Manager interface
// is consumed by the daemon wiring and the agent; implementations:
//   - device manager with wireguard-go / kernel WG (tasks T-18..T-21, T-57/58)
//   - Disabled (single-node mode, returned by NewDisabled)
//   - testutil fake (simcluster)
package mesh

import (
	"context"
	"net/netip"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
)

// NodeConfig is what a node needs to bring its mesh interface up.
type NodeConfig struct {
	// PrivateKeyPath stores the WG private key (0600). Generated when absent.
	PrivateKeyPath string
	MeshIP         netip.Addr
	// ListenPort for WireGuard UDP (default 51820).
	ListenPort uint16
	// InterfaceName defaults to "zt0" (utunN chosen automatically on darwin).
	InterfaceName string
}

// Status describes the current mesh device state.
type Status struct {
	Enabled   bool
	Up        bool
	MeshIP    netip.Addr
	PublicKey string
	// PeerPaths maps node id → path in use: "direct" | "punched" | "relay" | "hub".
	PeerPaths map[string]string
}

// Manager is the per-node mesh controller.
type Manager interface {
	// Up creates/configures the WG device. Idempotent.
	Up(ctx context.Context, cfg NodeConfig) error
	// ApplyPeers reconciles the device's peer set to exactly `peers`
	// (declarative full set — implementations diff internally).
	ApplyPeers(ctx context.Context, peers *clusterv1.PeerSet) error
	// Down tears the device down.
	Down(ctx context.Context) error
	Status() Status
	// PublicKey returns the node's WG public key (generating a keypair on
	// first use), valid before Up.
	PublicKey() (string, error)
}

// Disabled is the single-node no-mesh implementation: everything is a no-op
// and Status reports Enabled=false. The daemon then binds all internal
// services to 127.0.0.1.
type Disabled struct{}

func NewDisabled() Disabled { return Disabled{} }

func (Disabled) Up(context.Context, NodeConfig) error                { return nil }
func (Disabled) ApplyPeers(context.Context, *clusterv1.PeerSet) error { return nil }
func (Disabled) Down(context.Context) error                          { return nil }
func (Disabled) Status() Status                                      { return Status{Enabled: false} }
func (Disabled) PublicKey() (string, error)                          { return "", nil }
