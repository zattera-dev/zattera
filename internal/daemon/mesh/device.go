package mesh

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"strconv"
	"strings"
	"sync"

	"golang.zx2c4.com/wireguard/conn"
	wgdevice "golang.zx2c4.com/wireguard/device"
	"golang.zx2c4.com/wireguard/tun"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
)

const (
	// meshMTU leaves headroom for the WireGuard header over a 1500-byte path.
	meshMTU = 1420
	// defaultListenPort is the WireGuard UDP port.
	defaultListenPort = 51820
	// meshCIDR is the overlay network; hub-and-spoke control peers route all of
	// it.
	meshCIDR = "10.90.0.0/16"
)

// errKernelUnsupported is returned by newKernelBackend on platforms/hosts
// without kernel WireGuard, so Up falls back to the userspace device.
var errKernelUnsupported = errors.New("mesh: kernel wireguard unavailable")

// deviceBackend applies a full device configuration. Two implementations:
// userspace (wireguard-go, via uapi) and kernel (wgctrl). Both do a full
// replace so the peer set becomes exactly what is passed.
type deviceBackend interface {
	apply(cfg deviceConfig) error
	// handshakes returns seconds-since-epoch of the last handshake per peer,
	// keyed by hex public key (0 = never). Used for path diagnostics and the
	// integration test.
	handshakes() (map[string]int64, error)
	close() error
}

// DeviceManager implements mesh.Manager over a real WireGuard device.
type DeviceManager struct {
	log *slog.Logger

	mu         sync.Mutex
	priv       wgtypes.Key
	pub        wgtypes.Key
	keyLoaded  bool
	listenPort uint16
	ifname     string
	meshIP     netip.Addr

	backend   deviceBackend
	up        bool
	peerPaths map[string]string
}

// NewDeviceManager builds a mesh manager backed by a real WG device. The
// kernel-vs-userspace choice is made at Up time (kernel preferred on Linux).
func NewDeviceManager(log *slog.Logger) *DeviceManager {
	if log == nil {
		log = slog.Default()
	}
	return &DeviceManager{log: log, peerPaths: map[string]string{}}
}

// PublicKey returns the node's WG public key, generating the keypair on first
// use. Valid before Up (the key path is taken from the last Up config or, if
// Up has not run, this returns an error asking for a path).
func (dm *DeviceManager) PublicKey() (string, error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	if !dm.keyLoaded {
		return "", errors.New("mesh: public key unavailable until Up loads the key path")
	}
	return dm.pub.String(), nil
}

// Up creates and configures the WG device (idempotent). It requires
// root/CAP_NET_ADMIN.
func (dm *DeviceManager) Up(_ context.Context, cfg NodeConfig) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	if dm.up {
		return nil
	}
	if err := dm.ensureKeyLocked(cfg.PrivateKeyPath); err != nil {
		return err
	}

	dm.listenPort = cfg.ListenPort
	if dm.listenPort == 0 {
		dm.listenPort = defaultListenPort
	}
	dm.ifname = cfg.InterfaceName
	if dm.ifname == "" {
		dm.ifname = defaultInterfaceName()
	}
	dm.meshIP = cfg.MeshIP

	backend, actualName, err := dm.createBackend()
	if err != nil {
		return err
	}
	// Program the private key + listen port before bringing the interface up.
	if err := backend.apply(deviceConfig{privateKey: dm.priv, listenPort: dm.listenPort, replacePeers: true}); err != nil {
		_ = backend.close()
		return fmt.Errorf("mesh: initial configure: %w", err)
	}
	if err := configureInterface(actualName, dm.meshIP); err != nil {
		_ = backend.close()
		return fmt.Errorf("mesh: configure interface %s: %w", actualName, err)
	}

	dm.backend = backend
	dm.ifname = actualName
	dm.up = true
	dm.log.Info("mesh device up", "iface", actualName, "mesh_ip", dm.meshIP, "pubkey", dm.pub.String())
	return nil
}

// createBackend prefers kernel WireGuard on Linux, falling back to the
// userspace device.
func (dm *DeviceManager) createBackend() (deviceBackend, string, error) {
	if b, name, err := newKernelBackend(dm.ifname, dm.listenPort, dm.log); err == nil {
		dm.log.Debug("mesh using kernel wireguard", "iface", name)
		return b, name, nil
	} else if !errors.Is(err, errKernelUnsupported) {
		dm.log.Warn("mesh kernel wireguard init failed; using userspace", "err", err)
	}
	return newUserspaceBackend(dm.ifname, dm.log)
}

// ApplyPeers reconciles the device's peer set to exactly `peers`.
func (dm *DeviceManager) ApplyPeers(_ context.Context, peers *clusterv1.PeerSet) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	if dm.backend == nil {
		return errors.New("mesh: device is not up")
	}
	pcs, err := buildPeerConfigs(peers)
	if err != nil {
		return err
	}
	cfg := deviceConfig{privateKey: dm.priv, listenPort: dm.listenPort, replacePeers: true, peers: pcs}
	if err := dm.backend.apply(cfg); err != nil {
		return fmt.Errorf("mesh: apply peers: %w", err)
	}
	dm.peerPaths = pathsFor(peers)
	return nil
}

// LastHandshakes returns the last-handshake time (unix seconds, 0 = never) per
// peer, keyed by hex public key. Useful for path diagnostics and integration
// tests.
func (dm *DeviceManager) LastHandshakes() (map[string]int64, error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	if dm.backend == nil {
		return nil, errors.New("mesh: device is not up")
	}
	return dm.backend.handshakes()
}

// Down tears the device down.
func (dm *DeviceManager) Down(_ context.Context) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	if dm.backend == nil {
		return nil
	}
	err := dm.backend.close()
	dm.backend = nil
	dm.up = false
	dm.peerPaths = map[string]string{}
	return err
}

// Status reports the current device state.
func (dm *DeviceManager) Status() Status {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	st := Status{Enabled: true, Up: dm.up, MeshIP: dm.meshIP, PeerPaths: map[string]string{}}
	if dm.keyLoaded {
		st.PublicKey = dm.pub.String()
	}
	for k, v := range dm.peerPaths {
		st.PeerPaths[k] = v
	}
	return st
}

func (dm *DeviceManager) ensureKeyLocked(path string) error {
	if dm.keyLoaded {
		return nil
	}
	if path == "" {
		return errors.New("mesh: PrivateKeyPath is required")
	}
	k, err := loadOrCreatePrivateKey(path)
	if err != nil {
		return err
	}
	dm.priv = k
	dm.pub = k.PublicKey()
	dm.keyLoaded = true
	return nil
}

// buildPeerConfigs maps a proto PeerSet to resolved peer configs. The endpoint
// is the first candidate (smarter path selection is T-57).
func buildPeerConfigs(peers *clusterv1.PeerSet) ([]peerConfig, error) {
	out := make([]peerConfig, 0, len(peers.GetPeers()))
	for _, p := range peers.GetPeers() {
		pk, err := wgtypes.ParseKey(p.GetWireguardPublicKey())
		if err != nil {
			return nil, fmt.Errorf("mesh: peer %s public key: %w", p.GetNodeId(), err)
		}
		pc := peerConfig{
			publicKey:        pk,
			keepaliveSeconds: uint16(p.GetPersistentKeepaliveSeconds()),
			allowedIPs:       allowedIPsFor(p, peers.GetHubAndSpoke()),
		}
		if eps := p.GetEndpoints(); len(eps) > 0 {
			pc.endpoint = eps[0]
		}
		out = append(out, pc)
	}
	return out, nil
}

// allowedIPsFor resolves a peer's AllowedIPs: the explicit set if present, else
// the whole mesh for hub-and-spoke control peers, else the peer's /32.
func allowedIPsFor(p *clusterv1.Peer, hubAndSpoke bool) []string {
	if len(p.GetAllowedIps()) > 0 {
		return p.GetAllowedIps()
	}
	if hubAndSpoke && p.GetIsControl() {
		return []string{meshCIDR}
	}
	if ip := p.GetMeshIp(); ip != "" {
		return []string{ip + "/32"}
	}
	return nil
}

// pathsFor summarizes the path used per peer for Status/observability.
func pathsFor(peers *clusterv1.PeerSet) map[string]string {
	out := make(map[string]string, len(peers.GetPeers()))
	for _, p := range peers.GetPeers() {
		switch {
		case len(p.GetEndpoints()) > 0:
			out[p.GetNodeId()] = "direct"
		case p.GetIsControl():
			out[p.GetNodeId()] = "hub"
		default:
			out[p.GetNodeId()] = "pending"
		}
	}
	return out
}

// --- userspace backend (wireguard-go), cross-platform ---------------------

// ipcSetter is the config surface of a wireguard-go device (*device.Device
// satisfies it); the peer-diff tests inject a fake.
type ipcSetter interface {
	IpcSet(uapiConfig string) error
}

type userspaceBackend struct {
	ipc    ipcSetter
	device *wgdevice.Device
	tun    tun.Device
}

func (b *userspaceBackend) apply(cfg deviceConfig) error { return b.ipc.IpcSet(buildUAPI(cfg)) }

func (b *userspaceBackend) handshakes() (map[string]int64, error) {
	if b.device == nil {
		return nil, errors.New("mesh: device not initialized")
	}
	conf, err := b.device.IpcGet()
	if err != nil {
		return nil, err
	}
	return parseHandshakes(conf), nil
}

// parseHandshakes reads a uapi IpcGet dump into peer→last-handshake-seconds.
func parseHandshakes(conf string) map[string]int64 {
	out := map[string]int64{}
	var cur string
	for _, line := range strings.Split(conf, "\n") {
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		switch k {
		case "public_key":
			cur = v
			out[cur] = 0
		case "last_handshake_time_sec":
			if cur != "" {
				out[cur] = atoi64(v)
			}
		}
	}
	return out
}

func atoi64(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

func (b *userspaceBackend) close() error {
	if b.device != nil {
		b.device.Close() // also closes the tun
		return nil
	}
	if b.tun != nil {
		return b.tun.Close()
	}
	return nil
}

// newUserspaceBackend creates a TUN + wireguard-go device. Requires
// root/CAP_NET_ADMIN; the returned name is the actual interface (utunN on
// macOS).
func newUserspaceBackend(ifname string, log *slog.Logger) (deviceBackend, string, error) {
	tunDev, err := tun.CreateTUN(ifname, meshMTU)
	if err != nil {
		return nil, "", fmt.Errorf("mesh: create tun %q (need root/CAP_NET_ADMIN): %w", ifname, err)
	}
	name, err := tunDev.Name()
	if err != nil {
		_ = tunDev.Close()
		return nil, "", fmt.Errorf("mesh: tun name: %w", err)
	}
	dev := wgdevice.NewDevice(tunDev, conn.NewDefaultBind(), wgLogger(log))
	if err := dev.Up(); err != nil {
		dev.Close()
		return nil, "", fmt.Errorf("mesh: device up: %w", err)
	}
	return &userspaceBackend{ipc: dev, device: dev, tun: tunDev}, name, nil
}

// wgLogger routes wireguard-go's chatty logs into slog at debug/error.
func wgLogger(log *slog.Logger) *wgdevice.Logger {
	return &wgdevice.Logger{
		Verbosef: func(format string, args ...any) { log.Debug("wireguard: " + fmt.Sprintf(format, args...)) },
		Errorf:   func(format string, args ...any) { log.Error("wireguard: " + fmt.Sprintf(format, args...)) },
	}
}

var _ Manager = (*DeviceManager)(nil)
