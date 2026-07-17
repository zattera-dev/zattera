package daemon

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	"github.com/zattera-dev/zattera/internal/config"
	"github.com/zattera-dev/zattera/internal/daemon/ca"
	"github.com/zattera-dev/zattera/internal/daemon/mesh"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
)

// bootstrapControlMeshIP is the hub address the first control node takes.
// Multi-control mesh-IP allocation lands with M2; for Phase A the single
// control plane is the hub.
const bootstrapControlMeshIP = "10.90.0.1"

const defaultWGPort = 51820

// controlMeshIP returns this control node's mesh address, or "" when the mesh
// is disabled.
func controlMeshIP(cfg config.Config) string {
	if cfg.Mesh.Disabled {
		return ""
	}
	return bootstrapControlMeshIP
}

func meshListenPort(cfg config.Config) uint16 {
	if cfg.Mesh.ListenPort != 0 {
		return cfg.Mesh.ListenPort
	}
	return defaultWGPort
}

func wgKeyPath(dataDir string) string { return filepath.Join(dataDir, "wg.key") }

// bringUpControlHub brings a control node's WireGuard device up on meshIP as a
// routing hub and enables IP forwarding, so workers can route worker↔worker
// traffic through it. Every control node — the bootstrap node (meshIP =
// 10.90.0.1) AND every joined control node (its own 10.90.0.x) — is a hub, which
// is what lets a worker's whole-mesh route fail over from one control node to
// another (T-55c). initialPeers, when non-nil, is installed immediately: a
// joined control node receives the existing control /32 peers in its join
// response so its raft mTLS transport can reach the quorum before peer sync
// takes over. It writes NO cluster state, so it is safe to call before raft is
// up — the raft transport and the gossip detector both bind meshIP, so the
// device must exist first. Returns the manager so the caller can register and
// tear it down.
func bringUpControlHub(ctx context.Context, cfg config.Config, meshIP string, initialPeers *clusterv1.PeerSet, log *slog.Logger) (*mesh.DeviceManager, error) {
	addr, err := netip.ParseAddr(meshIP)
	if err != nil {
		return nil, fmt.Errorf("daemon: control mesh ip %q: %w", meshIP, err)
	}
	dm := mesh.NewDeviceManager(log)
	if err := dm.Up(ctx, mesh.NodeConfig{
		PrivateKeyPath: wgKeyPath(cfg.DataDir),
		MeshIP:         addr,
		ListenPort:     meshListenPort(cfg),
		InterfaceName:  cfg.Mesh.Interface,
	}); err != nil {
		return nil, err
	}
	enableIPForward(cfg, log)
	if initialPeers != nil {
		if err := dm.ApplyPeers(ctx, initialPeers); err != nil {
			log.Warn("mesh: applying initial control peers failed", "err", err)
		}
	}
	log.Info("control mesh hub up", "mesh_ip", meshIP)
	return dm, nil
}

// startControlMesh finishes a control hub's mesh wiring once raft + the local API
// are up: the leader records its own mesh identity (IP + WG public key + public
// endpoints) — a joined control node skips this because the Join handler already
// recorded its identity from the join request — and every control node starts
// peer sync (dialing its own MeshService over loopback) so its WireGuard peer set
// tracks cluster state. dm must already be up (see bringUpControlHub).
func startControlMesh(ctx context.Context, cfg config.Config, rs *raftstore.Store, dm *mesh.DeviceManager, authority *ca.CA, nodeID, meshIP string, log *slog.Logger) {
	if rs.IsLeader() {
		if pub, err := dm.PublicKey(); err != nil {
			log.Warn("mesh: control node public key", "err", err)
		} else if err := updateNodeMesh(ctx, rs, nodeID, meshIP, pub, cfg.Mesh.PublicEndpoints); err != nil {
			log.Warn("mesh: could not record control node mesh identity", "err", err)
		}
	}
	go func() {
		err := mesh.RunPeerSync(ctx, mesh.PeerSyncConfig{
			NodeID:  nodeID,
			Manager: dm,
			Logger:  log,
			Dial:    loopbackMeshDialer(authority, nodeID, cfg.API.Listen),
		})
		if err != nil && ctx.Err() == nil {
			log.Warn("control peersync stopped", "err", err)
		}
	}()
	log.Info("control mesh registered", "mesh_ip", meshIP)
}

// updateNodeMesh records the node's mesh IP + WG public key so peers can reach
// it.
func updateNodeMesh(ctx context.Context, rs *raftstore.Store, nodeID, meshIP, pubKey string, endpoints []string) error {
	n, ok := rs.State().Node(nodeID) // returns a clone
	if !ok {
		return fmt.Errorf("daemon: node %s not registered", nodeID)
	}
	n.MeshIp = meshIP
	n.WireguardPublicKey = pubKey
	if len(endpoints) > 0 {
		n.PublicEndpoints = endpoints
	}
	n.GetMeta().UpdatedAt = timestamppb.Now()
	return rs.Apply(ctx, &clusterv1.Command{
		RequestId: ids.New(),
		Actor:     "system:mesh",
		Time:      timestamppb.Now(),
		Mutation:  &clusterv1.Command_PutNode{PutNode: &clusterv1.PutNode{Node: n}},
	})
}

// enableIPForward turns on IPv4 forwarding AND installs the iptables FORWARD
// ACCEPT rules for the mesh interface, so a control node can actually route
// worker↔worker traffic through the hub. The sysctl alone is not enough: Docker
// (present on every worker-capable node) sets the FORWARD chain's default policy
// to DROP, which silently blackholes forwarded mesh packets. We add explicit
// ACCEPT rules for traffic entering or leaving the mesh interface (idempotent).
// Best-effort + Linux-only — non-Linux / unprivileged just logs at debug.
func enableIPForward(cfg config.Config, log *slog.Logger) {
	if err := os.WriteFile("/proc/sys/net/ipv4/ip_forward", []byte("1\n"), 0o644); err != nil {
		log.Debug("mesh: could not enable ip_forward (non-linux or unprivileged)", "err", err)
	}
	iface := cfg.Mesh.Interface
	if iface == "" {
		iface = defaultMeshInterface
	}
	for _, dir := range []string{"-i", "-o"} {
		// -C (check) first so we don't stack duplicate rules across restarts.
		check := exec.Command("iptables", "-C", "FORWARD", dir, iface, "-j", "ACCEPT")
		if check.Run() == nil {
			continue
		}
		// Insert at the top so mesh traffic is accepted BEFORE Docker's default
		// FORWARD DROP policy (Docker owns this chain).
		add := exec.Command("iptables", "-I", "FORWARD", "1", dir, iface, "-j", "ACCEPT")
		if out, err := add.CombinedOutput(); err != nil {
			log.Debug("mesh: could not add FORWARD ACCEPT rule (non-linux or unprivileged)",
				"dir", dir, "iface", iface, "err", err, "out", string(out))
		}
	}
}

// defaultMeshInterface is the WireGuard interface name when none is configured
// (matches mesh.defaultInterfaceName on Linux).
const defaultMeshInterface = "zt0"

// startWorkerMesh brings a joined worker's device up, installs the initial hub
// peers so the control plane becomes reachable, and keeps peers in sync over
// the mesh. Every peer set it receives refreshes ce so the worker's control
// failover set tracks the current control nodes (T-55c).
func startWorkerMesh(ctx context.Context, cfg config.Config, jr *joinResult, ce *controlEndpoints, log *slog.Logger) (*mesh.DeviceManager, error) {
	addr, err := netip.ParseAddr(jr.MeshIP)
	if err != nil {
		return nil, fmt.Errorf("daemon: worker mesh ip %q: %w", jr.MeshIP, err)
	}
	dm := mesh.NewDeviceManager(log)
	// meshsock datapath (T-57/T-58): build the punch + relay clients before Up
	// so the device comes up on the custom bind (NAT hole punching + relay).
	// dm.InjectRelayed/PunchNow are no-ops until Up creates the bind, so wiring
	// them here is safe.
	msCfg, err := meshsockSetup(ctx, cfg, jr, dm, log)
	if err != nil {
		log.Warn("meshsock setup failed; using plain userspace/kernel WG", "err", err)
	}
	if err := dm.Up(ctx, mesh.NodeConfig{
		PrivateKeyPath: wgKeyPath(cfg.DataDir),
		MeshIP:         addr,
		ListenPort:     meshListenPort(cfg),
		InterfaceName:  cfg.Mesh.Interface,
		Meshsock:       msCfg,
	}); err != nil {
		return nil, err
	}
	// Install the hub peers from the join response first so the control plane's
	// mesh IP routes before peersync dials it.
	if jr.initialPeers != nil {
		if err := dm.ApplyPeers(ctx, jr.initialPeers); err != nil {
			log.Warn("mesh: applying initial peers failed", "err", err)
		}
	}
	go func() {
		err := mesh.RunPeerSync(ctx, mesh.PeerSyncConfig{
			NodeID:  jr.NodeID,
			Manager: dm,
			Logger:  log,
			Dial:    meshServiceDialer(ce),
			OnPeers: ce.updateFromPeers,
		})
		if err != nil && ctx.Err() == nil {
			log.Warn("worker peersync stopped", "err", err)
		}
	}()
	log.Info("worker mesh up", "mesh_ip", jr.MeshIP)
	return dm, nil
}

// loopbackMeshDialer dials the local MeshService over 127.0.0.1 with a
// self-issued node cert (control node keeping its own peer set in sync).
func loopbackMeshDialer(authority *ca.CA, nodeID, apiListen string) func(context.Context) (grpc.ClientConnInterface, func() error, error) {
	_, port, err := net.SplitHostPort(apiListen)
	if err != nil || port == "" {
		port = "8443"
	}
	addr := net.JoinHostPort("127.0.0.1", port)
	opt := grpc.WithTransportCredentials(credentials.NewTLS(nodeClientTLS(authority, nodeID, "127.0.0.1")))
	return func(context.Context) (grpc.ClientConnInterface, func() error, error) {
		cc, err := grpc.NewClient(addr, opt)
		if err != nil {
			return nil, nil, err
		}
		return cc, cc.Close, nil
	}
}

// meshServiceDialer dials a control plane's MeshService over the mesh using the
// worker's signed node identity, picking the next control endpoint on every
// (re)connect so peer sync fails over when a control node dies (T-55c).
func meshServiceDialer(ce *controlEndpoints) func(context.Context) (grpc.ClientConnInterface, func() error, error) {
	return func(context.Context) (grpc.ClientConnInterface, func() error, error) {
		addr, creds := ce.pick()
		if addr == "" {
			return nil, nil, fmt.Errorf("daemon: no control endpoint available")
		}
		cc, err := grpc.NewClient(addr, controlDialOpts(creds)...)
		if err != nil {
			return nil, nil, err
		}
		return cc, cc.Close, nil
	}
}

// nodeClientTLS builds an mTLS client config presenting a freshly issued node
// cert for nodeID, trusting the cluster CA. serverName must match a SAN of the
// dialed server cert.
func nodeClientTLS(authority *ca.CA, nodeID, serverName string) *tls.Config {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: authority.Pool(), ServerName: serverName}
	if leaf, err := authority.IssueNode(nodeID, nil, ca.NodeCertTTL); err == nil {
		if cert, err := leaf.TLSCertificate(authority.CABundlePEM()); err == nil {
			cfg.Certificates = []tls.Certificate{cert}
		}
	}
	return cfg
}
