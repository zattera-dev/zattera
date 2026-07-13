package daemon

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"os"
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

// startControlMesh brings the control node's WireGuard device up, records its
// mesh IP + public key in state, enables IP forwarding, and keeps its own peer
// set in sync (dialing its own MeshService over loopback). Returns the manager
// so the caller can tear it down.
func startControlMesh(ctx context.Context, cfg config.Config, rs *raftstore.Store, authority *ca.CA, nodeID string, log *slog.Logger) (*mesh.DeviceManager, error) {
	ip := controlMeshIP(cfg)
	addr, err := netip.ParseAddr(ip)
	if err != nil {
		return nil, fmt.Errorf("daemon: control mesh ip: %w", err)
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
	pub, err := dm.PublicKey()
	if err != nil {
		_ = dm.Down(ctx)
		return nil, err
	}
	if err := updateNodeMesh(ctx, rs, nodeID, ip, pub, cfg.Mesh.PublicEndpoints); err != nil {
		log.Warn("mesh: could not record control node mesh identity", "err", err)
	}
	enableIPForward(log)

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
	log.Info("control mesh up", "mesh_ip", ip)
	return dm, nil
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

// enableIPForward turns on IPv4 forwarding so a control node can route
// worker↔worker traffic through the hub. The installer is responsible for the
// matching iptables FORWARD ACCEPT rules for zt0 (documented, not done here).
func enableIPForward(log *slog.Logger) {
	if err := os.WriteFile("/proc/sys/net/ipv4/ip_forward", []byte("1\n"), 0o644); err != nil {
		log.Debug("mesh: could not enable ip_forward (non-linux or unprivileged)", "err", err)
	}
}

// startWorkerMesh brings a joined worker's device up, installs the initial hub
// peers so the control plane becomes reachable, and keeps peers in sync over
// the mesh.
func startWorkerMesh(ctx context.Context, cfg config.Config, jr *joinResult, log *slog.Logger) (*mesh.DeviceManager, error) {
	addr, err := netip.ParseAddr(jr.MeshIP)
	if err != nil {
		return nil, fmt.Errorf("daemon: worker mesh ip %q: %w", jr.MeshIP, err)
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
			Dial:    meshServiceDialer(jr),
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

// meshServiceDialer dials the control plane's MeshService over the mesh using
// the worker's signed node identity.
func meshServiceDialer(jr *joinResult) func(context.Context) (grpc.ClientConnInterface, func() error, error) {
	host, _, err := net.SplitHostPort(jr.ControlGRPCAddr)
	if err != nil {
		host = jr.ControlGRPCAddr
	}
	return func(context.Context) (grpc.ClientConnInterface, func() error, error) {
		cert, err := tls.X509KeyPair(jr.certPEM, jr.keyPEM)
		if err != nil {
			return nil, nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(jr.caPEM) {
			return nil, nil, fmt.Errorf("daemon: parse cluster CA")
		}
		creds := credentials.NewTLS(&tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{cert},
			RootCAs:      pool,
			ServerName:   host,
		})
		cc, err := grpc.NewClient(jr.ControlGRPCAddr, grpc.WithTransportCredentials(creds))
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
