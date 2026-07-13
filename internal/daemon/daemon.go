// Package daemon is the node runtime: control plane and/or worker, selected
// by config roles. This file wires the subsystems; each subsystem lives in
// its own subpackage.
//
// Foundation status: boots a single-node control plane (raft + state) and
// waits for shutdown. API server (T-06), agent (T-16), proxy (T-41) and the
// rest plug in here per TASKS.md.
package daemon

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/config"
	"github.com/zattera-dev/zattera/internal/daemon/agent"
	"github.com/zattera-dev/zattera/internal/daemon/api"
	"github.com/zattera-dev/zattera/internal/daemon/ca"
	"github.com/zattera-dev/zattera/internal/daemon/livestate"
	"github.com/zattera-dev/zattera/internal/daemon/nodeinfo"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	crt "github.com/zattera-dev/zattera/internal/daemon/runtime"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/pkgutil/version"
	"github.com/zattera-dev/zattera/internal/state"
)

// Commands returns the daemon-side top-level commands.
func Commands() []*cobra.Command {
	var (
		cfgPath string
		dataDir string
		dev     bool
		joinTo  string
		token   string
	)
	server := &cobra.Command{
		Use:   "server",
		Short: "Run a Zattera node (control plane and/or worker)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.Load(cfgPath)
			if err != nil {
				return err
			}
			if dataDir != "" {
				cfg.DataDir = dataDir
			}
			if dev {
				cfg.Dev = true
				cfg.Mesh.Disabled = true
				cfg.ACME.Disabled = true
			}
			if joinTo != "" {
				cfg.Join.Addr = joinTo
				cfg.Join.Token = token
			}
			if err := cfg.Validate(); err != nil {
				return err
			}
			return Run(cmd.Context(), cfg)
		},
	}
	server.Flags().StringVar(&cfgPath, "config", "", "path to config.toml")
	server.Flags().StringVar(&dataDir, "data-dir", "", "override data_dir")
	server.Flags().BoolVar(&dev, "dev", false, "single-node developer mode (no mesh, no ACME, self-signed TLS)")
	server.Flags().StringVar(&joinTo, "join", "", "control-plane address to join (host:8443)")
	server.Flags().StringVar(&token, "token", "", "join token")

	return []*cobra.Command{server}
}

// Run boots the node and blocks until ctx is canceled or a signal arrives.
func Run(ctx context.Context, cfg config.Config) error {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Worker enrollment (T-17): --join enrolls with a control plane, persists the
	// signed node identity, then runs as a worker (no local raft/API).
	if cfg.Join.Addr != "" {
		jr, err := runJoin(ctx, cfg, log)
		if err != nil {
			return err
		}
		return runWorker(ctx, cfg, jr, log)
	}
	if !cfg.HasRole(config.RoleControl) {
		return fmt.Errorf("daemon: worker-only mode requires --join")
	}

	// Stable node identity.
	nodeID, err := loadOrCreateNodeID(cfg.DataDir)
	if err != nil {
		return err
	}

	st := state.New()
	rs, err := raftstore.New(raftstore.Config{
		NodeID:    nodeID,
		DataDir:   filepath.Join(cfg.DataDir, "raft"),
		BindAddr:  bindLoopback(cfg.Raft.Listen),
		Bootstrap: true,
		Logger:    log,
	}, st)
	if err != nil {
		return err
	}
	defer func() { _ = rs.Shutdown() }()

	if err := rs.WaitForLeader(ctx); err != nil {
		return err
	}
	log.Info("control plane started",
		"node_id", nodeID,
		"data_dir", cfg.DataDir,
		"dev", cfg.Dev,
	)

	// Cluster CA (T-01): issues the API server cert and node identities.
	authority, err := ca.LoadOrCreate(cfg.DataDir)
	if err != nil {
		return err
	}

	// First-boot bootstrap (T-03): the leader creates org/admin/token/cluster
	// key and prints the bootstrap token once. Followers skip silently.
	// Barrier first so a freshly elected leader has applied the persisted log
	// (the org entry) before the idempotency check — otherwise a restart would
	// reprint the token.
	var sealer secrets.Sealer
	if rs.IsLeader() {
		if err := rs.Barrier(ctx); err != nil {
			return err
		}
		keyring, err := Bootstrap(ctx, rs, BootstrapOptions{Logger: log})
		if err != nil {
			return err
		}
		if keyring != nil {
			if sealer, err = keyring.Sealer(); err != nil {
				return err
			}
		}
	}
	// TODO(T-3x): unseal-on-restart so followers/restarts recover the sealer.

	// Register this node in state (T-12): capacity, roles, os/arch. Leader only
	// for now; joining nodes are registered by the join flow (T-17).
	if rs.IsLeader() {
		if err := registerLocalNode(ctx, rs, cfg, nodeID, log); err != nil {
			log.Warn("local node registration failed", "err", err)
		}
	}

	// Public API services (T-04 auth, T-05 projects+rbac, T-06 apps) with the
	// auth → rbac interceptor chain. TODO(T-07): audit interceptor.
	clk := clock.Real{}
	authn := api.NewAuthenticator(st, rs, clk)
	rbac := api.NewRBAC(st)
	auditor := api.NewAuditor(st, rs, log, 0)
	go authn.RunTokenFlusher(ctx)
	go auditor.Run(ctx)

	// Leader-forward (T-08): followers proxy mutating unary calls to the leader.
	// Single-node/dev is always leader, so this is a no-op there.
	forwarder := api.NewLeaderForwarder(rs.IsLeader, leaderAPIResolver(rs, st, cfg), leaderDialOpts(authority), log)
	go func() {
		for range rs.LeaderCh() {
			forwarder.Invalidate()
		}
	}()

	// Leader-memory livestate (T-14): agent presence, heartbeats and live
	// samples. Rebuilt from the agent streams on every election; never in Raft.
	live := livestate.New(clk)
	syncSrv := api.NewSyncServer(st, rs, live, clk, log, sealer)

	// Join service (T-17): token-authenticated node enrollment. In dev (mesh
	// disabled) joined nodes reach the control plane over loopback.
	_, apiPort, _ := net.SplitHostPort(cfg.API.Listen)
	if apiPort == "" {
		apiPort = "8443"
	}
	joinSrv := api.NewJoinServer(st, rs, clk, authority, api.JoinConfig{
		MeshEnabled:     !cfg.Mesh.Disabled,
		ControlGRPCAddr: net.JoinHostPort(agentHostIP(cfg), apiPort),
		RegistryAddr:    net.JoinHostPort(agentHostIP(cfg), "5000"),
	}, log)

	apiSrv, err := api.New(api.Options{
		CA:               authority,
		Listen:           cfg.API.Listen,
		Logger:           log,
		DNSNames:         serverDNSNames(cfg),
		IPs:              serverIPs(cfg),
		AuthService:      api.NewAuthServer(st, rs, clk),
		ProjectService:   api.NewProjectServer(st, rs, clk, rbac),
		AppService:       api.NewAppServer(st, rs, clk, sealer),
		StateService:     api.NewStateServer(st, rs, clk),
		NodeService:      api.NewNodeServer(st, rs, clk, authority),
		AuditService:     auditor,
		AgentSyncService: syncSrv,
		JoinService:      joinSrv,
		MeshService:      api.NewMeshServer(st, clk, log),
		UnaryInterceptors: []grpc.UnaryServerInterceptor{
			forwarder.UnaryInterceptor, authn.UnaryInterceptor, rbac.UnaryInterceptor, auditor.UnaryInterceptor,
		},
		StreamInterceptors: []grpc.StreamServerInterceptor{authn.StreamInterceptor},
	})
	if err != nil {
		return err
	}
	apiErr := make(chan error, 1)
	go func() { apiErr <- apiSrv.Serve(ctx) }()

	// Mesh (T-19): on a mesh-enabled control node, bring WireGuard up as the hub
	// and keep its own peer set in sync. Single-node/dev disables the mesh.
	if !cfg.Mesh.Disabled && rs.IsLeader() {
		meshMgr, err := startControlMesh(ctx, cfg, rs, authority, nodeID, log)
		if err != nil {
			log.Warn("mesh bring-up failed; continuing without mesh", "err", err)
		} else {
			defer func() { _ = meshMgr.Down(context.Background()) }()
		}
	}

	// Node agent (T-14): on worker-capable nodes, open the AgentSync stream to
	// the control plane and send heartbeats. In single-node/dev the control
	// node dials its own API over loopback with a self-issued node cert (the
	// mTLS identity the Sync method requires). The executor (T-15) reconciles
	// the pushed assignments against Docker.
	if cfg.HasRole(config.RoleWorker) {
		// Attach the Docker runtime so the executor can converge assignments.
		// A node without a reachable engine still runs the agent (stream +
		// heartbeats), just without execution.
		var rt crt.ContainerRuntime
		if dk, err := crt.NewDocker(); err != nil {
			log.Warn("container runtime unavailable; agent runs without an executor", "err", err)
		} else {
			rt = dk
		}
		na := agent.New(agent.Config{
			NodeID:   nodeID,
			Version:  version.Version,
			Clock:    clk,
			Logger:   log,
			DiskPath: cfg.DataDir,
			Runtime:  rt,
			HostIP:   agentHostIP(cfg),
			Dial:     localAgentDialer(authority, nodeID, cfg.API.Listen, log),
		})
		go func() {
			if err := na.Run(ctx); err != nil && ctx.Err() == nil {
				log.Warn("node agent stopped", "err", err)
			}
		}()
	}

	select {
	case <-ctx.Done():
		log.Info("shutting down")
		return nil
	case err := <-apiErr:
		return err
	}
}

// registerLocalNode records this node in state at boot with its detected
// capacity and roles. Idempotent by node id; safe to re-run on restart.
func registerLocalNode(ctx context.Context, rs *raftstore.Store, cfg config.Config, nodeID string, log *slog.Logger) error {
	capacity := nodeinfo.Detect(cfg.DataDir, log)
	var roles []zatterav1.NodeRole
	if cfg.HasRole(config.RoleControl) {
		roles = append(roles, zatterav1.NodeRole_NODE_ROLE_CONTROL)
	}
	if cfg.HasRole(config.RoleWorker) {
		roles = append(roles, zatterav1.NodeRole_NODE_ROLE_WORKER)
	}
	now := timestamppb.Now()
	node := &zatterav1.Node{
		Meta:           &zatterav1.Meta{Id: nodeID, CreatedAt: now, UpdatedAt: now},
		Name:           cfg.NodeName,
		Roles:          roles,
		Status:         zatterav1.NodeStatus_NODE_STATUS_ALIVE,
		Schedulable:    true,
		Capacity:       &zatterav1.ResourceLimits{CpuMillis: capacity.CPUMillis, MemoryMb: capacity.MemoryMB},
		CapacityDiskMb: capacity.DiskMB,
		OsArch:         runtime.GOOS + "/" + runtime.GOARCH,
	}
	// Preserve creation time if already registered.
	if existing, ok := rs.State().Node(nodeID); ok {
		node.GetMeta().CreatedAt = existing.GetMeta().GetCreatedAt()
	}
	return rs.Apply(ctx, &clusterv1.Command{
		RequestId: ids.New(),
		Actor:     "system:node-register",
		Time:      timestamppb.Now(),
		Mutation:  &clusterv1.Command_PutNode{PutNode: &clusterv1.PutNode{Node: node}},
	})
}

// serverDNSNames returns the SANs for the API server cert: localhost, the
// cluster domain and per-app wildcard when a domain is configured.
func serverDNSNames(cfg config.Config) []string {
	names := []string{"localhost"}
	if cfg.Domain != "" {
		names = append(names, cfg.Domain, "*."+cfg.Domain)
	}
	return names
}

// serverIPs returns the IP SANs. 127.0.0.1 is always present (the gateway dials
// the public port over loopback); the control node's mesh IP is added when the
// mesh is enabled so joined nodes can verify the API cert over the tunnel.
func serverIPs(cfg config.Config) []net.IP {
	ips := []net.IP{net.ParseIP("127.0.0.1")}
	if ip := controlMeshIP(cfg); ip != "" {
		if parsed := net.ParseIP(ip); parsed != nil {
			ips = append(ips, parsed)
		}
	}
	return ips
}

// leaderAPIResolver maps the current raft leader to its API address for
// leader-forwarding. Returns "" when this node is the leader. The multi-node
// mesh mapping is exercised once nodes carry advertised endpoints (T-19/T-22).
func leaderAPIResolver(rs *raftstore.Store, st *state.Store, cfg config.Config) func() (string, error) {
	_, apiPort, err := net.SplitHostPort(cfg.API.Listen)
	if err != nil || apiPort == "" {
		apiPort = "8443"
	}
	return func() (string, error) {
		if rs.IsLeader() {
			return "", nil
		}
		transportAddr, id := rs.LeaderAddr()
		if transportAddr == "" || id == "" {
			return "", fmt.Errorf("daemon: leader unknown")
		}
		if n, ok := st.Node(id); ok {
			for _, ep := range n.GetPublicEndpoints() {
				if host, _, e := net.SplitHostPort(ep); e == nil && host != "" {
					return net.JoinHostPort(host, apiPort), nil
				}
			}
		}
		host, _, e := net.SplitHostPort(transportAddr)
		if e != nil || host == "" {
			return "", fmt.Errorf("daemon: cannot derive leader API host from %q", transportAddr)
		}
		return net.JoinHostPort(host, apiPort), nil
	}
}

// leaderDialOpts trusts the cluster CA for the forward dial. No client cert: the
// forwarded bearer token authenticates the original caller on the leader.
func leaderDialOpts(authority *ca.CA) []grpc.DialOption {
	creds := credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, RootCAs: authority.Pool()})
	return []grpc.DialOption{grpc.WithTransportCredentials(creds)}
}

// localAgentDialer returns a Dial for the node's own agent to reach the local
// control API over loopback. It presents a self-issued node identity cert so
// the AgentSync method (mTLS, node-tier) accepts the stream. In a multi-node
// mesh the join flow (T-17) provisions the node cert and the control address;
// this loopback path covers single-node/dev and a control node's own worker.
func localAgentDialer(authority *ca.CA, nodeID, apiListen string, log *slog.Logger) func(context.Context) (*agent.Conn, error) {
	_, port, err := net.SplitHostPort(apiListen)
	if err != nil || port == "" {
		port = "8443"
	}
	addr := net.JoinHostPort("127.0.0.1", port)

	// Issue the node cert once; the agent reuses it across reconnects.
	var dialOpt grpc.DialOption
	if leaf, err := authority.IssueNode(nodeID, nil, ca.NodeCertTTL); err != nil {
		log.Warn("agent: issue node cert failed", "err", err)
		dialOpt = grpc.WithTransportCredentials(insecure.NewCredentials())
	} else if tlsCert, err := leaf.TLSCertificate(authority.CABundlePEM()); err != nil {
		log.Warn("agent: build node tls cert failed", "err", err)
		dialOpt = grpc.WithTransportCredentials(insecure.NewCredentials())
	} else {
		tlsCfg := authority.ClientTLSConfig(tlsCert)
		tlsCfg.ServerName = "127.0.0.1"
		dialOpt = grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg))
	}

	return func(context.Context) (*agent.Conn, error) {
		cc, err := grpc.NewClient(addr, dialOpt)
		if err != nil {
			return nil, err
		}
		return &agent.Conn{ClientConnInterface: cc, Close: cc.Close}, nil
	}
}

// runWorker runs a joined node in worker mode: it opens the AgentSync stream to
// the control plane using the signed node identity and reconciles assignments.
// It runs no local raft or control API. Mesh transport (T-18..20) is wired in
// later; until then the control address must be directly reachable.
func runWorker(ctx context.Context, cfg config.Config, jr *joinResult, log *slog.Logger) error {
	// Bring the worker's WireGuard device up first (when the cluster uses a
	// mesh) so the control plane's mesh IP is routable for the agent stream.
	if jr.MeshEnabled {
		meshMgr, err := startWorkerMesh(ctx, cfg, jr, log)
		if err != nil {
			return err
		}
		defer func() { _ = meshMgr.Down(context.Background()) }()
	}

	dial, err := workerAgentDialer(jr)
	if err != nil {
		return err
	}

	var rt crt.ContainerRuntime
	if dk, err := crt.NewDocker(); err != nil {
		log.Warn("container runtime unavailable; worker runs without an executor", "err", err)
	} else {
		rt = dk
	}

	hostIP := jr.MeshIP
	if hostIP == "" {
		hostIP = "127.0.0.1"
	}
	var regAuth *crt.RegistryAuth
	if jr.RegistryUser != "" {
		regAuth = &crt.RegistryAuth{Username: jr.RegistryUser, Password: jr.RegistryPass, ServerAddress: jr.RegistryAddr}
	}

	na := agent.New(agent.Config{
		NodeID:       jr.NodeID,
		Version:      version.Version,
		Clock:        clock.Real{},
		Logger:       log,
		DiskPath:     cfg.DataDir,
		Runtime:      rt,
		HostIP:       hostIP,
		RegistryAuth: regAuth,
		Dial:         dial,
	})
	log.Info("worker started", "node", jr.NodeID, "control", jr.ControlGRPCAddr)
	return na.Run(ctx)
}

// workerAgentDialer dials the control plane's AgentSync over mTLS with the
// node's signed identity, trusting the cluster CA from the join response.
func workerAgentDialer(jr *joinResult) (func(context.Context) (*agent.Conn, error), error) {
	cert, err := tls.X509KeyPair(jr.certPEM, jr.keyPEM)
	if err != nil {
		return nil, fmt.Errorf("daemon: load node cert: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(jr.caPEM) {
		return nil, fmt.Errorf("daemon: parse cluster CA")
	}
	host, _, err := net.SplitHostPort(jr.ControlGRPCAddr)
	if err != nil {
		host = jr.ControlGRPCAddr
	}
	creds := credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ServerName:   host,
	})
	return func(context.Context) (*agent.Conn, error) {
		conn, err := grpc.NewClient(jr.ControlGRPCAddr, grpc.WithTransportCredentials(creds))
		if err != nil {
			return nil, err
		}
		return &agent.Conn{ClientConnInterface: conn, Close: conn.Close}, nil
	}, nil
}

// agentHostIP is where the executor publishes container ports and where joined
// nodes reach the control plane: the mesh IP when the mesh is enabled, else
// loopback (single-node/dev).
func agentHostIP(cfg config.Config) string {
	if ip := controlMeshIP(cfg); ip != "" {
		return ip
	}
	return "127.0.0.1"
}

// bindLoopback turns ":7480" into "127.0.0.1:7480" for single-node mode
// (never expose raft without the mesh).
func bindLoopback(listen string) string {
	if len(listen) > 0 && listen[0] == ':' {
		return "127.0.0.1" + listen
	}
	return listen
}

func loadOrCreateNodeID(dataDir string) (string, error) {
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return "", fmt.Errorf("daemon: data dir: %w", err)
	}
	path := filepath.Join(dataDir, "node-id")
	if b, err := os.ReadFile(path); err == nil && len(b) > 0 {
		return string(b), nil
	}
	id := ids.New()
	if err := os.WriteFile(path, []byte(id), 0o600); err != nil {
		return "", err
	}
	return id, nil
}
