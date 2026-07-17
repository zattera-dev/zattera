// Package daemon is the node runtime: control plane and/or worker, selected
// by config roles. This file wires the subsystems; each subsystem lives in
// its own subpackage.
//
// Foundation status: boots a single-node control plane (raft + state) and
// waits for shutdown. API server (T-06), agent (T-16), proxy (T-41) and the
// rest plug in here per TASKS.md.
package daemon

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hashicorp/raft"
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
	"github.com/zattera-dev/zattera/internal/daemon/builder"
	"github.com/zattera-dev/zattera/internal/daemon/ca"
	"github.com/zattera-dev/zattera/internal/daemon/livestate"
	"github.com/zattera-dev/zattera/internal/daemon/logstore"
	"github.com/zattera-dev/zattera/internal/daemon/mesh"
	"github.com/zattera-dev/zattera/internal/daemon/nodeinfo"
	"github.com/zattera-dev/zattera/internal/daemon/proxy"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	crt "github.com/zattera-dev/zattera/internal/daemon/runtime"
	"github.com/zattera-dev/zattera/internal/daemon/scheduler"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/daemon/tlsmgr"
	"github.com/zattera-dev/zattera/internal/daemon/tsdb"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/pkgutil/platform"
	"github.com/zattera-dev/zattera/internal/pkgutil/version"
	"github.com/zattera-dev/zattera/internal/state"
)

// Commands returns the daemon-side top-level commands.
func Commands() []*cobra.Command {
	var (
		cfgPath string
		dataDir string
		domain  string
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
			if domain != "" {
				cfg.Domain = domain
			}
			if dev {
				cfg.Dev = true
				cfg.Mesh.Disabled = true
				cfg.ACME.Disabled = true
				applyDevDefaults(&cfg)
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
	server.Flags().StringVar(&domain, "domain", "", "cluster app domain (overrides config; dev defaults to sslip.io)")
	server.Flags().BoolVar(&dev, "dev", false, "single-node developer mode (no mesh, no ACME, self-signed TLS)")
	server.Flags().StringVar(&joinTo, "join", "", "control-plane address to join (host:8443)")
	server.Flags().StringVar(&token, "token", "", "join token")

	// `server` runs the node; init/join/teardown manage its lifecycle on a host;
	// restore rebuilds a cluster from a backup (T-66).
	return append([]*cobra.Command{server, restoreCommand()}, nodeCommands()...)
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
		// A node that joined with the CONTROL role receives the cluster-secret
		// handover (data key, CA key, raft address) and brings up its own raft +
		// control stack, joining the quorum (T-55b). Requires the mesh (raft
		// binds the node's mesh IP); without a handover it runs as a worker.
		if jr.isControl() && jr.handover != nil {
			return runJoinedControl(ctx, cfg, jr, log)
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

	// Cluster CA (T-01): issues the API server cert and node identities. Loaded
	// before raft because the raft mTLS transport needs a node cert.
	authority, err := ca.LoadOrCreate(cfg.DataDir)
	if err != nil {
		return err
	}

	// On a mesh cluster, raft and gossip bind this node's MESH IP — so bring the
	// WireGuard device up first, then run raft over mTLS on the mesh (never the
	// public interface). This is what lets joined control nodes replicate with
	// the bootstrap node. Dev/single-node keeps loopback plain TCP.
	meshIP := controlMeshIP(cfg)
	var controlMesh *mesh.DeviceManager
	rsCfg := raftstore.Config{
		NodeID:    nodeID,
		DataDir:   filepath.Join(cfg.DataDir, "raft"),
		Bootstrap: true,
		Logger:    log,
	}
	if meshIP != "" {
		dm, derr := bringUpControlMeshDevice(ctx, cfg, log)
		if derr != nil {
			return fmt.Errorf("daemon: control mesh device: %w", derr)
		}
		controlMesh = dm
		defer func() { _ = controlMesh.Down(context.Background()) }()

		raftAddr := net.JoinHostPort(meshIP, raftPortOf(cfg))
		trans, terr := controlRaftTransport(authority, nodeID, meshIP, raftAddr, log)
		if terr != nil {
			return terr
		}
		rsCfg.Transport = trans
	} else {
		rsCfg.BindAddr = bindLoopback(cfg.Raft.Listen)
	}
	rs, err := raftstore.New(rsCfg, st)
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
		"mesh_ip", meshIP,
	)

	// First-boot bootstrap (T-03): the leader creates org/admin/token/cluster
	// key and prints the bootstrap token once. Followers skip silently.
	// Barrier first so a freshly elected leader has applied the persisted log
	// (the org entry) before the idempotency check — otherwise a restart would
	// reprint the token.
	var sealer secrets.Sealer
	var keyring *secrets.Keyring
	var bootToken, bootPassphrase string
	if rs.IsLeader() {
		if err := rs.Barrier(ctx); err != nil {
			return err
		}
		// In dev mode capture the one-time secrets so the startup banner shows
		// them (instead of Bootstrap's own stdout lines); otherwise Bootstrap
		// prints to stdout as usual.
		bootOpts := BootstrapOptions{Logger: log}
		var bootOut bytes.Buffer
		if cfg.Dev {
			bootOpts.Out = &bootOut
		}
		kr, err := Bootstrap(ctx, rs, bootOpts)
		if err != nil {
			return err
		}
		keyring = kr
		if keyring != nil {
			if sealer, err = keyring.Sealer(); err != nil {
				return err
			}
		}
		bootToken, bootPassphrase = bootstrapSecrets(bootOut.String())
	}
	// TODO(T-3x): unseal-on-restart so followers/restarts recover the sealer.

	// Cluster CA fingerprint (T-90): operators pin it with `zattera login
	// --ca-pin` (trust-on-first-use) so no CA file is needed out of band.
	caFP := caFingerprint(authority)
	log.Info("cluster CA fingerprint", "sha256", caFP)

	// Dev-mode startup banner (T-52): print effective URLs + first-boot secrets
	// as a friendly block plus DEVBANNER: machine-readable lines (T-54 parses).
	if cfg.Dev {
		info := newDevBannerInfo(cfg)
		info.AdminToken, info.RecoveryPassphrase = bootToken, bootPassphrase
		info.CAFingerprint = caFP
		renderDevBanner(os.Stdout, info)
	}

	// Register this node in state (T-12): capacity, roles, os/arch. Leader only
	// for now; joining nodes are registered by the join flow (T-17).
	if rs.IsLeader() {
		if err := registerLocalNode(ctx, rs, cfg, nodeID, log); err != nil {
			log.Warn("local node registration failed", "err", err)
		}
	}

	return runControlPlane(ctx, cfg, rs, nodeID, meshIP, authority, sealer, keyring, controlMesh, log)
}

// runControlPlane wires and runs the full control-plane stack — API server,
// scheduler, orchestrator, ingress, registry, node agent and (when it owns it)
// the WireGuard mesh hub — and blocks until ctx is canceled or the API server
// errors. It is shared by the bootstrap control node (Run) and a node that
// joined with the control role (runJoinedControl). meshIP is this node's own
// mesh address (gossip binds it; "" when the mesh is disabled).
// controlMesh, when non-nil, is this node's already-up WireGuard hub device
// (brought up before raft so it could bind the mesh IP); runControlPlane records
// its identity in state and starts peer sync. A joined control node passes nil
// because it already brought its mesh up as a spoke (see runJoinedControl).
func runControlPlane(ctx context.Context, cfg config.Config, rs *raftstore.Store, nodeID, meshIP string, authority *ca.CA, sealer secrets.Sealer, keyring *secrets.Keyring, controlMesh *mesh.DeviceManager, log *slog.Logger) error {
	st := rs.State()

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
	_, raftPort, _ := net.SplitHostPort(cfg.Raft.Listen)
	if raftPort == "" {
		raftPort = "7480"
	}
	joinSrv := api.NewJoinServer(st, rs, clk, authority, keyring, api.JoinConfig{
		MeshEnabled:     !cfg.Mesh.Disabled,
		ControlGRPCAddr: net.JoinHostPort(agentHostIP(cfg), apiPort),
		RegistryAddr:    registryClientAddr(cfg),
		RaftPort:        raftPort,
	}, log)

	// Route builder (T-39): builds the global RouteSnapshot from replicated state
	// and streams it to node proxies via RouteService.
	routeBuilder := scheduler.NewRouteBuilder(rs, clk, cfg.Domain, log)
	go routeBuilder.Run(ctx)

	// Production TLS manager (T-89/T-90): one cluster-wide ACME manager shared by
	// the ingress and the public API cert. Built only in production (dev uses
	// self-signed certs). apiHost is the public API hostname eligible for a
	// public cert (empty unless non-dev + ACME on + a hostname advertise_addr).
	routeSource := routeBuilderSource{rb: routeBuilder}
	apiHost := publicAPIHost(cfg)
	var prodTM *tlsmgr.Manager
	if !cfg.Dev {
		var extra []string
		if apiHost != "" {
			extra = []string{apiHost}
		}
		if tm, terr := newProdTLSManager(rs, routeSource, extra, cfg.ACME, clk, log); terr != nil {
			log.Warn("tls manager init failed; falling back to CA certs", "err", terr)
		} else {
			prodTM = tm
		}
	}

	// Ingress: serve HTTP/HTTPS and route to app instances via the live route
	// snapshot. Dev (T-54) uses self-signed certs on unprivileged ports;
	// production (T-89) binds :80/:443 with ACME certificates.
	//
	// proxyStats holds the L7 proxy's per-env counters once ingress is up, so the
	// metrics sampler (T-59/T-60) can record env-scoped series from them.
	var proxyStats atomic.Pointer[proxy.Stats]
	statsSink := func(s *proxy.Stats) { proxyStats.Store(s) }

	// Scale-to-zero activator (T-70): the control-node ingress parks a request
	// for a cold env and calls this to wake it. In-process apply — correct on the
	// leader (always in single-control). NOTE: on a follower control node this
	// returns ErrNotLeader, so wake-on-request currently needs the request to
	// reach the leader's ingress (tracked with the T-55b multi-control HA work).
	activatorSrv := api.NewActivatorServer(st, rs, clk, log)
	activateFn := func(ctx context.Context, envID string) error {
		_, err := activatorSrv.Activate(ctx, &clusterv1.ActivateRequest{EnvironmentId: envID, NodeId: nodeID})
		return err
	}
	switch {
	case cfg.Ingress.Disabled:
		// explicitly off
	case cfg.Dev:
		if err := startDevIngress(ctx, configForIngress{
			HTTPListen: cfg.Ingress.HTTPListen, HTTPSListen: cfg.Ingress.HTTPSListen,
		}, routeBuilder, authority, nodeID, clk, statsSink, activateFn, log); err != nil {
			log.Warn("ingress failed to start", "err", err)
		}
	case prodTM == nil:
		log.Warn("production ingress needs a TLS manager; ingress disabled")
	default:
		if err := startProdIngress(ctx, cfg, routeSource, prodTM, nodeID, clk, statsSink, activateFn, log); err != nil {
			log.Warn("ingress failed to start", "err", err)
		}
	}

	// Public API cert (T-90): serve an ACME cert for the API hostname so CLIs
	// need no cluster CA. Other SNIs keep the CA cert.
	var apiPubHost string
	var apiPubCert func(*tls.ClientHelloInfo) (*tls.Certificate, error)
	if apiHost != "" && prodTM != nil {
		apiPubHost = apiHost
		apiPubCert = prodTM.GetTLSConfig().GetCertificate
	}

	// Agent-local dial (T-54): the control plane reaches every node's
	// AgentLocalService (:8444, node mTLS) for build dispatch, log fan-out and
	// exec. In single-node/dev the node dials its own :8444 over loopback.
	agentLocalConnect := newAgentLocalConnect(authority, nodeID, log)
	uploadsDir := filepath.Join(cfg.DataDir, "uploads")
	deploySrv := api.NewDeployServer(st, rs, clk, uploadsDir)

	// Volume service + snapshot dispatcher (T-62/T-65). Snapshots need the
	// unsealed data key + backup config; a follower/sealed node serves volume
	// CRUD but returns FailedPrecondition for snapshot ops.
	volumeSrv := api.NewVolumeServer(st, rs, api.GRPCVolumeAgentDialer{Connect: agentLocalConnect}, clk, log)
	var snapDispatcher *api.SnapshotDispatcher
	var backupSrv zatterav1.BackupServiceServer // nil interface on a sealed node
	if sealer != nil && keyring != nil {
		snapDispatcher = api.NewSnapshotDispatcher(st, rs, sealer, keyring.DataKey(), agentLocalConnect, clk, log)
		volumeSrv.WithSnapshots(snapDispatcher)
		backupSrv = api.NewBackupServer(st, rs, sealer, authority, clk)
	}

	apiSrv, err := api.New(api.Options{
		CA:                authority,
		Listen:            cfg.API.Listen,
		Logger:            log,
		DNSNames:          serverDNSNames(cfg),
		IPs:               serverIPs(meshIP),
		AuthService:       api.NewAuthServer(st, rs, clk, cfg.Domain),
		ProjectService:    api.NewProjectServer(st, rs, clk, rbac),
		AppService:        api.NewAppServer(st, rs, clk, sealer),
		PublicHostname:    apiPubHost,
		PublicCertificate: apiPubCert,
		DeployService:     deploySrv,
		StateService:      api.NewStateServer(st, rs, clk),
		NodeService:       api.NewNodeServer(st, rs, clk, authority),
		AuditService:      auditor,
		LogService:        api.NewLogServer(st, api.GRPCLogDialer{Connect: agentLocalConnect}, clk, log),
		ExecService:       api.NewExecServer(st, api.GRPCExecDialer{Connect: agentLocalConnect}, log),
		MetricsService:    api.NewMetricsServer(st, live, api.GRPCStatsDialer{Connect: agentLocalConnect}, clk, log),
		JobService:        api.NewJobServer(st, rs, clk),
		VolumeService:     volumeSrv,
		BackupService:     backupSrv,
		AgentSyncService:  syncSrv,
		JoinService:       joinSrv,
		MeshService:       api.NewMeshServer(st, rs, clk, log),
		RouteService:      api.NewRouteServer(routeBuilder),
		ActivatorService:  activatorSrv,
		DomainService:     api.NewDomainServer(st, rs, clk, cfg.Domain),
		GitHubWebhook:     api.NewGitHubWebhook(st, rs, sealer, clk, log),
		SourceBlobHandler: api.SourceBlobHandler(uploadsDir),
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

	// Embedded registry (T-32): control nodes host image blobs on :5000, TLS
	// with the CA server cert, authenticated by node creds and user PATs.
	var retentionSweeper scheduler.RegistrySweeper
	if reg, err := startRegistry(ctx, cfg, st, authority, clk, log); err != nil {
		log.Warn("registry start failed; continuing without it", "err", err)
		deploySrv.Platforms = platformResolver(nil, "", log)
	} else if reg != nil {
		retentionSweeper = reg.Manifests // prune images for GC'd releases (T-38)
		deploySrv.Platforms = platformResolver(reg, registryClientAddr(cfg), log)
	}

	// Node liveness (T-21): the leader turns livestate heartbeats into durable
	// node status. evaluate() is a no-op on followers.
	livenessMon := api.NewLivenessMonitor(st, rs, live, clk, nodeID, log)
	// Gossip failure detection (T-56): mesh-enabled control nodes run memberlist
	// over the mesh so a node death is caught within seconds (vs the 30s
	// heartbeat deadline). Feeds the same SetNodeStatus path via the flap guard.
	if !cfg.Mesh.Disabled && meshIP != "" {
		if g, gerr := startGossip(meshIP, nodeID, authority, st, log); gerr != nil {
			log.Warn("gossip failure detector unavailable; using heartbeats only", "err", gerr)
		} else {
			livenessMon.WithGossip(g)
			defer func() { _ = g.Shutdown() }()
		}
	}
	go livenessMon.Run(ctx)

	// Scheduler (T-23): the leader reconciles desired replica counts into
	// assignments. Leader-gated internally.
	go scheduler.New(rs, clk, log).Run(ctx)

	// Autoscaler (T-61): the leader adjusts effective_replicas from observed
	// CPU/memory/RPS against each env's Autoscale targets. Leader-gated internally.
	go scheduler.NewAutoscaler(rs, live, clk, log).Run(ctx)

	// Scale-to-zero (T-69): the leader cools an idle scale_to_zero env down to
	// zero replicas after its idle_timeout; the activator (T-70) wakes it.
	go scheduler.NewScaleToZero(rs, live, clk, log).Run(ctx)

	// Scheduled volume snapshots (T-65): the leader fires SnapshotPolicy.schedule
	// snapshots and enforces keep_last. Only when snapshots are available.
	if snapDispatcher != nil {
		go scheduler.NewSnapshotScheduler(rs, snapDispatcher, clk, log).Run(ctx)
	}

	// Deployment orchestrator (T-26): the leader drives red/green deployments
	// through their phases. Leader-gated internally.
	orch := scheduler.NewOrchestrator(rs, clk, log)
	if cfg.Dev {
		orch.SetDrainWindow(devDrainWindow) // fast local iteration; prod keeps 10m
	}
	go orch.Run(ctx)

	// Release retention (T-38): the leader prunes old releases + their registry
	// images and stale source tarballs. The registry sweeper is wired when this
	// control node hosts a local registry.
	go scheduler.NewRetention(rs, clk, retentionSweeper, uploadsDir, log).Run(ctx)

	// Build dispatcher (T-35/T-54): the leader assigns QUEUED builds to builder
	// nodes over their AgentLocalService and records the outcome. Leader-gated.
	_, apiPortStr, _ := net.SplitHostPort(cfg.API.Listen)
	if apiPortStr == "" {
		apiPortStr = "8443"
	}
	go scheduler.NewBuildDispatcher(rs, clk,
		scheduler.GRPCBuildDialer{Connect: agentLocalConnect},
		scheduler.BuildDispatcherConfig{
			SourceURLBase: "https://" + net.JoinHostPort(agentHostIP(cfg), apiPortStr) + "/internal/blobs/",
			RegistryAddr:  registryClientAddr(cfg),
			LocalLoad:     cfg.Dev,
		}, log).Run(ctx)

	// Mesh (T-19): the hub device is already up (brought up before raft so it
	// could bind the mesh IP). Record its identity in state and start peer sync
	// now that raft + the API are up. The device is owned/torn down by the caller.
	// A joined control node passes controlMesh=nil — it set up its own spoke mesh.
	if controlMesh != nil && rs.IsLeader() {
		registerControlMesh(ctx, cfg, rs, controlMesh, authority, nodeID, log)
	}

	// DERP-lite relay (T-58): every mesh-enabled control node runs the TCP relay
	// so meshsock nodes can fall back to it when no UDP path works.
	if !cfg.Mesh.Disabled && meshIP != "" {
		startRelayServer(ctx, authority, cfg, log)
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

		// Dev is ephemeral: reap this node's managed containers on graceful
		// shutdown so a dev run leaves nothing behind. Production keeps them
		// running so a daemon restart re-adopts them without downtime.
		if cfg.Dev && rt != nil {
			defer reapManagedContainers(rt, log)
		}

		// Trust the embedded registry's TLS so this node can pull cluster-built
		// images (co-located control+worker registry is at its own mesh addr).
		if !cfg.Dev && rt != nil {
			ensureDockerRegistryTrust(registryClientAddr(cfg), authority.CABundlePEM(), log)
		}

		// Registry credential for the co-located node: in production the
		// embedded registry requires auth, but only joining workers are minted
		// one (T-17) — without this, the control node's builder cannot push and
		// its executor cannot pull cluster-built images. Mint a fresh credential
		// each boot under the same registry/creds/<id> key the join flow uses.
		var selfRegAuth *crt.RegistryAuth
		if !cfg.Dev && rs.IsLeader() {
			if pw, rerr := randomHex(24); rerr != nil {
				log.Warn("registry self-credential: generate", "err", rerr)
			} else if aerr := apply(ctx, rs, &clusterv1.Command{Mutation: &clusterv1.Command_PutKv{PutKv: &clusterv1.PutKV{
				Key:             "registry/creds/" + nodeID,
				Value:           []byte(api.HashToken(pw)),
				ExpectedVersion: -1,
			}}}); aerr != nil {
				log.Warn("registry self-credential: store", "err", aerr)
			} else {
				selfRegAuth = &crt.RegistryAuth{Username: "node-" + nodeID, Password: pw, ServerAddress: registryClientAddr(cfg)}
			}
		}

		metricsStore := openMetricsStore(cfg, clk, log)
		defer func() { _ = metricsStore.Close() }()
		na := agent.New(agent.Config{
			NodeID:       nodeID,
			Version:      version.Version,
			Clock:        clk,
			Logger:       log,
			DiskPath:     cfg.DataDir,
			Runtime:      rt,
			HostIP:       agentHostIP(cfg),
			RegistryAuth: selfRegAuth,
			Dial:         localAgentDialer(authority, nodeID, cfg.API.Listen, log),
			Store:        metricsStore,
			ProxyStats: func() map[string]*clusterv1.ProxySample {
				if s := proxyStats.Load(); s != nil {
					return s.Snapshot()
				}
				return nil
			},
		})
		go func() {
			if err := na.Run(ctx); err != nil && ctx.Err() == nil {
				log.Warn("node agent stopped", "err", err)
			}
		}()

		// Internal service mesh (F26): VIP proxy + internal DNS, reading routes
		// in-process from the RouteBuilder. Runs on the co-located control+worker
		// so its own containers can reach <svc>.internal.
		if !cfg.Dev && rt != nil {
			startInternalMesh(ctx, routeSource, na.Executor(), log)
		}

		// Agent-local service (T-54): serve build/exec/port-forward/logs on :8444
		// with node mTLS so the control plane can dispatch builds, interactive
		// sessions and log queries to this node. Needs a container runtime.
		if rt != nil {
			// Per-node log capture: follow container logs into the logstore and
			// serve them via AgentLocalService.QueryLogs.
			var logSrv *agent.LogServer
			if store, err := logstore.New(logstore.Options{Root: filepath.Join(cfg.DataDir, "logs"), Clock: clk}); err != nil {
				log.Warn("logstore init failed; logs unavailable", "err", err)
			} else {
				capture := agent.NewLogCapture(rt, store, log)
				go capture.Run(ctx)
				logSrv = agent.NewLogServer(store, na.Executor().MatchingStreams)
			}
			var pushAuth builder.RegistryAuth
			if selfRegAuth != nil {
				pushAuth = builder.RegistryAuth{Registry: selfRegAuth.ServerAddress, Username: selfRegAuth.Username, Password: selfRegAuth.Password}
			}
			// Serve this node's historical metrics from its ring TSDB (T-60).
			statsSrv := agent.NewStatsServer(metricsStore, nodeID, clk)
			if err := startAgentLocalServer(ctx, authority, cfg, nodeID, rt, clk, logSrv, statsSrv, pushAuth, log); err != nil {
				log.Warn("agent-local service failed to start", "err", err)
			}
		}
	}

	select {
	case <-ctx.Done():
		log.Info("shutting down")
		return nil
	case err := <-apiErr:
		return err
	}
}

// reapManagedContainers force-removes every container this node manages. Used on
// dev shutdown (ctx is already canceled, so it runs on a fresh bounded context).
func reapManagedContainers(rt crt.ContainerRuntime, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	infos, err := rt.ListContainers(ctx, nil) // ListContainers always filters on ManagedLabel
	if err != nil {
		log.Warn("dev shutdown: list managed containers", "err", err)
		return
	}
	for _, in := range infos {
		if err := rt.RemoveContainer(ctx, in.ID, true); err != nil {
			log.Warn("dev shutdown: remove container", "id", in.ID, "err", err)
		}
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
	// Worker nodes can build (T-54): label them builder=true so the dispatcher
	// can place builds. Single-node/dev thus builds locally. The os-arch label
	// mirrors the authoritative OsArch field for label-based constraints.
	labels := map[string]string{"zattera.dev/os-arch": platform.Local()}
	if cfg.HasRole(config.RoleWorker) {
		labels["builder"] = "true"
	}
	node := &zatterav1.Node{
		Meta:           &zatterav1.Meta{Id: nodeID, CreatedAt: now, UpdatedAt: now},
		Name:           cfg.NodeName,
		Roles:          roles,
		Labels:         labels,
		Status:         zatterav1.NodeStatus_NODE_STATUS_ALIVE,
		Schedulable:    true,
		Capacity:       &zatterav1.ResourceLimits{CpuMillis: capacity.CPUMillis, MemoryMb: capacity.MemoryMB},
		CapacityDiskMb: capacity.DiskMB,
		OsArch:         platform.Local(),
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
// the public port over loopback); this node's own mesh IP is added when the mesh
// is enabled so peers can verify the API cert over the tunnel. meshIP is the
// node's actual mesh address (10.90.0.1 for the bootstrap node, its allocated
// IP for a joined control node), NOT the hardcoded hub address.
func serverIPs(meshIP string) []net.IP {
	ips := []net.IP{net.ParseIP("127.0.0.1")}
	if meshIP != "" {
		if parsed := net.ParseIP(meshIP); parsed != nil {
			ips = append(ips, parsed)
		}
	}
	return ips
}

// leaderAPIResolver maps the current raft leader to its API address for
// leader-forwarding. Returns "" when this node is the leader. It prefers the
// leader's mesh IP: that address is a SAN on the leader's API cert AND every
// control node peers with it directly, so the forwarded dial both verifies and
// routes. In a mesh cluster the raft transport address is already the mesh
// IP:port, so it is the reliable fallback.
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
		if id == "" {
			return "", fmt.Errorf("daemon: leader unknown")
		}
		if n, ok := st.Node(id); ok && n.GetMeshIp() != "" {
			return net.JoinHostPort(n.GetMeshIp(), apiPort), nil
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

// runJoinedControl brings up a node that joined with the control role as a full
// control-plane member (T-55b): it installs the handed-over cluster CA + data
// key, joins the raft quorum over the mTLS transport bound to its mesh IP, then
// runs the shared control stack. The leader enrolls it (AddVoter) once its raft
// transport is reachable (see api.enrollControlVoter).
func runJoinedControl(ctx context.Context, cfg config.Config, jr *joinResult, log *slog.Logger) error {
	h := jr.handover
	if h == nil || h.raftBindAddr == "" {
		return fmt.Errorf("daemon: control join without a raft handover (control HA requires the mesh)")
	}

	// Bring the mesh up first (spoke to the existing hub) so this node's mesh IP
	// — the raft bind address — is a real local interface before raft binds it.
	if jr.MeshEnabled {
		meshMgr, err := startWorkerMesh(ctx, cfg, jr, log)
		if err != nil {
			return fmt.Errorf("daemon: joined control mesh: %w", err)
		}
		defer func() { _ = meshMgr.Down(context.Background()) }()
	}

	// Install the handed-over cluster CA (cert + private key) so this node signs
	// node certs and serves its own API cert like any control node, then load it.
	if err := persistHandoverCA(cfg.DataDir, jr.caPEM, h.caKeyPEM); err != nil {
		return err
	}
	authority, err := ca.LoadOrCreate(cfg.DataDir)
	if err != nil {
		return err
	}

	// Rebuild the cluster keyring/sealer from the handed-over data key so this
	// node comes up already unsealed (no recovery passphrase needed).
	keyring, err := secrets.NewKeyring(h.dataKey, h.dataKeyVersion)
	if err != nil {
		return fmt.Errorf("daemon: keyring from handover: %w", err)
	}
	sealer, err := keyring.Sealer()
	if err != nil {
		return err
	}

	// Join the raft quorum over the mTLS transport bound to our mesh IP. We start
	// Bootstrap=false and empty: the leader AddVoters us once it sees our
	// transport is up, then replicates the log + configuration to us.
	nodeCert, err := tls.X509KeyPair(jr.certPEM, jr.keyPEM)
	if err != nil {
		return fmt.Errorf("daemon: node cert: %w", err)
	}
	trans, err := raftstore.NewTLSTransport(h.raftBindAddr, h.raftBindAddr, nodeCert, authority.Pool(), os.Stderr)
	if err != nil {
		return err
	}
	rs, err := raftstore.New(raftstore.Config{
		NodeID:    jr.NodeID,
		DataDir:   filepath.Join(cfg.DataDir, "raft"),
		Transport: trans,
		Bootstrap: false,
		Logger:    log,
	}, state.New())
	if err != nil {
		return err
	}
	defer func() { _ = rs.Shutdown() }()

	log.Info("joined control node awaiting raft enrollment", "node", jr.NodeID, "raft_addr", h.raftBindAddr)
	if err := rs.WaitForLeader(ctx); err != nil {
		return fmt.Errorf("daemon: joined control node never saw a leader (enrollment failed?): %w", err)
	}
	log.Info("joined control plane", "node", jr.NodeID)

	return runControlPlane(ctx, cfg, rs, jr.NodeID, jr.MeshIP, authority, sealer, keyring, nil, log)
}

// raftPortOf returns the raft transport port from config (default 7480).
func raftPortOf(cfg config.Config) string {
	_, port, _ := net.SplitHostPort(cfg.Raft.Listen)
	if port == "" {
		port = "7480"
	}
	return port
}

// controlRaftTransport builds the mTLS raft transport for a control node bound to
// its mesh address, presenting a freshly issued node identity cert.
func controlRaftTransport(authority *ca.CA, nodeID, meshIP, bindAddr string, _ *slog.Logger) (raft.Transport, error) {
	leaf, err := authority.IssueNode(nodeID, net.ParseIP(meshIP), ca.NodeCertTTL)
	if err != nil {
		return nil, fmt.Errorf("daemon: raft node cert: %w", err)
	}
	cert, err := leaf.TLSCertificate(authority.CABundlePEM())
	if err != nil {
		return nil, fmt.Errorf("daemon: raft node tls cert: %w", err)
	}
	return raftstore.NewTLSTransport(bindAddr, bindAddr, cert, authority.Pool(), os.Stderr)
}

// persistHandoverCA writes the handed-over cluster CA cert + private key to the
// standard <data-dir>/ca location so ca.LoadOrCreate loads it — a joined control
// node shares the cluster root (T-55b).
func persistHandoverCA(dataDir string, caCertPEM, caKeyPEM []byte) error {
	if len(caCertPEM) == 0 || len(caKeyPEM) == 0 {
		return fmt.Errorf("daemon: control handover is missing CA material")
	}
	caDir := filepath.Join(dataDir, "ca")
	if err := os.MkdirAll(caDir, 0o700); err != nil {
		return fmt.Errorf("daemon: ca dir: %w", err)
	}
	if err := os.WriteFile(filepath.Join(caDir, "ca.crt"), caCertPEM, 0o600); err != nil {
		return fmt.Errorf("daemon: write ca.crt: %w", err)
	}
	if err := os.WriteFile(filepath.Join(caDir, "ca.key"), caKeyPEM, 0o600); err != nil {
		return fmt.Errorf("daemon: write ca.key: %w", err)
	}
	return nil
}

// startGossip brings up the memberlist failure detector on this control node's
// mesh IP (T-56), seeded with the other control nodes' mesh IPs. The gossip
// encryption key derives from the cluster CA hash, so only cluster members can
// join or read it.
func startGossip(meshIP, nodeID string, authority *ca.CA, st *state.Store, log *slog.Logger) (*mesh.Gossip, error) {
	caSum := sha256.Sum256(authority.Certificate().Raw)
	var peers []string
	for _, n := range st.ListNodes() {
		if n.GetMeta().GetId() == nodeID || !nodeHasControlRole(n) {
			continue
		}
		if ip := n.GetMeshIp(); ip != "" {
			peers = append(peers, ip)
		}
	}
	return mesh.NewGossip(mesh.GossipConfig{
		NodeID:   nodeID,
		BindAddr: meshIP,
		Peers:    peers,
		CAHash:   caSum[:],
		Clock:    clock.Real{},
		Logger:   log,
	})
}

// nodeHasControlRole reports whether a node carries the control-plane role.
func nodeHasControlRole(n *zatterav1.Node) bool {
	for _, r := range n.GetRoles() {
		if r == zatterav1.NodeRole_NODE_ROLE_CONTROL {
			return true
		}
	}
	return false
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

	// Trust the control node's embedded registry so image pulls over its TLS
	// succeed (the join response carries the registry address + cluster CA).
	if !cfg.Dev && rt != nil {
		ensureDockerRegistryTrust(jr.RegistryAddr, jr.caPEM, log)
	}

	hostIP := jr.MeshIP
	if hostIP == "" {
		hostIP = "127.0.0.1"
	}
	var regAuth *crt.RegistryAuth
	if jr.RegistryUser != "" {
		regAuth = &crt.RegistryAuth{Username: jr.RegistryUser, Password: jr.RegistryPass, ServerAddress: jr.RegistryAddr}
	}

	metricsStore := openMetricsStore(cfg, clock.Real{}, log)
	defer func() { _ = metricsStore.Close() }()
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
		Store:        metricsStore,
	})

	// Internal service mesh: subscribe to route snapshots from control, then run
	// the VIP proxy + internal DNS so containers on this worker can reach
	// <svc>.internal (F26). Best-effort — the executor still works without it.
	if !cfg.Dev && rt != nil {
		if creds, cerr := workerTLSCreds(jr); cerr != nil {
			log.Warn("intdns: route credentials", "err", cerr)
		} else {
			rc := proxy.NewRouteClient(
				grpcRouteDialer{addr: jr.ControlGRPCAddr, nodeID: jr.NodeID, creds: creds},
				jr.NodeID, filepath.Join(cfg.DataDir, "proxy", "routes.pb"), log)
			go rc.Run(ctx)
			startInternalMesh(ctx, rc, na.Executor(), log)
		}
	}

	log.Info("worker started", "node", jr.NodeID, "control", jr.ControlGRPCAddr)
	return na.Run(ctx)
}

// workerTLSCreds builds the node-mTLS transport credentials a worker uses to
// dial the control plane: the join-issued node cert/key, trusting the cluster CA
// from the join response, with the control host as SNI.
func workerTLSCreds(jr *joinResult) (credentials.TransportCredentials, error) {
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
	return credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ServerName:   host,
	}), nil
}

// workerAgentDialer dials the control plane's AgentSync over mTLS with the
// node's signed identity, trusting the cluster CA from the join response.
func workerAgentDialer(jr *joinResult) (func(context.Context) (*agent.Conn, error), error) {
	creds, err := workerTLSCreds(jr)
	if err != nil {
		return nil, err
	}
	return func(context.Context) (*agent.Conn, error) {
		conn, err := grpc.NewClient(jr.ControlGRPCAddr, grpc.WithTransportCredentials(creds))
		if err != nil {
			return nil, err
		}
		return &agent.Conn{ClientConnInterface: conn, Close: conn.Close}, nil
	}, nil
}

// openMetricsStore opens this node's ring TSDB (T-59) under the data dir. The
// metrics sampler records node/instance/proxy series into it; T-60 serves it via
// AgentLocalService.
func openMetricsStore(cfg config.Config, clk clock.Clock, log *slog.Logger) *tsdb.RingStore {
	return tsdb.Open(tsdb.Config{
		Path:   filepath.Join(cfg.DataDir, "metrics", "tsdb.bin"),
		Clock:  clk,
		Logger: log,
	})
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

// caFingerprint is the sha256 (hex) of the cluster CA certificate DER — the
// value operators pass to `zattera login --ca-pin` (mirrors the join token's
// CA hash). (T-90)
func caFingerprint(authority *ca.CA) string {
	sum := sha256.Sum256(authority.Certificate().Raw)
	return hex.EncodeToString(sum[:])
}

// publicAPIHost returns the API's public hostname eligible for a public (ACME)
// certificate: the host part of api.advertise_addr when production ACME is on
// and that host is a real DNS name (not empty, not an IP). Otherwise "" —
// the API keeps its cluster-CA cert. (T-90)
func publicAPIHost(cfg config.Config) string {
	if cfg.Dev || cfg.ACME.Disabled {
		return ""
	}
	addr := cfg.API.AdvertiseAddr
	if addr == "" {
		return ""
	}
	host := addr
	if h, _, err := net.SplitHostPort(addr); err == nil {
		host = h
	}
	if host == "" || net.ParseIP(host) != nil {
		return "" // ACME cannot issue for an IP/loopback
	}
	return host
}

// ensureDockerRegistryTrust writes the cluster CA into Docker's per-registry
// trust store (/etc/docker/certs.d/<addr>/ca.crt) so this node can pull
// cluster-built images over the embedded registry's TLS. Best-effort: a failure
// is logged, not fatal. No-op in dev (the registry is plain HTTP).
func ensureDockerRegistryTrust(registryAddr string, caPEM []byte, log *slog.Logger) {
	if registryAddr == "" || len(caPEM) == 0 {
		return
	}
	dir := filepath.Join("/etc/docker/certs.d", registryAddr)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Warn("registry trust: mkdir", "dir", dir, "err", err)
		return
	}
	path := filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(path, caPEM, 0o644); err != nil {
		log.Warn("registry trust: write", "path", path, "err", err)
		return
	}
	log.Info("registry CA trust installed", "registry", registryAddr)
}

// registryClientAddr is the "host:port" that builder + executor containers use
// to reach the embedded registry. In dev the registry runs on the host, so
// containers reach it via host.docker.internal (Docker Desktop and Linux with
// host-gateway); in a real cluster it is the control node's mesh address.
func registryClientAddr(cfg config.Config) string {
	_, port, _ := net.SplitHostPort(cfg.Registry.Listen)
	if port == "" {
		port = "5000"
	}
	if cfg.Dev {
		return net.JoinHostPort("host.docker.internal", port)
	}
	return net.JoinHostPort(agentHostIP(cfg), port)
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

// agentLocalPort is the node-mTLS AgentLocalService port (build/log/exec).
const agentLocalPort = "8444"

// newAgentLocalConnect returns a Connect func the control plane uses to dial a
// node's AgentLocalService (:8444) over node mTLS. It targets the node's mesh IP
// (single-node/dev falls back to loopback).
func newAgentLocalConnect(authority *ca.CA, nodeID string, _ *slog.Logger) func(context.Context, *zatterav1.Node) (*grpc.ClientConn, error) {
	return func(_ context.Context, node *zatterav1.Node) (*grpc.ClientConn, error) {
		host := node.GetMeshIp()
		if host == "" {
			host = "127.0.0.1"
		}
		tlsCfg := nodeClientTLS(authority, nodeID, host)
		return grpc.NewClient(net.JoinHostPort(host, agentLocalPort),
			grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	}
}

// startAgentLocalServer binds the node-local AgentLocalService on :8444 (node
// mTLS) serving builds + exec/top/port-forward. The build sub-server needs a
// container runtime + a source fetcher that pulls tarballs from the control
// blob endpoint over the same node identity.
func startAgentLocalServer(ctx context.Context, authority *ca.CA, cfg config.Config, nodeID string, rt crt.ContainerRuntime, clk clock.Clock, logSrv *agent.LogServer, statsSrv *agent.StatsServer, pushAuth builder.RegistryAuth, log *slog.Logger) error {
	tlsCfg := nodeClientTLS(authority, nodeID, "127.0.0.1")
	fetch := agent.HTTPSourceFetcher{Client: &http.Client{
		Timeout:   10 * time.Minute,
		Transport: &http.Transport{TLSClientConfig: tlsCfg},
	}}

	caPath := filepath.Join(cfg.DataDir, "ca", "ca.crt")
	bld := builder.New(rt, clk, cfg.DataDir, caPath, log)
	buildSrv := agent.NewBuildServer(agent.BuildServerConfig{
		Builder:          bld,
		Fetch:            fetch,
		RegistryAuth:     pushAuth,
		RegistryInsecure: cfg.Registry.InsecureHTTP,
		LocalLoad:        cfg.Dev,
		Logger:           log,
	})
	execSrv := agent.NewExecServer(rt, log)
	local := agent.NewLocalServer(buildSrv, execSrv, logSrv, statsSrv, rt)

	serverTLS, err := authority.ServerTLSConfig([]string{"localhost"}, agentLocalIPs(cfg))
	if err != nil {
		return fmt.Errorf("server tls: %w", err)
	}
	lis, err := net.Listen("tcp", ":"+agentLocalPort)
	if err != nil {
		return fmt.Errorf("listen :%s: %w", agentLocalPort, err)
	}
	grpcSrv := grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLS)))
	clusterv1.RegisterAgentLocalServiceServer(grpcSrv, local)
	go func() {
		if err := grpcSrv.Serve(lis); err != nil && ctx.Err() == nil {
			log.Warn("agent-local server stopped", "err", err)
		}
	}()
	go func() { <-ctx.Done(); grpcSrv.GracefulStop() }()
	log.Info("agent-local service listening", "addr", lis.Addr().String())
	return nil
}

// agentLocalIPs returns the IP SANs for the :8444 server cert: loopback plus
// the mesh IP when present.
func agentLocalIPs(cfg config.Config) []net.IP {
	ips := []net.IP{net.ParseIP("127.0.0.1")}
	if ip := controlMeshIP(cfg); ip != "" {
		if p := net.ParseIP(ip); p != nil {
			ips = append(ips, p)
		}
	}
	return ips
}
