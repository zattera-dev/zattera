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
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/zattera-dev/zattera/internal/config"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
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

	if cfg.Join.Addr != "" {
		// TODO(T-19): join flow (CSR → control plane → certs + mesh config).
		return fmt.Errorf("daemon: --join is not implemented yet (task T-19)")
	}
	if !cfg.HasRole(config.RoleControl) {
		return fmt.Errorf("daemon: worker-only mode requires --join (task T-19)")
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

	// TODO(T-04..T-06): bootstrap org/admin/token, cluster CA, API server.
	// TODO(T-16): start the agent when the node has the worker role.

	<-ctx.Done()
	log.Info("shutting down")
	return nil
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
