// Package cli implements every user-facing command. All commands are pure
// API clients (pkg/apiclient) — no hidden channels (spec F17).
//
// Foundation status: `login` manages contexts locally; the remaining
// commands are added by TASKS.md P1/P3/P5 tasks. Keep each command in its
// own file (deploy.go, logs.go, env.go, ...).
package cli

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/zattera-dev/zattera/internal/cli/cliconfig"
	"github.com/zattera-dev/zattera/pkg/apiclient"
)

// jsonFlag is the global --json toggle shared by all CLI commands.
var jsonFlag bool

// Commands returns all user-facing top-level commands.
func Commands() []*cobra.Command {
	cmds := []*cobra.Command{
		newLoginCmd(),
		newContextCmd(),
		newProjectsCmd(),
		newAppsCmd(),
		newEnvCmd(),
		newInitCmd(),
		newApplyCmd(),
		newDeployCmd(),
		newPsCmd(),
		newReleasesCmd(),
		newRollbackCmd(),
		newStateCmd(),
		newNodesCmd(),
		newGitHubCmd(),
		newLogsCmd(),
		newDomainsCmd(),
		newAttachCmd(),
		newTopCmd(),
		newPortForwardCmd(),
		newFsCmd(),
		newStatsCmd(),
		newJobsCmd(),
		newVolumesCmd(),
		newBackupCmd(),
	}
	for _, c := range cmds {
		c.PersistentFlags().BoolVar(&jsonFlag, "json", false, "machine-readable output")
	}
	return cmds
}

func newLoginCmd() *cobra.Command {
	var (
		server   string
		token    string
		name     string
		caPath   string
		caPin    string
		insecure bool
	)
	cmd := &cobra.Command{
		Use:   "login",
		Short: "Authenticate against a Zattera cluster",
		Long: `Stores a context (server + token) in ~/.config/zattera/config.toml after
verifying the token with WhoAmI. Get a token from your admin or from the
bootstrap output of 'zattera server --dev'.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			p := printerFor(cmd)
			if server == "" || token == "" {
				return fmt.Errorf("both --server and --token are required (browser flow lands in M4)")
			}
			cfg, err := cliconfig.Load()
			if err != nil {
				return err
			}
			ctx := cliconfig.Context{Server: server, Token: token, Insecure: insecure}
			switch {
			case caPath != "":
				pem, err := os.ReadFile(caPath)
				if err != nil {
					return err
				}
				ctx.CACertPEM = string(pem)
			case caPin != "":
				// Trust-on-first-use: fetch the cluster CA and pin it by fingerprint
				// (shown at cluster boot / in join tokens), so no CA file is needed.
				caPEM, err := fetchPinnedCA(server, caPin)
				if err != nil {
					return fmt.Errorf("pinning CA: %w", err)
				}
				ctx.CACertPEM = caPEM
			}

			// Verify the token BEFORE persisting anything, so a bad login never
			// disturbs the existing config / active context.
			client, err := apiclient.New(apiclient.Config{
				Address: ctx.Server, Token: ctx.Token,
				CACertPEM: []byte(ctx.CACertPEM), InsecureSkipVerify: ctx.Insecure,
			})
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			rctx, cancel := context.WithTimeout(cmd.Context(), 10*time.Second)
			defer cancel()
			who, err := client.Auth.WhoAmI(rctx, &emptypb.Empty{})
			if err != nil {
				return fmt.Errorf("login failed: %w", apiError(err))
			}

			cfg.Contexts[name] = ctx
			cfg.CurrentContext = name
			if err := cfg.Save(); err != nil {
				return err
			}
			if jsonFlag {
				return p.EmitJSON(who.GetUser())
			}
			p.Successf("Logged in to %s as %s (context %q)", server, who.GetUser().GetEmail(), name)
			return nil
		},
	}
	cmd.Flags().StringVar(&server, "server", "", "API address, e.g. https://paas.example.com:8443")
	cmd.Flags().StringVar(&token, "token", "", "API token")
	cmd.Flags().StringVar(&name, "context", "default", "context name")
	cmd.Flags().StringVar(&caPath, "ca-cert", "", "path to the cluster CA certificate (self-signed/dev clusters)")
	cmd.Flags().StringVar(&caPin, "ca-pin", "", "cluster CA sha256 fingerprint; fetches+pins the CA (trust-on-first-use)")
	cmd.Flags().BoolVar(&insecure, "insecure", false, "skip TLS verification (dev only)")
	return cmd
}

func newContextCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "context",
		Short: "Show or switch CLI contexts",
		RunE: func(cmd *cobra.Command, _ []string) error {
			p := printerFor(cmd)
			cfg, err := cliconfig.Load()
			if err != nil {
				return err
			}
			if jsonFlag {
				return p.EmitJSON(cfg)
			}
			rows := make([][]string, 0, len(cfg.Contexts))
			for cname, c := range cfg.Contexts {
				current := ""
				if cname == cfg.CurrentContext {
					current = "*"
				}
				rows = append(rows, []string{current, cname, c.Server})
			}
			p.Table([]string{"", "NAME", "SERVER"}, rows)
			return nil
		},
	}
	use := &cobra.Command{
		Use:   "use <name>",
		Short: "Switch the active context",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			cfg, err := cliconfig.Load()
			if err != nil {
				return err
			}
			if _, ok := cfg.Contexts[args[0]]; !ok {
				return fmt.Errorf("unknown context %q", args[0])
			}
			cfg.CurrentContext = args[0]
			if err := cfg.Save(); err != nil {
				return err
			}
			printerFor(cmd).Successf("Switched to context %q", args[0])
			return nil
		},
	}
	cmd.AddCommand(use)
	return cmd
}
