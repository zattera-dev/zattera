// Package cli implements every user-facing command. All commands are pure
// API clients (pkg/apiclient) — no hidden channels (spec F17).
//
// Foundation status: `login` manages contexts locally; the remaining
// commands are added by TASKS.md P1/P3/P5 tasks. Keep each command in its
// own file (deploy.go, logs.go, env.go, ...).
package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/zattera-dev/zattera/internal/cli/cliconfig"
	"github.com/zattera-dev/zattera/internal/cli/ui"
)

// jsonFlag is the global --json toggle shared by all CLI commands.
var jsonFlag bool

// printer builds the shared output printer.
func printer() *ui.Printer {
	return &ui.Printer{Out: os.Stdout, Err: os.Stderr, JSON: jsonFlag}
}

// Commands returns all user-facing top-level commands.
func Commands() []*cobra.Command {
	cmds := []*cobra.Command{
		newLoginCmd(),
		newContextCmd(),
	}
	for _, c := range cmds {
		c.PersistentFlags().BoolVar(&jsonFlag, "json", false, "machine-readable output")
	}
	return cmds
}

func newLoginCmd() *cobra.Command {
	var (
		server  string
		token   string
		name    string
		caPath  string
	)
	cmd := &cobra.Command{
		Use:   "login",
		Short: "Authenticate against a Zattera cluster",
		Long: `Stores a context (server + token) in ~/.config/zattera/config.toml.
Get a token from your admin or from the bootstrap output of 'zattera server --dev'.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			p := printer()
			if server == "" || token == "" {
				return fmt.Errorf("both --server and --token are required (browser flow lands in M4)")
			}
			cfg, err := cliconfig.Load()
			if err != nil {
				return err
			}
			ctx := cliconfig.Context{Server: server, Token: token}
			if caPath != "" {
				pem, err := os.ReadFile(caPath)
				if err != nil {
					return err
				}
				ctx.CACertPEM = string(pem)
			}
			// TODO(T-11): verify the token with WhoAmI before saving.
			cfg.Contexts[name] = ctx
			cfg.CurrentContext = name
			if err := cfg.Save(); err != nil {
				return err
			}
			p.Successf("Logged in to %s (context %q)", server, name)
			return nil
		},
	}
	cmd.Flags().StringVar(&server, "server", "", "API address, e.g. https://paas.example.com:8443")
	cmd.Flags().StringVar(&token, "token", "", "API token")
	cmd.Flags().StringVar(&name, "context", "default", "context name")
	cmd.Flags().StringVar(&caPath, "ca-cert", "", "path to the cluster CA certificate (self-signed/dev clusters)")
	return cmd
}

func newContextCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "context",
		Short: "Show or switch CLI contexts",
		RunE: func(cmd *cobra.Command, _ []string) error {
			p := printer()
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
			printer().Successf("Switched to context %q", args[0])
			return nil
		},
	}
	cmd.AddCommand(use)
	return cmd
}
