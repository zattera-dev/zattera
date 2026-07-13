// Package commands assembles the cobra command tree. CLI commands and server
// commands are registered from build-tagged files (ADR-0002): this file and
// Execute always compile.
package commands

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/zattera-dev/zattera/internal/pkgutil/version"
)

var root = &cobra.Command{
	Use:           "zattera",
	Short:         "Zattera — the single-binary PaaS",
	SilenceUsage:  true,
	SilenceErrors: false,
}

func init() {
	root.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Print the binary version",
		Run: func(cmd *cobra.Command, _ []string) {
			fmt.Fprintln(cmd.OutOrStdout(), version.Version)
		},
	})
}

// Register adds a top-level command (called from build-tagged registration
// files' init()).
func Register(cmds ...*cobra.Command) {
	root.AddCommand(cmds...)
}

// Execute runs the CLI.
func Execute() error {
	return root.Execute()
}
