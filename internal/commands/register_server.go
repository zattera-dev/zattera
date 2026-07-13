//go:build !cli_only

package commands

import "github.com/zattera-dev/zattera/internal/daemon"

// This file is the ONLY importer of internal/daemon. Excluding it via the
// cli_only tag drops the whole daemon tree from the binary (ADR-0002).
func init() {
	Register(daemon.Commands()...)
}
