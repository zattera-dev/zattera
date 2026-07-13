//go:build !server_only

package commands

import "github.com/zattera-dev/zattera/internal/cli"

// This file is the ONLY importer of internal/cli. Excluding it via the
// server_only tag drops the whole CLI tree from the binary (ADR-0002).
func init() {
	Register(cli.Commands()...)
}
