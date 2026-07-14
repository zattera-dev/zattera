//go:build !windows

package cli

import (
	"os"
	"os/signal"

	"golang.org/x/sys/unix"
)

// notifyWinch relays terminal-resize signals (SIGWINCH) to ch.
func notifyWinch(ch chan os.Signal) { signal.Notify(ch, unix.SIGWINCH) }
