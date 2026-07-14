//go:build windows

package cli

import "os"

// notifyWinch is a no-op on Windows (no SIGWINCH); the initial size is used.
func notifyWinch(chan os.Signal) {}
