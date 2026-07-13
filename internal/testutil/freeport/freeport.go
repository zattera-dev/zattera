// Package freeport hands out free TCP ports for tests.
package freeport

import (
	"net"
	"testing"
)

// Get returns a free TCP port on 127.0.0.1. There is an inherent race between
// closing the probe listener and the test binding it; keep usage close.
func Get(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freeport: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
