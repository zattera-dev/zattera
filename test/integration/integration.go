//go:build integration

// Package integration holds Docker-backed integration tests (`make
// test-integration`). Tests must skip cleanly when Docker is unreachable so
// the tag can run everywhere.
package integration

import (
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

// RequireDocker skips the test when no Docker daemon answers.
func RequireDocker(t *testing.T) {
	t.Helper()
	if err := exec.Command("docker", "version", "--format", "{{.Server.Version}}").Run(); err != nil {
		t.Skipf("integration: docker daemon not available: %v", err)
	}
}

// FixtureDir returns the absolute path of test/fixtures/apps/<name>.
func FixtureDir(t *testing.T, name string) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("integration: cannot locate source file")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "fixtures", "apps", name)
}
