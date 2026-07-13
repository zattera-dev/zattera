//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"
)

// meshTestImage is a small glibc-free base; the zattera binary is built static
// (CGO_ENABLED=0) so it runs directly. iproute2 is installed for the userspace
// WireGuard address/route setup.
const meshTestImage = "alpine:3.20"

// dockerServerArch returns the Docker engine's architecture as a GOARCH value
// (the bind-mounted binary must match the VM the containers run in).
func dockerServerArch(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("docker", "version", "--format", "{{.Server.Arch}}").Output()
	if err != nil {
		t.Skipf("integration: cannot query docker arch: %v", err)
	}
	arch := strings.TrimSpace(string(out))
	switch arch {
	case "amd64", "arm64":
		return arch
	case "x86_64":
		return "amd64"
	case "aarch64":
		return "arm64"
	default:
		return arch
	}
}

// buildLinuxBinary builds a static linux/<arch> zattera binary into a temp dir
// and returns its path.
func buildLinuxBinary(t *testing.T, arch string) string {
	t.Helper()
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Join(filepath.Dir(thisFile), "..", "..")
	out := filepath.Join(t.TempDir(), "zattera")

	cmd := exec.Command("go", "build", "-o", out, "./cmd/zattera")
	cmd.Dir = repoRoot
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux", "GOARCH="+arch)
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build linux binary: %v\n%s", err, b)
	}
	return out
}

// createNetwork makes a user-defined bridge with a fixed subnet so nodes get
// predictable IPs, and cleans it up.
func createNetwork(t *testing.T, subnet string) string {
	t.Helper()
	name := "zt-it-" + randSuffix()
	if b, err := exec.Command("docker", "network", "create", "--subnet", subnet, name).CombinedOutput(); err != nil {
		t.Fatalf("docker network create: %v\n%s", err, b)
	}
	t.Cleanup(func() { _ = exec.Command("docker", "network", "rm", name).Run() })
	return name
}

// runNode starts a privileged container running `zattera <serverArgs...>`. The
// binary is bind-mounted; iproute2 is installed before the daemon starts.
func runNode(t *testing.T, name, network, ip, binPath string, files map[string]string, serverArgs ...string) {
	t.Helper()
	args := []string{
		"run", "-d", "--rm", "--name", name,
		"--privileged", "--cap-add", "NET_ADMIN", "--device", "/dev/net/tun",
		"--network", network, "--ip", ip,
		"-v", binPath + ":/usr/local/bin/zattera:ro",
	}
	for hostRel, content := range files {
		dir := filepath.Join(t.TempDir(), name)
		_ = os.MkdirAll(dir, 0o755)
		p := filepath.Join(dir, filepath.Base(hostRel))
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", hostRel, err)
		}
		args = append(args, "-v", p+":"+hostRel+":ro")
	}
	shell := "apk add --no-cache iproute2 >/dev/null 2>&1 || true; exec zattera " + strings.Join(serverArgs, " ")
	args = append(args, meshTestImage, "sh", "-c", shell)

	out, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker run %s: %v\n%s", name, err, out)
	}
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", name).Run() })
}

// containerLogs returns the combined stdout+stderr captured so far.
func containerLogs(t *testing.T, name string) string {
	t.Helper()
	out, _ := exec.Command("docker", "logs", name).CombinedOutput()
	return string(out)
}

// waitForLog polls a container's logs until the pattern matches, returning the
// first submatch (or the whole match if no group).
func waitForLog(t *testing.T, name string, pattern *regexp.Regexp, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if m := pattern.FindStringSubmatch(containerLogs(t, name)); m != nil {
			if len(m) > 1 {
				return m[1]
			}
			return m[0]
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %q in %s logs:\n%s", pattern, name, containerLogs(t, name))
	return ""
}

// dockerExec runs a command inside a container.
func dockerExec(t *testing.T, name string, cmd ...string) (string, error) {
	t.Helper()
	args := append([]string{"exec", name}, cmd...)
	out, err := exec.CommandContext(context.Background(), "docker", args...).CombinedOutput()
	return string(out), err
}

var suffixSeq int

func randSuffix() string {
	suffixSeq++
	return fmt.Sprintf("%d-%d", os.Getpid(), suffixSeq)
}
