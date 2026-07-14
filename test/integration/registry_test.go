//go:build integration

package integration

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/zattera-dev/zattera/internal/daemon/registry"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// startRegistry serves the embedded registry over plain HTTP on a loopback
// port. Docker treats 127.0.0.0/8 registries as insecure by default, so no
// daemon configuration is needed. Returns the host:port and the live registry.
func startRegistry(t *testing.T) (string, *registry.Registry) {
	t.Helper()
	reg, err := registry.New(t.TempDir(), clock.Real{}, nil, nil)
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := &http.Server{Handler: reg.Handler(), ReadHeaderTimeout: 30 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		_ = srv.Close()
		_ = reg.Close()
	})
	return ln.Addr().String(), reg
}

// TestRegistryPushPull is the T-32 acceptance: a real docker push + pull
// round-trip of the go-hello image against the embedded registry.
func TestRegistryPushPull(t *testing.T) {
	RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	addr, reg := startRegistry(t)
	ref := addr + "/demo/go-hello:v1"

	build := exec.CommandContext(ctx, "docker", "build", "-t", ref, FixtureDir(t, "go-hello"))
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("docker build: %v\n%s", err, out)
	}
	t.Cleanup(func() { _ = exec.Command("docker", "rmi", "-f", ref).Run() })

	if out, err := exec.CommandContext(ctx, "docker", "push", ref).CombinedOutput(); err != nil {
		t.Fatalf("docker push: %v\n%s", err, out)
	}

	// tags/list reflects the pushed tag.
	tags := httpGet(t, "http://"+addr+"/v2/demo/go-hello/tags/list")
	if !strings.Contains(tags, `"v1"`) {
		t.Fatalf("tags/list missing v1: %s", tags)
	}

	// Remove the local image, then pull it back from the registry.
	if err := exec.Command("docker", "rmi", "-f", ref).Run(); err != nil {
		t.Fatalf("docker rmi: %v", err)
	}
	if out, err := exec.CommandContext(ctx, "docker", "pull", ref).CombinedOutput(); err != nil {
		t.Fatalf("docker pull: %v\n%s", err, out)
	}

	// A single-arch push stores a plain manifest; Platforms reports at most the
	// one platform (config-derived arch is out of scope here, so it may be one
	// entry or none — the manifest must at least resolve).
	if _, _, _, err := reg.Manifests.GetManifest("demo/go-hello", "v1"); err != nil {
		t.Fatalf("manifest not resolvable after push: %v", err)
	}
}

// TestRegistryMultiArchIndex pushes a two-platform image index via buildx and
// asserts the registry stored a multi-arch index whose children resolve per
// platform. Skips when buildx / a container builder is unavailable.
func TestRegistryMultiArchIndex(t *testing.T) {
	RequireDocker(t)
	if err := exec.Command("docker", "buildx", "version").Run(); err != nil {
		t.Skipf("buildx unavailable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	builder := fmt.Sprintf("zt-it-%d", time.Now().UnixNano())
	if out, err := exec.CommandContext(ctx, "docker", "buildx", "create",
		"--name", builder, "--driver", "docker-container").CombinedOutput(); err != nil {
		t.Skipf("cannot create buildx builder: %v\n%s", err, out)
	}
	t.Cleanup(func() { _ = exec.Command("docker", "buildx", "rm", "-f", builder).Run() })

	addr, reg := startRegistry(t)
	ref := addr + "/demo/go-hello:multi"

	build := exec.CommandContext(ctx, "docker", "buildx", "build",
		"--builder", builder,
		"--platform", "linux/amd64,linux/arm64",
		"--output", "type=image,name="+ref+",push=true,registry.insecure=true",
		FixtureDir(t, "go-hello"))
	if out, err := build.CombinedOutput(); err != nil {
		t.Skipf("multi-arch buildx build/push failed (needs QEMU emulation): %v\n%s", err, out)
	}

	// The registry served an index; both architectures resolve.
	plats, err := reg.Manifests.Platforms("demo/go-hello", "multi")
	if err != nil {
		t.Fatalf("platforms: %v", err)
	}
	if !containsStr(plats, "linux/amd64") || !containsStr(plats, "linux/arm64") {
		t.Fatalf("expected both arches, got %v", plats)
	}
	for _, p := range []string{"linux/amd64", "linux/arm64"} {
		if _, _, err := reg.Manifests.ResolveManifest("demo/go-hello", "multi", p); err != nil {
			t.Fatalf("resolve %s: %v", p, err)
		}
	}
}

func httpGet(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

func containsStr(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}
