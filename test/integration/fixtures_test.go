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
)

// TestGoHelloFixture builds and runs the go-hello fixture with real Docker,
// asserting the HTTP contract every later pipeline test relies on
// (/ echoes FIXTURE_MESSAGE, /healthz returns ok). It doubles as the living
// example of the integration test tier.
func TestGoHelloFixture(t *testing.T) {
	RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	image := "zattera-fixture-go-hello:test"
	build := exec.CommandContext(ctx, "docker", "build", "-t", image, FixtureDir(t, "go-hello"))
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("docker build: %v\n%s", err, out)
	}

	port := freePort(t)
	name := fmt.Sprintf("zattera-fixture-test-%d", time.Now().UnixNano())
	run := exec.CommandContext(ctx, "docker", "run", "-d", "--rm",
		"--name", name,
		"-e", "FIXTURE_MESSAGE=integration-check",
		"-p", fmt.Sprintf("127.0.0.1:%d:8080", port),
		image,
	)
	if out, err := run.CombinedOutput(); err != nil {
		t.Fatalf("docker run: %v\n%s", err, out)
	}
	t.Cleanup(func() {
		_ = exec.Command("docker", "rm", "-f", name).Run()
	})

	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	waitHTTP(t, base+"/healthz", 30*time.Second)

	resp, err := http.Get(base + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if got := strings.TrimSpace(string(body)); got != "integration-check" {
		t.Fatalf("fixture body = %q, want %q", got, "integration-check")
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func waitHTTP(t *testing.T, url string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("endpoint %s never became healthy", url)
}
