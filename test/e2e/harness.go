//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// bannerDeadline bounds how long we wait for the daemon to print its DEVBANNER
// block (raft election + first-boot bootstrap).
const bannerDeadline = 60 * time.Second

// harness drives a real `zattera server --dev` process and the CLI binary
// against it. It builds the binary once, boots the daemon, parses the DEVBANNER
// machine-readable lines (T-52), and offers CLI/HTTP helpers with deadlines.
type harness struct {
	t          *testing.T
	bin        string
	dataDir    string
	configPath string
	domain     string

	daemon *exec.Cmd
	out    *syncBuffer
	banner map[string]string
	caPool *x509.CertPool
}

// newHarness builds the binary and prepares isolated data/config dirs. It skips
// the test when Docker is unavailable (the daemon needs it to run containers).
func newHarness(t *testing.T) *harness {
	t.Helper()
	requireDocker(t)

	h := &harness{
		t:          t,
		domain:     "apps.127.0.0.1.sslip.io",
		dataDir:    t.TempDir(),
		configPath: filepath.Join(t.TempDir(), "cli-config.toml"),
	}
	h.bin = buildBinary(t)
	cleanupZattera() // clear leftover managed containers/networks from prior runs
	return h
}

// cleanupZattera removes managed app containers and their per-env bridge
// networks left by earlier runs, so a fresh subnet allocation cannot overlap an
// orphaned network.
func cleanupZattera() {
	out, _ := exec.Command("docker", "ps", "-aq", "--filter", "label=dev.zattera/managed=true").Output()
	if ids := strings.Fields(string(out)); len(ids) > 0 {
		_ = exec.Command("docker", append([]string{"rm", "-f"}, ids...)...).Run()
	}
	nets, _ := exec.Command("docker", "network", "ls", "--filter", "name=zt-", "-q").Output()
	if ids := strings.Fields(string(nets)); len(ids) > 0 {
		_ = exec.Command("docker", append([]string{"network", "rm"}, ids...)...).Run()
	}
}

// requireDocker skips the test unless a Docker daemon is reachable.
func requireDocker(t *testing.T) {
	t.Helper()
	if err := exec.Command("docker", "version").Run(); err != nil {
		t.Skipf("e2e: Docker is not available: %v", err)
	}
}

// buildBinary compiles the host zattera binary into a temp dir.
func buildBinary(t *testing.T) string {
	t.Helper()
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Join(filepath.Dir(thisFile), "..", "..")
	out := filepath.Join(t.TempDir(), "zattera")
	cmd := exec.Command("go", "build", "-o", out, "./cmd/zattera")
	cmd.Dir = repoRoot
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("e2e: build binary: %v\n%s", err, b)
	}
	return out
}

// start boots the daemon in dev mode and blocks until the DEVBANNER lines are
// parsed (or bannerDeadline elapses). It registers teardown that SIGTERMs the
// daemon and asserts no managed containers survive.
func (h *harness) start() {
	h.out = &syncBuffer{}
	h.daemon = exec.Command(h.bin, "server", "--dev",
		"--data-dir", h.dataDir, "--domain", h.domain)
	h.daemon.Stdout = h.out
	h.daemon.Stderr = h.out
	if err := h.daemon.Start(); err != nil {
		h.t.Fatalf("e2e: start daemon: %v", err)
	}
	h.t.Cleanup(h.stop)

	h.banner = h.awaitBanner()
	h.caPool = h.loadCA()
}

// awaitBanner polls the daemon output until every expected DEVBANNER key is present.
func (h *harness) awaitBanner() map[string]string {
	deadline := time.Now().Add(bannerDeadline)
	want := []string{"api", "domain", "ingress_http", "ingress_https", "ca", "token"}
	for time.Now().Before(deadline) {
		banner := parseBanner(h.out.String())
		if hasAll(banner, want) {
			return banner
		}
		if h.exited() {
			h.t.Fatalf("e2e: daemon exited before banner:\n%s", h.out.String())
		}
		time.Sleep(250 * time.Millisecond)
	}
	h.t.Fatalf("e2e: DEVBANNER not seen within %s:\n%s", bannerDeadline, h.out.String())
	return nil
}

var bannerLine = regexp.MustCompile(`(?m)^DEVBANNER:([a-z_]+)=(.*)$`)

// parseBanner extracts DEVBANNER:key=value lines.
func parseBanner(s string) map[string]string {
	out := map[string]string{}
	for _, m := range bannerLine.FindAllStringSubmatch(s, -1) {
		out[m[1]] = strings.TrimSpace(m[2])
	}
	return out
}

func hasAll(m map[string]string, keys []string) bool {
	for _, k := range keys {
		if m[k] == "" {
			return false
		}
	}
	return true
}

func (h *harness) loadCA() *x509.CertPool {
	pem, err := os.ReadFile(h.banner["ca"])
	if err != nil {
		h.t.Fatalf("e2e: read CA %s: %v", h.banner["ca"], err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		h.t.Fatalf("e2e: CA is not valid PEM")
	}
	return pool
}

// login authenticates the CLI against the dev cluster using the banner secrets.
func (h *harness) login() {
	h.mustCLI("login",
		"--server", h.banner["api"],
		"--ca-cert", h.banner["ca"],
		"--token", h.banner["token"])
}

// cli runs the CLI binary with an isolated config. cwd may be "" for the repo dir.
func (h *harness) cli(cwd string, args ...string) (string, error) {
	cmd := exec.Command(h.bin, args...)
	if cwd != "" {
		cmd.Dir = cwd
	}
	cmd.Env = append(os.Environ(), "ZATTERA_CONFIG="+h.configPath, "NO_COLOR=1")
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	return out.String(), err
}

// mustCLI runs the CLI and fails the test on a non-zero exit.
func (h *harness) mustCLI(args ...string) string {
	h.t.Helper()
	out, err := h.cli("", args...)
	if err != nil {
		h.t.Fatalf("e2e: cli %v failed: %v\n%s", args, err, out)
	}
	return out
}

// httpClient returns a client that trusts the dev CA and routes every
// connection to loopback, so requests can use the real app hostname (correct
// SNI + certificate validation) while reaching the local ingress.
func (h *harness) httpClient() *http.Client {
	dialer := &net.Dialer{Timeout: 4 * time.Second}
	return &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: h.caPool},
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				_, port, _ := net.SplitHostPort(addr)
				return dialer.DialContext(ctx, network, net.JoinHostPort("127.0.0.1", port))
			},
		},
	}
}

// pollBody GETs url (with an explicit Host header for name-based routing) until
// the response body contains want, or the deadline elapses.
func (h *harness) pollBody(url, host, want string, deadline time.Duration) {
	h.t.Helper()
	if err := h.pollBodyErr(url, host, want, deadline); err != nil {
		h.t.Fatalf("e2e: %v", err)
	}
}

func (h *harness) pollBodyErr(url, host, want string, deadline time.Duration) error {
	client := h.httpClient()
	end := time.Now().Add(deadline)
	var last string
	for time.Now().Before(end) {
		req, _ := http.NewRequest(http.MethodGet, url, nil)
		if host != "" {
			req.Host = host
		}
		resp, err := client.Do(req)
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			last = string(body)
			if strings.Contains(last, want) {
				return nil
			}
		} else {
			last = err.Error()
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("poll %s (host %s): never contained %q; last=%q", url, host, want, last)
}

func (h *harness) exited() bool {
	return h.daemon.ProcessState != nil && h.daemon.ProcessState.Exited()
}

// stop SIGTERMs the daemon, waits for exit, and asserts no managed containers
// remain (clean teardown, T-54 step 7).
func (h *harness) stop() {
	if h.daemon == nil || h.daemon.Process == nil {
		return
	}
	_ = h.daemon.Process.Signal(os.Interrupt)
	done := make(chan struct{})
	go func() { _, _ = h.daemon.Process.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		_ = h.daemon.Process.Kill()
	}
	h.assertNoManagedContainers()
}

func (h *harness) assertNoManagedContainers() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", "ps", "-aq",
		"--filter", "label=dev.zattera/managed=true").Output()
	if err != nil {
		h.t.Logf("e2e: could not list managed containers: %v", err)
		return
	}
	if ids := strings.TrimSpace(string(out)); ids != "" {
		h.t.Errorf("e2e: managed containers survived teardown:\n%s", ids)
	}
}

// fixtureDir returns the absolute path to a fixture app directory.
func fixtureDir(t *testing.T, name string) string {
	t.Helper()
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(thisFile), "..", "fixtures", "apps", name)
}

// syncBuffer is a goroutine-safe buffer for capturing the daemon's output.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
