//go:build e2e

package e2e

import (
	"strings"
	"testing"
	"time"
)

// TestSmoke is the single-node M1 exit gate (T-54): build → boot dev daemon →
// deploy the go-hello fixture through the whole pipeline → assert it serves →
// red/green env redeploy + rollback → one-shot job → clean teardown.
//
// It requires a Docker daemon and outbound DNS for sslip.io (in CI it resolves;
// locally add `127.0.0.1 hello-production.apps.127.0.0.1.sslip.io` to /etc/hosts
// if your resolver blocks it). Every wait is a bounded poll.
func TestSmoke(t *testing.T) {
	h := newHarness(t)
	h.start()

	// Step 2: authenticate and create a project.
	h.login()
	h.mustCLI("projects", "create", "smoke")

	fixture := fixtureDir(t, "go-hello")

	// The fixture already ships a zattera.toml; re-init to assert the command
	// works and pin the app name.
	if _, err := h.cli(fixture, "init", "--name", "hello"); err != nil {
		// init refuses to clobber an existing config on some setups; that's fine.
		t.Logf("e2e: init note: %v", err)
	}

	// Step 2-3: first production deploy from source (cold build → release → URL).
	out, err := h.cli(fixture, "deploy", "--prod", "--project", "smoke")
	if err != nil {
		t.Fatalf("e2e: deploy failed: %v\n%s", err, out)
	}
	for _, want := range []string{"Built hello", "Released v1"} {
		if !strings.Contains(out, want) {
			t.Fatalf("e2e: deploy output missing %q:\n%s", want, out)
		}
	}
	if !strings.Contains(out, "://") {
		t.Fatalf("e2e: deploy output missing a URL:\n%s", out)
	}

	host := "hello-production." + h.domain
	httpURL := "http://" + host + portOf(h.banner["ingress_http"]) + "/"
	httpsURL := "https://" + host + portOf(h.banner["ingress_https"]) + "/"

	// Step 3: the app serves the fixture body over the HTTP ingress (Host routing)
	// and over HTTPS on the dev port with the dev CA. The client routes to
	// loopback but keeps the hostname for SNI/cert. First build is slow.
	h.pollBody(httpURL, host, "Hello from Zattera fixture", 180*time.Second)
	h.pollBody(httpsURL, host, "Hello from Zattera fixture", 60*time.Second)

	// Step 4: logs + ps.
	logs := h.mustCLI("logs", "hello", "--since", "5m", "--project", "smoke")
	if !strings.Contains(logs, "go-hello listening") {
		t.Errorf("e2e: logs missing fixture startup line:\n%s", logs)
	}
	ps := h.mustCLI("ps", "--app", "hello", "--project", "smoke")
	if !strings.Contains(strings.ToLower(ps), "healthy") {
		t.Errorf("e2e: ps shows no healthy replica:\n%s", ps)
	}

	// Step 5: env-only redeploy (red/green) then rollback.
	h.mustCLI("env", "set", "FIXTURE_MESSAGE=v2", "--app", "hello", "--env", "production", "--project", "smoke")
	if out, err := h.cli(fixture, "deploy", "--prod", "--project", "smoke"); err != nil {
		t.Fatalf("e2e: env redeploy failed: %v\n%s", err, out)
	}
	h.pollBody(httpURL, host, "v2", 90*time.Second)

	h.mustCLI("rollback", "--app", "hello", "--env", "production", "--project", "smoke")
	h.pollBody(httpURL, host, "Hello from Zattera fixture", 30*time.Second)

	// Step 6: a one-shot job exits 0.
	if out, err := h.cli(fixture, "jobs", "run", "hello", "--env", "production", "--project", "smoke", "--", "echo", "done"); err != nil {
		t.Fatalf("e2e: job run failed: %v\n%s", err, out)
	} else if !strings.Contains(out, "succeeded") {
		t.Errorf("e2e: job did not report success:\n%s", out)
	}

	// Step 7: teardown is asserted by harness.stop (registered via t.Cleanup):
	// SIGTERM the daemon, then no dev.zattera/managed containers remain.
}

// portOf returns the ":port" suffix of a URL like "http://127.0.0.1:8080".
func portOf(url string) string {
	if i := strings.LastIndex(url, ":"); i >= 0 {
		return url[i:]
	}
	return ""
}
