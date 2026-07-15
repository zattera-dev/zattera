//go:build cloud

package cloud

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestWebApp deploys a basic Go web app (the go-hello fixture) as a 3-replica
// service across the cluster and verifies the full path on real infra: source
// build → embedded registry → red/green rollout → 3 healthy replicas spread
// over the nodes → the app serving HTTP through the ingress.
//
//	HCLOUD_TOKEN=... go test -tags cloud ./test/cloud/ -run TestWebApp -v
func TestWebApp(t *testing.T) {
	c := NewCluster(t)

	// 3-node cluster (control is also a worker + the builder). Empty domain →
	// a real sslip.io domain derived from the control's public IP, so the app's
	// URL resolves over public DNS to the ingress.
	c.StartControl("amd64", "")
	c.JoinWorker("amd64")
	c.JoinWorker("amd64")
	c.WaitNodesReady(3)

	// Every node trusts the embedded registry so workers can pull the built image.
	c.TrustRegistryCA()

	// Deploy the fixture (pinned to 3 replicas) as a source build, via the API,
	// retrying on the occasional transient red/green healthcheck hiccup that
	// shows up on small real nodes (cheap: the buildkit cache skips the rebuild).
	appDir := prepareHelloFixture(t, 3)
	_, nodes := c.DeploySourceHealthy("webapp", appDir, 3, 3)

	// Core assertion: 3 healthy replicas spread across ≥2 nodes. HEALTHY means
	// the agent's healthcheck GET succeeded against each container — i.e. the Go
	// web app is running and serving HTTP on all three replicas.
	if len(nodes) < 2 {
		t.Errorf("cloud: 3 replicas should spread across ≥2 nodes, landed on %d: %v", len(nodes), nodes)
	}

	// Additionally probe public routing: GET the app's real HTTPS URL (resolves
	// via sslip.io straight to the ingress). Best-effort — logs, never fails.
	c.ProbeIngressURL(c.AppHost("hello", "production"), "Hello from Zattera fixture", 90*time.Second)
}

// prepareHelloFixture copies the go-hello fixture to a temp dir and pins the
// production env to `replicas` replicas (the fixture ships min1/max2).
func prepareHelloFixture(t *testing.T, replicas int) string {
	t.Helper()
	src := filepath.Join(repoRootDir(), "test", "fixtures", "apps", "go-hello")
	dst := t.TempDir()
	for _, name := range []string{"main.go", "go.mod", "Dockerfile"} {
		b, err := os.ReadFile(filepath.Join(src, name))
		if err != nil {
			t.Fatalf("cloud: read fixture %s: %v", name, err)
		}
		if err := os.WriteFile(filepath.Join(dst, name), b, 0o644); err != nil {
			t.Fatalf("cloud: write %s: %v", name, err)
		}
	}
	toml := fmt.Sprintf(`[app]
name = "hello"

[build]
type = "dockerfile"

[deploy]
# Generous grace: on small/busy real nodes (1-vCPU, fresh off a buildkit
# build) a replica can take a while to answer its first healthcheck.
healthcheck = { path = "/healthz", timeout = "5s", grace_period = "180s" }

[env.production]
min_replicas = %d
max_replicas = %d
`, replicas, replicas)
	if err := os.WriteFile(filepath.Join(dst, "zattera.toml"), []byte(toml), 0o644); err != nil {
		t.Fatalf("cloud: write zattera.toml: %v", err)
	}
	return dst
}
