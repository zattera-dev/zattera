//go:build cloud

// Package cloud is a Go harness for testing Zattera on REAL cloud VMs (Hetzner
// today; the provider abstraction generalizes to others). It replaces the
// bash real-cluster scripts.
//
// Everything is gated: NewCluster skips the test unless HCLOUD_TOKEN is set, so
// `go test ./...` never spins paid infra. Cloud tests carry the `cloud` build
// tag and are run explicitly:
//
//	HCLOUD_TOKEN=... go test -tags cloud ./test/cloud/ -run TestCloudSmoke -v
//
// Safety: every resource is labelled zattera-harness=1 with a creation
// timestamp; NewCluster reaps harness resources older than the max age BEFORE
// each run, and each run tears its own down at the end. So even a crashed or
// kept-alive run cannot leak paying servers indefinitely.
//
// Debuggability (for humans and agents): on failure the harness captures a
// per-node debug bundle (journald, docker, wireguard, routes, cluster state)
// to a directory it prints. With ZT_CLOUD_KEEP=1 it ALSO leaves the cluster
// running and prints an "attach kit" (IPs + SSH key path + ready-to-run
// commands) so an agent can log in and inspect live state. The reaper still
// guarantees eventual cleanup.
package cloud

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/zattera-dev/zattera/pkg/apiclient"
	"github.com/zattera-dev/zattera/test/cloud/provider"
)

// Label keys stamped on every harness-created resource.
const (
	labelHarness = "zattera-harness" // always "1"
	labelRun     = "zattera-run"     // per-run id
	labelCreated = "zattera-created" // unix seconds (reaper input)
	labelRole    = "zattera-role"    // "control" | "worker"
	labelName    = "zattera-name"    // node name
)

// Defaults; override via env.
const (
	defaultRegion    = "nbg1"
	defaultImage     = "debian-12"
	defaultMaxAge    = 3 * time.Hour
	amd64ServerType  = "cx22"  // Intel/AMD, amd64
	arm64ServerType  = "cax11" // Ampere, arm64
	provisionTimeout = 5 * time.Minute
	joinTimeout      = 3 * time.Minute
)

// Cluster is a live set of cloud nodes for one test.
type Cluster struct {
	T   *testing.T
	Ctx context.Context

	driver *provider.Hetzner
	runID  string
	region string

	// ephemeral SSH identity uploaded to the provider for this run.
	signer   ssh.Signer
	sshKeyID int64
	keyDir   string

	nodes  []*Node
	keep   bool // keep resources on failure (ZT_CLOUD_KEEP)
	binDir string

	// memoized after StartControl.
	control *Node
	api     *apiclient.Client

	networkID int64 // private network for NAT simulation (lazily created)
}

// NewCluster builds a harness bound to t. It SKIPS the test when HCLOUD_TOKEN
// is unset. It reaps stale harness resources, generates an ephemeral SSH key,
// and registers teardown (destroy on success; on failure, capture a bundle and
// either destroy or — with ZT_CLOUD_KEEP=1 — keep + print an attach kit).
func NewCluster(t *testing.T) *Cluster {
	t.Helper()
	token := os.Getenv("HCLOUD_TOKEN")
	if token == "" {
		t.Skip("cloud: set HCLOUD_TOKEN to run real-cluster tests")
	}
	region := envOr("ZT_CLOUD_REGION", defaultRegion)
	c := &Cluster{
		T:      t,
		Ctx:    context.Background(),
		driver: provider.NewHetzner(token, os.Getenv("ZT_CLOUD_API")),
		runID:  newRunID(),
		region: region,
		keep:   os.Getenv("ZT_CLOUD_KEEP") == "1",
		keyDir: t.TempDir(),
	}

	c.reapStale()
	c.ensureSSHKey()
	t.Cleanup(c.teardown)
	t.Logf("cloud: run %s in %s (keep-on-fail=%v)", c.runID, region, c.keep)
	return c
}

// baseLabels are stamped on every resource for List/reaper/teardown scoping.
func (c *Cluster) baseLabels() map[string]string {
	return map[string]string{
		labelHarness: "1",
		labelRun:     c.runID,
		labelCreated: strconv.FormatInt(time.Now().Unix(), 10),
	}
}

// runSelector matches only THIS run's resources.
func (c *Cluster) runSelector() map[string]string {
	return map[string]string{labelHarness: "1", labelRun: c.runID}
}

// reapStale destroys harness servers older than the max age (any run). This is
// the cost backstop that makes keep-on-failure safe.
func (c *Cluster) reapStale() {
	maxAge := defaultMaxAge
	if v := os.Getenv("ZT_CLOUD_MAX_AGE"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			maxAge = d
		}
	}
	destroyed, err := provider.ReapOlderThan(c.Ctx, c.driver,
		map[string]string{labelHarness: "1"}, labelCreated, maxAge, time.Now())
	if err != nil {
		c.T.Logf("cloud: reaper warning (continuing): %v", err)
	}
	if len(destroyed) > 0 {
		c.T.Logf("cloud: reaped %d stale harness server(s): %v", len(destroyed), destroyed)
	}
}

// ensureSSHKey generates an ed25519 keypair, writes the private key into the
// test's temp dir (0600), and uploads the public key to the provider.
func (c *Cluster) ensureSSHKey() {
	priv, pub, pem := generateSSHKey(c.T)
	c.signer = priv
	name := "zt-harness-" + c.runID
	id, err := c.driver.EnsureSSHKey(c.Ctx, name, pub, c.baseLabels())
	if err != nil {
		c.T.Fatalf("cloud: upload ssh key: %v", err)
	}
	c.sshKeyID = id
	// Persist the private key so the attach kit can point ssh at it.
	if err := os.WriteFile(c.keyPath(), pem, 0o600); err != nil {
		c.T.Fatalf("cloud: write ssh key: %v", err)
	}
}

func (c *Cluster) keyPath() string { return c.keyDir + "/id_ed25519" }

// teardown runs at test end. It always tries to leave nothing paying — unless
// the test failed AND keep-on-fail is set, in which case it prints an attach
// kit and lets the reaper clean up later.
func (c *Cluster) teardown() {
	if c.T.Failed() {
		dir := c.collectBundles()
		if dir != "" {
			c.T.Logf("cloud: debug bundle → %s", dir)
		}
		if c.keep {
			c.printAttachKit()
			c.T.Logf("cloud: ZT_CLOUD_KEEP=1 — leaving %d node(s) UP for live debugging; the reaper destroys them after the max age", len(c.nodes))
			return
		}
	}
	c.destroyAll()
}

// destroyAll removes every server, firewall, network, and the SSH key for this
// run (idempotent). Order matters: servers first (a firewall/network still
// attached to a live server cannot be deleted), then the network resources.
func (c *Cluster) destroyAll() {
	machines, err := c.driver.List(c.Ctx, c.runSelector())
	if err != nil {
		c.T.Logf("cloud: teardown list failed: %v", err)
	}
	for _, m := range machines {
		if derr := c.driver.Destroy(c.Ctx, m.ProviderID); derr != nil {
			c.T.Logf("cloud: destroy %s (%s) failed: %v", m.Name, m.ProviderID, derr)
		}
	}
	c.cleanupNetworkResources()
	if c.sshKeyID != 0 {
		if err := c.driver.DeleteSSHKey(c.Ctx, c.sshKeyID); err != nil {
			c.T.Logf("cloud: delete ssh key failed: %v", err)
		}
	}
}

// --- helpers --------------------------------------------------------------

func serverTypeForArch(arch string) (string, error) {
	switch arch {
	case "amd64":
		return amd64ServerType, nil
	case "arm64":
		return arm64ServerType, nil
	default:
		return "", fmt.Errorf("cloud: unsupported arch %q (want amd64|arm64)", arch)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func newRunID() string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
