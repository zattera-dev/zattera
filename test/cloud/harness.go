//go:build cloud

// Package cloud is a Go harness for testing Zattera on REAL cloud VMs (Hetzner
// today; the provider abstraction generalizes to others). It replaces the
// bash real-cluster scripts.
//
// Everything is gated: NewCluster skips the test unless HCLOUD_TOKEN is set, so
// `go test ./...` never spins paid infra. Cloud tests carry the `cloud` build
// tag and are run explicitly:
//
//	HCLOUD_TOKEN=... go test -tags cloud ./test/cloud/ -run TestThreeNodeCluster -v
//
// Safety: NewCluster refuses to run in a project that already holds servers the
// harness did not create (use a dedicated Hetzner project). Every resource is
// labelled zattera-harness=1 with a creation
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
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/zattera-dev/zattera/internal/cloud/provider"
	"github.com/zattera-dev/zattera/pkg/apiclient"
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

	networkID   int64             // private network for NAT simulation (lazily created)
	serverTypes map[string]string // arch → resolved server type (cached)
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

	c.preflightDedicatedProject()
	c.reapStale()
	c.ensureSSHKey()
	t.Cleanup(c.teardown)
	t.Logf("cloud: run %s in %s (keep-on-fail=%v)", c.runID, region, c.keep)
	return c
}

// preflightDedicatedProject fails fast if the token's project contains any
// server the harness did NOT create (no zattera-harness label). It is the guard
// against pointing a shared-project token — one that also holds your real
// infrastructure — at a harness that creates and destroys resources. Use a
// dedicated, empty Hetzner project; override only if you fully understand the
// blast radius (ZT_CLOUD_ALLOW_SHARED_PROJECT=1).
func (c *Cluster) preflightDedicatedProject() {
	if os.Getenv("ZT_CLOUD_ALLOW_SHARED_PROJECT") == "1" {
		c.T.Log("cloud: ZT_CLOUD_ALLOW_SHARED_PROJECT=1 — shared-project guard DISABLED")
		return
	}
	all, err := c.driver.List(c.Ctx, nil) // nil selector = every server in the project
	if err != nil {
		c.T.Fatalf("cloud: preflight list failed (is HCLOUD_TOKEN valid?): %v", err)
	}
	var foreign []string
	for _, m := range all {
		if m.Labels[labelHarness] != "1" {
			foreign = append(foreign, m.Name)
		}
	}
	if len(foreign) > 0 {
		c.T.Fatalf("cloud: REFUSING to run — this token's project already contains %d server(s) "+
			"the harness did not create: %v.\n"+
			"Point the harness at a DEDICATED, empty Hetzner project so it can never touch your real "+
			"infrastructure. To override (not recommended), set ZT_CLOUD_ALLOW_SHARED_PROJECT=1.",
			len(foreign), foreign)
	}
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

// resolveServerType picks the Hetzner server type for an arch. An explicit env
// override (ZT_CLOUD_{AMD64,ARM64}_TYPE) always wins; otherwise it queries the
// API for the cheapest non-deprecated type of that arch orderable in the
// region — so a deprecated/unavailable type never breaks a run. Cached per arch.
func (c *Cluster) resolveServerType(arch string) string {
	c.T.Helper()
	var override string
	switch arch {
	case "amd64":
		override = os.Getenv("ZT_CLOUD_AMD64_TYPE")
	case "arm64":
		override = os.Getenv("ZT_CLOUD_ARM64_TYPE")
	default:
		c.T.Fatalf("cloud: unsupported arch %q (want amd64|arm64)", arch)
		return ""
	}
	if override != "" {
		return override
	}
	if c.serverTypes == nil {
		c.serverTypes = map[string]string{}
	}
	if t, ok := c.serverTypes[arch]; ok {
		return t
	}

	types, err := c.driver.AvailableServerTypes(c.Ctx, c.region)
	if err != nil {
		c.T.Fatalf("cloud: list server types in %s: %v", c.region, err)
	}
	// Prefer the cheapest ≥2-vCPU type: a 1-vCPU node is too weak to reliably
	// run a full Zattera node (control plane + raft + registry + buildkit +
	// docker + app replicas) — replicas' healthchecks flake under the
	// contention. Fall back to the cheapest 1-vCPU only if nothing bigger is
	// orderable. (The cheapest amd64 type in nbg1, cx23, is 2-vCPU anyway; the
	// harness only fell to 1-vCPU cpx12 when cx23 was momentarily unavailable.)
	best, bestPrice := "", math.MaxFloat64
	fallback, fbPrice := "", math.MaxFloat64
	for _, st := range types {
		if st.Arch != arch || st.HourlyPriceEUR <= 0 {
			continue
		}
		if st.HourlyPriceEUR < fbPrice {
			fallback, fbPrice = st.Name, st.HourlyPriceEUR
		}
		if st.Cores >= 2 && st.HourlyPriceEUR < bestPrice {
			best, bestPrice = st.Name, st.HourlyPriceEUR
		}
	}
	if best == "" {
		best, bestPrice = fallback, fbPrice
		if best != "" {
			c.T.Logf("cloud: no ≥2-vCPU %s type orderable in %s — falling back to 1-vCPU %s (workloads may be flaky)", arch, c.region, best)
		}
	}
	if best == "" {
		c.T.Fatalf("cloud: no non-deprecated %s server type is orderable in %s "+
			"(try a different ZT_CLOUD_REGION, or set ZT_CLOUD_%s_TYPE explicitly)",
			arch, c.region, strings.ToUpper(arch))
	}
	c.serverTypes[arch] = best
	c.T.Logf("cloud: auto-selected %s type %q (€%.4f/h) in %s", arch, best, bestPrice, c.region)
	return best
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
