//go:build cloud

package cloud

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/zattera-dev/zattera/test/cloud/provider"
)

// TestCloudSmoke brings up a REAL mixed-arch cluster on Hetzner — an amd64
// control node and an arm64 worker — and asserts they form a cluster and report
// the right architectures (the thing a single-host container rig cannot verify
// honestly). It destroys everything on exit.
//
//	HCLOUD_TOKEN=... go test -tags cloud ./test/cloud/ -run TestCloudSmoke -v
//
// On failure it captures a per-node debug bundle; add ZT_CLOUD_KEEP=1 to keep
// the cluster alive and print an attach kit for live debugging.
func TestCloudSmoke(t *testing.T) {
	c := NewCluster(t)

	control := c.StartControl("amd64", "cloud-smoke.zattera.invalid")
	worker := c.JoinWorker("arm64")

	// Both nodes present.
	archByName := c.nodeArchStrings()
	if len(archByName) < 2 {
		t.Fatalf("expected 2 nodes, cluster reports %d: %v", len(archByName), archByName)
	}

	// Arch reporting (T-87): the control is amd64, the worker arm64.
	if got := archByName[control.Name()]; got != "linux/amd64" {
		t.Errorf("control %s os_arch = %q, want linux/amd64", control.Name(), got)
	}
	if got := archByName[worker.Name()]; got != "linux/arm64" {
		t.Errorf("worker %s os_arch = %q, want linux/arm64", worker.Name(), got)
	}

	// Sanity: the worker actually runs Docker and the daemon is healthy.
	worker.MustRun("docker info --format '{{.ServerVersion}}' >/dev/null")
	t.Logf("cloud: mixed-arch cluster up — %v", archByName)

	// NOTE: extend here to exercise T-88 end to end — deploy an arm64-only image
	// and assert it lands only on the arm64 worker, never the amd64 control.
}

// TestCloudReap destroys ALL harness-labelled resources regardless of age. It
// is the manual cleanup entrypoint the attach kit points at after a
// keep-on-failure debugging session.
//
//	HCLOUD_TOKEN=... go test -tags cloud ./test/cloud/ -run TestCloudReap -v
func TestCloudReap(t *testing.T) {
	token := os.Getenv("HCLOUD_TOKEN")
	if token == "" {
		t.Skip("cloud: set HCLOUD_TOKEN to reap harness resources")
	}
	d := provider.NewHetzner(token, os.Getenv("ZT_CLOUD_API"))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	sel := map[string]string{labelHarness: "1"}

	// Servers (maxAge 0 = everything).
	destroyed, err := provider.ReapOlderThan(ctx, d, sel, labelCreated, 0, time.Now())
	if err != nil {
		t.Errorf("reap servers: %v", err)
	}
	t.Logf("cloud: reaped %d harness server(s): %v", len(destroyed), destroyed)

	// Firewalls + networks left behind by any run.
	if fws, err := d.ListFirewalls(ctx, sel); err == nil {
		for _, id := range fws {
			_ = d.DeleteFirewall(ctx, id)
		}
		t.Logf("cloud: reaped %d firewall(s)", len(fws))
	}
	if nets, err := d.ListNetworks(ctx, sel); err == nil {
		for _, id := range nets {
			_ = d.DeleteNetwork(ctx, id)
		}
		t.Logf("cloud: reaped %d network(s)", len(nets))
	}
}
