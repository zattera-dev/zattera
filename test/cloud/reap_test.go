//go:build cloud

package cloud

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/zattera-dev/zattera/internal/cloud/provider"
)

// TestCloudReap is the manual-cleanup entrypoint (not a scenario): it destroys
// ALL harness-labelled resources in the token's project regardless of age. The
// keep-on-failure attach kit points here; run it after a debugging session.
//
//	HCLOUD_TOKEN=... go test -tags cloud ./test/cloud/ -run TestCloudReap -v
//	# or: make cloud-reap
func TestCloudReap(t *testing.T) {
	token := os.Getenv("HCLOUD_TOKEN")
	if token == "" {
		t.Skip("cloud: set HCLOUD_TOKEN to reap harness resources")
	}
	d := provider.NewHetzner(token, os.Getenv("ZT_CLOUD_API"))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	sel := map[string]string{labelHarness: "1"}

	// Servers (maxAge 0 = everything harness-labelled).
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
