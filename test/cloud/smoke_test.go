//go:build cloud

package cloud

import "testing"

// TestSmoke is the minimal scenario: a 2-node mixed-arch cluster (amd64 control
// + arm64 worker) forms and reports the right architectures — the thing a
// single-host container rig cannot verify honestly. It destroys everything on
// exit. Use this as the cheapest end-to-end check that the harness + a real
// cluster work.
//
//	HCLOUD_TOKEN=... go test -tags cloud ./test/cloud/ -run TestSmoke -v
func TestSmoke(t *testing.T) {
	c := NewCluster(t)

	control := c.StartControl("amd64", "cloud-smoke.zattera.invalid")
	worker := c.JoinWorker("arm64")

	archByName := c.nodeArchStrings()
	if len(archByName) < 2 {
		t.Fatalf("expected 2 nodes, cluster reports %d: %v", len(archByName), archByName)
	}

	// Arch reporting (T-87): control amd64, worker arm64.
	if got := archByName[control.Name()]; got != "linux/amd64" {
		t.Errorf("control %s os_arch = %q, want linux/amd64", control.Name(), got)
	}
	if got := archByName[worker.Name()]; got != "linux/arm64" {
		t.Errorf("worker %s os_arch = %q, want linux/arm64", worker.Name(), got)
	}

	worker.MustRun("docker info --format '{{.ServerVersion}}' >/dev/null")
	t.Logf("cloud: mixed-arch cluster up — %v", archByName)

	// NOTE: extend to exercise T-88 end to end — deploy an arm64-only image and
	// assert it lands only on the arm64 worker, never the amd64 control.
}
