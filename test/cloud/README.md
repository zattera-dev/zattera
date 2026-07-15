# Real-cloud test harness (`test/cloud`)

A Go harness for testing Zattera on **real cloud VMs** — the fidelity a
single-host container rig cannot give: genuine mixed-arch nodes, kernel
WireGuard, real NAT/firewalls, real MTU. Hetzner today; the provider
abstraction generalizes.

This replaces the bash `test/real-cluster` scripts.

The cloud client itself lives in **`internal/cloud/provider`** (production code,
not test-only): its `Driver` interface + raw-REST Hetzner client are the frozen
provider-agnostic lifecycle that the Phase 8 node autoscaler (roadmap
T-82/T-84, `internal/daemon/provision`) will import directly — the same package,
no future move. This harness (`test/cloud`) is just its first consumer and adds
the test-only orchestration (NAT simulation, fault injection, debug bundles,
keep-on-fail reaper) on top.

## Running

```bash
export HCLOUD_TOKEN=...          # a Hetzner Cloud API token (see safety below)
make test-cloud                  # runs every scenario
# or a single scenario:
go test -tags cloud -v ./test/cloud/ -run TestThreeNodeCluster
```

Without `HCLOUD_TOKEN` the tests **skip** — `go test ./...` never spins paid
infra. The `cloud` build tag keeps the harness out of normal builds.

### Cost & safety

A 3-node cluster for a full run costs a few cents (Hetzner bills hourly:
cx22 ≈ €0.008/h, cax11 ≈ €0.007/h). Safety nets, strongest first:

1. **Use a dedicated, empty Hetzner project** and generate the token there. A
   Hetzner token is scoped to ONE project and cannot touch any other — this is
   the guarantee that does not depend on the harness being bug-free.
2. **Shared-project guard:** `NewCluster` lists the project and **refuses to
   run** if it finds any server the harness did not create (no
   `zattera-harness` label). Override only if you understand the blast radius:
   `ZT_CLOUD_ALLOW_SHARED_PROJECT=1`.
3. Every resource is labelled `zattera-harness=1` + a creation timestamp; every
   destroy is scoped to that label — the harness never enumerates-and-deletes
   unlabelled resources.
4. `NewCluster` **reaps** harness resources older than `ZT_CLOUD_MAX_AGE`
   (default 3h); each run destroys its own on exit; `make cloud-reap` destroys
   all leftover harness resources on demand.

## Scenarios

Each test case is one `*_test.go` file in this package; they share the harness
(the non-`_test.go` files). Current cases:

| file | test | what it covers |
|------|------|----------------|
| `smoke_test.go` | `TestSmoke` | cheapest check: 2-node mixed-arch cluster forms, arch reported |
| `threenode_test.go` | `TestThreeNodeCluster` | 1 control + 2 workers, all register + ALIVE |
| `reap_test.go` | `TestCloudReap` | operational: destroy all leftover harness resources |

**Add a scenario:** drop a new `<case>_test.go` with

```go
//go:build cloud

package cloud

func TestMyCase(t *testing.T) {
	c := NewCluster(t)                 // gated, reaped, torn down for you
	control := c.StartControl("amd64", "my.zattera.invalid")
	w := c.JoinWorker("arm64")
	c.WaitNodesReady(2)
	// ... exercise c.Nodes(), w.MustRun(...), c.IsolateInbound(w), w.KillDaemon(), etc.
}
```

The harness handles create/install/join, teardown, failure bundles, and the
keep-on-fail attach kit — a scenario just drives nodes and asserts.

## Debugging a failing run

On failure the harness writes a **per-node debug bundle** (journald, `docker
ps`/logs, `wg show`, routes, iptables, config, cluster node list) to a printed
directory under `$TMPDIR/zattera-cloud-<run>/` — read it after the fact, no
live session needed.

For **live** debugging, keep the cluster up and get an attach kit:

```bash
ZT_CLOUD_KEEP=1 make test-cloud
```

On failure it prints ready-to-run `ssh`/`journalctl`/`docker`/`wg` commands and
the SSH key path, then leaves the nodes running. The reaper still destroys them
after the max age, so a forgotten cluster cannot run up a bill. Clean up early
with `make cloud-reap`.

## What the harness gives a test

```go
c := cloud.NewCluster(t)                       // skips without HCLOUD_TOKEN
control := c.StartControl("amd64", "apps.test") // create→docker→binary→config→start→bootstrap
worker  := c.JoinWorker("arm64")                // mixed-arch join, waits until registered

c.Nodes()                                       // cluster's view via the API
node.MustRun("...")                             // arbitrary SSH command
```

Capabilities:

- **Lifecycle** — `CreateNode` (OS/arch → server type, install, IPv4/IPv6),
  `StartControl`, `JoinWorker`; binaries cross-compiled per arch and uploaded.
- **Firewall / NAT** — `OpenZatteraPorts`, `IsolateInbound` (simulate an
  unreachable/NAT peer via a drop-all-inbound-but-SSH firewall — forces
  disco/hole-punch/DERP), `SimulateNATNoPublicIP` (true no-public-IP node on a
  private network with a NAT gateway, reached via SSH jump host). All idempotent.
- **Faults / load** — `StopDaemon`/`KillDaemon`/`Reboot`, `KillContainer`,
  `CPULoad`, `FillDisk`, `AddNetLatency` (tc netem), `BlockMeshUDP` (partition).
- **Observability** — failure bundles + `ZT_CLOUD_KEEP` attach kit (above).

## Env vars

| var | default | meaning |
|-----|---------|---------|
| `HCLOUD_TOKEN` | — | required; Hetzner API token |
| `ZT_CLOUD_REGION` | `nbg1` | Hetzner location |
| `ZT_CLOUD_KEEP` | off | on failure, keep the cluster up + print an attach kit |
| `ZT_CLOUD_MAX_AGE` | `3h` | reaper max age for harness resources |
| `ZT_CLOUD_ALLOW_SHARED_PROJECT` | off | disable the guard that refuses to run in a non-dedicated project |
| `ZT_CLOUD_API` | real API | override the API base URL (tests) |
