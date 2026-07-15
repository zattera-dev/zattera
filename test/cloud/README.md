# Real-cloud test harness (`test/cloud`)

A Go harness for testing Zattera on **real cloud VMs** — the fidelity a
single-host container rig cannot give: genuine mixed-arch nodes, kernel
WireGuard, real NAT/firewalls, real MTU. Hetzner today; the provider
abstraction generalizes.

This replaces the bash `test/real-cluster` scripts. It is the test-side
prototype of the Phase 8 autoscaling driver (roadmap T-82/T-83): the
`provider` package's `Driver` interface + raw-REST Hetzner client are shaped to
be promoted into `internal/daemon/provision` when that lands.

## Running

```bash
export HCLOUD_TOKEN=...          # a Hetzner Cloud API token
make test-cloud                  # or: go test -tags cloud -v ./test/cloud/ -run TestCloudSmoke
```

Without `HCLOUD_TOKEN` the tests **skip** — `go test ./...` never spins paid
infra. The `cloud` build tag keeps the harness out of normal builds.

### Cost & safety

A mixed-arch 2-node cluster for a full run costs well under €0.05 (Hetzner
bills hourly: cx22 ≈ €0.008/h, cax11 ≈ €0.007/h). Safety nets:

- Every resource is labelled `zattera-harness=1` + a creation timestamp.
- `NewCluster` **reaps** harness resources older than `ZT_CLOUD_MAX_AGE`
  (default 3h) before each run.
- Each run destroys its own servers, firewalls, networks, and SSH key on exit.
- `make cloud-reap` destroys **all** leftover harness resources on demand.

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
| `ZT_CLOUD_API` | real API | override the API base URL (tests) |
