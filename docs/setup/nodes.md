---
title: Nodes
description: Add, inspect, drain, and remove cluster nodes.
---

# Nodes

A Zattera cluster starts at **one node** and grows one `join` at a time — VPSes, bare metal, or NAT'd home servers, anywhere with Docker and outbound connectivity.

## How to use

### Add a node

Mint a join token from your workstation, then run one command on the new machine:

```bash
# from your laptop (logged-in CLI)
zt nodes join-token create              # worker token (single-use by default)
# prints: K10<ca-hash>::<secret>

# on the new server (binary installed, Docker running)
zattera cluster join <control-addr>:8443 --token 'K10…::…'
```

`cluster join` writes the config, installs the systemd unit, and starts the node. (For a foreground/dev setup you can run `zattera server --join <addr> --token …` directly.) Flags on token creation: `--single-use` (default `true`), `--control` for a future HA control node.

The new node needs to reach the control node on `8443/tcp`, other nodes on `51820/udp`, and the registry on `5000/tcp` — see the [port table](configuration#fixed-ports).

### Inspect

```bash
zt nodes ls
# NAME    ROLES            STATUS          VERSION  MESH IP     LABELS
# cp1     control,worker   ALIVE           v0.4.0   10.90.0.1   builder=true
# w1      worker           ALIVE,CORDONED  v0.3.0!  10.90.1.1   …
```

`VERSION` marks anything behind the newest node with `!`, and the command prints a hint to run [`zt cluster upgrade`](../operations/upgrades) when nodes disagree. `ALIVE,CORDONED` is a healthy node that is deliberately receiving no new work — see below.

`zt stats` adds live CPU/memory/disk per node. Every `nodes` subcommand accepts `--json` for scripting, and the common ones have aliases (`node`, `ls`/`list`, `rm`/`remove`).

### Take a node out of service

Two levels, and the difference matters:

```bash
zt nodes cordon w1      # stop NEW placements; running containers keep running
zt nodes uncordon w1    # put it back in service
```

```bash
zt nodes drain w1       # migrate instances away, then mark it DRAINED
zt nodes rm w1          # remove a drained node (--force to skip the drain requirement)
```

**Cordon** is the gentle one, and usually what you want for maintenance: nothing new lands on the node, but everything already there keeps serving. This is what [`cluster upgrade`](../operations/upgrades) uses, and a node left cordoned by a failed upgrade is returned to service with `uncordon`.

**Drain** actually empties the node. Stateless replicas reschedule onto other nodes before it empties, so no traffic drops — routing only ever targets healthy instances.

::: callout warning Draining is not safe for stateful apps
A [volume](../data/volumes) is pinned to its node, so a workload that owns one **cannot** be migrated — draining hard-stops it and it stays down until the node returns. For a database node, cordon (or a rolling `cluster upgrade`, which cordons rather than drains) instead of draining.
:::

`drain` blocks while it polls, and the CLI gives up after 10 minutes. That deadline is client-side only — the drain itself continues on the control plane, and `zt nodes ls` shows when the node reaches `DRAINED`.

## How it works

The join token embeds a **hash of the cluster CA**, so the joining node authenticates the cluster before sending anything (trust pinned cryptographically, not by DNS). The node generates a keypair locally, sends a CSR, and receives: its identity certificate (the private key never leaves the machine), the CA bundle, a mesh IP from `10.90.0.0/16`, registry credentials, and its initial [WireGuard peer set](../networking/mesh).

From then on the node's **agent** holds a stream to the control plane: heartbeats every 10 seconds carry capacity and liveness; assignment updates flow down and get reconciled against local Docker. A node missing heartbeats past the threshold is marked DOWN and the scheduler replaces its stateless replicas on live nodes within seconds. When a DOWN node comes back, it re-syncs and resumes.

## Labels and placement

Node **labels** (shown in `nodes ls`) are matched by apps through `[env.<name>.placement]` constraints in [`zattera.toml`](../deploy/zattera-toml). Each node assigns itself two at boot:

| Label | Value | Meaning |
| ----- | ----- | ------- |
| `zattera.dev/os-arch` | e.g. `linux/arm64` | The platform the node's **container engine** executes (not the daemon binary's — on macOS dev nodes those differ); how multi-arch images land on the right hardware |
| `builder` | `true` | Set on every node with the `worker` role — the build dispatcher places builds here |

Anything else is yours to set:

```bash
zt nodes label w1 region=eu tier=db     # add
zt nodes label w1 region=us --overwrite # change an existing key
zt nodes label w1 tier-                 # remove
```

Labels **merge** by default, and changing a key that already exists requires `--overwrite` — placement is matched exactly, so a silent overwrite would move workloads with nothing to review. `zattera.dev/*` labels are refused: they're facts the node asserts about its own hardware, and overwriting `os-arch` would place images on machines that can't run them.

`region` is special beyond matching: the scheduler spreads an environment's replicas across distinct `region` values, so labeling nodes by region buys you multi-region spread even without a placement constraint.

::: callout note Constraints that match nothing fail the deploy
If no node carries the labels an environment asks for, placement fails with `no node matches constraints region=eu` rather than leaving replicas pending — a typo in a label surfaces as an error, not as a deploy that never finishes.
:::
