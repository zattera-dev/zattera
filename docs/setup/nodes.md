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
# NAME    ROLES            STATUS   MESH IP     LABELS
# cp1     control,worker   alive    10.90.0.1   region=eu
# w1      worker           alive    10.90.1.1   …
```

`zt stats` adds live CPU/memory/disk per node.

### Drain and remove

```bash
zt nodes drain w1     # migrates instances away, waits until DRAINED (up to 10m)
zt nodes rm w1        # remove a drained node (--force to skip the drain requirement)
```

Draining reschedules stateless replicas onto other nodes before the node empties — no dropped traffic, since routing only ever targets healthy instances.

## How it works

The join token embeds a **hash of the cluster CA**, so the joining node authenticates the cluster before sending anything (trust pinned cryptographically, not by DNS). The node generates a keypair locally, sends a CSR, and receives: its identity certificate (the private key never leaves the machine), the CA bundle, a mesh IP from `10.90.0.0/16`, registry credentials, and its initial [WireGuard peer set](../networking/mesh).

From then on the node's **agent** holds a stream to the control plane: heartbeats every 10 seconds carry capacity and liveness; assignment updates flow down and get reconciled against local Docker. A node missing heartbeats past the threshold is marked DOWN and the scheduler replaces its stateless replicas on live nodes within seconds. When a DOWN node comes back, it re-syncs and resumes.

Node **labels** (shown in `nodes ls`) can be matched by apps via `[env.<name>.placement]` constraints in [`zattera.toml`](../deploy/zattera-toml) — e.g. pin an environment to `region=eu` nodes.
