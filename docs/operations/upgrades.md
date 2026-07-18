---
title: Upgrades
description: Roll the whole cluster to one version with minimal downtime.
---

# Upgrades

```bash
zt cluster upgrade --dry-run          # see the plan
zt cluster upgrade --yes              # apply it
zt cluster upgrade --version v0.4.0 --yes
```

Nodes are upgraded **one at a time**. For each: cordon, swap the binary, restart, wait for the node to report the new version, uncordon. Any failure stops the run with the remaining nodes untouched.

## What stays up

**Your containers keep running.** They are managed by docker and are not children of `zatterad` — the agent stops reconciling while the daemon restarts, then picks up exactly where it left off. This is why an upgrade **does not drain** nodes: draining hard-stops stateful workloads (a node-pinned volume cannot move), which would turn a seconds-long binary swap into a database outage on every stateful node. Cordon is the right primitive — it stops *new* placements and leaves running work alone.

**What does blink** is the restarting node's own ingress proxy and its agent stream, for a few seconds. Traffic landing on other nodes is unaffected, so a cluster with two or more ingress nodes has no user-visible downtime. With a single node, `--dry-run` says so plainly:

```
! only one node serves ingress: requests to it will fail for a few seconds while it restarts
```

## Order: the leader goes last

The plan upgrades workers first, then control-plane followers, then the raft leader.

This is a **correctness requirement**, not a preference. The FSM only ever gains new mutation types, and a node that receives a mutation it doesn't understand logs an error *without halting* — so a new leader proposing a new mutation would silently diverge older followers' state. An old leader only ever proposes mutations that newer followers understand, so upgrading it last is the safe direction.

```
NODE              ROLE           CURRENT  TARGET   ACTION
worker-a          worker         v0.3.0   v0.4.0   upgrade
worker-b          worker         v0.3.0   v0.4.0   upgrade
control-follower  worker         v0.3.0   v0.4.0   upgrade
control-leader    leader (last)  v0.3.0   v0.4.0   upgrade
```

## Checking versions

```bash
zt nodes ls
```

The VERSION column marks anything not on the newest version with `!`. A node reports its version on every agent connection, so it refreshes after an upgrade without a rejoin.

## How the binary gets there

Binaries come from **GitHub Releases** — the same artifacts `install.sh` uses, with the same `sha256sums.txt`. (`get.zattera.dev` serves only the install script; it never hosts binaries.) So a node upgraded by `zt cluster upgrade` and one upgraded by hand end up on byte-identical builds.

The control plane resolves the target version to a per-architecture asset URL **and its SHA-256**, then hands both to the node. The node verifies the digest before executing anything.

That ordering is deliberate: a node that fetched its own checksum from the same host it fetched the binary from would be trusting one source to vouch for itself.

The outgoing binary is kept as `zattera.prev` next to the new one, by both this path and `install.sh`.

## Security

`cluster upgrade` is, by construction, a "download this and execute it" primitive. Three things bound it:

- **Admin only.** Both `UpgradePlan` and `UpgradeNode` require an org owner/admin token.
- **Checksum-pinned.** A node refuses to install a binary whose digest does not match, and refuses a request that carries no digest at all — an empty checksum is never read as "skip verification".
- **Download allowlist.** Each node only downloads from its configured `upgrade.base_url`, whatever URL the control plane hands it. The default is the official release host; an unset value means that default, **not** "any URL". Set `base_url = "*"` to opt out for a private mirror.

```toml
[upgrade]
base_url = "https://mirror.internal/zattera/releases"
```

## When something goes wrong

A node that does not come back on the new version within five minutes aborts the run and is **left cordoned** — visible in `zt nodes ls` rather than silently degraded. Fix it, then:

```bash
zt nodes uncordon <name>     # put it back in service
zt cluster upgrade --yes     # resume; nodes already upgraded are skipped
```

To go back a version, re-run with the older `--version`. The plan skips anything already on the target, so re-running is safe.

## Manual upgrade of a single node

The installer is idempotent and upgrades in place:

```bash
curl -sfL https://get.zattera.dev | sh -
```

Prefer `zt cluster upgrade` for a whole cluster: it enforces the ordering, and a hand-upgraded leader ahead of its followers is the one case that can diverge state.
