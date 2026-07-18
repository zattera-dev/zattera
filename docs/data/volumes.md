---
title: Volumes
description: Node-pinned persistent volumes for stateful apps.
---

# Volumes

Zattera gives stateful services (Postgres, Redis, …) a **node-pinned** persistent
volume — honest single-writer semantics, no fake distributed storage. A volume
lives on exactly one node; the service that mounts it is pinned to that node.

Volume lifecycle, pinning, the single-writer fencing lease, the `zattera volume`
CLI, [snapshots to S3](backup-restore) and read-only file browsing are all
available. Writing into a volume from your machine (`volume cp`) is still on the
[roadmap](../roadmap/tasks).

## Declare a volume

Mark the service `stateful` and declare a mount in
[`zattera.toml`](../deploy/zattera-toml):

```toml
[env.production]
stateful = true

[[env.production.volumes]]
name = "data"
mount_path = "/var/lib/postgresql/data"
```

The scheduler auto-creates the volume the first time it places the service,
pinning it to the least-used healthy node. From then on the service always runs
on that node. You can also manage volumes explicitly:

```bash
zattera volume create data --app api --env production  # picks a node, or --node <id>
zattera volume ls                                       # ID, NAME, ENV, NODE, STATUS
zattera volume rm <id>                                  # refused while the service runs
zattera volume snapshot <id>                            # snapshot now (see Backup & DR)
zattera volume snapshots <id>                           # list snapshots
zattera volume restore <id> --snapshot <snap-id>        # service must be stopped
zattera volume browse data                              # read-only file browser
```

Deleting a volume removes its record and best-effort deletes the underlying
docker volume on its node (a down node leaves it to be reaped later). Snapshots,
scheduling and retention are covered in [Backup & disaster recovery](backup-restore).

## Browsing files

`zattera volume browse <name-or-id>` opens a read-only file browser against the
volume's node:

```
pg-data /data
  base/                                       -  2026-07-18 09:12
> dump.sql                              2.0KB    2026-07-18 09:14
  postgresql.conf                        29.4KB  2026-07-14 11:02
↑/↓ move · enter open · backspace up · d download · r refresh · q quit
```

`d` downloads the selected file into your current directory. It works while the
service is running — reading a live volume is the main reason to look at one —
unlike snapshot, restore and delete, which refuse a mounted volume.

**Read-only by design.** There are no delete or upload keys, and the API has no
write path behind the browser. Two guards enforce the boundary on the node: a
path that escapes the volume lexically (`../../etc/shadow`) is rejected, and so
is a symlink *inside* the volume that points outside it — volume contents are
written by your workload, so that link may not be yours.

A directory with more than 5 000 entries is truncated, with a warning line
saying so. There is no pagination; narrow the path instead.

## Single-writer fencing

A stateful service must never run twice against the same volume — that would
corrupt the data. Two mechanisms guarantee it (spec §9.1):

- **Pinning** — a stateful+volume service is only ever placed on the volume's
  node. If that node goes down the volume is marked `NODE_LOST` and the service
  **stops** rather than moving; it resumes when the node returns (the data is on
  that node's disk and cannot follow it).
- **A fencing lease** — the leader grants the volume a 60-second lease naming the
  node and instance allowed to mount it, renewed every ~20s. The agent **refuses
  to start** a container unless the lease names it. During a network partition an
  isolated node's lease can't be renewed, so no other node can acquire one until
  it expires — closing any double-run window.

## Deploys are stop-then-start

Because a volume has a single writer, stateful services can't do a red/green
deploy (two instances live at once). Instead they **stop the old instance, then
start the new one** on the same node and volume:

```
stopping-old → starting → healthchecking → promoting → succeeded
```

There is a **brief maintenance downtime** between stopping the old instance and
the new one passing its health check — expected and surfaced as
`deploy.maintenance_start` / `deploy.maintenance_end` events. If the new instance
fails to start or never becomes healthy, Zattera **restarts the old one**
(best effort) and marks the deploy failed, so you're never left with nothing
running.

## Under the hood

`VolumeService` (`internal/daemon/api/volumes.go`) is the CRUD API, with file
browsing in `volumefiles.go` proxying to the volume's node
(`internal/daemon/agent/volumefiles.go`); the scheduler
owns auto-create, pinning, `NODE_LOST` tracking and lease renewal
(`internal/daemon/scheduler/volumes.go`); the agent enforces the lease before
starting a container (`internal/daemon/agent/executor.go`); the deployment
orchestrator runs the stop-then-start machine for stateful releases
(`internal/daemon/scheduler/stateful.go`). Snapshots back up to S3 — see
[Backup & disaster recovery](backup-restore).
