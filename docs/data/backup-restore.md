---
title: Backup & disaster recovery
description: Incremental S3 snapshots and one-command full-platform restore — work in progress.
---

# Backup & disaster recovery

::: callout warning Work in progress
Full-platform `zatterad restore` (T-66) is still on the
[roadmap](../roadmap/tasks). Volume snapshots — the engine (T-64) and the
scheduling/CLI (T-65) — have landed.
:::

## Volume snapshots

Configure a destination bucket once (cluster-wide `BackupConfig`: S3 endpoint,
bucket, and credentials, which are encrypted at rest), then snapshot on demand or
on a schedule:

```bash
zattera volume snapshot <id>            # take one now, waits for completion
zattera volume snapshots <id>           # id, status, size, created
zattera volume restore <id> --snapshot <snap-id>   # service must be stopped first
```

Scheduled snapshots and retention come from the volume's `SnapshotPolicy`
(settable when creating the volume):

- **`schedule`** — a 5-field cron expression; the leader fires a snapshot each
  due slot. An optional **`pre_hook`** command runs inside the mounting container
  first (e.g. `pg_dump` to quiesce the database).
- **`keep_last`** (default 7) — older snapshots beyond this count are deleted and
  their now-orphaned chunks garbage-collected.

A snapshot runs on the volume's pinned node: the control plane dials that node,
which streams progress back. Restore refuses while the volume is mounted — stop
the service (scale its environment to 0) first.

**What it will do:** content-addressed, encrypted, incremental snapshots of
volumes and platform state to any S3-compatible bucket — and `zatterad restore`
to rebuild the entire platform (state + volumes + images) onto fresh
infrastructure with one command.

## The snapshot engine (T-64)

Volume snapshots are **content-addressed and deduplicated**, so an incremental
snapshot only uploads what changed:

1. The volume's directory is serialized to a **deterministic tar** (sorted walk,
   zeroed access/change times, preserved uid/gid/mode) — byte-identical trees
   produce byte-identical tars.
2. The tar stream is split into ~1 MB **content-defined chunks** (FastCDC), so a
   small edit re-chunks only locally (a one-byte change touches one or two
   chunks, not the whole file).
3. Each chunk is keyed by `sha256(plaintext)` — if that object already exists it
   is **skipped** (dedup across all snapshots) — otherwise compressed (zstd),
   **encrypted** (AES-256-GCM with the cluster data key and a random per-object
   nonce), and stored as `chunks/<hash>`.
4. A per-snapshot **manifest** lists the ordered chunk hashes (encrypted too).
   Restore streams the chunks back through a tar extract; a prune pass refcounts
   every manifest and deletes only orphaned chunks (shared chunks survive).

The engine (`internal/daemon/volumes`) operates on an already-quiesced path;
quiescing a live database with a pre-hook is the scheduling layer's job (T-65).

Today, [`zattera state export`](../operations/state-export) already gives you a
GitOps-style export of all desired state (projects, apps, environments, domains)
that can be re-applied to a cluster.
