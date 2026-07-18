---
title: Sealing & unsealing
description: What the cluster data key protects, why a node can start sealed, how it unseals itself, and how to unseal one by hand.
---

# Sealing & unsealing

Zattera encrypts secrets at rest with a single **cluster data key**: environment
variables, alert channel credentials (Slack URLs, webhook HMAC secrets, SMTP
passwords), backup S3 credentials, and archived audit objects. A node that holds
that key in memory is **unsealed**. A node that does not is **sealed**.

The key itself is stored encrypted in replicated cluster state, wrapped by your
**recovery passphrase** — the one printed once when the cluster was
bootstrapped. Keep it. It is the only way back if a node cannot recover the key
on its own.

## What a sealed node can and cannot do

A sealed node is not down. It serves the API, routes ingress traffic, runs
containers, streams logs and reports metrics exactly as usual. What it cannot do
is touch anything sealed:

| Disabled while sealed | Symptom |
| --- | --- |
| `zt env set` | `cluster key is not unsealed; cannot store secrets` |
| `zt env ls --reveal` | `…cannot reveal secrets` |
| Alert delivery | Rules still evaluate; sending fails (credentials are sealed) |
| `zt backup run`, backup config | `cluster is not unsealed` |
| Volume snapshots to S3 | `cluster is not unsealed; cannot snapshot` |
| GitHub push-to-deploy | Webhook signature cannot be verified; deploys skipped |
| Handing the key to a joining control node | The join fails with a clear error |

Reads of non-secret state, deploys of existing releases, and everything else
keep working.

## Why a node starts sealed

Only two events put the key into a process's memory: **bootstrapping** the
cluster, and **joining** it as a control node. Both happen once. Every
subsequent start — a reboot, an upgrade, a crash restart — begins with no key,
because the key is deliberately not part of raft state a node can read on its
own. Without recovery, a restarted cluster would silently lose alerting, env-var
writes and backups.

So Zattera recovers it at startup, in this order:

1. **Local key file.** A node that has been unsealed before caches the key at
   `<data-dir>/node/data.key` (mode `0600`) and reads it back on the next start.
   This is what makes an ordinary reboot come back working.
2. **A control peer.** Failing that, the node asks another control node for the
   key over mTLS, authenticating with the node certificate it already holds. The
   peer hands it over only if the caller holds the control role.
3. **Sealed, loudly.** If neither works, the node logs a prominent warning
   naming exactly what is disabled, and `zt` reports it — it does not fail to
   start.

## Unsealing by hand

Needed when a node has no cached key and no reachable unsealed peer — a
cold-started single-node cluster running with [`sealed_at_rest`](#sealing-unsealing-keeping-the-key-off-disk),
or a whole cluster rebooted at once.

```bash
zt unseal --passphrase-file /path/to/recovery-passphrase
```

Unsealing is **per node**: the key lives in one process's memory and is never
replicated, so on a multi-node cluster point your context at each sealed node in
turn. After a successful unseal the key is cached locally (unless
`sealed_at_rest` is set), so the next restart is automatic.

Check whether the node you are talking to is sealed:

```bash
zt whoami          # reports "sealed" when the serving node holds no key
```

## Keeping the key off disk

Caching the key locally is a deliberate trade: it means a reboot needs no
operator, and it means an attacker with the disk has the key. Set:

```toml
sealed_at_rest = true
```

and the node never writes the key down. It will still auto-unseal from a control
peer, so an HA cluster keeps recovering by itself; a single-node cluster then
requires `zt unseal` after every restart.

Before reaching for this, note what it does *not* buy you. The cluster CA
private key already lives unencrypted in the data directory, and anyone holding
it can mint a node certificate and ask a peer for the data key. On a single-disk
node, `sealed_at_rest` narrows the window rather than closing it. The property
it genuinely protects is **backups**: objects in S3 stay wrapped by the recovery
passphrase regardless of this setting. See
[ADR-0006](../contributing/architecture-decision-records/0006-auto-unseal-on-restart)
for the full reasoning.

## Disaster recovery is a different path

`zattera restore --passphrase-file …` rebuilds a cluster from a backup into a
**fresh** data directory. It also uses the recovery passphrase, but it is not a
way to unseal a running node — use `zt unseal` for that. See
[Backup & restore](../data/backup-restore).
