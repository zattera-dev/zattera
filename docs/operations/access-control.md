---
title: Access control & audit
description: Users, API tokens, project roles, and the audit log.
---

# Access control & audit

Zattera is **API-first with fail-closed auth**: every RPC requires an identity, every project-scoped method checks a role, and every mutation lands in the audit log.

## How to use

### Users and tokens

The first boot prints a bootstrap admin token (`zpat_…`) exactly once. From there:

```bash
zt login --server https://… --token zpat_…   # stores a context after verifying the token
```

Personal access tokens double as **registry credentials** (HTTP basic auth password) if you need to pull cluster-built images directly.

### Project roles

Membership is per project:

```bash
zt projects members add dev@acme.com --role developer
zt projects members ls
zt projects members rm <user-id>
```

| Role | Can |
| ---- | --- |
| `viewer` | Read: ps, logs, releases, env var *keys* |
| `developer` | Deploy, rollback, jobs, set/reveal env vars, attach/debug |
| `admin` | Manage members, domains, delete apps |
| `owner` | Everything, including deleting the project |

Org-level owners/admins bypass project membership. Non-members don't see a project at all.

## How it works

Identity is resolved per request: nodes authenticate with **mTLS certificates** (their identity is in the cert, issued at join), humans with **bearer tokens** (stored hashed; expiry enforced at lookup). Authorization is a fail-closed method table — any API method without an explicit policy is **denied**, so new endpoints can't accidentally ship open. Project-scoped requests then check your project role against the method's minimum.

Every mutating API call is recorded in the **audit log** — actor, method, project, and outcome (including failures) — batched into replicated state so the trail survives node loss. Passwords and secret values are never included.

## Querying the audit log

```bash
zt audit                                  # whole cluster, newest first (org admin)
zt audit --project demo --since 24h       # one project, last day
zt audit --method /zattera.v1.DeployService/   # only deploys and rollbacks
zt audit --actor usr_01H... --json        # one user, machine-readable
```

Reading the cluster-wide log requires an **org owner/admin** token. `--method` matches a prefix of the full gRPC method name, so `/zattera.v1.AppService/` narrows to one service and `/zattera.v1.AppService/SetEnvVars` to one call.

The audit log is a **capped ring** in replicated state (50 000 entries; events keep 10 000), so on a busy cluster it holds days, not years. For durable retention, turn on archiving.

## Events

Events are the platform's own narration — deploys, node health, certificate renewals — and are what the [alert engine](metrics-and-alerts) evaluates.

```bash
zt events                                 # newest first
zt events -f                              # follow, oldest first as they arrive
zt events --project demo --kind deploy.   # only deployment events
zt events --severity error --since 1h
```

Unlike the audit log, events are **not** admin-only: any project member can read their own project's events. Cluster-wide (`zt events` with no `--project`) still requires an org owner/admin. `--kind` matches a prefix, so `deploy.` covers `deploy.succeeded` and `deploy.failed`.

Follow mode polls every two seconds — there is no server-side event stream — and prints each event exactly once.

## Long-term retention

Both rings can be archived to object storage before entries age out. Archiving reuses the [backup](../data/backup-restore) destination and credentials — there is nothing separate to configure:

```bash
zt backup config --bucket zattera-backups --archive
```

From then on the leader sweeps settled audit entries and events to the bucket every five minutes, as gzipped NDJSON **encrypted with the cluster data key**, exactly like a backup. Query across the seam with `--archive`:

```bash
zt audit --since 90d --archive
zt events --since 90d --archive --kind deploy.
```

The result merges the live ring with the archive, deduplicated and newest-first; the CLI prints how many entries came from each. Archived reads go through the API, so the CLI never needs bucket credentials, and normal access control still applies — a project member reading `--archive` sees only their own project's events.

Object layout, if you need to reach it directly:

```
<prefix>/audit/<YYYY-MM-DD>/<startMs>-<endMs>-<ulid>.ndjson.gz.enc
<prefix>/events/<YYYY-MM-DD>/<startMs>-<endMs>-<ulid>.ndjson.gz.enc
```

The millisecond range in each name lets a time-scoped query skip objects without downloading them. Objects are sealed with the cluster data key, so reading them outside Zattera means unsealing with the recovery passphrase — the same trade as backups.

Notes:

- **Nothing is ever deleted.** Zattera only writes; how long objects live is your bucket's lifecycle policy to decide.
- **Archiving does not shorten the ring.** Recent history stays queryable without touching the bucket, and `--archive` is only needed to reach past it.
- **A sweep that crashes mid-flight re-archives its batch** rather than risk losing it; the reader deduplicates by id, so duplicates never surface.
- **`--archive` cannot be combined with `-f`** — following re-queries every two seconds, and the archive only holds settled history.

Sensitive values get extra gates on top of RBAC: env var reveal requires developer+ *and* an unsealed cluster key (see [Environment variables](../deploy/environment-variables#how-it-works)).

SSO/OIDC is planned for M4.
