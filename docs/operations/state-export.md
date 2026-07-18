---
title: State export & apply
description: Export the cluster's desired state as YAML and re-apply it — GitOps-lite for your platform config.
---

# State export & apply

The whole platform's **desired state** — projects, apps, environments, env vars, domains — can be exported as one YAML document and applied back. Keep it in git, diff it in review, rebuild a cluster's config from it.

## How to use

```bash
zt state export > cluster.yaml               # whole cluster
zt state export --project shop > shop.yaml   # one project

zt state apply -f cluster.yaml --dry-run     # validate + count, write nothing
zt state apply -f cluster.yaml
# Applied: 2 created, 1 updated, 14 unchanged
```

`apply` reads from stdin with `-f -`, so it pipes: `zt state export | zt state apply --dry-run` should always report everything unchanged.

## How it works

The export contains **desired state only** — what you've declared, not what's running. Observed state, instances, tokens, users, and certificates are excluded; env var values are included in their **sealed (encrypted) form**, so the file is safe to store but only re-imports into the *same* cluster (the data key must match).

Names are the identity: `apply` diffs by project/app/environment name and proposes creates and updates, which makes it idempotent — applying the same file twice is a no-op. Unknown fields produce warnings rather than hard failures, and `--dry-run` runs the full validation path without writing.

Full disaster recovery — state *plus* volumes and images onto fresh metal — is a separate, bigger hammer: see [Backup & DR](../data/backup-restore).
