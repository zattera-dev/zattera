---
title: Zattera — The single-binary PaaS
description: Turn any pool of machines into a Heroku/Vercel-like cluster platform. One Go binary — CLI, control plane, scheduler, proxy, cert manager, and registry. The only host dependency is Docker.
---

::: hero glow:true
# ⛵ Zattera

**Your servers. One binary. A full PaaS.**

Turn any pool of machines — bare metal, VPS, multi-cloud, the server under your desk — into a Heroku/Vercel-like cluster platform. One Go binary that is the CLI, the control plane, the scheduler, the proxy, the cert manager, and the registry. The only thing your servers need is Docker.

::: button "Get started" getting-started/quickstart color:#0e7490 icon:rocket
::: button "View on GitHub" https://github.com/adileo/zattera.dev icon:github
:::

::: callout warning Pre-alpha
Zattera is an ambitious, fast-moving experiment. We're looking for **early adopters and alpha testers** — expect sharp edges, and please [report what you find](https://github.com/adileo/zattera.dev/issues).
:::

## From zero to deployed in four commands

::: steps

1. **Start the cluster** — on your first server. It asks for a domain (e.g. `mycluster.example.com`) and prints your login + join commands.

   ```bash
   curl -fsSL https://get.zattera.dev | sh
   sudo zattera cluster init
   ```

2. **Join more machines** — anywhere in the world; NAT'd home servers included. The previous command prints this line for you.

   ```bash
   curl -fsSL https://get.zattera.dev | sh && sudo zattera cluster join <control-ip>:8443 --token <JOIN_TOKEN>
   ```

3. **Log in from your laptop** — the CLI is a pure API client.

   ```bash
   zt login --server https://<control-ip>:8443 --ca-pin <FINGERPRINT> --token <ADMIN_TOKEN> --context prod
   ```

4. **Ship it** — `cd` into any Nixpacks or Dockerfile app and run:

   ```bash
   zt deploy --prod
   ```

   ```
   ✓ Built api (nixpacks, 34s)
   ✓ Released v42 → production (red/green, 2 replicas healthy)
   ● https://api.yourcustomdomain.com
     https://api-prod.mycluster.example.com
   ```

:::

No YAML. No web panel to install. No cloud account. Your metal, your platform.

## Why Zattera

Every alternative makes you choose: a web panel bolted on Docker with no real orchestration, a bare CLI that leaves scheduling and state to you, or the full cloud experience *if* you operate Kubernetes first. Zattera takes the untaken quadrant: **multi-server orchestration with zero platform dependencies**.

::: grids
::: grid
::: card No Kubernetes icon:box
No etcd, no CNI/CSI/Ingress zoo, no YAML sprawl. Setting up your infrastructure shouldn't require a platform team.
:::
:::
::: grid
::: card No external database icon:database
Platform state lives in embedded Raft, replicated across nodes. The control plane can't die because "its database died".
:::
:::
::: grid
::: card No proxy/certbot stack icon:shield-check
Traffic proxy, load balancing, and Let's Encrypt certificates live in-process. No config generation, no version skew, no "cert renewed but proxy didn't reload".
:::
:::
::: grid
::: card No heavy panel icon:feather
Workers run an agent measured in tens of MB — not a 2 GB dashboard per host. CLI + API first.
:::
:::
::: grid
::: card No vendor anything icon:server
Builds, images, logs, metrics — all on your metal. Works air-gapped. No lock-in.
:::
:::
::: grid
::: card Nodes anywhere icon:globe
WireGuard mesh + gossip: multi-region, multi-cloud, and NAT'd home servers are first-class. Start with one node, grow with a single `--join`.
:::
:::
:::

## What you get

::: grids
::: grid
::: card Deploy anything icon:package
Nixpacks auto-detection or your Dockerfile, built via BuildKit on your own builders, stored in the embedded OCI registry — no Docker Hub required.
:::
:::
::: grid
::: card Vercel-style flow icon:git-branch
`zt deploy --prod`, GitHub push-to-deploy, staging/production/preview environments, env vars & secrets, custom domains with automatic HTTPS.
:::
:::
::: grid
::: card Red/green releases icon:refresh-cw
The new version must be fully healthy **before** traffic switches. Instant rollback — the previous release stays warm.
:::
:::
::: grid
::: card Scale icon:trending-up
Replica autoscaling, cross-node load balancing, scale-to-zero with wake-on-request, serverless concurrency mode.
:::
:::
::: grid
::: card Internal DNS icon:network
Services talk cross-node via `db.production.myproject.internal` over the encrypted mesh. Staging never sees production.
:::
:::
::: grid
::: card Stateful apps icon:hard-drive
Volumes pinned to nodes for Postgres/Redis/…, browsable from the CLI, snapshotted to S3 — incremental and content-addressed.
:::
:::
::: grid
::: card Disaster recovery icon:life-buoy
Restore the whole platform — state, volumes, and images — onto fresh infrastructure with one command.
:::
:::
::: grid
::: card Real operations icon:activity
Logs, metrics, alerts, jobs & cron, `zattera attach/top/fs/port-forward`, audit log, RBAC. Everything scriptable through the API.
:::
:::
:::

## How it works

```
 CLI ──HTTPS──▶ Control plane (1–5 nodes: API · Raft · Scheduler · Builder · ACME)
                      │ mTLS over WireGuard mesh
        ┌─────────────┴─────────────┐
   Worker node                 Worker node
   agent · proxy · docker      agent · proxy · docker
   (Hetzner, eu)               (home server, NAT)
```

You declare desired state; Raft replicates it; the scheduler continuously reconciles reality against it. Kill a node and stateless replicas reschedule in seconds. Export the whole platform as YAML with `zattera state export`.

## Explore the docs

::: grids
::: grid
::: card Quickstart icon:rocket
Install Zattera, init a cluster, and deploy your first app in minutes.

[Get started →](getting-started/quickstart)
:::
:::
::: grid
::: card Binary distribution icon:download
How `get.zattera.dev` works: GitHub Actions builds, Releases hosts, Pages serves the installer.

[Read more →](distribution)
:::
:::
::: grid
::: card Contributing icon:users
Design discussions happen in Issues/Discussions; architecture decisions are recorded as ADRs.

[Architecture decision records →](contributing/architecture-decision-records/)
:::
:::
:::

::: callout info License
Zattera is open source under the [Apache-2.0 license](https://github.com/adileo/zattera.dev/blob/main/LICENSE).
:::
