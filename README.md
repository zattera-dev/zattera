<div align="center">

# ⛵ Zattera

**The single-binary PaaS. Your servers, the Vercel experience.**

_Zattera — Italian for "raft". It runs on Raft._

[zattera.dev](https://zattera.dev) · [Docs](https://zattera.dev/docs) · [Quickstart](https://zattera.dev/docs/getting-started/quickstart)

![Status](https://img.shields.io/badge/status-pre--alpha-orange)
![License](https://img.shields.io/badge/license-Apache--2.0-blue)
![Go](https://img.shields.io/badge/go-%3E%3D1.24-00ADD8)
![Dependencies](https://img.shields.io/badge/host_dependencies-docker_only-green)

</div>

---

Turn any pool of machines — bare metal, VPS, multi-cloud, the server under your desk — into a Heroku/Vercel-grade platform. **One Go binary** that is the CLI, the control plane, the scheduler, the proxy, the cert manager, and the registry. The only thing your servers need is Docker.

```bash
# on your first server
curl -fsSL https://get.zattera.dev | sh

# on every other machine, anywhere in the world
curl -fsSL https://get.zattera.dev | sh -s -- --join zattera.example.com --token <JOIN_TOKEN>

# on your laptop
zattera deploy --prod
```

```
✓ Built api (nixpacks, 34s)
✓ Released v42 → production (red/green, 2 replicas healthy)
● https://api.example.com
```

## Why Zattera

Every alternative makes you choose: a web panel bolted on Docker with no real orchestration (Coolify, Dokploy), a bare CLI that leaves scheduling and state to you (Kamal), or the full cloud experience _if_ you operate Kubernetes first (Kubero, Cozystack). Zattera takes the untaken quadrant: **real multi-server orchestration with zero platform dependencies.**

- **No Kubernetes.** No etcd, no CNI/CSI/Ingress zoo, no YAML sprawl.
- **No external database.** State lives in embedded Raft — the platform that runs your Postgres doesn't die when _its_ Postgres dies. It doesn't have one.
- **No web-stack panel on your servers.** Workers run an agent measured in tens of MB, not a 2GB dashboard.
- **No bundled nginx/Traefik/certbot.** Proxy and ACME live in-process. No config generation, no version skew, no "cert renewed but proxy didn't reload".
- **No vendor anything.** Builds, images, logs, metrics — all on your metal. Works air-gapped.

## Features

- **Nodes anywhere** — WireGuard mesh + gossip: multi-region, multi-cloud, NAT'd home servers, all first-class. Starts at **one node**; grow with a single `--join`, drain and remove nodes freely.
- **Deploy anything** — Nixpacks auto-detection or Dockerfile, built on your own builders, stored in the embedded registry.
- **Vercel-style flow** — `zattera deploy --prod`, GitHub push-to-deploy, staging/production/preview environments, env vars & secrets, custom domains + automatic Let's Encrypt.
- **Red/green releases** — new version fully healthy before traffic switches; instant rollback.
- **Scale** — replica autoscaling, load balancing across nodes, scale-to-zero with wake-on-request, serverless concurrency mode.
- **Internal DNS** — services talk cross-node via `db.production.myproject.internal` over the encrypted mesh; staging never sees production.
- **Stateful apps** — pinned volumes for Postgres/Redis/…, browsable from the CLI, snapshotted to S3.
- **Disaster recovery** — full platform restore (state + volumes + images) onto fresh infrastructure with one command.
- **Operate it** — logs, metrics, alerts, jobs & cron, `zattera attach/top/fs/port-forward`, audit log, RBAC, API-first (the CLI is a pure API client).
- **Coming later** — node autoprovisioning: Zattera buys Hetzner/DO/AWS machines when the pool is full and destroys them when idle, with budget caps.

## What Zattera does

> **Status note:** Pre-alpha. The spec is complete and core foundations exist (Raft, protos, scheduler, mesh, registry), but M1 (deploy → HTTPS → rollback end-to-end) is not finished yet. Most items below are designed/planned, not all shipped today.

### Platform model

- **Single Go binary** — CLI, control plane, worker agent, proxy, and registry in one binary
- **Docker only** — no Postgres, Redis, etcd, nginx, or certbot on hosts
- **Embedded Raft state** — no external control-plane database
- **One-line install & join** — workers join with `--join` + token
- **Single node → cluster** — fully functional on one machine; grow with `--join`, drain/remove nodes

### Deploy & build

- **Nixpacks + Dockerfile** builds via BuildKit
- **Pre-built image** deploys
- **Embedded OCI registry** — no Docker Hub required
- **Git push-to-deploy** — GitHub webhook/App, branch → environment mapping
- **Environments** — staging, production, preview-\*
- **Red/green deploys** — traffic switches only after health checks pass
- **Instant rollback** — previous release kept warm (~10 min)
- **Config as code** — `zattera.toml` + CLI/API

### Multi-server orchestration

- **Own scheduler** — bin-packing, spread, node labels/constraints
- **True multi-server pooling** — not independent per-server management
- **WireGuard mesh** — cross-region, multi-cloud, NAT/home servers as first-class
- **HA control plane** — 3–5 node Raft quorum
- **Node drain/remove** — graceful migration for stateless workloads
- **Autoscaling** — CPU/RAM/RPS-driven replica scaling
- **Scale-to-zero** — idle timeout → 0 replicas; wake on request (M3)
- **Serverless mode** — concurrency-based scaling (M3)

### Networking & traffic

- **Embedded L7/L4 proxy** — HTTP/2, WebSocket, TCP passthrough
- **Load balancing** — P2C, health checks, optional sticky sessions
- **TLS / ACME** — embedded Let's Encrypt; HTTP-01 in M1–M3
- **Internal DNS** — `service.env.project.internal` across nodes over the mesh
- **Any node as ingress** — traffic can enter anywhere and route over the mesh

### Stateful & data

- **Volumes** — pinned to nodes for Postgres, Redis, etc.
- **Volume CLI** — browse, cp, snapshot
- **S3 snapshots** — incremental, content-addressed
- **Full disaster recovery** — one-command restore: state + volumes + images (M2)
- **Honest data HA** — snapshots + app-level replication; no fake sync replication

### Operations

- **Logs** — tail, stream, retention; `zattera logs -f`
- **Metrics** — per-app/node; historical TSDB (M2)
- **Alerts** — webhook/Slack/email (M3)
- **Jobs & cron** — one-shot jobs + scheduled cron (M2)
- **Remote debug** — attach, top, fs, port-forward over API tunnel
- **RBAC** — org → project → environment roles
- **Audit log** — mutating API calls recorded
- **API-first** — CLI is a pure API client; everything scriptable
- **State export/apply** — GitOps-lite cluster config export

### Coming later

- **SSO/OIDC** (M4)
- **Wildcard certs via DNS-01** (M4)
- **Node autoprovisioning** — Hetzner, then DO/AWS (M5)
- **External log sinks, Prometheus endpoint** (M4)

## What Zattera deliberately doesn't do

Zattera's edge is not a longer feature list — it's focus on the deploy-and-run path and refusing what makes alternatives heavy or fragile.

| Doesn't do                      | Who typically does         | Why Zattera skips it                            |
| ------------------------------- | -------------------------- | ----------------------------------------------- |
| Run Kubernetes                  | Kubero, Cozystack, Devtron | No etcd/CNI/CSI/Ingress zoo, no YAML sprawl     |
| Multi-container pods / sidecars | Kubernetes                 | One container per service instance              |
| Service mesh / network policies | Istio, Linkerd, K8s        | Complexity far exceeds typical app-deploy needs |
| Operators / CRDs / plugins      | K8s ecosystem              | No extension sprawl                             |
| Docker Swarm orchestration      | Dokploy, CapRover          | Weak for cross-region/NAT; maintenance mode     |
| General-purpose orchestrator    | Nomad, K8s                 | App platform, not a generic scheduler           |
| Web GUI on servers              | Coolify, Dokploy, CapRover | CLI + API first; no 2GB panel on every host     |
| 280+ one-click app templates    | Coolify, CapRover          | Maintenance treadmill; docs/recipes instead     |
| Separate Traefik/Nginx/certbot  | Coolify, Dokploy, CapRover | Proxy + ACME in-process                         |
| External DB for the platform    | Coolify (Postgres+Redis)   | Control plane shouldn't die when its DB dies    |
| Distributed volume replication  | Longhorn, Ceph             | Honest RPO via snapshots, not fake HA           |
| Vendor-hosted builds/images     | Vercel, Railway, Fly       | Builds and images stay on your metal            |

See [What we deliberately don't do](./paas-specification.md#101-what-we-deliberately-dont-do) in the full specification.

## How Zattera compares

| Capability                      | **Zattera**       | **Dokploy**     | **Kubernetes**  | **Coolify**            | **Kamal**          |
| ------------------------------- | ----------------- | --------------- | --------------- | ---------------------- | ------------------ |
| Real multi-server scheduling    | ✅ Own scheduler  | ⚠️ Docker Swarm | ✅ Native       | ❌ Independent servers | ⚠️ Manual per-host |
| NAT / home servers              | ✅ WireGuard mesh | ❌              | ❌              | ❌ SSH-only            | ❌ SSH-only        |
| Control-plane footprint         | ✅ One binary     | Panel + Swarm   | Full cluster    | ~2GB panel + DB        | Client-only CLI    |
| Scale-to-zero / serverless      | ✅ (M3)           | ❌              | ⚠️ Via add-ons  | ❌                     | ❌                 |
| Red/green + instant rollback    | ✅                | Rolling         | Rollouts        | Rolling                | ✅                 |
| Web admin UI                    | ❌ CLI/API        | ✅              | Many            | ✅                     | ❌                 |
| One-click app catalog           | ❌                | ✅              | Helm charts     | ✅                     | ❌                 |
| K8s ecosystem (CRDs, mesh, CSI) | ❌                | ❌              | ✅              | ❌                     | ❌                 |
| Full DR from S3                 | ✅ (M2)           | Partial         | Velero (add-on) | Partial                | Git config         |
| Requires K8s expertise          | ❌                | ❌              | ✅              | ❌                     | ❌                 |

**Pick Zattera** if you want Vercel/Heroku-style deploys on your own servers, multi-region/home-lab workers without K8s, and red/green + scale-to-zero + DR as first-class features.

**Pick Dokploy/Coolify** if you want a web UI today, one-click templates, and are fine with a heavier control-plane stack.

**Pick Kubernetes** if you need pods, sidecars, CRDs, service mesh, CSI storage, and have platform-engineering capacity.

**Pick Kamal** if you want minimal explicit deploys to known hosts and will manage scaling and state yourself.

Read the full [comparison](./paas-specification.md#10-comparison--zattera-vs-the-field) in the specification.

## How it works

```
 CLI ──HTTPS──▶ Control plane (1–5 nodes: API · Raft · Scheduler · Builder · ACME)
                      │ mTLS over WireGuard mesh
        ┌─────────────┴─────────────┐
   Worker node                 Worker node
   agent · proxy · docker      agent · proxy · docker
   (Hetzner, eu)               (home server, NAT)
```

Desired state is declared, replicated via Raft, and continuously reconciled. Kill a node: stateless replicas reschedule in seconds. Export the whole platform as YAML with `zattera state export`. Read the full [specification](./paas-specification.md).

## Status

⚠️ **Pre-alpha — spec stage.** The [specification](./paas-specification.md) is complete; implementation is starting with the [M1 milestone](./paas-specification.md#8-roadmap) (single control node, builds, red/green deploys, proxy + ACME, CLI, GitHub integration). Star the repo to follow along, open a Discussion to influence the design.

## Non-goals

Multi-container pods, service mesh, plugins/CRDs, template catalogs, Windows containers, being a general-purpose orchestrator. The subtraction is the product — see [What we deliberately don't do](./paas-specification.md#101-what-we-deliberately-dont-do).

## Contributing

Design discussions happen in Issues/Discussions; architecture decisions are recorded as [ADRs](./docs/contributing/architecture-decision-records/). Dev setup in [CONTRIBUTING.md](./CONTRIBUTING.md).

## License

[Apache-2.0](./LICENSE)
