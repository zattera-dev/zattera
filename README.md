<div align="center">

# ⛵ Zattera

### Your servers. One binary. A full PaaS.

**Turn any pool of machines into your own Heroku — in the time it takes to brew a coffee.**

_Zattera is Italian for "raft". Fitting: it floats on Raft consensus._

[zattera.dev](https://zattera.dev) · [Docs](https://zattera.dev) · [Quickstart](https://zattera.dev/getting-started/quickstart)

![Status](https://img.shields.io/badge/status-pre--alpha-orange)
![License](https://img.shields.io/badge/license-Apache--2.0-blue)
![Go](https://img.shields.io/badge/go-%3E%3D1.24-00ADD8)
![Dependencies](https://img.shields.io/badge/host_dependencies-docker_only-green)

</div>

---

You have machines: a bare-metal box, a couple of VPSes, maybe the server humming under your desk. What you _don't_ have is a weekend to spend wiring together Kubernetes, Traefik, certbot, a registry, and a Postgres just to deploy your app.

Zattera is **one Go binary** that _is_ the CLI, the control plane, the scheduler, the proxy, the cert manager, and the registry. Your servers need Docker. That's it. That's the whole dependency list.

## From zero to deployed in four commands

```bash
# 1. On your first server — start the cluster
#    Asks for a domain (e.g. mycluster.example.com), prints your login + join commands
curl -fsSL https://get.zattera.dev | sh
sudo zattera cluster init

# 2. On every other machine, anywhere in the world — join it
#    (the previous command prints this line for you)
curl -fsSL https://get.zattera.dev | sh && sudo zattera cluster join <control-ip>:8443 --token <JOIN_TOKEN>

# 3. From your laptop — log in and take the helm
zt login --server https://<control-ip>:8443 --ca-pin <FINGERPRINT> --token <ADMIN_TOKEN> --context prod

# 4. cd into your app (any Nixpacks / Dockerfile app) and ship it
zt deploy --prod
```

```
✓ Built api (nixpacks, 34s)
✓ Released v42 → production (red/green, 2 replicas healthy)
● https://api.yourcustomdomain.com
  https://api-prod.mycluster.example.com
```

No YAML. No panel to install. No cloud account. Your metal, your platform.

## Why Zattera exists

Every self-hosting tool today forces a trade-off:

- **Web panels on Docker** (Coolify, Dokploy) — friendly, but no real multi-server orchestration underneath.
- **Bare deploy CLIs** (Kamal) — minimal, but scheduling, state, and failover are _your_ problem.
- **"Just run Kubernetes first"** (Kubero, Cozystack) — the full cloud experience, if you staff a platform team.

Zattera takes the quadrant nobody claimed: **real multi-server orchestration with zero platform dependencies.**

That means a long list of things you'll never install, configure, or debug at 3 AM:

- 🚫 **No Kubernetes.** No etcd, no CNI/CSI/Ingress zoo, no YAML sprawl. Setting up your infrastructure shouldn't require five Kubernetes DevOps engineers.
- 🚫 **No Docker Swarm.** No orchestrator in maintenance mode holding your production together.
- 🚫 **No external database.** State lives in embedded Raft, replicated across nodes. The control plane can't die because "its database died".
- 🚫 **No web-stack panel on your servers.** Workers run an agent measured in tens of MB — not a 2 GB dashboard per host.
- 🚫 **No bundled nginx/Traefik/certbot.** Proxying, load balancing, and Let's Encrypt certificates live in-process. No config generation, no version skew, no "cert renewed but the proxy didn't reload".
- 🚫 **No vendor anything.** Builds, images, logs, metrics — all on your metal. Works air-gapped. Leaves with you.

## What you get

- 🌍 **Nodes anywhere** — WireGuard mesh + gossip make multi-region, multi-cloud, and NAT'd home servers first-class citizens. Start with **one node**; grow with a single `--join`; drain and remove freely.
- 📦 **Deploy anything** — Nixpacks auto-detection or your Dockerfile, built on your own builders, stored in the embedded registry. No Docker Hub required.
- ⚡ **Vercel-style flow** — `zattera deploy --prod`, GitHub push-to-deploy, staging/production/preview environments, env vars & secrets, custom domains with automatic Let's Encrypt.
- 🔄 **Red/green releases** — the new version must be fully healthy _before_ traffic switches. Rollback is instant; the previous release stays warm.
- 📈 **Scale** — replica autoscaling, cross-node load balancing, scale-to-zero with wake-on-request, serverless concurrency mode.
- 🔒 **Internal DNS** — services talk cross-node via `db.production.myproject.internal` over the encrypted mesh. Staging never sees production.
- 💾 **Stateful apps** — volumes pinned to nodes for Postgres/Redis/…, browsable from the CLI, snapshotted to S3.
- 🛟 **Disaster recovery** — restore the whole platform (state + volumes + images) onto fresh infrastructure with one command.
- 🔧 **Real operations** — logs, metrics, alerts, jobs & cron, `zattera attach/top/fs/port-forward`, audit log, RBAC. API-first: the CLI is a pure API client, so everything is scriptable.
- 🔮 **Coming later** — node autoprovisioning: Zattera buys Hetzner/DO/AWS machines when the pool is full and destroys them when idle, with budget caps.

## The full picture

> **Status note:** Pre-alpha. Zattera.dev is an ambitious PoC/experiment — the base structure is largely "vibe-coded", though architectural choices and most tests are made and verified manually by the maintainer. **We're looking for early adopters and alpha testers.**

<details>
<summary><b>🏗️ Deploy & build</b></summary>

- **Nixpacks + Dockerfile** builds via BuildKit
- **Pre-built image** deploys
- **Embedded OCI registry** — no Docker Hub required
- **Git push-to-deploy** — GitHub webhook/App, branch → environment mapping
- **Environments** — staging, production, preview-\*
- **Red/green deploys** — traffic switches only after health checks pass
- **Instant rollback** — previous release kept warm (~10 min)
- **Config as code** — `zattera.toml` + CLI/API

</details>

<details>
<summary><b>🌐 Multi-server orchestration</b></summary>

- **Own scheduler** — bin-packing, spread, node labels/constraints
- **True multi-server pooling** — not independent per-server management
- **WireGuard mesh** — cross-region, multi-cloud, NAT/home servers as first-class
- **HA control plane** — 3–5 node Raft quorum
- **Node drain/remove** — graceful migration for stateless workloads
- **Autoscaling** — CPU/RAM/RPS-driven replica scaling
- **Scale-to-zero** — idle timeout → 0 replicas; wake on request (M3)
- **Serverless mode** — concurrency-based scaling (M3)

</details>

<details>
<summary><b>🚦 Networking & traffic</b></summary>

- **Embedded L7/L4 proxy** — HTTP/2, WebSocket, TCP passthrough
- **Load balancing** — P2C, health checks, optional sticky sessions
- **TLS / ACME** — embedded Let's Encrypt; HTTP-01 in M1–M3
- **Internal DNS** — `service.env.project.internal` across nodes over the mesh
- **Any node as ingress** — traffic can enter anywhere and route over the mesh

</details>

<details>
<summary><b>💾 Stateful & data</b></summary>

- **Volumes** — pinned to nodes for Postgres, Redis, etc.
- **Volume CLI** — browse, cp, snapshot
- **S3 snapshots** — incremental, content-addressed
- **Full disaster recovery** — one-command restore: state + volumes + images (M2)
- **Honest data HA** — snapshots + app-level replication; no fake sync replication

</details>

<details>
<summary><b>🔧 Operations</b></summary>

- **Logs** — tail, stream, retention; `zattera logs -f`
- **Metrics** — per-app/node; historical TSDB (M2)
- **Alerts** — webhook/Slack/email (M3)
- **Jobs & cron** — one-shot jobs + scheduled cron (M2)
- **Remote debug** — attach, top, fs, port-forward over API tunnel
- **RBAC** — org → project → environment roles
- **Audit log** — mutating API calls recorded
- **API-first** — CLI is a pure API client; everything scriptable
- **State export/apply** — GitOps-lite cluster config export

</details>

<details>
<summary><b>🔮 Coming later</b></summary>

- **SSO/OIDC** (M4)
- **Wildcard certs via DNS-01** (M4)
- **Node autoprovisioning** — Hetzner, then DO/AWS (M5)
- **External log sinks, Prometheus endpoint** (M4)

</details>

## What Zattera deliberately doesn't do

The subtraction _is_ the product. Every row below is a deliberate "no":

| Doesn't do                      | Who typically does         | Why Zattera skips it                            |
| ------------------------------- | -------------------------- | ----------------------------------------------- |
| Run Kubernetes                  | Kubero, Cozystack, Devtron | No etcd/CNI/CSI/Ingress zoo, no YAML sprawl     |
| Multi-container pods / sidecars | Kubernetes                 | One container per service instance              |
| Service mesh / network policies | Istio, Linkerd, K8s        | Complexity far exceeds typical app-deploy needs |
| Operators / CRDs / plugins      | K8s ecosystem              | No extension sprawl                             |
| Docker Swarm orchestration      | Dokploy, CapRover          | Weak for cross-region/NAT; maintenance mode     |
| General-purpose orchestrator    | Nomad, K8s                 | App platform, not a generic scheduler           |
| Web GUI on servers              | Coolify, Dokploy, CapRover | CLI + API first; no 2 GB panel on every host    |
| 280+ one-click app templates    | Coolify, CapRover          | Maintenance treadmill; docs/recipes instead     |
| Separate Traefik/Nginx/certbot  | Coolify, Dokploy, CapRover | Proxy + ACME in-process                         |
| External DB for the platform    | Coolify (Postgres+Redis)   | Control plane shouldn't die when its DB dies    |
| Distributed volume replication  | Longhorn, Ceph             | Honest RPO via snapshots, not fake HA           |
| Vendor-hosted builds/images     | Vercel, Railway, Fly       | Builds and images stay on your metal            |

## How it compares

| Capability                      | **Zattera**       | **Dokploy**     | **Kubernetes**  | **Coolify**            | **Kamal**          |
| ------------------------------- | ----------------- | --------------- | --------------- | ---------------------- | ------------------ |
| Real multi-server scheduling    | ✅ Own scheduler  | ⚠️ Docker Swarm | ✅ Native       | ❌ Independent servers | ⚠️ Manual per-host |
| NAT / home servers              | ✅ WireGuard mesh | ❌              | ❌              | ❌ SSH-only            | ❌ SSH-only        |
| Control-plane footprint         | ✅ One binary     | Panel + Swarm   | Full cluster    | ~2 GB panel + DB       | Client-only CLI    |
| Scale-to-zero / serverless      | ✅ (M3)           | ❌              | ⚠️ Via add-ons  | ❌                     | ❌                 |
| Red/green + instant rollback    | ✅                | Rolling         | Rollouts        | Rolling                | ✅                 |
| Web admin UI                    | ❌ CLI/API        | ✅              | Many            | ✅                     | ❌                 |
| One-click app catalog           | ❌                | ✅              | Helm charts     | ✅                     | ❌                 |
| K8s ecosystem (CRDs, mesh, CSI) | ❌                | ❌              | ✅              | ❌                     | ❌                 |
| Full DR from S3                 | ✅ (M2)           | Partial         | Velero (add-on) | Partial                | Git config         |
| Requires K8s expertise          | ❌                | ❌              | ✅              | ❌                     | ❌                 |

**Pick Zattera** if you want Vercel/Heroku-style deploys on your own servers, multi-region and home-lab workers without K8s, and red/green + scale-to-zero + DR as first-class features.

**Pick Dokploy/Coolify** if you want a web UI today, one-click templates, and are fine with a heavier control-plane stack.

**Pick Kubernetes** if you need pods, sidecars, CRDs, service mesh, CSI storage — and have the platform-engineering capacity to run it.

**Pick Kamal** if you want minimal, explicit deploys to known hosts and will manage scaling and state yourself.

## How it works

```
 CLI ──HTTPS──▶ Control plane (1–5 nodes: API · Raft · Scheduler · Builder · ACME)
                      │ mTLS over WireGuard mesh
        ┌─────────────┴─────────────┐
   Worker node                 Worker node
   agent · proxy · docker      agent · proxy · docker
   (Hetzner, eu)               (home server, NAT)
```

You declare desired state; Raft replicates it; the scheduler continuously reconciles reality against it. Kill a node and stateless replicas reschedule in seconds. Want the whole platform as code? `zattera state export` gives you the cluster as YAML.

## Non-goals

Multi-container pods, service mesh, plugins/CRDs, template catalogs, Windows containers, being a general-purpose orchestrator. See [What we deliberately don't do](./paas-specification.md#101-what-we-deliberately-dont-do).

## Contributing

Design discussions happen in Issues/Discussions; architecture decisions are recorded as [ADRs](./docs/contributing/architecture-decision-records/). Dev setup lives in [CONTRIBUTING.md](./docs/contributing/index.md).

## License

[Apache-2.0](./LICENSE)
