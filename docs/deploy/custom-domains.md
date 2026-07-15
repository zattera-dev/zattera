---
title: Custom domains
description: Attach your own hostnames to any environment, with automatic Let's Encrypt certificates.
---

# Custom domains

Every environment is reachable out of the box at its **cluster subdomain** — `<app>-<env>.<cluster-domain>` (e.g. `api-production.apps.example.com`), with TLS included. Custom domains put your own hostname in front of the same environment.

## How to use

Point DNS at your cluster first — an `A` record (or `CNAME`) from your hostname to any ingress node's public IP. Traffic can enter through **any** node; it routes over the mesh to wherever the app runs.

```bash
zt domains add api.mycompany.com --app api --prod
# Added api.mycompany.com → api (production)
# certificate: pending

zt domains ls        # HOSTNAME + certificate status (pending / issued / failed)
zt domains rm api.mycompany.com
```

Options:

- `--env NAME | --prod` — target environment (default: staging).
- `--path /admin` — route only a path prefix, so different apps can share one hostname.
- `--port NAME` — target a specific service port (default: the first HTTP port).

You can also declare domains per environment in [`zattera.toml`](zattera-toml) (`domains = ["api.mycompany.com"]`).

The certificate is issued automatically on the first HTTPS request once DNS resolves to the cluster — usually within seconds. `zt domains ls` shows the status.

## How it works

### Routing

The control plane builds a routing table from desired state: each hostname (+ optional path prefix) maps to the healthy instances of its environment. Every ingress node streams this table and serves `:80`/`:443` — hostnames match exactly, then the longest path prefix wins. Requests balance across instances (P2C — power of two choices, preferring node-local instances) and only ever reach **healthy** ones.

Custom hostnames may not collide with the reserved `<app>-<env>.<cluster-domain>` namespace — the API rejects those.

### TLS certificates

Zattera embeds an ACME client (Let's Encrypt) — no certbot, no nginx reload dance:

- **On-demand issuance**: the first TLS handshake for a hostname triggers issuance, but *only* for hostnames present in the routing table. Random strangers pointing DNS at your cluster can't mint certificates.
- **HTTP-01** challenges are answered on the `:80` listener (which otherwise 308-redirects to HTTPS). The hostname must publicly resolve to a cluster node for issuance to succeed.
- **Certificates live in replicated cluster state**, not on any single node's disk — every ingress node can serve every certificate, issuance is serialized cluster-wide, and renewal is automatic.
- The Let's Encrypt **staging** endpoint can be selected in the [server config](../setup/configuration) (`[acme] staging = true`) while testing, to avoid rate limits.

Wildcard certificates (DNS-01) are on the [roadmap](../roadmap/tasks) (T-72/T-73, M4).
