---
title: Configuration
description: The complete server configuration reference — config.toml keys, CLI flags, dev-mode defaults, and the data directory layout.
---

# Configuration

A Zattera node is configured by one TOML file — by convention `/etc/zattera/config.toml` — plus a handful of `zattera server` flags that override it. Precedence: **built-in defaults → TOML file → CLI flags**.

You rarely write this file by hand: `zattera cluster init` and `zattera cluster join` generate it (and a systemd unit) for you. This page is the reference for when you want to customize.

::: callout tip Strict parsing
Unknown keys are a **hard error**, not a silent ignore. A typo like `listn = ":8443"` fails fast at startup with `config: <path>: unknown keys: …`.
:::

## Example

```toml
node_name = "cp1"
data_dir  = "/var/lib/zattera"
roles     = ["control", "worker"]
domain    = "apps.example.com"        # apps get <app>-<env>.apps.example.com

[api]
listen         = ":8443"
advertise_addr = "cp1.example.com:8443"

[registry]
listen = ":5000"

[mesh]
listen_port = 51820

[acme]
email = "ops@example.com"
```

## Reference

### Top-level

| Key | Default | Meaning |
| --- | ------- | ------- |
| `node_name` | OS hostname | This node's name (required) |
| `data_dir` | `/var/lib/zattera` | Root for raft state, registry blobs, logs, certs |
| `roles` | `["control", "worker"]` | Any of `control`, `worker` |
| `domain` | — | Cluster app domain; apps are served at `<app>-<env>.<domain>` |
| `dev` | `false` | Single-node developer mode (see [dev mode](#dev-mode-defaults)) |

### `[api]`

| Key | Default | Meaning |
| --- | ------- | ------- |
| `listen` | `:8443` | Public gRPC + REST API listener (TLS) |
| `advertise_addr` | — | How other nodes and the CLI reach this API (`host:port`); also drives the public ACME certificate for the API |

### `[ingress]`

| Key | Default | Meaning |
| --- | ------- | ------- |
| `http_listen` | `:80` | Ingress HTTP listener (also serves ACME HTTP-01 challenges) |
| `https_listen` | `:443` | Ingress HTTPS listener |
| `disabled` | `false` | Turn the ingress proxy off on this node |

### `[registry]`

| Key | Default | Meaning |
| --- | ------- | ------- |
| `listen` | `:5000` | Embedded image registry listener |
| `insecure_http` | `false` | Serve the registry over plain HTTP (dev/tests only; forced on in dev mode) |

### `[mesh]`

| Key | Default | Meaning |
| --- | ------- | ------- |
| `disabled` | `false` | Disable the WireGuard mesh (implied in single-node dev) |
| `listen_port` | `51820` | WireGuard UDP port |
| `interface` | `zt0` | Interface name (macOS auto-picks `utunN`) |
| `public_endpoints` | autodetected | `ip:port` endpoints this node is reachable at |

### `[acme]`

| Key | Default | Meaning |
| --- | ------- | ------- |
| `email` | — | Let's Encrypt contact email |
| `disabled` | `false` | Disable ACME certificate issuance |
| `staging` | `false` | Use the Let's Encrypt staging endpoint (for testing without rate limits) |

### `[join]`

| Key | Default | Meaning |
| --- | ------- | ------- |
| `addr` | — | Any control node's API address (`host:8443`) to join |
| `token` | — | Join token (`K10<ca-hash>::<secret>`); required with `addr` |

Worker-only nodes (no `control` role) **must** set `join.addr`.

### `[raft]`

| Key | Default | Meaning |
| --- | ------- | ------- |
| `listen` | `:7480` | Raft transport (control nodes; loopback-bound on single-node) |

### `[logs]`

| Key | Default | Meaning |
| --- | ------- | ------- |
| `max_stream_mb` | `100` | Retained log bytes per stream |
| `retention_days` | `7` | Log age cap in days |

## `zattera server` flags

Flags override the corresponding config keys (only when non-empty):

| Flag | Overrides | Notes |
| ---- | --------- | ----- |
| `--config PATH` | — | Config file to load; empty = built-in defaults |
| `--data-dir DIR` | `data_dir` | |
| `--domain DOMAIN` | `domain` | |
| `--join ADDR` | `join.addr` | Control node to join |
| `--token TOKEN` | `join.token` | Applied together with `--join` |
| `--dev` | `dev`, `mesh.disabled`, `acme.disabled` | Forces all three on, then applies dev defaults |

## Dev-mode defaults

`--dev` runs a self-contained single node with self-signed TLS. It only overrides values you haven't set explicitly (except where marked *forced*):

| Setting | Production | Dev |
| ------- | ---------- | --- |
| Mesh | on | off *(forced)* |
| ACME | on | off *(forced)* — cluster-CA self-signed certs instead |
| `domain` | — | `apps.127.0.0.1.sslip.io` |
| Ingress HTTP / HTTPS | `:80` / `:443` | `:8080` / `:9443` |
| Registry | `:5000`, TLS | `:5001`, plain HTTP *(forced)* — avoids macOS AirPlay on 5000 |
| Built images | pushed to registry | loaded straight into local Docker |
| Managed containers on shutdown | kept running | cleaned up |

Dev mode prints a startup banner with every effective endpoint, plus machine-readable `DEVBANNER:` lines (`api`, `domain`, `ingress_http`, `ingress_https`, `registry`, `ca`, `ca_fingerprint`, `data_dir`, and — first boot only — `token`) for scripting.

## Data directory layout

Everything a node knows lives under `data_dir` (mode `0700`):

| Path | Contents |
| ---- | -------- |
| `ca/ca.crt`, `ca/ca.key` | Cluster CA (control nodes) |
| `raft/raft.db`, `raft/snapshots/` | Raft log + snapshots (control nodes) |
| `node-id` | Stable control-node identity |
| `node/` | Worker join identity: `node.crt`, `node.key`, `ca.crt`, `id`, `mesh.json` |
| `registry/blobs/sha256/` | Content-addressed image blobs |
| `uploads/` | Deploy source tarballs |
| `logs/` | Segmented container log store |
| `proxy/routes.pb` | Persisted route snapshot (workers) |

`zattera cluster init/join` additionally write `/etc/zattera/config.toml`, the `zattera.service` systemd unit, and (production) the registry CA trust under `/etc/docker/certs.d/`.

## Fixed ports

| Port | Purpose |
| ---- | ------- |
| `8443/tcp` | Public API |
| `80` / `443/tcp` | Ingress HTTP / HTTPS |
| `5000/tcp` | Embedded registry |
| `7480/tcp` | Raft transport |
| `8444/tcp` | Agent-local gRPC (mTLS) |
| `51820/udp` | WireGuard mesh |
