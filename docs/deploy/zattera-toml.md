---
title: zattera.toml reference
description: The app configuration file — build settings, per-environment replicas, ports, health checks, resources, and cron.
---

# zattera.toml

`zattera.toml` lives in your app's repository and declares how it builds and runs, per environment. `zt init` generates a starter file; `zt deploy` (source deploys) and `zt apply` push it to the cluster.

::: callout tip Strict parsing
Like the server config, unknown keys are a **hard error** with the offending keys listed — typos fail fast instead of being silently ignored. Durations are strings like `"15m"`, `"30s"`.
:::

## Example

```toml
[app]
name = "web"

[build]
type = "dockerfile"            # dockerfile | nixpacks | image (auto-detected if omitted)
dockerfile = "docker/Dockerfile"
context = "src"
[build.args]
NODE_ENV = "production"

[github]
repo = "acme/web"

[deploy.healthcheck]
type = "http"                  # http | tcp | exec
path = "/status"
interval = "15s"
timeout = "3s"
grace_period = "30s"
unhealthy_threshold = 5

[env.production]
min_replicas = 2
max_replicas = 6
domains = ["web.example.com", "www.example.com"]
command = "server --prod"

[env.production.resources]
cpu_millis = 500
memory_mb = 512

[[env.production.ports]]
name = "http"
container_port = 3000
protocol = "http"              # http | tcp | udp

[env.staging]
replicas = 1
```

The minimal valid file is `[app] name` plus one `[env.<name>]` section.

## Reference

### `[app]`

| Key | Meaning |
| --- | ------- |
| `name` | App name (required, DNS-safe) |

### `[build]`

| Key | Default | Meaning |
| --- | ------- | ------- |
| `type` | auto | `dockerfile`, `nixpacks`, or `image`. Omitted: a `Dockerfile` in the context → dockerfile, else nixpacks |
| `dockerfile` | `Dockerfile` | Dockerfile path |
| `context` | `.` | Build context directory |
| `image` | — | Prebuilt image ref (with `type = "image"`) |
| `platforms` | cluster default | Target platforms to build, e.g. `["linux/amd64", "linux/arm64"]`. Normalized at parse time (`aarch64` → `arm64`, …); an unrecognized value is a hard error. Omitted = resolved at build time |
| `[build.args]` | — | Build arguments (string map) |

Declared `platforms` flow into the release and become a **placement constraint**: a node whose engine can't run any of them is filtered out. See [Builds](builds) for multi-arch.

### `[github]`

| Key | Meaning |
| --- | ------- |
| `repo` | `owner/name` for [push-to-deploy](github) |
| `previews` | Enable [preview environments](preview-environments): each pull request gets its own environment cloned from `staging` |

### `[deploy.healthcheck]`

| Key | Default | Meaning |
| --- | ------- | ------- |
| `type` | `http` if an HTTP port exists | `http`, `tcp`, or `exec` |
| `path` | `/healthz` | HTTP path (2xx/3xx = pass) |
| `port` | first port | Port to probe |
| `command` | — | Command for `exec` checks (exit 0 = pass) |
| `interval` / `timeout` | `10s` / `5s` | Probe cadence |
| `grace_period` | `60s` | Time allowed to become healthy after start |
| `unhealthy_threshold` | `3` | Consecutive failures before UNHEALTHY |

If you declare nothing and the app has an HTTP port, you get the HTTP `/healthz` check with these defaults. Health checks gate [red/green promotion](./) — traffic never switches to instances that haven't passed.

::: callout note A TCP/UDP-only service gets no health check by default
The implicit check is only added when the service exposes an **HTTP** port. If every declared port is `tcp` or `udp` and you declare no `[deploy.healthcheck]`, the service has **no health check at all** — it is considered ready as soon as its container runs. Declare `type = "tcp"` (or `exec`) explicitly for databases and other L4 services.
:::

### `[env.<name>]` — one section per environment

| Key | Default | Meaning |
| --- | ------- | ------- |
| `replicas` | `1` | Fixed replica count (sets min = max). **Ignored** when `min_replicas`/`max_replicas` are present |
| `min_replicas` / `max_replicas` | `1` / `1` | Replica range (the range beyond min is used by [autoscaling](../scaling/autoscaling)). Setting only `min_replicas` makes `max` follow it, not `1` |
| `domains` | — | [Custom domains](custom-domains) for this environment |
| `command` | image default | Override the container command |
| `stop_grace` | `10s` | Graceful-stop window before kill |
| `stateful` | `false` | Stateful service: node-pinned [volumes](../data/volumes), exactly-one, stop-then-start deploys |
| `scale_to_zero` / `idle_timeout` | — | Cool an idle env to 0 replicas after the window — [Scale-to-zero](../scaling/scale-to-zero) |
| `max_concurrency` | — | Serverless mode: scale on in-flight requests per replica — [Scale-to-zero](../scaling/scale-to-zero#scale-to-zero-serverless-serverless-concurrency-mode) |
| `[env.<name>.resources]` | — | `cpu_millis`, `memory_mb` reservations (used for placement) |
| `[env.<name>.autoscale]` | — | [Autoscaling](../scaling/autoscaling) targets: `target_cpu_percent`, `target_memory_percent`, `target_rps_per_replica` |
| `[env.<name>.placement]` | — | Node label constraints (string map); set labels with [`zt nodes label`](../setup/nodes#nodes-labels-and-placement) |
| `[env.<name>.rate_limit]` | off | Per-client-IP request cap at the ingress — see below |

#### `[env.<name>.rate_limit]`

| Key | Default | Meaning |
| --- | ------- | ------- |
| `requests_per_second` | **none — required** | Sustained requests allowed per client IP |
| `burst` | `requests_per_second` | Bucket depth: how many requests a client may fire back-to-back before the sustained rate applies |

Omit the section and there is no limiting — this is off by default and opt-in per
environment.

```toml
[env.production.rate_limit]
requests_per_second = 20
burst = 40
```

Requests over the limit get `429` with a `Retry-After` header, and never reach
your container. The limit applies to every hostname serving that environment:
custom domains and the built-in `<app>-<env>.<cluster-domain>` subdomain alike.

**The limit is per ingress node.** Each node keeps its own counters with no
cross-node coordination, so with N nodes serving traffic the cluster-wide
ceiling is `requests_per_second × N`. In practice a single client is pinned to
one node by DNS caching, so per-client enforcement matches what you configured —
it is the aggregate across many clients that can exceed it. Size the number for
"one abusive client", not for a global quota.

Clients are identified by the connection's source address. `X-Forwarded-For` is
deliberately ignored: it is trivially spoofable, and trusting it would let any
caller mint unlimited identities. If you run another proxy or CDN in front of
Zattera, every request will appear to come from that proxy's IP and share a
single bucket.

#### `[[env.<name>.ports]]`

| Key | Default | Meaning |
| --- | ------- | ------- |
| `name` | `http` | Port name (referenced by `port-forward`, domains `--port`) |
| `container_port` | **none — required** | Port the app listens on |
| `protocol` | `http` | `http`, `tcp` (L4 passthrough), or `udp` |

Omitting the `[[ports]]` array entirely gives you one `http` port on `8080`. But **once you declare a port block, `container_port` has no default** — leaving it out yields port `0`, which nothing can route to or health-check. The `8080` default applies to the *absent array*, not to an incomplete entry.

#### `[[env.<name>.volumes]]`

`name` + `mount_path`, both required. Declares a node-pinned persistent volume
for a `stateful` service. See [Volumes](../data/volumes).

### `[[cron]]`

Global cron entries: `name`, `schedule` (5-field cron, required), `command`, `concurrency` (`forbid` default / `replace` / `allow`), `max_retries`. The leader fires each due schedule as a one-shot job in the environment's active-release image; inspect with `zt cron ls`. See [Jobs & cron](../operations/jobs#jobs-cron).

An environment's `[[env.<name>.cron]]` **replaces** the global list for that environment rather than adding to it — if you declare even one entry there, the global entries do not run in that environment. To keep a global schedule and add one, repeat both.

## What gets rejected

Two layers validate, and they fail at different moments.

**Parsed locally** — `zt apply`/`zt deploy` fails before anything reaches the cluster:

| Rule | Error |
| ---- | ----- |
| Unknown keys anywhere | `unknown keys: <list>` (every offender, sorted) |
| `[app] name` missing | `[app] name is required` |
| `replicas` negative | `replicas must be >= 0` |
| `min_replicas` > `max_replicas` | `replicas.min > max (N > M)` |
| A volume missing `name` or `mount_path` | `volumes: name and mount_path are required` |
| A cron entry with no `schedule`, or one that isn't 5 fields | `schedule is required` / `must be a 5-field cron expression` |
| Any duration that `time.ParseDuration` rejects, or is negative | `invalid duration "…"` / `duration must be >= 0` |
| A `platforms` entry that isn't a known `os/arch` | `build.platforms: …` |
| `rate_limit` with no `requests_per_second` | `requests_per_second must be > 0 (omit the section to disable)` |
| `rate_limit.burst` below `requests_per_second` | `burst (N) must be >= requests_per_second (M)` |

**Checked by the server** on apply:

| Rule | Why |
| ---- | --- |
| Environment names must be DNS-safe — `^[a-z0-9]([a-z0-9-]{0,38}[a-z0-9])?$`, so 1–40 chars, lowercase alphanumeric and dashes, not starting or ending with a dash | Names become hostnames |
| `[app] name` follows the same rule — enforced when the app is first created, which `zt apply` does for you | Same |
| `scale_to_zero` cannot be combined with `stateful` | A stateful service holds a single-writer volume lease and must not be torn down when idle |

Environment **names are meaningful**: `production` and `staging` get those types; anything else is treated as a preview environment.

## How it's applied

`zt apply` (or a source `zt deploy`) parses the file locally and sends the result to the API: build config on the app, one **service spec** per `[env.*]` section (environments not mentioned are left untouched; new ones are created). Each deploy then freezes the current spec into the release, and the spec's deterministic hash — combined with an env-var fingerprint — becomes the release's **config hash**, which is how agents know a container must be replaced.
