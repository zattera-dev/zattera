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
| `[build.args]` | — | Build arguments (string map) |

### `[github]`

| Key | Meaning |
| --- | ------- |
| `repo` | `owner/name` for [push-to-deploy](github) |
| `previews` | Enable [preview environments](preview-environments) *(work in progress)* |

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

### `[env.<name>]` — one section per environment

| Key | Default | Meaning |
| --- | ------- | ------- |
| `replicas` | `1` | Fixed replica count (sets min = max) |
| `min_replicas` / `max_replicas` | `1` / `1` | Replica range (the range beyond min is used by [autoscaling](../scaling/autoscaling), WIP) |
| `domains` | — | [Custom domains](custom-domains) for this environment |
| `command` | image default | Override the container command |
| `stop_grace` | `10s` | Graceful-stop window before kill |
| `stateful` | `false` | Stateful service semantics *(volumes are WIP)* |
| `idle_timeout` | — | [Scale-to-zero](../scaling/scale-to-zero) idle window *(WIP)* |
| `scale_to_zero` / `max_concurrency` | — | Serverless mode *(WIP)* |
| `[env.<name>.resources]` | — | `cpu_millis`, `memory_mb` reservations (used for placement) |
| `[env.<name>.autoscale]` | — | `target_cpu_percent`, `target_memory_percent`, `target_rps_per_replica` *(WIP)* |
| `[env.<name>.placement]` | — | Node label constraints (string map) |

#### `[[env.<name>.ports]]`

| Key | Default | Meaning |
| --- | ------- | ------- |
| `name` | `http` | Port name (referenced by `port-forward`, domains `--port`) |
| `container_port` | `8080` | Port the app listens on |
| `protocol` | `http` | `http`, `tcp` (L4 passthrough), or `udp` |

No ports declared = one `http` port on `8080`.

#### `[[env.<name>.volumes]]` *(work in progress)*

`name` + `mount_path`, both required. See [Volumes](../data/volumes).

### `[[cron]]` *(scheduling is work in progress)*

Global cron entries, overridable per environment with `[[env.<name>.cron]]`: `name`, `schedule` (5-field cron), `command`, `concurrency` (`forbid` default / `replace` / `allow`), `max_retries`. The parser accepts these today; the cron scheduler ships with T-67 — see [Jobs](../operations/jobs).

## How it's applied

`zt apply` (or a source `zt deploy`) parses the file locally and sends the result to the API: build config on the app, one **service spec** per `[env.*]` section (environments not mentioned are left untouched; new ones are created). Each deploy then freezes the current spec into the release, and the spec's deterministic hash — combined with an env-var fingerprint — becomes the release's **config hash**, which is how agents know a container must be replaced.
