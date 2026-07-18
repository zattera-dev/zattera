---
title: Command reference
description: Every zattera CLI command, flag, and default.
---

# Command reference

All commands support `--json` for machine-readable output. `zt` is the installed shorthand for `zattera`. Shared flag conventions (project/app/env resolution) are covered in the [CLI overview](./).

## Authentication & contexts

| Command | Description |
| ------- | ----------- |
| `zt login --server ADDR --token TOKEN [--context NAME] [--ca-cert PATH \| --ca-pin FP] [--insecure]` | Authenticate and store a context; verified via WhoAmI before saving |
| `zt context` | List contexts (`*` marks active) |
| `zt context use <name>` | Switch the active context |
| `zt version` | Print the binary version |

## Projects & members

| Command | Description |
| ------- | ----------- |
| `zt projects create <name>` | Create a project |
| `zt projects ls` | List projects |
| `zt projects rm <name>` | Delete a project (cascades apps, envs, domains, volumes) |
| `zt projects members add <email> [--role owner\|admin\|developer\|viewer]` | Add a member (default role: `developer`) |
| `zt projects members ls` / `members rm <user-id>` | List / remove members |

## Apps & configuration

| Command | Description |
| ------- | ----------- |
| `zt apps create <name>` | Create an app (auto-creates `production` + `staging` environments) |
| `zt apps ls` / `apps rm <name>` | List / delete apps |
| `zt init [--name NAME]` | Detect app type in cwd and write a starter [`zattera.toml`](../deploy/zattera-toml) |
| `zt apply [-f zattera.toml]` | Apply a `zattera.toml` (build config + per-env service specs) |

## Environment variables

Defaults to `--env production`. See [Environment variables](../deploy/environment-variables).

| Command | Description |
| ------- | ----------- |
| `zt env set KEY=VALUE [KEY=VALUE…] [--app NAME] [--env NAME]` | Set variables (sealed at rest) |
| `zt env pull [--reveal]` | Print `KEY=value` lines; values are redacted without `--reveal` (developer+) |
| `zt env unset KEY [KEY…]` | Remove variables |

## Deploy & releases

Defaults to `--env staging`; `--prod` = `--env production`. See [Deploying](../deploy/).

| Command | Description |
| ------- | ----------- |
| `zt deploy [--image REF] [--app NAME] [--env NAME \| --prod]` | Deploy: with `--image` skips the build; without, uploads the cwd source and builds server-side. Streams phase progress; exits non-zero if the rollout fails |
| `zt releases [--app NAME] [--env NAME \| --prod]` | List releases (version, image, config hash, created) |
| `zt rollback [--to vN] [--app NAME] [--env NAME \| --prod]` | Roll back (default: previous release); same red/green watch as deploy |

## Inspect & observe

| Command | Description |
| ------- | ----------- |
| `zt ps [--app NAME]` | Running instances: app, env, release, node, state, restarts |
| `zt logs [app] [--env NAME] [--since 10m] [-f]` | Stream logs across instances, color-coded per instance |
| `zt stats [--nodes \| --app NAME \| --node ID] [--since 1h] [--step 5m]` | Stats: current values from heartbeats, or history from the TSDB with `--since` (rendered as sparklines) |

## Remote debugging

See [Remote debugging](../operations/remote-debug). These pick the first healthy instance unless `--instance` is given, and propagate the remote exit code.

| Command | Description |
| ------- | ----------- |
| `zt attach [app] [-- command…] [--no-tty]` | Interactive shell (`/bin/sh`) or one-off command in a running instance |
| `zt top [app]` | Process table of a running instance |
| `zt fs ls <app>:<path>` / `fs cat <app>:<path>` | List / print files inside a running instance |
| `zt port-forward [app] <localPort>[:<portName>]` | Forward `127.0.0.1:<localPort>` to a healthy instance's service port |

## Jobs

Defaults to `--env production`. See [Jobs](../operations/jobs).

| Command | Description |
| ------- | ----------- |
| `zt jobs run [app] [--max-retries N] [--no-wait] -- <command…>` | One-shot job in the env's active release image; waits and exits with the job's exit code |
| `zt jobs ls [app] [--env NAME]` | Recent jobs: status, exit code, attempt, command |
| `zt cron ls [app] [--env NAME]` | Cron schedules from `zattera.toml`: next run + last run's status |

## Volumes

Node-pinned persistent volumes for stateful services. See [Volumes](../data/volumes).

| Command | Description |
| --- | --- |
| `zt volume ls` | List the project's volumes: id, name, env, node, status |
| `zt volume create <name> [--app NAME] [--env NAME] [--node ID]` | Create a volume (pins to `--node` or the least-used healthy node) |
| `zt volume rm <id>` | Delete a volume (refused while its service is running) |
| `zt volume snapshot <id>` | Take an on-demand snapshot |
| `zt volume snapshots <id>` | List a volume's snapshots: id, status, size, created |
| `zt volume restore <id> --snapshot <snap-id>` | Restore a snapshot (service must be stopped) |

## Backup & disaster recovery

Cluster-wide backups (admin). See [Backup & disaster recovery](../data/backup-restore).

| Command | Description |
| --- | --- |
| `zt backup config --bucket NAME [--endpoint URL] [--region R] [--prefix P] [--access-key K] [--secret-key S]` | Set the S3 destination (credentials sealed server-side) |
| `zt backup run` | Run a full backup now (state + CA + volume snapshot refs) |
| `zt backup ls` | List past backups and the current destination |

## Alerts

Alert rules and notification channels. See [Metrics & alerts](../operations/metrics-and-alerts#alerts).

| Command | Description |
| --- | --- |
| `zt alerts rules ls` | List alert rules |
| `zt alerts rules add NAME (--metric M --scope S --op OP --threshold T --for D \| --event KIND) [--channel ID…]` | Add a metric-threshold or event rule |
| `zt alerts rules rm ID` | Delete a rule |
| `zt alerts channels ls` | List channels (secrets redacted) |
| `zt alerts channels add webhook\|slack\|telegram\|email NAME [type flags]` | Add a channel (secrets sealed server-side) |
| `zt alerts channels rm ID` | Delete a channel |

## Custom domains

Defaults to `--env staging`. See [Custom domains](../deploy/custom-domains).

| Command | Description |
| ------- | ----------- |
| `zt domains add <hostname> [--path PREFIX] [--port NAME] [--env NAME \| --prod]` | Attach a hostname (optionally a path prefix / specific port) |
| `zt domains ls` / `domains rm <hostname>` | List (with cert status) / remove |

## GitHub

| Command | Description |
| ------- | ----------- |
| `zt github connect --app NAME --repo owner/name [--prod-branch main] [--staging-branch NAME]` | Wire a repo for push-to-deploy and print webhook setup instructions |

## Nodes

See [Nodes](../setup/nodes).

| Command | Description |
| ------- | ----------- |
| `zt nodes ls` | Nodes: name, roles, status, version, mesh IP, labels |
| `zt nodes join-token create [--worker] [--control] [--single-use=true]` | Mint a join token (worker by default) |
| `zt nodes label <name> KEY=VALUE\|KEY- […] [--overwrite]` | Set or remove node labels (merges; `--overwrite` to change an existing key) |
| `zt nodes cordon <name>` | Stop scheduling new work on a node; running containers stay up |
| `zt nodes uncordon <name>` | Return a cordoned or drained node to service |
| `zt nodes drain <name>` | Migrate instances away, wait until DRAINED |
| `zt nodes rm <name> [--force]` | Remove a drained node |

## Cluster state

See [State export & apply](../operations/state-export).

| Command | Description |
| ------- | ----------- |
| `zt state export [--project NAME]` | Export desired state as YAML (whole cluster without `--project`) |
| `zt state apply [-f FILE] [--dry-run]` | Apply a YAML document; `--dry-run` validates and counts only |

## Server & cluster lifecycle

These run on the servers themselves (Linux binary):

| Command | Description |
| ------- | ----------- |
| `zattera cluster init --domain DOMAIN [--email …] [--advertise …]` | Write `/etc/zattera/config.toml`, install the systemd unit, start the control node |
| `zattera cluster join <control-addr> --token TOKEN` | Configure + start a joining node |
| `zattera cluster teardown [--keep-data]` | Stop and clean up a node |
| `zattera server [--config PATH] [--dev] [--join ADDR --token TOKEN]` | Run the node daemon in the foreground (see [Configuration](../setup/configuration)) |
