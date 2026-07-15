---
title: Logs
description: Tail and stream app logs across all instances and nodes, with built-in retention.
---

# Logs

App output is captured on every node, kept with sensible retention, and streamed to your terminal merged across instances — no log shipper to install.

## How to use

```bash
zt logs api -f                    # follow live, all environments
zt logs api --env production -f   # one environment
zt logs api --since 1h            # history only
zt logs --json                    # structured lines for scripting
```

Inside an app directory, plain `zt logs -f` works (`--app` defaults from `zattera.toml`). Each line is prefixed `app-env-instance │ …` with a distinct color per instance (respects `NO_COLOR`).

Build and [job](jobs) output land in the same system — a deploy's build log and a job's output are streamed back through the same machinery.

## How it works

Each node's agent captures its containers' stdout/stderr into a local **segmented log store**: the active segment rotates at 8 MB or 1 hour, closed segments are zstd-compressed and indexed for fast time-based seeks. A retention janitor enforces per-stream size and age caps (defaults: 100 MB, 7 days — tunable in the [server config](../setup/configuration) under `[logs]`).

When you run `zt logs`, the API fans out to the nodes holding the app's instances, reads history from segments, then subscribes live; lines from different nodes are merged with a small reorder buffer so timestamps interleave correctly. If a node is unreachable you still get everyone else's logs plus a warning, and followers that can't keep up are dropped with an explicit "stream lagged" marker rather than silently losing lines.

External log sinks (ship to Loki/S3/etc.) are on the [roadmap](../roadmap/tasks) (M4).
