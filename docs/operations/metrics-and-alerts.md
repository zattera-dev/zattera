---
title: Metrics & alerts
description: Historical metrics and webhook/Slack/email alerting — work in progress.
---

# Metrics & alerts

::: callout warning Work in progress
The alert engine (T-74) is on the [roadmap](../roadmap/tasks) and not implemented yet. Historical metrics (T-59 + T-60) have landed.
:::

**What it will do:** alert rules firing to webhook/Slack/email channels.

**What works today:** every node runs an embedded ring TSDB (T-59) — a metrics sampler records node CPU/memory/disk/net, per-instance CPU/memory/network, and per-env proxy series (RPS, in-flight, error rate, p50/p99 latency) every 15s into a two-resolution ring buffer (15s for 24h, 5m for 30d) that survives restarts (`<data-dir>/metrics/tsdb.bin`).

- **Current values** — `zattera stats` (per node) or `zattera stats --app NAME` (per env) reads the latest heartbeat.
- **History** — `zattera stats --since 1h [--step 5m]` reads the TSDB: the control plane fans the query out to each relevant node's local store and merges the series (node series per node; env series sum RPS/in-flight and average CPU/latency across instances). The CLI renders each series as a sparkline; `--json` returns the raw points. Scope it with `--node ID`, `--app NAME`, or leave it cluster-wide.

**Under the hood:** the store is `internal/daemon/tsdb` (`tsdb.Store` — per-series float32 rings, downsample-on-write, 48h GC of idle series, flat-file persistence). The sampler is `internal/daemon/agent/metrics.go`; each node serves its history over `AgentLocalService.Stats` (`internal/daemon/agent/statsserver.go`), and the control plane fans out and merges in `internal/daemon/api/metricshistory.go`. See [Logs](logs) for log tailing and retention.
