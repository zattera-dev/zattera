---
title: Metrics & alerts
description: Historical metrics and webhook/Slack/email alerting — work in progress.
---

# Metrics & alerts

::: callout warning Work in progress
Historical metrics (T-59, T-60) and the alert engine (T-74) are on the [roadmap](../roadmap/tasks) and not implemented yet.
:::

**What it will do:** a built-in ring TSDB with per-app and per-node history (`zattera stats --history`), and alert rules firing to webhook/Slack/email channels.

**What works today:** [`zattera stats`](../cli/reference) shows live node CPU/memory from agent heartbeats, and [`zattera ps`](../cli/reference) shows per-instance health. See [Logs](logs) for log tailing and retention.
