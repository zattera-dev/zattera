---
title: Volumes
description: Node-pinned persistent volumes for stateful apps — work in progress.
---

# Volumes

::: callout warning Work in progress
This feature is on the [roadmap](../roadmap/tasks) (T-62, T-63, T-77) and not implemented yet. This page will be written when it ships.
:::

**What it will do:** declare volumes in `zattera.toml` for stateful services (Postgres, Redis, …). A volume pins its service to one node (honest single-writer semantics — no fake distributed storage), deploys switch to stop-then-start for stateful apps, and the CLI will let you browse, copy from, and snapshot volumes.
