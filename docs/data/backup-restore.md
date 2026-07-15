---
title: Backup & disaster recovery
description: Incremental S3 snapshots and one-command full-platform restore — work in progress.
---

# Backup & disaster recovery

::: callout warning Work in progress
This feature is on the [roadmap](../roadmap/tasks) (T-64 … T-66) and not implemented yet. This page will be written when it ships.
:::

**What it will do:** content-addressed, encrypted (AES-GCM), incremental snapshots of volumes and platform state to any S3-compatible bucket — and `zatterad restore` to rebuild the entire platform (state + volumes + images) onto fresh infrastructure with one command.

Today, [`zattera state export`](../operations/state-export) already gives you a GitOps-style export of all desired state (projects, apps, environments, domains) that can be re-applied to a cluster.
