---
title: Node autoprovisioning
description: Automatic cloud node provisioning with budget caps — work in progress.
---

# Node autoprovisioning

::: callout warning Work in progress
This feature is on the [roadmap](../roadmap/tasks) (T-81 … T-86) and not implemented yet. This page will be written when it ships.
:::

**What it will do:** define a *node pool* backed by a cloud provider (Hetzner first, then DigitalOcean/AWS). When the scheduler can't place replicas, Zattera buys a machine, cloud-inits it straight into the cluster, and destroys it again after a cooldown when idle — with hard budget caps and alerting as guard rails.
