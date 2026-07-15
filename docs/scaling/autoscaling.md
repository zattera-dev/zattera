---
title: Autoscaling
description: CPU/RAM/RPS-driven replica autoscaling — work in progress.
---

# Autoscaling

::: callout warning Work in progress
This feature is on the [roadmap](../roadmap/tasks) (T-59 … T-61) and not implemented yet. This page will be written when it ships.
:::

**What it will do:** scale replicas between `replicas.min` and `replicas.max` (already declarable in [`zattera.toml`](../deploy/zattera-toml)) based on CPU, memory, or request-rate targets, using the built-in metrics sampler. Until it ships, set replica counts explicitly — the scheduler already spreads them across nodes and replaces them on node failure.
