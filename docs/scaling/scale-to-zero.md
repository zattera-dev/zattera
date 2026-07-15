---
title: Scale to zero & serverless
description: Idle apps scale to zero replicas and wake on request — work in progress.
---

# Scale to zero & serverless

::: callout warning Work in progress
This feature is on the [roadmap](../roadmap/tasks) (T-69 … T-71) and not implemented yet. This page will be written when it ships.
:::

**What it will do:** an app idle past its `idle_timeout` scales to 0 replicas; the ingress holds the next incoming request, wakes an instance, and flushes the request to it once healthy. A serverless mode will scale replicas on concurrent in-flight requests (`max_concurrency`) rather than resource usage.
