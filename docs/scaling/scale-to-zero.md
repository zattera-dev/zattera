---
title: Scale to zero & serverless
description: Idle apps scale to zero replicas and wake on request — work in progress.
---

# Scale to zero & serverless

::: callout warning Partly implemented
Idle **scale-down** works (an idle app cools to 0 replicas). **Wake-on-request** — the ingress holding a request and starting an instance — lands with T-70; until then a cooled app must be brought back up with a deploy or `zt apply`. Serverless concurrency mode (`max_concurrency`) is T-71.
:::

Turn it on per environment in `zattera.toml`:

```toml
[env.production]
scale_to_zero = true
idle_timeout  = "15m"   # cool down after 15 minutes with no requests
```

## Scaling down (available now)

The leader tracks each environment's request activity from the ingress proxies (last request time and in-flight count, carried on node heartbeats). When a `scale_to_zero` environment has seen no traffic for `idle_timeout`, it sets the environment's effective replica count to 0 and the scheduler stops the instances.

- **Never cools while busy** — any in-flight request, or a request within the window, keeps it warm.
- **Conservative on failover** — a newly elected leader grants every environment a full idle window before cooling it, and never cools during a heartbeat blackout (missing data ≠ idle).
- **Stateful is excluded** — `scale_to_zero` cannot be combined with `stateful` (rejected at `zt apply`), since a stateful instance holds a single-writer volume lease.
- A fresh deploy or `zt apply` brings the app back up to `replicas.min`.

## Waking up

Automatic wake-on-request (park the incoming request, start an instance, flush once healthy) is not wired yet — it arrives with the activator (T-70). Today, redeploy or re-apply to wake a cooled app.
