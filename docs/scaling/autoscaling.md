---
title: Autoscaling
description: CPU/RAM/RPS-driven replica autoscaling.
---

# Autoscaling

Zattera scales an environment's replica count between `replicas.min` and
`replicas.max` based on observed CPU, memory, or request-rate targets. The
leader evaluates every 15 seconds from live heartbeat metrics; you only declare
the targets.

## Configure

In [`zattera.toml`](../deploy/zattera-toml), set a replica range and one or more
autoscale targets:

```toml
[env.production]
min_replicas = 2
max_replicas = 10

[env.production.autoscale]
target_cpu_percent     = 60   # average CPU across replicas
target_memory_percent  = 75   # average memory vs the container limit
target_rps_per_replica = 100  # requests/sec per replica (needs an HTTP port)
```

Any target left at `0` is ignored. `target_memory_percent` needs a memory limit
(`resources.memory_mb`) to form a percentage; without one that signal is skipped.

## How it decides

For each configured signal the autoscaler computes
`desired = ceil(running_replicas × observed / target)` and takes the **maximum**
across signals, clamped to `[max(min, 1), max]`. (`effective_replicas = 0` is
reserved for [scale-to-zero](scale-to-zero), so autoscaling never drops below 1.)

- **Scale up** happens immediately when the target is exceeded.
- **Scale down** happens only after the load stays below **80%** of target for
  **5 minutes** — a hysteresis window that avoids flapping.
- A **3-minute cooldown** after any change gates the next one in either direction.
- **Missing metrics** (an agent gap, or no running replicas yet) **freeze** the
  env — it never scales on absent data.

Each change writes the new `effective_replicas` and emits an `autoscale.scaled`
event; the [scheduler](../index) converges the replica count into assignments.
A running deployment owns its env, so autoscaling pauses until the deploy
finishes. Leadership changes reset the in-memory hold timers (conservative:
the 5-minute window restarts).

## Under the hood

The loop is `internal/daemon/scheduler/autoscaler.go`, reading the leader's
livestate (per-instance CPU/memory and per-env RPS carried on heartbeats, fed by
the [metrics sampler](../operations/metrics-and-alerts)).
