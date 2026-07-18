---
title: Autoscaling
description: CPU/RAM/RPS-driven replica autoscaling.
---

# Autoscaling

Zattera scales an environment's replica count between `min_replicas` and
`max_replicas` based on observed CPU, memory, or request-rate targets. The
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
target_rps_per_replica = 100  # requests/sec per replica
```

Any target left at `0` is ignored. `target_memory_percent` needs a memory limit
(`resources.memory_mb`) to form a percentage; without one that signal is
dropped — the other signals still decide.

**`max_replicas` must be greater than `min_replicas`, or autoscaling never
runs.** This is the most common reason it appears to do nothing: an environment
that sets only `min_replicas` gets `max = min`, and the loop skips it silently —
no event, no log line. Set both.

## How it decides

For each configured signal the autoscaler computes
`desired = ceil(running_replicas × observed / target)` and takes the **maximum**
across signals, clamped to `[max(min_replicas, 1), max_replicas]`.
(`effective_replicas = 0` is reserved for [scale-to-zero](scale-to-zero), so
autoscaling never drops below 1.)

- **Scale up** skips the hysteresis window, but still waits out the cooldown
  below.
- **Scale down** happens only after the load stays continuously below **80%** of
  target for **5 minutes**. Re-entering the deadband resets the window.
- A **3-minute cooldown** after any change gates the next one **in both
  directions**, including scale-up. A spike arriving within 3 minutes of a
  previous change waits.
- **Missing metrics** (an agent gap, or no running replicas yet) **freeze** the
  env — it never scales on absent data.
- **There is no step limit.** One evaluation can jump straight from
  `min_replicas` to `max_replicas` if the signal calls for it.

Each change writes the new `effective_replicas` and emits an `autoscale.scaled`
event; the [scheduler](../index) converges the replica count into assignments.

## When autoscaling stays out of the way

The loop skips an environment entirely when:

- **A deployment is running.** The deploy owns placement until it finishes.
- **`max_concurrency` is set.** Concurrency-scaled (serverless) environments are
  driven by the [scale-to-zero](scale-to-zero#scale-to-zero-serverless-serverless-concurrency-mode)
  loop instead; the CPU/memory/RPS targets are not consulted at all.
- **No active release**, or `max_replicas <= min_replicas` (above).

Two behaviours worth knowing because they are not obvious:

- **Stateful services are not excluded.** Unlike scale-to-zero, which refuses to
  touch a `stateful` env, autoscaling will happily raise its replica count — and
  a [node-pinned volume](../data/volumes) has a single writer, so extra replicas
  are not what you want. Leave `[autoscale]` unset on stateful environments.
- **A missing-metrics gap does not restart the scale-down window.** The 5-minute
  hold survives a blackout, so a scale-down can fire on the first tick after
  metrics return.

## Leadership changes

A new leader starts with empty in-memory timers: the 5-minute scale-down window
restarts from zero (conservative), **and** the 3-minute cooldown is cleared
(not conservative — the first evaluation of a new term can scale immediately).
Both are process-local and deliberately not replicated.

## Under the hood

The loop is `internal/daemon/scheduler/autoscaler.go`, reading the leader's
livestate (per-instance CPU/memory and per-env RPS carried on heartbeats, fed by
the [metrics sampler](../operations/metrics-and-alerts)).

The replica base in the formula is the **observed running** count, while the
decision compares that result against the env's current `effective_replicas`.
The two diverge during a rollout or an agent gap, when fewer replicas are
reporting than are meant to exist.
