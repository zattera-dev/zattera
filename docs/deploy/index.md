---
title: Deploying
description: How a Zattera deploy works end to end — build, release, health-gated red/green rollout, and instant rollback.
---

# Deploying

Every deploy in Zattera follows the same path, whatever the source:

```
source / image  →  Release vN (immutable)  →  red/green rollout  →  traffic switch  →  old version drains
```

## How to use it

From an app directory containing a `Dockerfile` or anything Nixpacks can detect (see [Builds](builds)):

```bash
zt deploy --prod                 # build from cwd, deploy to production
zt deploy                        # same, to staging
zt deploy --image nginx:alpine   # skip the build, deploy a prebuilt image
```

Watch and manage what happened:

```bash
zt ps                    # instances + health
zt releases ls           # release history
zt rollback              # back to the previous release
```

Configuration comes from [`zattera.toml`](zattera-toml) (replicas, ports, health checks, resources), and secrets from [environment variables](environment-variables).

## How it works

### Releases are immutable

Each deploy produces a **Release**: an auto-incrementing version that freezes the image reference *and* a full copy of the service spec (replicas, ports, health checks, resources) plus a deterministic **config hash**. The scheduler works only from that frozen contract — changing the environment's config later never mutates what a running release does. This is what makes rollback trivial and exact.

### The red/green state machine

A deploy never touches the running version ("blue") until the new one ("green") has proven itself. The orchestrator drives each Deployment through explicit phases, every transition recorded in replicated state:

1. **Placing** — green instances are scheduled *alongside* blue (capacity permitting; otherwise in rolling batches). Placement filters nodes by liveness, labels, and capacity, then spreads replicas across nodes and regions.
2. **Starting** — agents on the chosen nodes pull the image and start containers.
3. **Health checking** — every green instance must pass its health check (HTTP/TCP/exec, with the grace period from your spec). Any failure aborts the deploy: green is torn down, **blue keeps serving, traffic never moved**.
4. **Promoting** — one atomic state change flips the routing generation. All ingress proxies switch traffic to green together.
5. **Draining** — blue stays warm for **~10 minutes**, then stops.

Because every phase transition lives in raft-replicated state (not in any process's memory), a control-plane failover mid-deploy resumes exactly where it left off.

### Rollback

```bash
zt rollback [--release N]
```

Rollback is just a deployment whose target is a previous release — same machinery, same safety. Within the drain window the old instances are still warm, so rollback skips placement and health checking entirely and re-promotes in seconds.

### Superseding

Deploying again while a deploy is in flight marks the older one **superseded** and reaps its green instances. You never end up with two half-finished rollouts fighting.

### Failure recovery

If a node dies, the scheduler notices missing replicas (heartbeat-based liveness) and places replacements on healthy nodes — stateless replicas reschedule in seconds without operator action.
