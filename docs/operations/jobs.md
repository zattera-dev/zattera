---
title: Jobs & cron
description: One-shot jobs in your app's release image — migrations, batch tasks, maintenance commands.
---

# Jobs & cron

Run one-off commands — database migrations, backfills, maintenance scripts — in the **exact image of the environment's active release**, with the same env vars, network, and internal DNS as the running app.

## How to use

```bash
zt jobs run api --env production -- rails db:migrate
# Job 01J… queued
# … streamed job output …

zt jobs run api --max-retries 2 -- ./backfill.sh   # retry on failure
zt jobs run api --no-wait -- ./slow-task.sh        # fire and forget
zt jobs ls api                                     # recent jobs: status, exit code, attempt
```

Everything after `--` is the command. By default the CLI waits, streams the job's logs, and **exits with the job's exit code** — so jobs compose in CI pipelines (`zt jobs run api -- rake db:migrate && zt deploy --prod`). `jobs run` defaults to `--env production`.

## How it works

A job is scheduled like a service instance — placed on a node with capacity by the same scheduler — but with run-once semantics: no restart policy, excluded from the environment's replica math, and cleaned up on completion. The container uses the environment's **active release image** with its sealed env vars injected, plus your command override. On failure the scheduler retries with exponential backoff up to `--max-retries`, then marks the job FAILED with its exit code. Output goes to a dedicated `job/<id>` log stream, readable while running or after.

## Cron

Declare scheduled runs with [`[[cron]]` sections in zattera.toml](../deploy/zattera-toml#cron) — `name`, `schedule` (5-field cron), `command`, `concurrency` policy, and `max_retries`:

```toml
[[cron]]
name = "nightly-report"
schedule = "0 2 * * *"      # 02:00 every day
command = "./bin/report"
concurrency = "forbid"       # forbid (default) | replace | allow
max_retries = 2
```

```bash
zt cron ls api               # each schedule: next run + last run's status
zt cron ls api --env staging # limit to one environment
```

### How it works

The leader evaluates every environment's schedules on a sub-minute tick. When a slot is due it enqueues a normal one-shot [job](#how-it-works) tagged with the cron name — so a cron run behaves exactly like `zt jobs run` (active-release image, sealed env vars, retries, `job/<id>` logs), and `zt jobs ls --env <env>` shows the history.

- **Concurrency policy** governs what happens when a slot arrives while the previous run is still active: `forbid` (default) skips the new run, `replace` cancels the active run and starts fresh, `allow` runs them concurrently.
- **Jitter.** Each schedule gets a deterministic 0–30s offset (hashed from env + cron name) so distinct crons sharing a slot don't all place at once.
- **Failover.** Next-run is always computed forward from the moment a node becomes leader — slots missed during a leader election are **not** replayed (a cron guarantees at-most-once per slot, not catch-up). For guaranteed backfill, make the command idempotent and reconcile from within it.
