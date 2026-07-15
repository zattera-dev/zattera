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

::: callout warning Work in progress
Scheduled (cron) jobs are on the [roadmap](../roadmap/tasks) (T-67). The [`[[cron]]` sections in zattera.toml](../deploy/zattera-toml#cron-scheduling-is-work-in-progress) — schedule, command, concurrency policy (`forbid`/`replace`/`allow`), retries — are already parsed and stored, and will drive the cron scheduler when it ships. Until then, trigger `zt jobs run` from an external scheduler.
:::
