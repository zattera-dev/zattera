---
title: Preview environments
description: Per-pull-request preview deployments — work in progress.
---

# Preview environments

::: callout warning Work in progress
This feature is on the [roadmap](../roadmap/tasks) (T-75) and not implemented yet. This page will be written when it ships.
:::

**What it will do:** every pull request gets its own `preview-*` environment with its own URL, spun up on PR open and torn down on merge/close — wired into [GitHub push-to-deploy](github). The environment model already supports `preview-*` names today; the automatic PR lifecycle is what's missing.
