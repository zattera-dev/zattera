---
title: Preview environments
description: Every pull request gets its own deployed environment and URL.
---

# Preview environments

Every pull request gets its own environment, its own URL, and its own HTTPS certificate — created when the PR opens, redeployed on every push, deleted when the PR closes.

Preview environments are part of [GitHub push-to-deploy](github): connect the repo once and previews come with it.

## Setup

Previews ride on the same webhook as push-to-deploy. When you create the repository webhook, subscribe it to the **pull request** event in addition to **push**:

```bash
zt github connect --app api --repo acme/api \
  --prod-branch main --staging-branch develop
```

Nothing else to configure — an app with a `staging` environment is ready for previews.

## How it works

1. **PR opened** — Zattera creates an environment named `preview-<pr-number>` for the app, cloning its **service spec and environment variables from `staging`** (falling back to `production` if there is no staging). It then builds the PR's head commit and deploys it through the usual [red/green pipeline](./).
2. **URL comment** — once the environment exists, Zattera comments the preview URL on the pull request using the GitHub App installation token.
3. **New commits** — each push to the PR branch redeploys the same environment at the new commit. The environment, and therefore the URL, stays stable for the life of the PR.
4. **PR closed** — the environment is deleted (merged or not). Containers, routes, and the certificate are torn down as a cascade.

## URLs

A preview is reachable on the cluster subdomain, exactly like any other environment:

```
https://<app>-preview-<pr-number>.<cluster-domain>
```

So PR #42 on app `api` with cluster domain `example.com` lands at `https://api-preview-42.example.com`. The certificate is issued automatically like every other route — see [custom domains](custom-domains) for the certificate machinery.

## Limits and lifetime

| Limit | Default | Why |
| --- | --- | --- |
| Concurrent previews per app | 5 | Each preview needs its own certificate. The cap is what keeps a busy repo from burning through the Let's Encrypt issuance rate limit. |
| Time to live | 7 days | Reclaims resources from PRs that stall. Every PR event resets the clock, so an active PR never expires. |

A leader-side janitor sweeps expired previews hourly and deletes them the same way a PR close does. Reopening or pushing to a swept PR recreates the environment.

When an app is already at the cap, the next PR gets a comment explaining that no preview was created rather than silently getting nothing. Close a preview to free a slot.

## Notes

- **Repeated pushes of the same commit** (force-push storms, webhook redeliveries) do not trigger a rebuild — Zattera skips the build when the environment is already deployed at that commit, but still extends the TTL.
- **Environment variables are copied at creation**, not kept in sync. Change a preview's variables with `zt env set --env preview-42` like any other environment; they are discarded when the PR closes.
- **Previews are real environments.** `zt ps`, `zt logs`, `zt exec`, and the rest work against `--env preview-42` exactly as they do against staging.
- **Secrets reach previews.** Because the spec and variables are cloned from staging, anyone who can open a pull request against the repo can reach an environment holding staging's credentials. Give staging credentials that are safe under that assumption.
