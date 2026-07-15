---
title: GitHub push-to-deploy
description: Deploy on git push with branch → environment mapping.
---

# GitHub push-to-deploy

Connect a repository once and every push deploys automatically: `main` → production, a branch of your choice → staging.

::: callout warning Early feature
The webhook, signature verification, and deploy pipeline work end to end, but provisioning the GitHub App credentials onto the control plane is still a manual/out-of-band step in this pre-alpha — `zt github connect` prints what goes where. Expect this to get smoother.
:::

## How to use

```bash
zt github connect --app api --repo acme/api \
  --prod-branch main --staging-branch develop
```

This stores the repo and the **branch → environment mapping** on the app, generates a webhook secret, and prints the setup instructions: create a webhook on the repo pointing at

```
https://<your-api-address>/v1/github/webhook
```

with content type `application/json`, the printed secret, and the **push** event.

You can also declare the repo in [`zattera.toml`](zattera-toml) under `[github]`.

## How it works

1. **Push arrives** — GitHub POSTs to `/v1/github/webhook`. Zattera verifies the HMAC-SHA256 signature (`X-Hub-Signature-256`) in constant time and dedupes redeliveries by delivery ID.
2. **Branch mapping** — the pushed branch is looked up in the app's branch → environment map. Unmapped branches and tag pushes are ignored (the webhook answers `branch not mapped`).
3. **Deploy** — the webhook responds immediately, then in the background Zattera mints a short-lived GitHub App installation token, records a git **build** (pinned to the pushed commit SHA), a new **release**, and a **deployment** — the same [red/green pipeline](./) as a CLI deploy. The builder clones the repo at that exact SHA using the installation token; no deploy keys to manage.
4. **Commit status** — deploy progress is reported back to the commit as a `zattera` status check (pending → success/failure).

Per-PR [preview environments](preview-environments) build on this and are on the roadmap.
