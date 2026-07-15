---
title: Environment variables
description: Set per-environment variables and secrets — encrypted at rest, injected at start, never stored in plaintext.
---

# Environment variables

Every environment (production, staging, …) has its own set of variables. They are treated as secrets by default: encrypted at rest, redacted in output unless you explicitly reveal them.

## How to use

```bash
zt env set DATABASE_URL=postgres://… STRIPE_KEY=sk_live_… --app api --env production
zt env pull --app api --env production              # keys only, values redacted
zt env pull --reveal --app api --env production     # KEY=value lines (developer+ role)
zt env unset STRIPE_KEY --app api --env production
```

- `zt env …` defaults to `--env production` (unlike `deploy`, which defaults to staging).
- Inside an app directory, `--app` defaults to the name in `zattera.toml`.
- `env pull` prints sorted `KEY=value` lines — pipe it into a local `.env` file if you need one.

### Changes apply on the next deploy

Setting a variable does **not** hot-restart running instances. The change is folded into the next release's config hash and takes effect on the next `zt deploy` (or rollback):

```bash
zt env set FEATURE_FLAG=on --app api --env production
zt deploy --prod --app api        # instances restart with the new value
```

This is deliberate: a running release is immutable, so what's live is always exactly what `zt releases` says was deployed.

### Variables Zattera injects

At container start, alongside your variables:

| Variable | Value |
| -------- | ----- |
| `PORT` | The first container port (unless you set `PORT` yourself) |
| `ZATTERA_ENV` | Environment name (`production`, `staging`, …) |
| `ZATTERA_APP` | App name |

## How it works

Values are protected with **envelope encryption**:

1. At bootstrap the cluster generates a random 32-byte **data key**, sealed with a key derived (argon2id) from the recovery passphrase printed once at first boot. Only the sealed form is stored in replicated state.
2. Each variable value is encrypted with **AES-GCM** under the data key *before* it enters the raft log — plaintext secrets never persist anywhere on disk.
3. The plaintext data key lives only in control-node memory. When an agent needs to start a container, the control plane decrypts the variables at that moment and sends them over the **mTLS agent stream** — they exist in plaintext only inside that frame and in the container's process environment.

Revealing values through the API (`env pull --reveal`) is gated by RBAC (developer role or higher) and requires the cluster key to be unsealed. The release config hash covers a fingerprint of the (encrypted) variables, which is how the platform knows a redeploy is needed to pick up changes.
