---
title: Access control & audit
description: Users, API tokens, project roles, and the audit log.
---

# Access control & audit

Zattera is **API-first with fail-closed auth**: every RPC requires an identity, every project-scoped method checks a role, and every mutation lands in the audit log.

## How to use

### Users and tokens

The first boot prints a bootstrap admin token (`zpat_…`) exactly once. From there:

```bash
zt login --server https://… --token zpat_…   # stores a context after verifying the token
```

Personal access tokens double as **registry credentials** (HTTP basic auth password) if you need to pull cluster-built images directly.

### Project roles

Membership is per project:

```bash
zt projects members add dev@acme.com --role developer
zt projects members ls
zt projects members rm <user-id>
```

| Role | Can |
| ---- | --- |
| `viewer` | Read: ps, logs, releases, env var *keys* |
| `developer` | Deploy, rollback, jobs, set/reveal env vars, attach/debug |
| `admin` | Manage members, domains, delete apps |
| `owner` | Everything, including deleting the project |

Org-level owners/admins bypass project membership. Non-members don't see a project at all.

## How it works

Identity is resolved per request: nodes authenticate with **mTLS certificates** (their identity is in the cert, issued at join), humans with **bearer tokens** (stored hashed; expiry enforced at lookup). Authorization is a fail-closed method table — any API method without an explicit policy is **denied**, so new endpoints can't accidentally ship open. Project-scoped requests then check your project role against the method's minimum.

Every mutating API call is recorded in the **audit log** — actor, method, project, and outcome (including failures) — batched into replicated state so the trail survives node loss. Passwords and secret values are never included. Querying the audit log from the CLI is on the [roadmap](../roadmap/tasks) (T-76); it's available via the API today.

Sensitive values get extra gates on top of RBAC: env var reveal requires developer+ *and* an unsealed cluster key (see [Environment variables](../deploy/environment-variables#how-it-works)).

SSO/OIDC is planned for M4.
