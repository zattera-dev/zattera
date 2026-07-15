---
title: CLI overview
description: How the zattera CLI works — login, contexts, project resolution, JSON output, and conventions shared by every command.
---

# CLI overview

The `zattera` CLI (installed with a `zt` shorthand symlink — the two are interchangeable) is a **pure API client**: everything it does goes through the same public gRPC/REST API you can script against directly. No SSH, no local state beyond a config file with contexts.

```bash
zt login --server https://cp1.example.com:8443 --ca-pin <FINGERPRINT> --token zpat_…
zt projects create demo
zt deploy --prod
```

See the [command reference](reference) for every command and flag.

## Logging in

```bash
zt login --server https://<host>:8443 --token zpat_… [--context prod]
```

Three ways to trust the cluster's TLS certificate:

- **`--ca-pin <sha256>`** (recommended) — pass the CA fingerprint printed at cluster boot; the CLI fetches the CA over TLS, verifies it matches the pin, and stores it (trust-on-first-use, no file copying).
- **`--ca-cert <path>`** — point at a copy of `ca.crt` (dev clusters).
- **Nothing** — when the API serves a public Let's Encrypt certificate, system roots just work.

`login` verifies the token with a `WhoAmI` call **before** saving anything — a bad login never disturbs your existing config.

## Contexts

Each login is stored as a named **context** (server + token + CA) in `~/.config/zattera/config.toml`, so you can manage several clusters:

```bash
zt context               # list contexts, * marks the active one
zt context use prod      # switch
```

## Shared conventions

- **`--project`** — most commands are project-scoped. Precedence: `--project` flag → the context's default project → error asking you to pick one.
- **`--app`** — defaults to the `name` in `./zattera.toml` when you're inside an app directory, so `zt deploy`, `zt logs -f`, `zt ps` work with no arguments.
- **`--env` / `--prod`** — deploy-family commands default to `staging`; `--prod` is shorthand for `--env production`. (Exception: `zt env …` and `zt jobs run` default to `production`.)
- **`--json`** — every command supports machine-readable output for scripting.
- **Exit codes** — non-zero on failure; `attach`, `fs`, and `jobs run` propagate the *remote* command's exit code, so they compose in shell scripts.
- **Errors** — shown as plain messages (`project demo not found`), no gRPC noise.
