---
title: Installation
description: Install the Zattera binary on servers and workstations — one command, zero dependencies beyond Docker.
---

# Installation

Zattera ships as **one static Go binary** that contains the CLI, the control plane, the scheduler, the proxy, the cert manager, and the registry. Servers additionally need **Docker Engine** running — that's the entire dependency list.

## One-line install

```bash
curl -sfL https://get.zattera.dev | sh -
```

This installs `/usr/local/bin/zattera` plus a `zt` symlink (the short alias used throughout these docs). The same command **upgrades** in place — it's idempotent, and it stops/restarts a running `zattera.service` around the binary swap.

### Pin a version or change the install dir

```bash
curl -sfL https://get.zattera.dev | INSTALL_ZATTERA_VERSION=v0.1.0 sh -
curl -sfL https://get.zattera.dev | INSTALL_ZATTERA_BIN_DIR=$HOME/.local/bin sh -
```

## Per-platform binaries

| Platform | Binary | Contains |
| -------- | ------ | -------- |
| Linux amd64 / arm64 | `zattera-linux-{amd64,arm64}` | Full: server + CLI |
| macOS amd64 / arm64 | `zattera-darwin-{amd64,arm64}` | CLI only |
| Windows amd64 | `zattera-windows-amd64.exe` | CLI only |

Servers run Linux. On macOS and Windows you install the CLI to manage clusters remotely; to run a full **dev-mode node** on macOS, build from source (`go build ./cmd/zattera`) — see the [Quickstart](../getting-started/quickstart).

## How it works

There is no dynamic backend behind the installer: GitHub Actions builds and tests each tagged release, GitHub Releases hosts the binaries with `sha256sums.txt`, and GitHub Pages serves the install script at `get.zattera.dev`. "Latest" resolves through GitHub's built-in `releases/latest/download/…` redirect. Every node upgrades with the same `curl | sh` one-liner.

## Next steps

- [Quickstart](../getting-started/quickstart) — cluster up and first app deployed
- [Configuration](configuration) — the server `config.toml` reference
- [Nodes](nodes) — joining, draining, and removing machines
