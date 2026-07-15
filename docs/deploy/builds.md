---
title: Builds & registry
description: Dockerfile and Nixpacks builds on your own hardware, stored in the embedded OCI registry.
---

# Builds & registry

Zattera builds your app **on your own machines** and stores images in an **embedded OCI registry** replicated with the cluster — no Docker Hub account, no external registry, no vendor build minutes.

## How to use

Three build types, set in [`zattera.toml`](zattera-toml) or auto-detected:

| Type | When |
| ---- | ---- |
| `dockerfile` | A `Dockerfile` exists in the build context (auto-detected) |
| `nixpacks` | No Dockerfile — Nixpacks detects Node, Go, Python, Rust, … and generates the image plan (auto-detected) |
| `image` | You already have an image: `zt deploy --image ghcr.io/acme/api:v3` — skips building entirely |

```bash
cd my-app
zt deploy --prod
#   uploaded source; deployment …
#   building
# ✓ Built api (nixpacks, 34s)
# ✓ Released v42 → production (red/green, 2 replicas healthy)
```

Build logs stream live into the deploy output, and are kept — `zt logs` can read past build streams.

The first build on a fresh cluster is the slow one (BuildKit cold start + no layer cache); subsequent builds reuse cached layers.

## How it works

### The build pipeline

1. `zt deploy` tars the current directory (honoring ignore files) and streams it to the API in 1 MB chunks. If you hit Ctrl-C after the upload, the build continues server-side.
2. The leader queues a **Build** and dispatches it to a node with the `builder` role, which fetches the source tarball over the mesh (mTLS).
3. Builds run in a managed **BuildKit** daemon (`zt-system-buildkitd` container) — the platform starts and upgrades it; you never install anything. Nixpacks runs first when needed, generating a Dockerfile plan that feeds the same BuildKit path.
4. The resulting image is pushed to the embedded registry tagged `<registry>/<project>/<app>:<build-id>`, and the deploy continues into the [red/green rollout](./).

### The embedded registry

Every control node serves an OCI registry on `:5000` (TLS with the cluster CA; `:5001` plain HTTP in dev mode). Blobs are content-addressed — shared layers are stored once. Agents pull images from it over the mesh with per-node credentials minted at join time.

::: callout note Registry CA trust on multi-node clusters
Docker verifies registry TLS against its own trust store, so nodes that pull cluster-built images need the cluster CA installed under `/etc/docker/certs.d/<registry-addr>/ca.crt`. `zattera cluster join` handles this; prebuilt public images need nothing.
:::

### Retention & garbage collection

You never clean the registry by hand. An hourly leader loop keeps, per environment: the **last 10 releases**, plus the active release, the previous one (the rollback window), and anything referenced by an in-flight deployment. Older releases are deleted and their image tags swept; blobs are reference-counted, so a layer is only removed when no image uses it. Uploaded source tarballs expire after 24 hours.

### Multi-arch

The registry and builder already understand multi-platform images (OCI image indexes, QEMU emulation for cross-builds), but **arch-aware scheduling is not implemented yet** ([roadmap](../roadmap/tasks) T-87/T-88) — on mixed amd64/arm64 clusters, build and run on matching architectures for now.
