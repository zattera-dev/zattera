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

Build logs stream live into the deploy output and are retained afterwards under the build's own stream. Reading them back is currently **API-only** — the log service accepts a `build_id` (or `deployment_id`) selector, but `zt logs` has no flag for it yet, so re-reading a finished build's output means calling `LogService.Query` directly.

The first build on a fresh cluster is the slow one (BuildKit cold start + no layer cache); subsequent builds reuse cached layers, which live on a Docker volume (`zt-buildkit-cache`) on the builder node. A single build is capped at **30 minutes**.

::: callout note Nixpacks needs outbound internet on the builder, once
The pinned Nixpacks CLI is downloaded from GitHub Releases on first use, checksum-verified, and cached under the node's data dir; the plan it generates then pulls Nix packages during the build. Dockerfile builds have no such dependency beyond your own base images. An air-gapped cluster should prefer `dockerfile` or `image` builds, or pre-seed the cache.
:::

## How it works

### The build pipeline

1. `zt deploy` tars the current directory (honoring ignore files) and streams it to the API in 1 MB chunks. If you hit Ctrl-C after the upload, the build continues server-side.
2. The leader queues a **Build** and dispatches it to a node carrying the `builder=true` label — every worker gets it at boot, and you can take one out of the pool with [`zt nodes label <node> builder=false --overwrite`](../setup/nodes#nodes-labels-and-placement). The chosen node fetches the source tarball from the control plane over the mesh, authenticated with its own mTLS identity. See [where a build runs](#builds-registry-how-it-works-where-a-build-runs) for how the node is chosen.
3. Builds run in a managed **BuildKit** daemon (the `zt-system-buildkitd` container, pinned to a version matching the client library so the wire protocol lines up). The builder starts it on demand before the first build and reuses it afterwards — you never install anything. Nixpacks runs first when needed, generating a Dockerfile plan that feeds the same BuildKit path.
4. The resulting image is pushed to the embedded registry tagged `<registry>/<project>/<app>:<build-id>`, and the deploy continues into the [red/green rollout](./).

The builder reuses any running `zt-system-buildkitd` without checking its version, so a Zattera upgrade that moves the BuildKit pin won't replace an already-running daemon. If builds start failing right after an upgrade, `docker rm -f zt-system-buildkitd` and let the next build recreate it.

### Where a build runs

The dispatcher picks an **alive, schedulable** node labelled `builder=true`, ordered by how many builds are already running on it, with the node id as a deterministic tie-break. In practice:

- **Idle cluster → the same node every time.** With every builder at zero, the tie-break wins, so consecutive builds reuse one machine's warm layer cache.
- **Busy builder → the next one.** Work spills onto another builder only once the preferred one is actually building, so parallel deploys don't queue behind each other.
- **Cordoned nodes are skipped.** [`zt nodes cordon`](../setup/nodes) takes a node out of the build pool too — which is what makes [`zt cluster upgrade`](../operations/upgrades) safe, since it cordons each node before restarting its daemon.
- **`builder=false`** removes a node from the pool permanently.

If no builder is schedulable — you cordoned the only one — the build **stays queued** rather than failing, and a `build.waiting_for_builder` event says so once. It dispatches on its own as soon as a builder returns; no need to redeploy.

### The embedded registry

Every **control** node serves an OCI registry on `:5000` (TLS with the cluster CA; `:5001` plain HTTP in dev mode) — worker nodes host no blobs, and the join flow points them at a control node's registry address. Blobs are content-addressed, so shared layers are stored once. Agents pull over the mesh with per-node credentials minted at join time.

::: callout note Registry CA trust on multi-node clusters
Docker verifies registry TLS against its own trust store, so nodes that pull cluster-built images need the cluster CA installed under `/etc/docker/certs.d/<registry-addr>/ca.crt`. `zattera cluster join` handles this; prebuilt public images need nothing.
:::

### Retention & garbage collection

You never clean the registry by hand. An hourly leader loop keeps, per environment: the **last 10 releases**, plus the active release, the previous one (the rollback window), and anything referenced by an in-flight deployment. Older releases are deleted and their image tags swept; blobs are reference-counted, so a layer is only removed when no image uses it. Uploaded source tarballs expire after 24 hours.

### Multi-arch

Mixed amd64/arm64 clusters work. The registry and builder handle multi-platform images (OCI image indexes, QEMU emulation for cross-builds), and the scheduler is arch-aware: it resolves the platforms a release actually ships and filters out nodes that can't run them, using the `zattera.dev/os-arch` label each node reports about itself. An image with no runnable node fails placement with a clear error instead of landing on hardware that can't execute it.
