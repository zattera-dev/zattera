---
title: Ingress & load balancing
description: How traffic enters the cluster — embedded L7/L4 proxy, HTTPS, and health-gated load balancing on every node.
---

# Ingress & load balancing

Every node can be an entry point. The proxy is **in-process** — no nginx, no Traefik, no config files to regenerate, no "cert renewed but proxy didn't reload".

## How to use

Point DNS at any node (or several, for DNS-level spreading):

- The cluster wildcard: `*.apps.example.com` → node IP(s) gives every environment its `<app>-<env>.apps.example.com` URL automatically.
- [Custom domains](../deploy/custom-domains) point wherever you like; add them with `zt domains add`.

Ports `:80` and `:443` are the public listeners (configurable — see [Configuration](../setup/configuration)). Plain HTTP is 308-redirected to HTTPS, except ACME challenges. That's the whole setup.

For raw TCP services (databases, game servers), declare a port with `protocol = "tcp"` and a public port in [`zattera.toml`](../deploy/zattera-toml) — the L4 proxy forwards the stream as-is.

## How it works

### Route distribution

The control plane derives a **routing table** from desired state — hostname (+ optional path prefix) → the set of healthy instances, with their node mesh addresses. Every node streams this table over mTLS and keeps a copy cached on disk, so ingress keeps serving the last-known routes even while reconnecting.

### L7 proxying

Requests match by exact hostname, then longest path prefix. The proxy speaks HTTP/2 and WebSockets, adds `X-Forwarded-*` headers, and picks an instance with **P2C** (power-of-two-choices) over live in-flight counters — preferring an instance on the same node when load is equal, so co-located traffic never crosses the network. Only instances that pass [health checks](../deploy/zattera-toml#deployhealthcheck) are candidates; a request entering node A for an app on node B rides the [encrypted mesh](mesh).

Per-route middleware is available (HTTPS redirect, compression, basic auth, IP allowlists, body size limits), including **sticky sessions** — an opaque `zt_sticky` cookie pins a client to an instance, re-validated each request and re-pinned automatically if the instance drains or fails.

### TLS everywhere

HTTPS terminates in-process with certificates from the embedded ACME manager — issued on demand, stored in replicated cluster state so every node can serve every certificate. Details in [Custom domains](../deploy/custom-domains#tls-certificates). In dev mode the cluster CA signs certificates instead, no internet required.

### Traffic switches atomically

A [red/green promotion](../deploy/) bumps the routing generation in one replicated write — every ingress node flips to the new release's instances together. Rollback is the same operation pointed backwards.
