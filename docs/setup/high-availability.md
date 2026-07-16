---
title: High availability
description: Multi-control-node HA with raft quorum — work in progress.
---

# High availability

::: callout warning Work in progress
The raft HA core has landed (T-55): the control plane replicates over an mTLS raft transport, control nodes can be added to and removed from the quorum, and leader failover keeps writes flowing. The user-facing step — joining a node with a `--control` token and having it bring up its own control plane — is still being wired (T-55b, blocked on multi-control mesh addressing). Until then a `--control` join runs as a worker. This page will be completed when that ships.
:::

**What it will do:** run 3–5 control nodes as a raft quorum, so the control plane survives node loss. Joining a node with a `--control` join token will add it to the quorum; memberlist gossip will speed up failure detection between nodes. Today a cluster has exactly one control node (which can also run workloads), and worker nodes can already be added freely — see [Nodes](nodes).
