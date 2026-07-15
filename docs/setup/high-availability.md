---
title: High availability
description: Multi-control-node HA with raft quorum — work in progress.
---

# High availability

::: callout warning Work in progress
This feature is on the [roadmap](../roadmap/tasks) (T-55, T-56) and not implemented yet. This page will be written when it ships.
:::

**What it will do:** run 3–5 control nodes as a raft quorum, so the control plane survives node loss. Joining a node with a `--control` join token will add it to the quorum; memberlist gossip will speed up failure detection between nodes. Today a cluster has exactly one control node (which can also run workloads), and worker nodes can already be added freely — see [Nodes](nodes).
