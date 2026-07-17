---
title: High availability
description: Multi-control-node HA with raft quorum — work in progress.
---

# High availability

::: callout warning Work in progress
Multi-control HA works and is verified on a real 3-node cluster (T-55, T-55b, T-56): the control plane replicates over an mTLS raft transport on the mesh; a node joined with a `--control` token installs the handed-over cluster CA + data key, joins the raft quorum, and runs its own control stack; killing the leader, the survivors re-elect and keep serving; and a gossip failure detector (memberlist over the mesh) marks a dead node DOWN within seconds. Still to polish before we call it done: a joined control node participates in raft + API over the mesh but isn't yet a WireGuard *hub* that workers route through, and hub/ingress bring-up on a later leadership change isn't wired. This page will be fleshed out with operator steps when those land.
:::

**What it will do:** run 3–5 control nodes as a raft quorum, so the control plane survives node loss. Joining a node with a `--control` join token will add it to the quorum; memberlist gossip will speed up failure detection between nodes. Today a cluster has exactly one control node (which can also run workloads), and worker nodes can already be added freely — see [Nodes](nodes).

## When quorum is lost

Losing the raft majority (e.g. 2 of 3 control nodes down) stops the control plane from accepting **writes** — deploys, config changes, and scheduling all pause, and the API refuses mutations rather than risk a split-brain. The **data plane keeps running** on its own:

- **Ingress proxies keep serving the last routes.** Every proxy caches the most recent `RouteSnapshot` to disk and reloads it on start, so traffic to already-running instances is unaffected even if no control node is reachable — a restarted proxy still routes from its cache.
- **Running containers stay up.** Agents only reconcile against a leader; with none, they hold their current assignments rather than tearing anything down. Heartbeats buffer until a leader returns.
- **Relay failover.** A worker relaying mesh traffic through a control node that dies reconnects to another control relay within seconds.

When quorum is restored the cluster resumes accepting writes and the scheduler reconciles any change that was requested during the outage. These autonomy properties are verified by the chaos suite (`go test -tags chaos ./test/chaos/ -run TestQuorum`).
