---
title: WireGuard mesh
description: How Zattera connects nodes across regions, clouds, and NATs with an encrypted WireGuard mesh — and how to operate it.
---

# WireGuard mesh

Every node in a Zattera cluster talks to every other node over an encrypted **WireGuard** mesh. This is what makes "nodes anywhere" real: machines in different clouds, different regions, or behind home NATs join the same cluster and address each other with stable mesh IPs — no VPN appliance, no port-forwarding gymnastics on workers.

## How to use it

You mostly don't have to. The mesh is wired automatically:

- `zattera cluster` bootstrap (or `zattera server` with mesh enabled) brings up the WireGuard device on the control node.
- `zattera server --join <control>:8443 --token <JOIN_TOKEN>` enrolls a new node: it receives a mesh IP, the CA bundle, a signed node certificate, and an initial peer set — then brings its own device up.

Requirements per node:

- **UDP `51820`** reachable between nodes (workers behind NAT only need *outbound* UDP — see below).
- Root or `CAP_NET_ADMIN` (the daemon creates a `zt0` interface, or `utunN` on macOS dev machines).

Check the mesh:

```bash
zt nodes ls          # every node's mesh IP + liveness
```

Mesh addressing (fixed):

| Range | Used for |
| ----- | -------- |
| `10.90.0.0/16` | node mesh IPs (control nodes low `.0.x`, workers from `.1.1`) |
| `10.97.0.0/16` | internal service VIPs |
| `10.201.0.0/16` | per-(project, env, node) container subnets |

## How it works

### Device layer

On Linux, Zattera prefers the **kernel WireGuard** module (configured via netlink/wgctrl) and falls back to the embedded **userspace wireguard-go** implementation with a TUN device when the module isn't available. MTU is fixed at 1420. Keys are Curve25519; each node generates its private key locally on first use — it never leaves the machine.

### Hub-and-spoke first, direct when possible

Peer topology is phased (see [ADR-0003](../contributing/architecture-decision-records/0003-mesh-nat-traversal-phasing/)):

1. **Hub-and-spoke** — workers initially peer only with control nodes, which carry an `allowed_ips = 10.90.0.0/16` route and forward traffic between workers. NAT'd workers (no public endpoint) set a 25s persistent keepalive so the NAT hole stays open from their side; the hub never has to initiate.
2. **Direct worker↔worker** — every node continuously pings the control nodes over a lightweight disco protocol (STUN-like, HMAC-authenticated). The control node observes each worker's public `ip:port` as seen from outside and folds it into the peer set. When **both** sides of a pair have an observed endpoint, they get direct `/32` peers — traffic stops hairpinning through the hub. The hub route always remains as fallback (WireGuard's most-specific AllowedIP wins).
3. **UDP hole punching and TCP relay** (for the NAT pairs that can't connect directly) are on the [roadmap](../roadmap/tasks) (T-57, T-58) — until then, those pairs simply keep using the hub path.

### Peer distribution

The control plane streams each node's peer set over an mTLS gRPC watch: whenever nodes join, leave, or change endpoints, every affected node receives a fresh set (debounced) and diffs it into its WireGuard device — peers are added, updated, or removed without restarting anything.

### What rides the mesh

Everything internal: raft replication between control nodes, the agent sync streams, image pulls from the embedded registry, [internal DNS](internal-dns) and service-to-service traffic, and cross-node proxying when a request enters one node but the app runs on another (see [Ingress](ingress)).
