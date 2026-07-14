# Zattera — deploy a cluster and run apps

Turn any pool of Linux hosts into a PaaS. Each host needs only **Docker**; the
single `zattera` binary is the control plane, worker, scheduler, proxy, registry
and CLI. The `zt` alias is installed alongside it.

The whole flow is three commands to a running cluster, then `zt deploy`.

---

## 1. Start the cluster

On your **first server** (this becomes the control node):

```bash
curl -fsSL https://get.zattera.dev | sh      # install the binary
sudo zattera cluster init                    # configure + start this node
```

`cluster init` asks a couple of questions (node name, your **cluster app
domain**, an email for Let's Encrypt), writes `/etc/zattera/config.toml`,
installs and starts a `systemd` service, and prints two ready-to-paste commands:

```
✓ Control node "mario" is up.

  Log in from your workstation (installs the CLI too):
    curl -fsSL https://get.zattera.dev | sh
    zt login --server https://203.0.113.10:8443 --ca-pin ab12…ef --token zpat_… --context mario

  Add more nodes — run this on each new server:
    curl -fsSL https://get.zattera.dev | sh && sudo zattera cluster join 203.0.113.10:8443 --token zjoin_…

  The admin token above is shown once — store it safely.
```

> Non-interactive: `sudo zattera cluster init --yes --domain apps.example.com --email you@example.com`.

---

## 2. Connect your CLI

On your **laptop**, run the two commands `cluster init` printed:

```bash
curl -fsSL https://get.zattera.dev | sh
zt login --server https://203.0.113.10:8443 --ca-pin ab12…ef --token zpat_… --context mario

zt nodes ls        # control node shows ALIVE
```

`--context mario` saves this cluster under a name. The CLI is a pure API client
— run it from anywhere that can reach the control node.

---

## 3. Add more nodes

On **each additional server**, paste the join one-liner `cluster init` printed:

```bash
curl -fsSL https://get.zattera.dev | sh && sudo zattera cluster join 203.0.113.10:8443 --token zjoin_…
```

Within a minute it registers. Confirm from your CLI:

```bash
zt nodes ls        # all nodes ALIVE
```

That's the whole cluster. Everything below is deploying and running apps.

---

## 4. Deploy an app

Any directory with a `zattera.toml` deploys. We'll use the **`mesh-demo`**
fixture (`test/fixtures/apps/mesh-demo`) — an HTTP service whose replicas call
each other across nodes to prove intra-cluster networking works.

```bash
zt projects create demo

cd test/fixtures/apps/mesh-demo
zt deploy --prod --project demo      # builds on a cluster builder, red/green rollout
```

It runs **3 replicas**, spread one per node. Watch and inspect:

```bash
zt ps   --app mesh --project demo    # replicas + health + node
zt logs mesh -f --project demo
```

It's live at its auto-domain: `https://mesh-production.apps.example.com/`
(swap in your cluster domain).

**Env vars** (the fixture reads `PEERS` and `PEER_COUNT`, both optional):

```bash
zt env set PEER_COUNT=12 --app mesh --env production --project demo
zt deploy --prod --project demo      # re-deploy to apply
```

---

## 5. Attach a custom domain

Point the domain's A record at an ingress node, then:

```bash
zt domains add app.example.com --prod --app mesh --project demo
curl https://app.example.com/        # HTTPS cert issues automatically on first hit
```

---

## 6. Test intra-cluster communication

Each replica reaches its siblings over the internal DNS name **`mesh.internal`**,
which resolves to the service VIP and load-balances (P2C) across every healthy
replica over the encrypted WireGuard mesh. `GET /` fans out and reports which
distinct replicas answered:

```bash
curl -s https://app.example.com/ | jq
```

```jsonc
{
  "summary": "I am 7f3c… ; reached 3 distinct replica(s) across 6 call(s) to mesh.internal",
  "self":    { "instance": "7f3c…", "app": "mesh", "env": "production" },
  "distinct_replicas_reached": [
    { "instance": "1a2b…" }, { "instance": "7f3c…" }, { "instance": "c9d8…" }
  ],
  "reached_self": true
}
```

Three distinct `instance` values = the request reached three replicas on three
nodes. `curl …/whoami` returns just one replica's identity; point `PEERS` at
another service's `*.internal` name to call across apps.

---

## Managing multiple clusters

`zt` keeps a named context per cluster:

```bash
zt login --context prod    --server https://prod-ip:8443    --ca-pin … --token …
zt login --context staging --server https://staging-ip:8443 --ca-pin … --token …

zt context            # list contexts, marking the active one
zt context use prod   # switch
zt nodes ls           # now targets prod
```

Most commands also accept `--context <name>` for a one-off.

---

## Everyday commands

```bash
zt ps       --app mesh --project demo
zt logs     mesh -f    --project demo
zt releases --app mesh --prod --project demo
zt rollback --app mesh --env production --project demo
zt env set  KEY=VALUE  --app mesh --env production --project demo
zt domains ls --project demo
zt drain    luigi           # migrate stateless workloads off a node before maintenance
```

---

## Remove a node

On the node itself:

```bash
sudo zattera cluster teardown          # stop + remove service, config, data, managed containers
sudo zattera cluster teardown --keep-data   # keep /var/lib/zattera (state, volumes, images)
```

Docker is left installed. This only removes the local node; the rest of the
cluster keeps running.

---

## Reference

**Ports** (open between nodes / to the internet as noted):

| Port        | Purpose                                  |
| ----------- | ---------------------------------------- |
| `80`, `443` | ingress + ACME HTTP-01 (internet-facing) |
| `8443/tcp`  | control-plane API (CLI + workers)        |
| `5000/tcp`  | embedded registry (between nodes)        |
| `51820/udp` | WireGuard mesh (between nodes)           |

**DNS** (at your provider, pointing at an ingress node's public IP):

- `*.apps.example.com` → the cluster app domain wildcard (app URLs)
- `app.example.com` → any custom domain you attach

**Config** lives at `/etc/zattera/config.toml`; **state/volumes/images** under
`/var/lib/zattera`. `cluster init`/`join` write both for you — edit and
`systemctl restart zattera` to change them by hand.
