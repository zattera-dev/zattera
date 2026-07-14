# Production example (server deploy + optional extra nodes)

Bring up a real control node on a Linux server, deploy an app from your laptop's
CLI, and (optionally) join more nodes.

> ## Status — read this first
>
> On a real server today you can: run a control node, bootstrap it, log in,
> create projects/apps, **deploy an app, run/scale it, serve it publicly on
> `:80`/`:443` with a Let's Encrypt certificate, view `ps`/`stats`, exec/attach,
> and join worker nodes**.
>
> **Requires validation on a real host** (built + unit-tested, but can't be
> exercised in CI):
> - **ACME / Let's Encrypt** (T-89/T-90) needs public DNS pointing at the node
>   and `:80`/`:443` reachable from the internet for the challenge. Without that,
>   HTTPS falls back to failing handshakes — reach apps via `zt port-forward`
>   meanwhile.
> - **Source builds** need each node's Docker to trust the cluster registry CA
>   (see "Container image trust"). **A prebuilt public image avoids this** — do
>   that first.
>
> For a fully-verified end-to-end demo on one machine, use
> [MINIMAL_EXAMPLE.md](MINIMAL_EXAMPLE.md) (`--dev`).

---

## 1. Prerequisites (control server)

- A Linux server (amd64 or arm64) with a public IP.
- **Docker Engine** installed and running.
- Open these ports as appropriate:
  - `8443/tcp` — API (needed by your CLI and by joining nodes).
  - `51820/udp` — WireGuard mesh (needed only for multi-node).
  - `5000/tcp` — embedded registry (only needs to be reachable by other nodes).
  - `80,443/tcp` — reserved for the public ingress (not wired yet, see status).
- DNS (for later, when ingress lands): a wildcard `*.apps.example.com` →
  the server's public IP.

## 2. Install the binary

Build it (on the server, or cross-compile and copy):

```bash
git clone <repo> && cd zattera.dev
go build -o /usr/local/bin/zattera ./cmd/zattera
```

## 3. Control node config

`/etc/zattera/config.toml`:

```toml
node_name = "cp1"
data_dir  = "/var/lib/zattera"
roles     = ["control", "worker"]   # co-located worker runs + builds apps
domain    = "apps.example.com"      # apps get <app>-<env>.apps.example.com

[api]
listen         = ":8443"
advertise_addr = "cp1.example.com:8443"   # how the CLI and nodes reach this API

[registry]
listen = ":5000"

[mesh]
listen_port      = 51820
# public_endpoints = ["203.0.113.10:51820"]   # set if autodetect can't

[acme]
email = "ops@example.com"          # used once the production ingress is wired
```

## 4. Run the control node

Foreground (to see the first-boot banner), or under systemd:

```bash
zattera server --config /etc/zattera/config.toml
```

On **first boot** it prints, once, the bootstrap admin token and a recovery
passphrase — store both securely:

```
Bootstrap admin token: zpat_XXXXXXXX
Recovery passphrase (STORE THIS SAFELY): apple-berry-...
```

### systemd unit (recommended)

`/etc/systemd/system/zattera.service`:

```ini
[Unit]
Description=Zattera node
After=network-online.target docker.service
Wants=network-online.target
Requires=docker.service

[Service]
ExecStart=/usr/local/bin/zattera server --config /etc/zattera/config.toml
Restart=always
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
```

```bash
systemctl daemon-reload && systemctl enable --now zattera
journalctl -u zattera -f          # watch the first-boot banner for the token
```

## 5. Log in from your workstation

The API serves a cluster-CA-signed cert. You have three ways to trust it:

- **`--ca-pin` (recommended, no file copy)** — the node logs its CA fingerprint
  at boot (`cluster CA fingerprint sha256=…`, also `DEVBANNER:ca_fingerprint` in
  dev). Pass it and the CLI fetches + pins the CA on first use:

  ```bash
  zattera login --server https://cp1.example.com:8443 \
    --ca-pin 8bd1be3e…  --token zpat_XXXXXXXX
  ```

- **`--ca-cert`** — copy `$data_dir/ca/ca.crt` from the server and point at it.

- **Nothing**, once the API has a public **ACME** cert: if `api.advertise_addr`
  is a public hostname with DNS + reachable `:443`, the API serves a
  Let's Encrypt cert and the CLI trusts it via system roots — no pin, no file.

## 6. Deploy an app

Start with a **prebuilt public image** — the reliable path today (no build, no
registry-CA trust needed):

```bash
zattera projects create web
zattera apps create site --project web
# create/point an environment named "production" for the app, then:
zattera deploy --image nginx:alpine --prod --app site --project web

zattera ps --app site --project web        # running + healthy?
zattera stats --nodes                       # live node CPU/mem
```

Reach it publicly on the app hostname (`<app>-<env>.<domain>`) — the ingress
serves `:80`/`:443` and obtains a Let's Encrypt cert on first request, given
public DNS + reachable ports:

```bash
curl https://site-production.apps.example.com/
```

Before DNS/ACME is set up, reach it via a forwarded port instead:

```bash
zattera port-forward site 8080:http --project web
curl http://127.0.0.1:8080/
```

> To deploy from **source** (a Dockerfile/nixpacks build) instead of an image,
> the build runs on a builder node and pushes to the embedded registry; each
> node that runs the app must trust that registry — see below.

## 7. (Optional) Add more nodes

### a. Mint a join token on the control node's CLI

```bash
zattera nodes join-token create --worker          # worker node
# or: zattera nodes join-token create --control    # another control (HA)
# prints: K10<ca-hash>::<secret>
```

### b. On the new server

Install Docker + the `zattera` binary, then join. A worker needs no local
config file — just the control address and token:

```bash
zattera server \
  --data-dir /var/lib/zattera \
  --join cp1.example.com:8443 \
  --token 'K10<ca-hash>::<secret>'
```

The token pins the control CA (the `K10<ca-hash>` prefix), the node enrolls,
receives a signed identity + a mesh (WireGuard) address, and starts running
assignments. Requires `51820/udp` reachable between nodes.

### c. Manage nodes

```bash
zattera nodes ls
zattera nodes drain <name>          # migrate workload off, then:
zattera nodes rm <name>
```

## Container image trust (source builds / multi-node pulls)

The embedded registry serves TLS with the **cluster CA**. Docker Engine verifies
registry certs against its own trust store, so every node that pulls a
cluster-built image must trust that CA:

```bash
# on each node, for the registry address other nodes use (e.g. cp1:5000):
sudo mkdir -p /etc/docker/certs.d/cp1.example.com:5000
sudo cp cluster-ca.crt /etc/docker/certs.d/cp1.example.com:5000/ca.crt
```

(Public images like `nginx:alpine` don't need this — prefer image deploys until
this is automated.)

## Teardown

```bash
systemctl disable --now zattera
docker ps -aq   --filter label=dev.zattera/managed=true | xargs -r docker rm -f
docker network ls --filter name=zt- -q                  | xargs -r docker network rm
rm -rf /var/lib/zattera
```

## Summary of ports

| Port        | Purpose                         | Who needs it            |
|-------------|---------------------------------|-------------------------|
| `8443/tcp`  | API (gRPC + REST, TLS)          | your CLI, joining nodes |
| `51820/udp` | WireGuard mesh                  | node ↔ node             |
| `5000/tcp`  | embedded registry               | other nodes             |
| `80/tcp`    | ingress HTTP + ACME challenge   | the internet            |
| `443/tcp`   | ingress HTTPS                   | the internet            |
