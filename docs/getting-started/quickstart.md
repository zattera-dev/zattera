---
title: Quickstart
description: Get a Zattera cluster running and deploy your first app — locally in dev mode, or on a real Linux server with HTTPS.
---

# Quickstart

Two ways to get your first app running on Zattera:

- **Local dev mode** — everything on your own machine (macOS or Linux), app served on an `sslip.io` hostname. The fastest way to see the full deploy flow, fully verified end to end.
- **Real server** — a Linux server with a public IP, apps served on `:80`/`:443` with Let's Encrypt certificates, plus optional worker nodes over the WireGuard mesh.

::: callout warning Pre-alpha
Zattera is pre-alpha and moving fast. The dev-mode path below is verified end to end; on real servers, ACME/Let's Encrypt and source builds need public DNS and registry-CA trust respectively — the notes below call this out where it matters.
:::

::: tabs

== tab Local dev mode

Boot a one-node cluster in dev mode and deploy an app over HTTP and HTTPS. Everything runs on your machine; app URLs resolve to `127.0.0.1` via sslip.io, so there's no `/etc/hosts` editing.

**Prerequisites**

- **Docker** running (Docker Desktop is fine — builds and apps run in it).
- The **Go toolchain** (dev mode currently builds from source; the installer ships CLI-only binaries for macOS).
- Free TCP ports on `127.0.0.1`: `8443` (API), `8080` (ingress HTTP), `9443` (ingress HTTPS), `5001` (embedded registry).

::: steps

1. **Build the binary**

   ```bash
   git clone https://github.com/zattera-dev/zattera && cd zattera
   go build -o zt ./cmd/zattera
   export PATH="$PWD:$PATH"
   ```

2. **Start the node** (Terminal A)

   ```bash
   zt server --dev \
     --data-dir /tmp/zattera-dev \
     --domain apps.127.0.0.1.sslip.io
   ```

   On first boot it prints a startup banner with a ready-to-run **login command** — the admin token is shown only once. Leave this terminal running.

3. **Log in and deploy** (Terminal B)

   Paste the login command from the banner:

   ```bash
   zt login \
     --server https://127.0.0.1:8443 \
     --ca-cert /tmp/zattera-dev/ca/ca.crt \
     --token zpat_XXXXXXXX            # <-- from the banner

   zt projects create demo

   # from the directory of any Dockerfile / Nixpacks app:
   zt deploy --prod --project demo
   ```

   The first deploy is the slow one (cold BuildKit start + image build):

   ```
     uploaded source; deployment ...
     building
     ✓ Built hello (dockerfile, ~15s)
     starting → health checking → promoting
     ✓ Released v1 → production (red/green, 1 replica(s) healthy)
   ```

4. **Hit the app**

   Apps are served at `<app>-<env>.<domain>`:

   ```bash
   # HTTP (port 8080)
   curl http://hello-production.apps.127.0.0.1.sslip.io:8080/

   # HTTPS (port 9443) — dev clusters sign certs with their own CA
   curl --cacert /tmp/zattera-dev/ca/ca.crt \
     https://hello-production.apps.127.0.0.1.sslip.io:9443/
   ```

5. **Inspect and iterate**

   ```bash
   zt ps --app hello --project demo             # running instances + health
   zt stats --nodes                             # live node CPU/mem
   zt attach hello --project demo -- /bin/sh    # shell into the container

   # change an env var and redeploy
   zt env set MESSAGE="hello v2" --app hello --env production --project demo
   zt deploy --prod --project demo
   ```

:::

**Tear down** — stop the node with `Ctrl-C`, then:

```bash
docker ps -aq   --filter label=dev.zattera/managed=true | xargs -r docker rm -f
docker network ls --filter name=zt- -q                  | xargs -r docker network rm
docker rm -f zt-system-buildkitd 2>/dev/null
docker volume rm zt-buildkit-cache 2>/dev/null
rm -rf /tmp/zattera-dev
```

== tab Real server

Bring up a control node on a Linux server, deploy from your laptop, and optionally join more nodes.

**Prerequisites**

- A Linux server (amd64/arm64) with a public IP and **Docker Engine** running.
- Open ports: `8443/tcp` (API), `80`/`443/tcp` (public ingress + ACME), `51820/udp` (mesh, multi-node only), `5000/tcp` (registry, reachable by other nodes).
- DNS: a wildcard `*.apps.example.com` → the server's public IP (needed for public HTTPS).

::: steps

1. **Install the binary** (on the server)

   ```bash
   curl -sfL https://get.zattera.dev | sh -
   ```

   Installs `/usr/local/bin/zattera` plus a `zt` symlink. The same command upgrades in place later.

2. **Configure the control node**

   `/etc/zattera/config.toml`:

   ```toml
   node_name = "cp1"
   data_dir  = "/var/lib/zattera"
   roles     = ["control", "worker"]   # co-located worker runs + builds apps
   domain    = "apps.example.com"      # apps get <app>-<env>.apps.example.com

   [api]
   listen         = ":8443"
   advertise_addr = "cp1.example.com:8443"

   [registry]
   listen = ":5000"

   [mesh]
   listen_port = 51820

   [acme]
   email = "ops@example.com"
   ```

3. **Run it** (systemd recommended)

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
   journalctl -u zattera -f
   ```

   On **first boot** the log prints, exactly once, the bootstrap admin token and a recovery passphrase — store both securely. It also logs the cluster CA fingerprint (`cluster CA fingerprint sha256=…`).

4. **Log in from your workstation**

   `--ca-pin` fetches and pins the cluster CA on first use — no file copying:

   ```bash
   zattera login --server https://cp1.example.com:8443 \
     --ca-pin <sha256-fingerprint> --token zpat_XXXXXXXX
   ```

5. **Deploy an app**

   Start with a prebuilt public image — the reliable path today (no build, no registry-CA trust needed):

   ```bash
   zattera projects create web
   zattera apps create site --project web
   zattera deploy --image nginx:alpine --prod --app site --project web

   zattera ps --app site --project web
   ```

   With public DNS in place, the ingress obtains a Let's Encrypt certificate on first request:

   ```bash
   curl https://site-production.apps.example.com/
   ```

   Before DNS/ACME is set up, use a forwarded port instead:

   ```bash
   zattera port-forward site 8080:http --project web
   curl http://127.0.0.1:8080/
   ```

6. **Add more nodes** (optional)

   Mint a join token against the control node:

   ```bash
   zattera nodes join-token create --worker
   # prints: K10<ca-hash>::<secret>
   ```

   On each new server (Docker + binary installed) — no config file needed:

   ```bash
   zattera server \
     --data-dir /var/lib/zattera \
     --join cp1.example.com:8443 \
     --token 'K10<ca-hash>::<secret>'
   ```

   The token pins the control CA, the node enrolls, gets a WireGuard mesh address, and starts running assignments. Manage the pool with `zattera nodes ls / drain / rm`.

:::

::: callout note Source builds on multi-node clusters
The embedded registry serves TLS signed by the cluster CA, and Docker verifies registry certs against its own trust store. Until this is automated, every node that pulls cluster-built images needs the CA installed:

```bash
sudo mkdir -p /etc/docker/certs.d/cp1.example.com:5000
sudo cp cluster-ca.crt /etc/docker/certs.d/cp1.example.com:5000/ca.crt
```

Public images (`nginx:alpine`, …) don't need this — prefer image deploys until then.
:::

**Port summary**

| Port        | Purpose                       | Who needs it            |
| ----------- | ----------------------------- | ----------------------- |
| `8443/tcp`  | API (gRPC + REST, TLS)        | your CLI, joining nodes |
| `51820/udp` | WireGuard mesh                | node ↔ node             |
| `5000/tcp`  | embedded registry             | other nodes             |
| `80/tcp`    | ingress HTTP + ACME challenge | the internet            |
| `443/tcp`   | ingress HTTPS                 | the internet            |

:::

## Next steps

::: grids
::: grid
::: card Installation icon:download
Installing and upgrading the binary, pinning versions, per-platform builds.

[Read more →](../setup/installation)
:::
:::
::: grid
::: card Contributing icon:users
Architecture decisions are recorded as ADRs; design discussions happen in Issues/Discussions.

[Architecture decision records →](../contributing/architecture-decision-records/)
:::
:::
:::
