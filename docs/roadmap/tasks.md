# Zattera — Implementation Tasks (M1 → M3, + F27 node autoprovisioning)

This is the dependency-ordered implementation plan. The **foundation is already
implemented and tested** (see "What already exists"). Work through tasks in
order; a task may start when its `Depends` are done. Each phase ends with
something runnable.

> **Status:** tasks marked ✅ **DONE** are complete. This currently covers
> **T-01 … T-54** (the full M1 milestone, exit gate green in CI), **T-87** and
> **T-88** (multi-arch), plus **T-89** and **T-90** (production ingress +
> public API TLS/ACME). Phase 6 (M2) is underway: **T-55** (raft HA core),
> **T-55b** (daemon join-as-control), and **T-56** (gossip failure detection)
> are done and **verified GREEN on a real 3-node Hetzner cluster**
> (`test/cloud/ha_test.go`: quorum forms, leader-kill failover, dead node DOWN
> in ~19s). **T-55c** (multi-hub mesh + hub/control failover) completes HA: every
> control node is a WireGuard hub, a worker's whole-mesh route fails over between
> hubs, and workers roll their control-plane connection onto a surviving control
> node (`test/cloud/multihub_test.go`). **T-55d** makes a follower control+worker's
> own agent stream to the leader so workloads on control nodes stay visible to the
> scheduler (`test/cloud/controlworkload_test.go`). **T-57/T-57c** (meshsock),
> **T-59/T-60** (ring TSDB metrics sampler + historical stats API/CLI),
> **T-61** (CPU/mem/RPS autoscaler), **T-62** (node-pinned volumes + fencing
> leases), **T-63** (stateful stop-then-start deploys), **T-64** (content-
> addressed snapshot engine), **T-65** (snapshot orchestration + CLI) and
> **T-66** (full backup + `zatterad restore` DR) are done. In Phase 7, **T-75**
> (per-PR preview environments: `pull_request` webhooks → `preview-<n>` envs
> cloned from staging, PR-comment URLs, per-app cap, TTL janitor) and **T-76**
> (`zattera audit` + `zattera events -f`, backed by a new `ListEvents` RPC) and
> **T-77** (read-only `volume browse` TUI, which also implemented the
> ListFiles/ReadFile data path end to end) are done. In Phase 9, **T-93**
> (per-node version reporting), **T-94** (cordon/uncordon) and **T-95**
> (`zt cluster upgrade` — rolling, leader-last, checksum-verified) are done.

## What already exists (do not rebuild)

- **Protos** (`api/proto/…`, generated code committed in `api/gen/…`):
  full public API (`zattera.v1`), node↔control contracts (`zattera.cluster.v1`:
  agent/mesh/routes), and the Raft FSM contract (`fsm.proto`).
- **`internal/state`** — in-memory typed store: indexes, watch hub
  (`Store.Watch(kinds…)` → coalesced notifications), CAS KV, rings,
  snapshot/restore. Fully tested.
- **`internal/daemon/raftstore`** — raft wiring, FSM with **all command
  handlers implemented**, `Store.Apply(ctx, cmd)`, single/multi-node
  bootstrap, `NewTestStore/NewTestNode`.
- **`internal/daemon/secrets`** — argon2id-sealed cluster data key, AES-GCM
  `Sealer`. Tested.
- **Frozen interfaces**: `runtime.ContainerRuntime` (runtime/types.go),
  `mesh.Manager`, `proxy.RouteSource`, `logstore.Store`, `tsdb.Store`,
  `clock.Clock`.
- **`internal/testutil`**: `fakeruntime` (scriptable), `simcluster`
  (in-process multi-node raft: kill/partition/heal), `freeport`;
  `clock.Fake` in pkgutil.
- **`internal/config`** (server TOML), **`internal/cli`** skeleton
  (login/context, `ui` printer, `cliconfig`), **`pkg/apiclient`**,
  **`cmd/zattera`** with inverted build tags (ADR-0002), Makefile, CI.
- **Fixtures** `test/fixtures/apps/{go-hello,node-hello,postgres-demo}` and a
  working Docker integration test (`test/integration/fixtures_test.go`).
- **`zattera server --dev`** boots a single-node raft control plane.

## Conventions (read before every task)

1. **Never change or reuse a proto field number.** Additive changes only.
   After proto edits: `make generate` and commit `api/gen`. CI runs
   `buf breaking`.
2. **Every state mutation goes through `raftstore.Store.Apply`** with a
   `clusterv1.Command`. Fill `request_id` (`ids.New()`), `actor`, `time`
   (`timestamppb.Now()`). Never mutate `internal/state` directly outside FSM
   handlers. FSM apply handlers must stay deterministic: no `time.Now()`, no
   randomness, no I/O — values travel in the command.
3. **Only `internal/daemon/runtime/docker.go` may import the Docker SDK**
   (enforced: `grep -r "docker/docker" internal/ | grep -v runtime` must stay
   empty).
4. **Timeouts/tickers in control loops use `clock.Clock`** injected at
   construction; tests use `clock.Fake`.
5. **Logging**: `log/slog`, lowercase messages, key-value fields
   (`log.Info("deployment promoted", "env", envID, "release", relID)`).
   Never log secret values, tokens, or key material.
6. **Errors**: wrap with `%w` and package prefix
   (`fmt.Errorf("scheduler: …: %w", err)`). API handlers return gRPC status
   errors (`status.Error(codes.NotFound, …)`).
7. **Test tiers**: unit (no tag, `make test`, must stay Docker-free and fast),
   `integration` (real Docker), `e2e` (full binary), `chaos` (simcluster,
   slow). Tag files with `//go:build integration` etc.
8. **CLI/daemon separation** (ADR-0002): CLI code never imports
   `internal/daemon…`; shared surface = `pkg/apiclient` + `api/gen`.
9. **File ownership**: only touch files listed in your task; if you must edit
   another task's file, say so in the commit message.
10. Commit per task: `T-NN: <imperative summary>`.

### Ports & addresses (fixed)

| What                                          | Where                                                                          |
| --------------------------------------------- | ------------------------------------------------------------------------------ |
| Public API (gRPC+REST, TLS)                   | `:8443`                                                                        |
| Ingress HTTP / HTTPS                          | `:80` / `:443`                                                                 |
| Embedded registry (TLS)                       | `:5000` (control nodes)                                                        |
| Raft transport                                | `:7480` (mesh IP / 127.0.0.1)                                                  |
| Agent-local gRPC (mTLS)                       | `:8444` (mesh IP / 127.0.0.1)                                                  |
| WireGuard UDP                                 | `:51820`                                                                       |
| Mesh CIDR / VIP CIDR / per-env docker subnets | `10.90.0.0/16` / `10.97.0.0/16` / `10.201.0.0/16` (/24 per (project,env,node)) |

### Task template

```
### T-NN — Title
Phase N · Depends: … · Size: S/M/L
Files: exact paths (create/modify)
Steps: numbered plan
Gotchas: traps to avoid
Tests: what to write, which tier
Acceptance: commands that must pass
```

---

# Phase 1 — Control plane API & CLI core

**Exit criterion:** `bin/zattera server --dev` prints a bootstrap admin token;
`zattera login --server https://127.0.0.1:8443 --token …` verifies via WhoAmI;
`zattera projects create demo`, `zattera init`, env vars set/pull, and
`zattera state export` all work over TLS.

### T-01 — Embedded cluster CA ✅ **DONE**

Phase 1 · Depends: — · Size: M
**Files:** `internal/daemon/ca/ca.go`, `ca_test.go`
**Steps:**

1. `type CA struct` holding an ECDSA P-256 root (10y validity, CN
   `zattera-cluster-ca`). `LoadOrCreate(dir string)` persists
   `ca.crt`/`ca.key` (0600) under `<data-dir>/ca/`.
2. `IssueServer(dnsNames []string, ips []net.IP, ttl)` → server cert for the
   API/registry listeners (include `127.0.0.1`, the node's mesh IP, the
   cluster domain, and `localhost`).
3. `IssueNode(nodeID string, meshIP net.IP, ttl)` → client+server cert with
   **URI SAN `zattera://node/<nodeID>`** and DNS SAN `node-<nodeID>`; role
   detection in the API reads the URI SAN. 1y TTL.
4. `SignCSR(csrPEM []byte, nodeID string, ttl)` — verify CSR signature, ignore
   requested SANs, impose ours (join flow, T-17).
5. `CABundlePEM()`, `ServerTLSConfig()`, `ClientTLSConfig(nodeCert)` helpers
   returning `*tls.Config` with `MinVersion: TLS12`.
   **Gotchas:** set `BasicConstraintsValid`, `IsCA`, `KeyUsageCertSign` on the
   root; leaf certs need `ExtKeyUsageServerAuth` **and** `ClientAuth` (node certs
   are used both ways); serial numbers from `crypto/rand`; never reuse the root
   key file if it fails to parse — fail loudly rather than regenerate (a
   regenerated CA silently bricks every node's trust).
   **Tests:** unit — round-trip: create CA, issue node cert, verify chain with
   `x509.Verify`; SAN contents; CSR signing rejects a CSR with a bad signature.
   **Acceptance:** `go test ./internal/daemon/ca/`

### T-02 — gRPC + gateway server on one TLS port ✅ **DONE**

Phase 1 · Depends: T-01 · Size: M
**Files:** `internal/daemon/api/server.go`, `server_test.go`; modify
`internal/daemon/daemon.go` (wire it)
**Steps:**

1. `api.Server` starts one TLS listener (`cfg.API.Listen`) with the CA server
   cert; TLS config requests (not requires) client certs
   (`tls.VerifyClientCertIfGiven`) so both CLI (token) and nodes (mTLS) share
   the port.
2. Route by content type on an `http.Handler`: if
   `r.ProtoMajor == 2 && strings.HasPrefix(ct, "application/grpc")` → grpc
   server, else → grpc-gateway mux. Serve with `golang.org/x/net/http2`
   h2 enabled via `http.Server` + TLS NextProtos `["h2","http/1.1"]`.
3. Register all public services from `api/gen` and the internal services
   (registered by later tasks through an options struct
   `api.Options{AuthService: …, ProjectService: …}` — a nil service is simply
   not registered, so tasks can land incrementally).
4. Gateway: `runtime.NewServeMux` from grpc-gateway, register
   `RegisterXHandlerFromEndpoint` pointing at the same port over a loopback
   dial with the CA-trusted TLS config; forward the `authorization` header
   (default header matcher passes it — verify).
5. grpc health service + `GET /healthz` on the gateway side returning 200.
   **Gotchas:** the gateway dials the public port — use
   `grpc.WithTransportCredentials` with the CA pool, NOT insecure; body size
   limits: set `grpc.MaxRecvMsgSize(64<<20)` (source tarballs stream in 1MB
   chunks but headroom matters); keepalive enforcement policy must allow the
   agent's long-lived streams (`MinTime: 10s, PermitWithoutStream: true`).
   **Tests:** unit — start on a freeport with a self-signed CA, hit `/healthz`
   via HTTPS, make a gRPC health check call over the same port.
   **Acceptance:** `go test ./internal/daemon/api/ -run TestServerDualProtocol`

### T-03 — First-boot bootstrap: org, admin, token, cluster key ✅ **DONE**

Phase 1 · Depends: — · Size: M
**Files:** `internal/daemon/bootstrap.go`, `bootstrap_test.go`
**Steps:**

1. On leader start, if `state.Org()` is absent: create Org ("default"), admin
   user `admin@local` with a random password hash placeholder, a
   `TOKEN_KIND_PERSONAL` bootstrap token, and the sealed cluster key.
2. Data key: `secrets.GenerateDataKey()`; passphrase:
   `secrets.GeneratePassphrase()` (or `--recovery-passphrase-file`); commit
   `PutClusterKeyMaterial`; keep the plaintext key in a `*secrets.Keyring`
   struct on the daemon (in-memory only) and construct the `Sealer`.
3. Print exactly once to stdout (not the logger):
   `Bootstrap admin token: zpat_<secret>` and
   `Recovery passphrase (STORE THIS SAFELY): <passphrase>`.
4. Token secret: 32 random bytes, base62; store SHA-256 hex in the Token via
   `PutToken`. Token string format: `zpat_<base62>`.
5. Idempotency: guard on `state.Org()` existence — a restart must not print a
   new token. Use a single `request_id` derived… no: just check-then-apply on
   the leader at startup; races are impossible (single leader, sequential
   startup).
   **Gotchas:** everything through Apply (rule 2) — bootstrap runs only when
   `IsLeader()`; on followers skip silently. Never log the token/passphrase via
   slog (stdout print only). The `time` in commands comes from `timestamppb.Now()`
   at the proposer.
   **Tests:** unit with `raftstore.NewTestStore`: bootstrap runs → org+user+token
   exist; second run is a no-op; token hash verifies.
   **Acceptance:** `go test ./internal/daemon/ -run TestBootstrap`

### T-04 — AuthService + token auth interceptor ✅ **DONE**

Phase 1 · Depends: T-02, T-03 · Size: L
**Files:** `internal/daemon/api/auth.go`, `interceptors.go`,
`auth_test.go`
**Steps:**

1. Implement `zatterav1.AuthServiceServer`: `Login` (verify argon2id password
   → create `TOKEN_KIND_SESSION` with 24h TTL), `WhoAmI`,
   `CreateToken`/`ListTokens`/`RevokeToken`, `CreateUser`/`ListUsers`
   (admin only). Password hashing: argon2id PHC string (reuse params from
   `internal/daemon/secrets`; add a small `hashPassword/verifyPassword` here).
2. Unary + stream interceptors resolving identity, in order: (a) peer mTLS
   cert with URI SAN `zattera://node/<id>` → node identity; (b)
   `authorization: Bearer zpat_…` → SHA-256 → `state.TokenByHash` → user
   identity (reject expired). Put an `Identity{UserID, NodeID, OrgRole}` into
   the context (`api.IdentityFrom(ctx)`).
3. Method policy table `methodAuth: map[string]Requirement` — every full
   method name maps to `Public` (Login only), `User`, `Node`, or `Admin`.
   **Unlisted methods are DENIED** (fail closed) — add an init-time check
   that every registered service method appears in the table.
4. Token `last_used_at`: accumulate in a memory map, flush every 60s via one
   `TouchTokens` Apply (leader only; drop on followers for now).
   **Gotchas:** compare token hashes via `state.TokenByHash` (constant-time not
   needed on hash equality — hashes aren't secret); session token TTL enforced
   at lookup, not only at creation; grpc-gateway lowercases header names —
   read via `metadata.FromIncomingContext` key `authorization`; stream
   interceptor must wrap `ServerStream` to keep the identity context.
   **Tests:** unit — full server on freeport: Login → WhoAmI with session token;
   expired token rejected; unknown method denied; node cert reaches Node-tier
   methods; user token cannot call Node-tier.
   **Acceptance:** `go test ./internal/daemon/api/ -run TestAuth`

### T-05 — RBAC + ProjectService ✅ **DONE**

Phase 1 · Depends: T-04 · Size: M
**Files:** `internal/daemon/api/rbac.go`, `projects.go`, `rbac_test.go`
**Steps:**

1. `rbac.go`: `minRole: map[string]zatterav1.Role` for every project-scoped
   method (e.g. `Deploy → DEVELOPER`, `AddMember → ADMIN`,
   `GetEnvVars(reveal) → DEVELOPER`). Resolver: org owner/admin bypass;
   otherwise `state.ProjectMember(projectID, userID)` and compare (OWNER >
   ADMIN > DEVELOPER > VIEWER).
2. Extract `project_id` generically: all project-scoped request messages have
   field `project_id` — use proto reflection
   (`msg.ProtoReflect().Descriptor().Fields().ByName("project_id")`) in the
   interceptor. Also accept **project name** and resolve to id here, so the
   CLI can pass names everywhere.
3. `projects.go`: implement `ProjectServiceServer` — Create (validate
   DNS-safe name `[a-z0-9-]{1,40}`, unique; creator becomes OWNER member),
   List (filter to the caller's memberships unless admin), Get, Delete
   (cascade: apps, envs, env vars, domains, volumes — propose the deletes in
   one batch of commands), Add/Remove/List members (AddMember resolves
   `user_email` → user).
   **Gotchas:** RBAC interceptor runs AFTER auth interceptor; methods without a
   `project_id` field (auth, nodes) are governed by the T-04 table only; deletes
   must also stop what's running — leave a `TODO(T-27)` comment where instance
   teardown hooks in (scheduler reconciles assignments away when envs vanish).
   **Tests:** unit — matrix: viewer cannot create app, developer can deploy but
   not add members, non-member sees no project; name uniqueness.
   **Acceptance:** `go test ./internal/daemon/api/ -run 'TestRBAC|TestProjects'`

### T-06 — AppService: apps, environments, env vars ✅ **DONE**

Phase 1 · Depends: T-05 · Size: M
**Files:** `internal/daemon/api/apps.go`, `apps_test.go`
**Steps:**

1. CreateApp (name DNS-safe, unique per project) auto-creates `production`
   and `staging` environments with default ServiceSpec
   (replicas 1/1, port http/8080, healthcheck defaults from the proto docs).
2. `ApplyAppConfig`: upsert build config + per-env ServiceSpec from the
   request map (create envs not yet present; envs absent from the request are
   left untouched).
3. `SetEnvVars`: seal each value with the daemon `Sealer` → one `SetEnvVars`
   command. `GetEnvVars`: keys with empty values by default; `reveal=true`
   (DEVELOPER+) opens the sealed values.
4. `SetReplicas` updates `Environment.Service.Replicas` via `PutEnvironment`.
5. GetApp returns app + its environments.
   **Gotchas:** environment ids are ULIDs — `EnvironmentByName(appID, name)` for
   name resolution; when updating `Environment` always re-read from state, apply
   the delta, then `PutEnvironment` (last-write-wins is fine on the leader);
   never return `password_hash`/`secret_hash`/sealed bytes in responses (clear
   fields).
   **Tests:** unit — create app → envs exist; env var round trip (set, list
   redacted, reveal); ApplyAppConfig creates a preview env.
   **Acceptance:** `go test ./internal/daemon/api/ -run TestApps`

### T-07 — Audit middleware ✅ **DONE**

Phase 1 · Depends: T-04 · Size: S
**Files:** `internal/daemon/api/audit.go`, `audit_test.go`; implement
`AuditServiceServer` here too.
**Steps:**

1. Interceptor after RBAC: for mutating methods (everything not `Get*`/
   `List*`/`Watch*`/`Query*`/streams), build an `AuditEntry` (actor from
   identity, method, project id, outcome = grpc code string) and enqueue.
2. Batcher: buffered channel + goroutine flushing ≤100 entries or 2s (real
   ticker fine here) via one `AppendAudit` Apply on the leader.
3. `QueryAudit` API over `state.QueryAudit` with the filter fields.
   **Gotchas:** record on error too (outcome carries the code); drop (with a
   counter log) when the buffer is full rather than blocking requests; skip
   audit for `Login` failures? No — record with outcome, but NEVER include the
   password (request_summary only for allow-listed message types).
   **Tests:** unit — mutating call lands in audit with actor+outcome; reads
   don't; query filters by method prefix.
   **Acceptance:** `go test ./internal/daemon/api/ -run TestAudit`

### T-08 — Leader-forward interceptor ✅ **DONE**

Phase 1 · Depends: T-02 · Size: M
**Files:** `internal/daemon/api/leaderforward.go`, `leaderforward_test.go`
**Steps:**

1. Unary interceptor: if the handler returns `raftstore.ErrNotLeader` (or the
   store says `!IsLeader()` for known-mutating methods), look up the leader's
   **API address** and transparently proxy the call:
   `grpc.NewClient(leaderAPIAddr).Invoke(ctx, method, req, resp)` with the
   node's mTLS client cert + original metadata.
2. Leader API address resolution: raft gives transport addr; map raft server
   id → `state.Node(id).PublicEndpoints`/mesh IP + `:8443`. Store a
   `leaderResolver` func for testability.
3. Cap at one hop (`x-zattera-forwarded: 1` metadata; refuse to forward a
   forwarded request).
4. Single-node: no-op (leader is always local). Streams: don't forward —
   return `codes.FailedPrecondition` with the leader address in the error
   details so the client redials (document in pkg/apiclient).
   **Gotchas:** forwarding must preserve deadlines and the `authorization`
   metadata; the connection pool to the leader must be invalidated on leadership
   change (subscribe to `LeaderCh()`).
   **Tests:** unit with two `NewTestNode`s + fake resolver: apply on follower
   lands on leader; forwarded-loop guard trips.
   **Acceptance:** `go test ./internal/daemon/api/ -run TestLeaderForward`

### T-09 — zattera.toml parser + config hash ✅ **DONE**

Phase 1 · Depends: — · Size: M
**Files:** `internal/appconfig/appconfig.go`, `appconfig_test.go`,
`testdata/*.toml`
**Steps:**

1. Parse the spec §4 format (BurntSushi/toml): `[app] name`,
   `[build] type|dockerfile|image`, `[deploy] healthcheck`, `[env.<name>]`
   replicas/autoscale/domains/idle_timeout/stateful/max_concurrency/
   command/resources, `[[env.<name>.volumes]]`, `[[cron]]` (global) and
   `[[env.<name>.cron]]` (per-env, overrides global).
2. Convert into `(BuildConfig, map[envName]*zatterav1.ServiceSpec, domains
map[envName][]string)` — the exact shape `ApplyAppConfigRequest` wants.
3. Defaults: port http/8080 when none declared; healthcheck HTTP `/healthz`
   if a `[deploy] healthcheck` is absent but an http port exists.
4. `ConfigHash(spec *zatterav1.ServiceSpec, envVarVersion uint64) string`:
   sha256 over `proto.MarshalOptions{Deterministic: true}` bytes + the env
   var version counter. Lives here; used by releases (T-28) and the agent.
5. Validation with actionable errors ("env.production.replicas.min > max").
   **Gotchas:** durations come as TOML strings ("15m") — parse with
   `time.ParseDuration`; unknown keys = hard error (same style as
   internal/config); deterministic proto marshaling is REQUIRED for the hash —
   plain `proto.Marshal` is not stable across builds.
   **Tests:** unit — golden: full-featured file parses to the expected specs;
   defaulting; every validation error case; hash stability (same input twice)
   and sensitivity (any field change → new hash).
   **Acceptance:** `go test ./internal/appconfig/`

### T-10 — CLI: client factory, verified login, projects/apps/env commands, init ✅ **DONE**

Phase 1 · Depends: T-04..T-06, T-09 · Size: L
**Files:** `internal/cli/client.go`, `projects.go`, `apps.go`, `env.go`,
`init.go`; modify `internal/cli/cli.go` (register)
**Steps:**

1. `client.go`: `fromContext()` loads cliconfig → `apiclient.New` (CA pem
   from context); helper `projectFlag` resolution (`--project` or context
   default).
2. `login`: after saving, call `WhoAmI`; on failure remove the context and
   error. Print the user's email on success. Add `--insecure` mapping to
   `InsecureSkipVerify` for dev.
3. `projects create/ls/rm`, `members add/rm/ls` (table + `--json` via
   `ui.Printer`).
4. `apps create/ls/rm`; `env set KEY=V… --env production`, `env pull [--reveal]`
   (prints `KEY=value` lines), `env unset`.
5. `init`: detect app type in cwd (Dockerfile → dockerfile; package.json →
   nixpacks; go.mod → nixpacks), write `zattera.toml` via internal/appconfig
   helpers, print next steps. `--name` flag, default = directory name.
6. `apply`: parse zattera.toml (T-09) → `ApplyAppConfig` (used by deploy later).
   **Gotchas:** every command works with `--json` (emit the proto-derived
   struct); exit code 1 on API errors with the gRPC message shown plainly (strip
   the `rpc error: code = …` prefix — users see "project demo not found");
   never print secrets without `--reveal`.
   **Tests:** unit — run the real API server (T-02..06) on a freeport in-process,
   point the CLI at it via `ZATTERA_CONFIG` in a temp dir, execute cobra commands
   with captured output (`cmd.SetArgs`, `cmd.Execute`). Test login-verify
   failure path, project create/ls, env round trip, init detection matrix.
   **Acceptance:** `go test ./internal/cli/ -run TestCLI` — and manually:
   `bin/zattera server --dev` + `bin/zattera login && bin/zattera projects create demo`.

### T-11 — State export / apply ✅ **DONE**

Phase 1 · Depends: T-05, T-06 · Size: M
**Files:** `internal/daemon/api/statesvc.go`, `statesvc_test.go`,
`internal/cli/state.go`
**Steps:**

1. Define the export document (YAML, human-readable, **desired state only**):
   projects → apps → environments (spec, env var KEYS with sealed values
   base64), domains, volumes, alert rules, channels. Exclude: observed state,
   assignments, tokens, users, certs, audit.
2. `Export` streams the YAML (marshal via `sigs.k8s.io/yaml` over a plain
   export struct — do NOT hand-roll YAML). Project-scoped or cluster-wide.
3. `Apply` parses, diffs against state by (project name, app name, env name),
   proposes creates/updates; returns counts + warnings for unknown fields.
   `--dry-run` flag → validate and count only.
4. CLI: `zattera state export [> file]`, `zattera state apply -f file
   [--dry-run]`.
   **Gotchas:** sealed env values only re-import into the SAME cluster (data key
   matches) — document in the file header comment; ids are not exported (names
   are the identity), so apply must be idempotent by name; never export
   `password_hash`/token hashes.
   **Tests:** unit — export→wipe→apply round trip reproduces projects/apps/envs;
   apply is idempotent (second run = all "unchanged").
   **Acceptance:** `go test ./internal/daemon/api/ -run TestState`

### T-12 — NodeService + join tokens + `zattera nodes ls` ✅ **DONE**

Phase 1 · Depends: T-04 · Size: S
**Files:** `internal/daemon/api/nodes.go`, `nodes_test.go`,
`internal/cli/nodes.go`
**Steps:**

1. `ListNodes/GetNode` from state; register the local node in state at
   daemon start (`PutNode` with roles/labels/capacity from
   `gopsutil` — cpu count ×1000 millis, total RAM, disk of data dir).
2. `CreateJoinToken`: secret = 32 random bytes base62; store hash;
   token string = `K10<sha256-of-CA-cert-hex>::<secret>` (CA hash from T-01).
3. `SetNodeLabels`, and stubs returning `codes.Unimplemented` for
   Drain/Remove (real logic T-30).
4. CLI `nodes ls` (table: name, roles, status, mesh IP, labels),
   `nodes join-token create`.
   **Gotchas:** capacity detection must not crash on exotic platforms — fall
   back to zeros with a warning; the CA hash in the token is over the DER bytes
   (`sha256(cert.Raw)`), hex-encoded.
   **Tests:** unit — local node registered at boot; join token round trip
   (create → hash matches secret).
   **Acceptance:** `go test ./internal/daemon/api/ -run TestNodes`

---

# Phase 2 — Node runtime & WireGuard mesh

**Exit criterion:** two zatterad instances in privileged containers form a
mesh (hub-and-spoke via the control node), the worker appears in
`zattera nodes ls` as ALIVE, and cross-node ping over `10.90.0.0/16` works.

### T-13 — Docker ContainerRuntime implementation ✅ **DONE**

Phase 2 · Depends: — · Size: M
**Files:** `internal/daemon/runtime/docker.go`, `docker_test.go`,
`test/integration/runtime_test.go`
**Steps:**

1. `NewDocker() (*Docker, error)` via `client.NewClientWithOpts(client.FromEnv,
client.WithAPIVersionNegotiation())`. Pin `github.com/docker/docker` v28.x
   in go.mod.
2. Implement every `ContainerRuntime` method mapping our types to Docker's:
   `EnsureImage` (pull with progress callback; "already exists" = success),
   `CreateContainer` (always `Tty:false`; map ports to
   `nat.PortMap{HostIP: spec.HostIP}`, resources to `NanoCPUs = CPUMillis*1e6`
   and `Memory = MB<<20`, restart policy, network + DNS, labels + always
   `ManagedLabel=true`), Start/Stop(timeout→`container.StopOptions.Timeout`
   seconds)/Remove(force), Inspect → normalized `ContainerState` (fill
   effective host ports from `NetworkSettings.Ports`), List (label filters +
   ManagedLabel), Logs (demux with `stdcopy.StdCopy` into a goroutine feeding
   the channel; parse the timestamp prefix from `Timestamps: true`), Exec
   (ExecCreate/ExecAttach/ExecInspect loop for exit code; resize channel →
   ContainerExecResize), Stats one-shot (`ContainerStatsOneShot`; CPU% =
   delta/systemDelta×onlineCPUs×100), Top, CopyFrom/CopyTo
   (`CopyFromContainer`/`CopyToContainer`), EnsureNetwork (inspect first;
   create bridge with IPAM subnet), EnsureVolume/RemoveVolume,
   `VolumeHostPath` (VolumeInspect → Mountpoint), Ping.
   **Gotchas:** `ContainerLogs` on TTY containers is NOT multiplexed — we always
   create with `Tty:false` so ALWAYS demux; `stdcopy` needs the raw stream;
   timestamps arrive as RFC3339Nano prefix + space; context cancellation must
   close the pull reader (wrap in goroutine select); normalize Docker's
   `ErrNotFound` (`errdefs.IsNotFound`) to `runtime.ErrNotFound`; never List
   without the ManagedLabel filter.
   **Tests:** integration only (mock-based unit tests are low-value):
   full lifecycle against real Docker — pull alpine, create+start with a label
   and a port on 127.0.0.1, logs (echo something), exec `true` (exit 0) and
   `false` (exit 1), stats, stop, remove; EnsureNetwork/EnsureVolume idempotent;
   VolumeHostPath returns an existing path (skip content check on macOS — the
   path lives in the VM).
   **Acceptance:** `go test -tags integration -run TestDockerRuntime
./test/integration/`; `grep -r "docker/docker" internal/ | grep -v
daemon/runtime` empty.

### T-14 — Agent skeleton: AgentSync stream + heartbeats ✅ **DONE**

Phase 2 · Depends: T-02, T-12 · Size: M
**Files:** `internal/daemon/agent/agent.go`, `sync.go`, `agent_test.go`;
control side: `internal/daemon/api/agentsync.go`
**Steps:**

1. `agent.Agent{NodeID, Runtime, Clock, LocalControlAddr | mTLS creds}` with
   `Run(ctx)`: dial control gRPC (mesh addr or 127.0.0.1 single-node), open
   `AgentSyncService.Sync`, send `AgentHello{node_id, version,
assignment_version}`, then a heartbeat every 10s (Clock ticker) with node
   CPU/mem/disk from gopsutil.
2. Reconnect loop with exponential backoff (1s..30s + jitter); resend Hello
   each time.
3. Control side (`agentsync.go`): implement `AgentSyncServiceServer.Sync` —
   authenticate node identity (T-04), register the stream in a
   `*livestate.Registry` (new small struct: map nodeID → stream + last
   heartbeat + live samples; THIS is the leader-memory livestate from the
   design), push `AssignmentSet` on registration and on assignment changes
   (subscribe `state.Watch(KindAssignment)`, filter by node, debounce 200ms,
   full set with version = `state.Version()`).
4. Heartbeats update livestate only. StatusBatch → debounced (≤1 per node per
   2s) `SetAssignmentsObserved` Apply.
   **Gotchas:** the stream context dying must deregister the node from livestate;
   version-skip: if `AgentHello.assignment_version` equals current, skip the
   initial resend; never Apply on non-leader — the agent always connects to the
   leader? NO: agents connect to any control node; the Sync handler must forward
   StatusBatch applies through the leader-forward path (call the local Apply and
   tolerate ErrNotLeader by proxying via T-08's helper — factor
   `api.applyAnywhere(ctx, cmd)`).
   **Tests:** unit — in-process server + agent with fakeruntime and fake clock:
   hello registers, heartbeat lands in livestate, assignment change pushes a new
   AssignmentSet, disconnect+reconnect resyncs.
   **Acceptance:** `go test ./internal/daemon/agent/ -run TestAgentSync`

### T-15 — Assignment executor (agent reconciler) ✅ **DONE**

Phase 2 · Depends: T-14 · Size: L
**Files:** `internal/daemon/agent/executor.go`, `executor_test.go`
**Steps:**

1. On every `AssignmentSet`, reconcile local Docker to it:
   - desired RUN, no container → EnsureImage (registry creds from join, T-17)
     → EnsureVolume/EnsureNetwork as referenced → CreateContainer → Start.
   - desired STOP or assignment gone → Stop (grace) → Remove.
   - config_hash changed → stop+remove old, create new (the scheduler makes
     red/green decisions; the agent only converges).
2. Container naming: `zt-<app>-<env>-<assignment-id[:8]>`; identity via labels
   (`LabelAssignmentID` etc.) — reconcile matches on labels, never names.
3. Env vars: sealed values arrive IN the AssignmentSet? NO — they'd transit
   fine (mTLS) but bloat state; instead the control side injects decrypted env
   into `Assignment` push messages? Decision: control decrypts at push time
   and sends env in the AssignmentSet stream message (add nothing to protos —
   `Assignment.mesh_port_bindings` exists; env travels via a parallel field?).
   **Resolved design:** add to `AssignmentSet` a per-assignment
   `map<string,string> env` field (new proto field, additive) filled by the
   control stream handler from sealed state + Sealer. Env never persists in
   Raft as plaintext, only in stream frames.
4. Report status transitions (StatusBatch): PULLING→STARTING→RUNNING on
   start; FAILED with message on any error (backoff retry ×3 then park);
   restarts counted from Docker events? Simpler: poll Inspect every 5s
   (Clock) for liveness until T-16 health probes land; report STOPPED on
   clean exit for jobs.
5. Port allocation: for each PortSpec, HostIP = mesh IP (or 127.0.0.1),
   HostPort = 0 (Docker allocates); after Start, Inspect and report the
   effective ports in the StatusBatch (extend `AssignmentObserved`? ports are
   already in `Assignment.mesh_port_bindings` — the AGENT fills them via a
   status message; control commits them into the assignment on the next
   observed-batch apply). Keep it simple: agent includes bindings in
   `AssignmentObserved.message`? NO — add proto field
   `AssignmentObserved.mesh_port_bindings` (additive) and merge it in
   `Store.SetAssignmentObserved`.
   **Gotchas:** reconcile must be idempotent and crash-safe: on agent restart,
   List(ManagedLabel + role=service) and adopt existing containers by
   assignment-id label; NEVER touch containers without ManagedLabel; image pull
   failures must not wedge the loop (per-assignment goroutine with backoff, or
   sequential loop with per-item error capture — pick sequential for
   determinism); apply STOP before RUN when both queues exist (free ports).
   **Tests:** unit with fakeruntime + fake clock: converge from empty to 2
   assignments; remove one → container stopped+removed; config hash change →
   replace; adoption after restart; pull failure → FAILED status reported,
   retries.
   **Acceptance:** `go test ./internal/daemon/agent/ -run TestExecutor`

### T-16 — Health probes ✅ **DONE**

Phase 2 · Depends: T-15 · Size: M
**Files:** `internal/daemon/agent/health.go`, `health_test.go`
**Steps:**

1. Per running assignment, run its `HealthCheck` on the Clock: HTTP (GET
   `http://containerIP:port/path`, 2xx/3xx = pass), TCP (dial), EXEC
   (Exec, exit 0 = pass).
2. State machine per instance: RUNNING → HEALTHY after first pass within
   grace_period; HEALTHY → UNHEALTHY after `unhealthy_threshold` consecutive
   fails; UNHEALTHY → HEALTHY on pass. Report transitions via StatusBatch
   ONLY on change.
3. Defaults (proto docs): interval 10s, timeout 5s, grace 60s, threshold 3.
4. No healthcheck configured → HEALTHY immediately after RUNNING.
   **Gotchas:** probes hit the container IP on the per-env bridge — from the
   host netns this works on Linux (bridge is host-visible); on macOS dev it
   doesn't — probe via the published mesh/127.0.0.1 port instead when
   `runtime.GOOS == "darwin"` (add a helper choosing the probe address).
   Timeouts via context; a hung HTTP probe must not skip ticks (run each probe
   in its own goroutine guarded by the per-instance serial loop).
   **Tests:** unit — httptest server as the "container": grace period respected
   (fake clock), flap threshold, exec probe path with fakeruntime.
   **Acceptance:** `go test ./internal/daemon/agent/ -run TestHealth`

### T-17 — Join flow: RPC + client side ✅ **DONE**

Phase 2 · Depends: T-01, T-12 · Size: L
**Files:** `internal/daemon/api/join.go`, `join_test.go`; client side in
`internal/daemon/join.go`; modify `internal/daemon/daemon.go`
**Steps:**

1. Server (`JoinService.Join`, reachable with TLS but NO auth — the token IS
   the auth): parse `K10<ca-hash>::<secret>` client-side; server verifies
   `sha256(secret)` against unexpired join tokens; single-use tokens are
   consumed via `ConsumeJoinToken` (its handler rejects double-use —
   idempotency guard).
2. Allocate mesh IP: next free in `10.90.0.0/16` scanning `state.ListNodes`
   (control nodes get `.0.x` low addresses, workers upward from `.1.1`);
   `SignCSR` → node cert; `PutNode` (status ALIVE, schedulable, labels from
   request merged with `zattera.dev/os-arch` etc.); create registry
   credential (basic auth user `node-<id>`, random password; store hash in
   KV `registry/creds/<id>`; return plaintext once).
3. Response: node id, mesh IP, CA bundle, signed cert, initial `PeerSet`,
   control gRPC addr (mesh IP of this control node, or 127.0.0.1 when mesh
   disabled), registry addr+creds, `mesh_enabled`.
4. Client (`--join addr --token …`): validate CA pinning — dial with
   `InsecureSkipVerify` + custom `VerifyPeerCertificate` asserting
   `sha256(leafCA) == token hash part` (k3s trick), send CSR (key generated
   locally, never leaves the node), persist response under
   `<data-dir>/node/{node.crt,ca.crt,id,mesh.json}` then proceed with normal
   startup in worker mode (agent → T-14, mesh → T-18..20).
5. Wire into daemon.go replacing the current `--join` error.
   **Gotchas:** the CSR's private key stays client-side; Join must be rate-limit
   friendly (single RPC); mesh IP allocation must be raft-serialized — do the
   scan+PutNode inside ONE apply? Can't (validation in API layer): acceptable
   race window is zero because Join runs on the leader (leader-forward) and
   Apply is sequential — still, re-check uniqueness in a retry loop on conflict.
   Never reuse a mesh IP of a deleted node until M2 (tombstone via KV
   `meshipsalloc/<ip>` entries).
   **Tests:** unit — happy join (token verify, cert chain validates against CA,
   mesh IP allocated); expired/used token rejected; CA-pin mismatch client
   error.
   **Acceptance:** `go test ./internal/daemon/api/ -run TestJoin`

### T-18 — WireGuard device manager ✅ **DONE**

Phase 2 · Depends: — · Size: L
**Files:** `internal/daemon/mesh/device.go`, `device_linux.go`,
`device_darwin.go`, `kernel_linux.go`, `uapi.go`, `device_test.go`,
`test/integration/mesh_device_test.go`
**Steps:**

1. Keys: Curve25519 via `golang.zx2c4.com/wireguard/device` types; private
   key at `NodeConfig.PrivateKeyPath` (0600), generated on first use;
   `PublicKey()` derives without bringing the device up.
2. Linux kernel path (`kernel_linux.go`): detect via `wgctrl` probe; create
   link `zt0` (netlink), configure device+peers with `wgctrl`, add
   `<meshIP>/16` addr, set MTU 1420, link up.
3. Userspace path (`device.go` + tun): `tun.CreateTUN(name, 1420)`,
   `device.NewDevice(tun, conn.NewDefaultBind(), devLogger)`; configure via
   `IpcSet` with a `uapi.go` builder (struct → uapi text; golden-tested).
   macOS: name MUST be `utun[0-9]+` — pass `utun` and read the assigned name.
4. `ApplyPeers(ctx, *clusterv1.PeerSet)`: diff against current (IpcGet/wgctrl)
   by pubkey: add/update changed (endpoint = first reachable candidate for
   now; smarter path selection is T-57), `remove=true` for absent; set
   `persistent_keepalive_interval` and `allowed_ip` from the Peer message;
   hub-and-spoke → control peers get `10.90.0.0/16`.
5. Route management (linux): ensure `10.90.0.0/16 dev zt0` route exists.
   `Down()` closes device before TUN.
6. Register a `mesh.NewDeviceManager(cfg)` constructor selecting kernel vs
   userspace; keep implementing `mesh.Manager`.
   **Gotchas:** wireguard-go's logger is chatty — map to slog debug; AllowedIPs
   collisions: WG silently steals routes to the last peer set — the uapi builder
   must emit `replace_peers=true` on full reconfigure and the diff path must
   never assign the same allowed IP to two peers; userspace needs
   root/CAP_NET_ADMIN — return a clear error mentioning it; MTU 1420 everywhere;
   do NOT instantiate any of this when `cfg.Mesh.Disabled` (daemon uses
   `mesh.NewDisabled()`).
   **Tests:** unit — uapi builder golden tests; peer-diff logic against a fake
   ipc layer. Integration (linux + NET_ADMIN, in CI's privileged container):
   two userspace devices on 127.0.0.1 distinct ports, exchange peers, UDP echo
   over tunnel IPs.
   **Acceptance:** `go test ./internal/daemon/mesh/`; integration job green in
   CI (`go test -tags integration -run TestWGDevice ./test/integration/`).

### T-19 — Peer distribution + hub-and-spoke (Phase A) ✅ **DONE**

Phase 2 · Depends: T-17, T-18 · Size: M
**Files:** `internal/daemon/api/meshsvc.go`, `internal/daemon/mesh/peersync.go`,
`meshsvc_test.go`; modify `internal/daemon/daemon.go`
**Steps:**

1. Control: implement `MeshService.WatchPeers` — build each node's `PeerSet`
   from `state.ListNodes()` (watch KindNode, debounce 200ms, full set):
   workers get ONLY control peers with `allowed_ips=[10.90.0.0/16]`,
   `hub_and_spoke=true`, keepalive 25 for NAT'd nodes (no public endpoint);
   control nodes get every peer with /32s.
2. Node: `peersync.Run(ctx)` keeps a WatchPeers stream and calls
   `Manager.ApplyPeers` on every message; reconnect with backoff.
3. Control nodes enable forwarding at startup (linux):
   `sysctl net.ipv4.ip_forward=1` (via /proc write) + document iptables
   FORWARD accept for zt0 in the task's doc comment (installer's job later).
4. Daemon wiring: after join (worker) or bootstrap (control), bring mesh up
   with the allocated IP, start peersync; raft/API/registry listeners bind
   the mesh IP when mesh is enabled.
   **Gotchas:** peer endpoints for NAT'd workers are EMPTY — the hub never
   initiates; keepalive keeps the NAT hole open from the worker side; the
   WatchPeers stream authenticates via node mTLS; single-node/dev: skip
   entirely.
   **Tests:** unit — PeerSet builder: worker sees only controls with /16;
   control sees all /32; NAT keepalive set exactly when no public endpoint.
   **Acceptance:** `go test ./internal/daemon/api/ -run TestPeerSets`

### T-20 — Disco (STUN-lite) + direct worker↔worker peering (Phase B) ✅ **DONE**

Phase 2 · Depends: T-19 · Size: L
**Files:** `internal/daemon/mesh/disco.go`, `disco_test.go`; extend
`meshsvc.go` peer builder
**Steps:**

1. Disco protocol (UDP, on the WG listen socket? NO — separate port
   `listen_port+1` for phase B): 3 messages — `ping{node_id, txid, hmac}`,
   control echoes `pong{observed ip:port, txid, hmac}`; HMAC key = sha256 of
   the node's WG private key ⊕ cluster CA hash (both sides can derive; good
   enough for observation integrity).
2. Every node pings each control node every 30s; on pong, calls
   `MeshService.ReportObservedEndpoint` (its own observed addr).
3. Control folds observed endpoints into `Node.public_endpoints`
   (livestate + periodic PutNode batch, NOT per-pong applies), and the peer
   builder now emits worker↔worker peers with /32 AllowedIPs when BOTH sides
   have at least one endpoint (same-endpoint pairs = same NAT → also peer
   directly via their private addrs? Phase C problem — skip: keep hub
   fallback).
4. Hub remains: control peers still carry the /16 route — WG most-specific
   AllowedIP wins for /32 peers, hub catches the rest.
   **Gotchas:** never trust `ReportObservedEndpoint` for OTHER nodes (only
   self-reports, verified by mTLS identity); endpoint expiry (livestate TTL 5m);
   peers behind the SAME NAT often can't hairpin — that's why the hub route must
   survive (do not remove control peers' /16).
   **Tests:** unit — disco codec + HMAC round trip; peer builder: two workers
   with endpoints get direct /32 peers AND keep hub route.
   **Acceptance:** `go test ./internal/daemon/mesh/ -run TestDisco`

### T-21 — Node liveness from heartbeats ✅ **DONE**

Phase 2 · Depends: T-14 · Size: S
**Files:** `internal/daemon/api/liveness.go`, `liveness_test.go`
**Steps:**

1. Leader loop (Clock ticker 5s): nodes with livestate heartbeat older than
   30s (or no stream) → `SetNodeStatus{DOWN}`; fresh heartbeat on a DOWN
   node → ALIVE. Durable transitions only on change (one Apply each).
2. `last_heartbeat_at` batched: fold into the same SetNodeStatus at most
   every 60s per node.
3. New leader takes over cleanly: livestate empty at election → give nodes a
   45s grace window (from leadership acquisition) before declaring DOWN.
   **Gotchas:** never mark the local node DOWN; the grace window is the classic
   failover false-positive trap — test it with fake clock.
   **Tests:** unit — fake clock: stale → DOWN, recover → ALIVE, leader-change
   grace respected.
   **Acceptance:** `go test ./internal/daemon/api/ -run TestLiveness`

### T-22 — Two-node join integration rig ✅ **DONE**

Phase 2 · Depends: T-17, T-19 · Size: M
**Files:** `test/integration/twonode_test.go`, `test/integration/rig.go`
**Steps:**

1. Rig: build `bin/zattera` (linux/amd64 via `make` if missing — or
   `go build` with GOOS=linux into a temp dir), start two privileged
   `alpine`-based containers (or `debian:stable-slim`) with the binary
   bind-mounted, `--cap-add NET_ADMIN --device /dev/net/tun`, on one docker
   network.
2. Node A: `zattera server --data-dir /data --config` (control+worker, mesh
   enabled, domain test.local). Wait for bootstrap token in logs.
3. Create a join token via the API (client from the test, trusting the CA
   printed/copied from A's data dir).
4. Node B: `zattera server --join <A-ip>:8443 --token …`.
5. Assert: `ListNodes` shows 2 ALIVE nodes within 60s; exec `ping -c1
   10.90.0.1` from B's container succeeds (hub path).
   **Gotchas:** Docker Desktop runs containers in a VM — everything works but
   the binary must be linux/GOARCH-of-the-VM (detect via `docker version`);
   give raft/API time (retry loops, not sleeps); ALWAYS `t.Cleanup` the
   containers (`--rm` + explicit kill).
   **Tests:** this IS the test (`integration` tag).
   **Acceptance:** `go test -tags integration -run TestTwoNodeJoin
./test/integration/ -timeout 15m`

---

# Phase 3 — Scheduler & red/green deploys

**Exit criterion:** on a single node, `zattera deploy --image nginx:alpine
--env production` performs a health-gated red/green rollout;
`zattera rollback` restores the previous release in <5s; killing the fake
node in simcluster reschedules stateless replicas.

### T-23 — Scheduler evaluation loop ✅ **DONE**

Phase 3 · Depends: T-15 · Size: L
**Files:** `internal/daemon/scheduler/scheduler.go`, `scheduler_test.go`
**Steps:**

1. `scheduler.New(store *raftstore.Store, clock clock.Clock)` runs on the
   LEADER only (subscribe `LeaderCh`; stop cleanly on loss). Trigger:
   `state.Watch(KindEnvironment, KindRelease, KindDeployment, KindNode,
KindAssignment, KindVolume)` + 15s tick.
2. Evaluation (single-threaded, synchronous): for each environment with an
   `active_release_id` (plus green sets from in-flight deployments — see
   T-26): desired replica count = `effective_replicas` if >0 else
   `replicas.min`, 0 if env deleted/stopped. Diff desired vs live assignments
   (`state.ListAssignments(envID)` where desired=RUN and release matches).
3. Missing replicas → placement (T-24) → `PutAssignments` batch (ULID ids,
   desired=RUN, config_hash from release). Excess → flip to desired=STOP via
   PutAssignments (agent stops), then `DeleteAssignments` once observed
   STOPPED (a later evaluation collects them).
4. Assignments on DOWN nodes (status from T-21): stateless → replace
   immediately on another node (delete + place new); stateful → leave, mark
   volume NODE_LOST (T-62 refines).
5. Emit `AppendEvents` for placement failures ("no node with capacity").
   **Gotchas:** the loop must be idempotent and converge: never place more than
   (desired - live) in one pass; skip environments while a Deployment is in a
   non-terminal phase EXCEPT the phases that own placement (T-26 drives green
   placement through the same helper) — coordinate via
   `deployment.PhaseOwnsPlacement()`; treat `ErrNotLeader` from Apply as a
   signal to stop the loop (leadership lost mid-evaluation is normal); one
   evaluation must not block on agent convergence — it only writes desired
   state.
   **Tests:** unit (simcluster single node + fakeruntime agent executor wired,
   or scheduler against bare state): scale up 0→3, scale down 3→1, node DOWN →
   replacement assignments on live node, no double-placement across repeated
   evaluations.
   **Acceptance:** `go test ./internal/daemon/scheduler/ -run TestEvaluate`

### T-24 — Placement: filters + spread scoring ✅ **DONE**

Phase 3 · Depends: — · Size: M
**Files:** `internal/daemon/scheduler/placement.go`, `placement_test.go`
**Steps:**

1. `Place(st *state.Store, spec *zatterav1.ServiceSpec, envID string, n int,
exclude map[nodeID]bool) ([]nodeID, error)`.
2. Filters: node ALIVE + schedulable; `placement_constraints` labels all
   match; volume-pinned (stateful with volumes → ONLY the volume's node);
   capacity: sum of reserved cpu/mem of RUN assignments + this spec's
   resources ≤ node capacity (zero-valued resources reserve a default
   256MB/100m to avoid infinite stacking).
3. Scoring (per replica, re-scored after each pick): fewest replicas of THIS
   env on the node (spread), then per-`region` label spread, then most free
   memory. Deterministic tie-break by node id.
   **Gotchas:** must be a pure function over state (no I/O) so tests are
   table-driven; document that capacity uses RESERVATIONS not live usage;
   exclude arg lets red/green place green alongside blue but avoid double-
   placing on failed candidates.
   **Tests:** unit — table-driven: label filter, capacity exhaustion,
   spread across 3 nodes for 3 replicas, volume pinning, deterministic output.
   **Acceptance:** `go test ./internal/daemon/scheduler/ -run TestPlacement`

### T-25 — Deploy API: releases from image refs ✅ **DONE**

Phase 3 · Depends: T-06, T-09 · Size: M
**Files:** `internal/daemon/api/deploy.go`, `deploy_test.go`
**Steps:**

1. `DeployService.Deploy`: resolve env; build a Release (version =
   `state.NextReleaseVersion`, image_ref from request or completed build,
   frozen ServiceSpec copy from the Environment, config_hash via
   internal/appconfig) → `PutRelease`; create Deployment (phase PENDING,
   previous_release_id = env.active_release_id) → `PutDeployment`.
2. Reject a new deployment when one is already in a non-terminal phase for
   the env (supersede logic lives in T-26; the API just 409s —
   `codes.FailedPrecondition` — unless `--force` field added later).
3. `GetDeployment`, `ListDeployments`, `ListReleases`, `ListInstances`
   (assignments joined with env/app names), `WatchDeployment` (poll state on
   watch hub, push on phase change).
4. `Rollback`: validate target release exists (default: previous), create a
   Deployment with `is_rollback=true` → the orchestrator does the rest.
   **Gotchas:** the frozen ServiceSpec in the Release is THE contract the
   scheduler uses (env spec may change later); image-ref deploys skip builds
   entirely (BUILDING phase never entered).
   **Tests:** unit — deploy creates release v1, v2, …; concurrent deploy 409s;
   rollback targets previous.
   **Acceptance:** `go test ./internal/daemon/api/ -run TestDeploy`

### T-26 — Red/green Deployment orchestrator ✅ **DONE**

Phase 3 · Depends: T-23, T-16, T-25 · Size: L
**Files:** `internal/daemon/scheduler/deployment.go`, `deployment_test.go`
**Steps:**

1. Reconciler on the leader subscribed to KindDeployment + tick; big switch
   on phase, EVERY arm idempotent, EVERY transition one
   `SetDeploymentPhase`/`PromoteRelease` Apply (crash-safe resume):
   - `PENDING` → validate release/image → `PLACING`.
   - `PLACING`: ensure green assignments exist (deployment_id set on them;
     full set if capacity allows, else rolling batches of
     `max(1, replicas/3)`; use placement with exclude to sit beside blue) →
     when all created → `STARTING`.
   - `STARTING`: all green observed RUNNING/HEALTHY → `HEALTHCHECKING`;
     any FAILED → abort path.
   - `HEALTHCHECKING`: all green HEALTHY within grace (per-instance
     grace_period from spec; overall deadline = grace × 2 + 60s from phase
     entry, tracked via `meta.updated_at`) → `PROMOTING`; timeout/FAILED →
     abort.
   - `PROMOTING`: single `PromoteRelease` (bumps route generation —
     atomically shifts traffic) + `SetDeploymentPhase(DRAINING_OLD,
promoted_at=now, drain_deadline=now+10m)`.
   - `DRAINING_OLD`: after drain_deadline (Clock), flip blue assignments to
     STOP → `SUCCEEDED`.
   - Abort: stop+delete green assignments, `FAILED` with error; blue
     untouched (traffic never moved); emit event `deploy.failed`.
2. Supersede: newer non-terminal deployment for same env → older gets
   `SUPERSEDED` (its green set is reaped like an abort).
3. Rollback deployments: same machine; if within the previous deployment's
   drain window (old instances still warm) skip PLACING/STARTING/
   HEALTHCHECKING and promote immediately.
4. Stateful services NEVER enter this machine — route them to T-63's
   stop-then-start (assert here, `FAILED` with clear error until T-63 lands).
   **Gotchas:** resume-from-any-phase after leader failover is THE invariant —
   no in-memory-only progress; timeouts computed from state timestamps + Clock,
   never from local monotonic time; green assignments carry `deployment_id` so
   abort can find exactly its own; don't fight the T-23 evaluator: it must
   ignore envs with an active deployment except through PhaseOwnsPlacement.
   **Tests:** unit (fake clock + fakeruntime through simcluster single node):
   happy path phase walk; health failure → FAILED, blue untouched; rollback
   within window is instant; supersede; drain reaps blue after 10m.
   **Acceptance:** `go test ./internal/daemon/scheduler/ -run TestDeployment`

### T-27 — Environment/app deletion teardown ✅ **DONE**

Phase 3 · Depends: T-23 · Size: S
**Files:** `internal/daemon/scheduler/teardown.go`, `teardown_test.go`
**Steps:**

1. Evaluator handles orphan assignments: assignment whose environment or
   release no longer exists → STOP then delete (same two-step as scale-down).
2. Delete app/project cascades (T-05/T-06 already delete state objects) thus
   converge to zero containers.
   **Tests:** unit — delete env with 2 running assignments → both stopped and
   removed within two evaluations.
   **Acceptance:** `go test ./internal/daemon/scheduler/ -run TestTeardown`

### T-28 — CLI: deploy --image, ps, releases, rollback ✅ **DONE**

Phase 3 · Depends: T-25, T-10 · Size: M
**Files:** `internal/cli/deploy.go`, `ps.go`, `releases.go`, `rollback.go`
**Steps:**

1. `zattera deploy --image nginx:alpine [--env staging|--prod]`: resolve
   app from zattera.toml in cwd (or `--app`), call Deploy, then
   WatchDeployment rendering phase progress with a spinner
   (`✓ Released v42 → production (red/green, 2 replicas healthy)`); end with
   the URL line (`ui.URL`) — URL = first domain of the env or the cluster
   subdomain.
2. `zattera ps [--app]`: instances table (app, env, release, node, state,
   restarts).
3. `zattera releases [--env]`; `zattera rollback [--to vN] [--env]` with the
   same watch UX.
4. `--prod` maps to env `production`; default env = `staging` (spec §5).
   **Gotchas:** exit non-zero when the deployment ends FAILED/SUPERSEDED; the
   watch stream must survive leader failover (redial ×3); `--json` = stream of
   deployment objects, no spinner.
   **Tests:** unit — against the in-process server: deploy image → phases
   observed → success line printed (fakeruntime makes instances healthy? — wire
   the agent executor + health prober with fakeruntime in the test daemon
   harness; add `internal/daemon/testharness` helper if needed — document it).
   **Acceptance:** `go test ./internal/cli/ -run TestDeployCLI`

### T-29 — Node drain & remove ✅ **DONE**

Phase 3 · Depends: T-23 · Size: M
**Files:** `internal/daemon/api/nodes.go` (extend), `internal/cli/nodes.go`
(extend), `internal/daemon/scheduler/drain_test.go`
**Steps:**

1. `DrainNode`: `SetNodeStatus{DRAINING, schedulable=false}`; evaluator
   treats DRAINING like DOWN for placement (no new work) and migrates
   stateless replicas away (place replacement first, wait HEALTHY, then stop
   old — reuse deployment-style two-step inside the evaluator: place
   replacements with normal flow, drain check passes when the node has zero
   RUN assignments) → then `SetNodeStatus{DRAINED}`.
2. `RemoveNode`: only DRAINED (or `force`); `DeleteNode` + reap its
   assignments; if it was a control node, also `RemoveServer` from raft
   (guard: never remove the last control node / self without force).
3. CLI: `nodes drain <name>` (waits, progress), `nodes rm <name>`.
   **Gotchas:** stateful/pinned services on the node STOP by design (spec F25)
   — emit event + CLI warning listing them; drain must be resumable (leader
   failover mid-drain).
   **Tests:** unit — drain a 2-node simcluster-ish state: stateless moved,
   stateful stopped with event, node reaches DRAINED.
   **Acceptance:** `go test ./internal/daemon/scheduler/ -run TestDrain`

### T-30 — Chaos suite: scheduler + deployment invariants ✅ **DONE**

Phase 3 · Depends: T-26, T-29 · Size: M
**Files:** `test/chaos/deployment_test.go`, `test/chaos/scheduler_test.go`,
`test/chaos/harness.go`
**Steps:**

1. Harness: simcluster (3 control nodes) + scheduler+orchestrator running on
   whoever is leader (start them via LeaderCh on every node, as production
   does) + fakeruntime agents driven in-process.
2. Tests: kill leader in EVERY deployment phase → deployment still reaches a
   terminal phase with consistent end state (no orphan green assignments,
   traffic switched iff promoted); partition minority during PLACING; node
   kill mid-deploy → replicas land elsewhere.
3. Invariant checks after every scenario: at most one active release per env;
   every RUN assignment references an existing release+node; **never two RUN
   assignments of a stateful service** (fencing precheck — full lease logic
   in T-62).
   **Gotchas:** these tests are slow — keep under `chaos` tag; use generous
   retry-until deadlines (30s+) not sleeps; seed determinism: iterate phases in
   a table, one sub-test each.
   **Acceptance:** `go test -tags chaos ./test/chaos/ -run
'TestDeploymentFailover|TestSchedulerInvariants' -timeout 20m`

---

# Phase 4 — Builds & embedded registry (multi-arch)

**Exit criterion:** `zattera deploy` from `test/fixtures/apps/go-hello`
(tarball upload) builds via BuildKit **for every architecture present in the
cluster** (an OCI image index / multi-arch manifest list), pushes to the
embedded registry, and red/green-deploys the result onto nodes of any
supported architecture; a GitHub push does the same end-to-end
(webhook-simulated in tests). A mixed amd64/arm64 cluster can run the same
release, and the scheduler never places a workload on a node whose
architecture the release does not support.

**Multi-arch design (read once, applies across T-31..T-35, T-87, T-88):**

- Node architecture is `Node.os_arch` (`"linux/amd64"`, already in the proto),
  reported at boot and merged at join (T-87 makes this reliable).
- A `Release` carries the set of OCI platforms its image can run on
  (`Release.platforms`, added in T-87). Empty = unconstrained (legacy /
  "runs anywhere") so pre-existing single-arch flows keep working.
- Placement (T-88) filters candidate nodes to those whose `os_arch` is in the
  release's platform set. Docker's `EnsureImage` (T-15) then pulls the matching
  arch from the manifest list automatically — no agent change needed.
- Builds (T-33/T-35) target multiple platforms and push ONE image index; the
  registry (T-32) stores/serves image indexes and refcounts through them.

### T-31 — Registry: CAS blob store + OCI push protocol ✅ **DONE**

Phase 4 · Depends: T-02 · Size: M
**Files:** `internal/daemon/registry/blobstore.go`, `uploads.go`,
`httpapi.go`, `blobstore_test.go`, `uploads_test.go`
**Steps:**

1. Blob layout `<data-dir>/registry/blobs/sha256/<first2>/<digest>`; writes
   to `uploads/` tmp then digest-verify then atomic rename.
2. OCI push: `POST /v2/<name>/blobs/uploads/` → session id; `PATCH` appends
   (honor `Content-Range`, return `Range`); `PUT ?digest=` finalizes with
   verification; `HEAD /v2/<name>/blobs/<digest>`; `mount=` param → respond
   202 with a new upload session (spec-legal fallback).
3. `GET /v2/` → 200 `{}` (version probe). All errors in OCI error JSON
   (`{"errors":[{"code":"BLOB_UNKNOWN",…}]}`) — clients parse the codes.
4. Set `Docker-Content-Digest` and `Location` headers exactly per the
   distribution spec (docker push loops otherwise).
5. Upload-session janitor: expire after 24h (Clock).
   **Gotchas:** `<name>` may contain `/` (`myproject/api`) — wildcard routing,
   not single-segment; stream bodies (multi-GB), never buffer; digest
   verification is non-negotiable. Blob storage is content-addressed by digest,
   so identical layers shared across architectures of a multi-arch image (or
   across repos) are stored exactly once for free — do NOT key blobs by repo or
   platform. Manifests and image indexes are NOT uploaded through this blob path
   (they go to `/manifests/`, T-32); this task only moves config + layer blobs.
   **Tests:** unit — session state machine, chunked+monolithic upload, digest
   mismatch rejection, crash-safety (partial tmp file, restart, no corrupt
   blob).
   **Acceptance:** `go test ./internal/daemon/registry/`

### T-32 — Registry: manifests, tags, pull, auth, GC ✅ **DONE**

Phase 4 · Depends: T-31 · Size: M
**Files:** `internal/daemon/registry/manifests.go`, `auth.go`, `gc.go`,
`manifests_test.go`, `test/integration/registry_test.go`
**Steps:**

1. Manifests stored as blobs; tag→digest index + per-repo refcounts in a
   registry-local bbolt file (`registry/meta.db` — NOT raft). Manifest PUT
   validates every referenced blob exists (config + layers; accept both
   OCI and Docker v2 schema media types).
   **Multi-arch:** also accept image indexes / manifest lists
   (`application/vnd.oci.image.index.v1+json`,
   `application/vnd.docker.distribution.manifest.list.v2+json`). An index PUT
   validates that every child manifest it references already exists in this
   repo (one level deep — children are pushed before the index, per the OCI
   push order); store the child `platform` descriptors verbatim (needed by
   T-88's platform resolution, and by docker clients selecting an arch on
   pull). Reject an index whose child digest is missing (`MANIFEST_UNKNOWN`).
2. Pull: `GET/HEAD /v2/<name>/manifests/<tag|digest>` (set correct
   Content-Type from the stored media type — an index MUST come back as its
   index media type so clients then fetch the per-arch child by digest),
   `GET /v2/<name>/blobs/…` (http.ServeContent for ranges),
   `GET /v2/<name>/tags/list`. Expose a small helper
   `ResolveManifest(repo, ref, platform string) (digest, mediaType)` that,
   given an index, returns the child manifest digest for a platform (used by
   T-88 to learn a release's supported platforms without a docker client).
3. Auth middleware: basic auth — node creds (KV `registry/creds/<nodeID>`
   hashes, from T-17) and user PATs (`zpat_…` as password) both accepted.
4. Ref-counted GC: `DeleteManifest(repo, digest)` decrements layer refs,
   deletes zero-ref blobs. **For an index, recurse:** deleting the index
   decrements refs on each child manifest (and a child hitting zero refs
   cascades to its config+layers) — walk index → child manifests → blobs so a
   multi-arch tag frees all architectures. `gc.go` exposes
   `UntagAndSweep(repo, tag)` for T-38's retention hook.
5. Mount the whole handler on `:5000` TLS (CA server cert) in daemon wiring
   on control nodes.
   **Gotchas:** the SAME blob may be referenced by many manifests/repos —
   refcount at the (digest) level with a bbolt bucket, transactionally with
   manifest ops; HEAD responses need Content-Length but no body; docker clients
   send `Accept` lists — match media type or they fall back badly.
   **Tests:** unit — manifest PUT validation, refcount math, GC leaves shared
   layers; **index PUT rejects a missing child, index GET returns the index media
   type, `ResolveManifest` picks the right child per platform, GC of a multi-arch
   tag frees every architecture's blobs**. Integration — real `docker
buildx`/`docker push` + `docker pull` round-trip; push a two-platform
   (`linux/amd64,linux/arm64`) image index of go-hello against the registry over
   TLS and pull it back, asserting both child manifests resolve (add the CA to a
   dir-scoped DOCKER_CONFIG? Simpler: serve the integration registry on 127.0.0.1
   with plain HTTP behind a flag `RegistryConfig.InsecureHTTP` usable only in
   tests).
   **Acceptance:** `go test ./internal/daemon/registry/`;
   `go test -tags integration -run TestRegistryPushPull ./test/integration/`

### T-33 — Builder: managed buildkitd + Dockerfile builds ✅ **DONE**

Phase 4 · Depends: T-13, T-32 · Size: L
**Files:** `internal/daemon/builder/buildkit.go`, `dockerfile.go`,
`builder_test.go`, `test/integration/build_test.go`
**Steps:**

1. On `builder=true` nodes the agent ensures a `moby/buildkit:v0.x` (pin a
   digest) container: privileged, named `zt-system-buildkitd`, unix socket in
   `<data-dir>/buildkit/buildkitd.sock` via bind mount, CA bundle mounted at
   `/etc/ssl/certs/zattera-ca.pem` (so pushes to the registry verify).
2. `builder.Build(ctx, req RunBuildRequest, logs chan<- BuildEvent)`: unpack
   source (tarball from control URL or git clone with the installation
   token), `client.New(ctx, "unix://…")`, `Solve` with `dockerfile.v0`
   frontend, context via `llb.Local`/filesync from the source dir, exporter
   `image` with `push=true`, `name=<registry>/<project>/<app>:<build-id>`,
   registry auth via a session `auth.NewDockerAuthProvider` fed from the
   node's registry creds.
   **Multi-arch:** `req.Platforms` (e.g. `["linux/amd64","linux/arm64"]`)
   drives the build. Pass the `platform` frontend attr as a comma-joined list
   so the dockerfile frontend fans out per platform; the `image` exporter then
   emits an OCI image index (multi-arch manifest list) pushed under the single
   tag. One platform → a plain manifest (no index); the code path is uniform.
   Default when `req.Platforms` is empty: the builder node's own `os_arch`
   only (single-arch, current behavior).
3. Cross-arch emulation: building `linux/arm64` on an amd64 builder needs QEMU
   binfmt handlers. On builder nodes, once, ensure the
   `tonistiigi/binfmt:qemu-*` (pin a digest) install container has run
   (`--privileged`, writes `/proc/sys/fs/binfmt_misc`) before the first
   cross-arch solve; expose `EnsureEmulators(ctx, platforms)` that no-ops for
   the native platform and installs handlers for the rest. Emulated builds are
   slow but correct; native remote builders are backlog (M4).
4. Convert `SolveStatus` vertex logs → `BuildEvent{log}` lines; final digest
   from the exporter response (the INDEX digest when multi-arch) →
   `BuildEvent{status: SUCCEEDED, image_digest, platforms}`.
5. Agent-local `RunBuild` RPC (T-35 wires the server side; here expose the
   `Build` func).
   **Gotchas:** buildkitd needs time to boot — health-poll `client.Info` with
   backoff before first build; the registry hostname must be the CONTROL node's
   mesh IP (or 127.0.0.1 single-node) exactly as in the cert SANs; tarball paths
   must be sanitized (no `..` escapes — use `filepath.Clean` + prefix check when
   unpacking); cap build context size (512MB) and build duration (30m ctx, wider
   when emulating a second arch). Emulation traps: binfmt handlers must be
   registered with the `F` (fix-binary) flag so they survive across the buildkitd
   container boundary; a build for a platform without a working emulator must fail
   loudly (`EnsureEmulators` verifies registration), never silently build the
   wrong arch; the index digest (not a child digest) is what gets deployed.
   **Tests:** unit — tarball unpack sanitization, SolveStatus→log conversion
   with a recorded fixture, platform list → frontend attr encoding,
   `EnsureEmulators` skips the native arch. Integration — real single-arch build
   of go-hello via buildkitd container, image lands in a test registry instance,
   `docker run` of the result serves HTTP; a two-platform build produces an image
   index with both children present in the registry (running the emulated arch is
   not required in CI — assert the index, gate the actual arm64 `docker run`
   behind a `TestDockerfileBuildEmulated` name).
   **Acceptance:** `go test ./internal/daemon/builder/`;
   `go test -tags integration -run TestDockerfileBuild ./test/integration/`

### T-34 — Nixpacks build path ✅ **DONE**

Phase 4 · Depends: T-33 · Size: M
**Files:** `internal/daemon/builder/nixpacks.go`, `nixpacks_test.go`
**Steps:**

1. Run `ghcr.io/railwayapp/nixpacks:latest` (pin digest) via
   ContainerRuntime: source dir bind-mounted, command
   `nixpacks build /src --out /src/.nixpacks-out --name ignored` — it emits a
   Dockerfile + context under `.nixpacks-out`.
2. Feed the generated Dockerfile dir into the T-33 Dockerfile pipeline.
3. Stream the nixpacks container logs as BuildEvents (phase "plan").
4. `BUILD_TYPE_UNSPECIFIED` resolution: Dockerfile present → dockerfile,
   else nixpacks (implement here as `ResolveBuildType(dir)`).
   **Gotchas:** nixpacks needs network for its plan (package downloads happen in
   the BuildKit stage, fine); the generated Dockerfile references the build
   context relative to `.nixpacks-out` — pass THAT dir as context; delete the
   out dir between retries. Multi-arch is free here: nixpacks emits an ordinary
   Dockerfile that the T-33 pipeline builds for every requested platform — run
   the nixpacks planner container ONCE (its output is arch-independent), then let
   BuildKit fan out. Do not run the planner per platform.
   **Tests:** unit — ResolveBuildType matrix. Integration — node-hello fixture
   builds via nixpacks → runs.
   **Acceptance:** `go test -tags integration -run TestNixpacksBuild
./test/integration/`

### T-35 — Build pipeline: queue, dispatch, source upload, logs ✅ **DONE**

Phase 4 · Depends: T-33, T-25 · Size: L
**Files:** `internal/daemon/scheduler/builds.go`,
`internal/daemon/api/upload.go`, `internal/daemon/agent/buildserver.go`,
`builds_test.go`
**Steps:**

1. `UploadSource` (client-stream): spool tarball to
   `<data-dir>/uploads/<digest>` on the control node, create Build (QUEUED,
   with target `platforms`) + Deployment (PENDING with build_id) — return both.
   **Platform resolution:** `Build.platforms` = `BuildConfig.platforms` if the
   app declares them (zattera.toml `[build] platforms`), else the DISTINCT set
   of `os_arch` across schedulable ALIVE workers of the target env's eligible
   nodes (so a build covers exactly the cluster it will deploy to), else the
   control node's own arch. Cap the set (≤4) and validate each is a known OCI
   platform.
2. Build dispatcher (leader loop, watch KindBuild): QUEUED builds → pick a
   builder node (label `builder=true`, ALIVE; prefer least-busy from
   livestate; a builder must be able to serve every target platform natively
   OR via emulation — for v1 assume every builder can emulate, so any builder
   qualifies) → call its AgentLocalService.RunBuild over the mesh
   (`RunBuildRequest.platforms` carried through; source_url
   = `https://<control>:8443/internal/blobs/<digest>` served by a small
   authenticated handler; node-mTLS-only) → stream BuildEvents: logs →
   logstore under `build/<id>` (T-40 makes this durable; until then keep an
   in-memory ring on control), status transitions → PutBuild (record the built
   `platforms` on the Build).
3. Agent side (`buildserver.go`): implement the AgentLocalService server
   skeleton with RunBuild/CancelBuild wired to internal/daemon/builder;
   serve on `:8444` mTLS (this task creates the agent-local server; later
   tasks add methods).
4. Deployment orchestrator: PENDING with build_id waits in BUILDING until
   the build SUCCEEDED (then continues with image_ref = built INDEX digest ref
   AND copies `Build.platforms` onto the Release it creates/updates — this is
   what T-88's placement filters on) or FAILED (deployment FAILED).
5. GitHub-independent retry: build FAILED → stays failed (user redeploys);
   no auto-retry in v1.
   **Gotchas:** the tarball digest dedupes repeat uploads — but the CACHE KEY
   must include the target `platforms` (a rebuild for a new arch is a different
   output, don't serve a single-arch build for a two-arch request); stream
   backpressure: BuildEvents can be chatty — batch log lines (≤50/frame); a
   builder dying mid-build → dispatcher times out (no event for 60s) and marks
   FAILED with "builder lost"; single-node: control IS the builder (default label
   builder=true on bootstrap) and `platforms` defaults to its own arch, so the
   single-node path never emulates. `RunBuildRequest` and the success `BuildEvent`
   gain additive `platforms` fields (defined with the AgentLocalService protos in
   this task).
   **Tests:** unit — dispatcher with a fake AgentLocal server: queue→run→
   succeed; builder-lost timeout; deployment gating on build.
   **Acceptance:** `go test ./internal/daemon/scheduler/ -run TestBuilds`

### T-36 — CLI: deploy from source (the Vercel moment) ✅ **DONE**

Phase 4 · Depends: T-35, T-28 · Size: M
**Files:** `internal/cli/deploy.go` (extend), `internal/cli/tar.go`
**Steps:**

1. `zattera deploy [--prod]` without `--image`: apply zattera.toml (T-10's
   apply), tar.gz the cwd (respect `.gitignore` + `.zatteraignore`; exclude
   `.git`), stream via UploadSource (1MB chunks, header first), then attach
   to build logs (LogService Query follow on `build/<id>`) rendering lines
   dim, then the deployment watch from T-28.
2. Output exactly:
   `✓ Built api (nixpacks, 34s)` / `✓ Released v42 → production (red/green,
   2 replicas healthy)` / `● https://api.example.com`.
   **Gotchas:** honor context cancel (Ctrl-C aborts upload cleanly; build
   continues server-side — print the deployment id so users can re-attach);
   tar must set deterministic uid/gid=0 and strip xattrs (portability).
   **Tests:** unit — ignore-file handling, tar determinism; CLI E2E happens in
   T-54.
   **Acceptance:** `go test ./internal/cli/ -run TestDeploySource`

### T-37 — GitHub push-to-deploy ✅ **DONE**

Phase 4 · Depends: T-35 · Size: L
**Files:** `internal/daemon/github/webhook.go`, `app.go`, `webhook_test.go`,
`internal/daemon/api/githubroutes.go`, `internal/cli/github.go`
**Steps:**

1. Webhook endpoint `POST /v1/github/webhook` on the gateway mux (raw HTTP
   handler, not proto): verify `X-Hub-Signature-256` HMAC against the app's
   webhook secret (sealed in state); handle `push` (branch →
   `GitHubConfig.branch_environments` → env → create Build+Deployment with
   GitSource clone via installation token) and `ping`.
2. GitHub App auth (`app.go`): app private key sealed in KV
   (`github/app-key`); JWT → installation token (`ghinstallation` or
   hand-rolled JWT + POST, prefer `ghinstallation`); tokens cached until
   expiry-5m.
3. Builder git path: agent's RunBuild with GitSource does a shallow clone
   (`git` CLI inside a pinned `alpine/git` container via ContainerRuntime —
   NOT host git) at the pushed SHA.
4. Commit status updates: pending on build start, success/failure with the
   deployment URL.
5. CLI `zattera github connect --app <app> --repo owner/name` prints setup
   instructions (App installation URL, webhook URL + secret it generates).
   **Gotchas:** webhook must return 200 fast (<1s) — enqueue and process async;
   signature check with `hmac.Equal`; replay protection via delivery-id KV
   dedupe (TTL 1h); pushes to unmapped branches are silently ignored (log
   debug).
   **Tests:** unit — signature verify (recorded payload fixture), branch
   mapping, dedupe; integration optional (needs real GitHub — skip).
   **Acceptance:** `go test ./internal/daemon/github/`

### T-38 — Release retention → registry GC ✅ **DONE**

Phase 4 · Depends: T-32, T-26 · Size: S
**Files:** `internal/daemon/scheduler/retention.go`, `retention_test.go`
**Steps:**

1. Leader loop (hourly Clock): per environment keep the last 10 releases +
   anything referenced by active/in-drain deployments; older → delete
   Release + `UntagAndSweep` its image tag.
2. Uploaded tarballs older than 24h → delete.
   **Gotchas:** NEVER GC the active or previous (rollback-window) release; the
   registry sweep runs on control nodes that host blobs (call locally, not over
   the mesh).
   **Tests:** unit — retention keeps active/previous/last-10, sweeps the rest.
   **Acceptance:** `go test ./internal/daemon/scheduler/ -run TestRetention`

### T-87 — Multi-arch protos + reliable node arch reporting ✅ **DONE**

Phase 4 · Depends: — · Size: S
**Files:** `api/proto/zattera/v1/deploy.proto` (additive),
`api/proto/zattera/v1/app.proto` (additive), `api/gen` (regenerate),
`internal/daemon/api/nodes.go` (boot registration — extend, file owned by
T-12), `internal/daemon/api/join.go` (join label/arch merge — extend, owned by
T-17), `internal/appconfig/appconfig.go` (parse `[build] platforms` — extend,
owned by T-09), `internal/pkgutil/platform/platform.go` (new), tests.
**Steps:**

1. Proto (additive only — never renumber): `Release.platforms` =
   `repeated string platforms = 11;` (OCI platform strings the image runs on;
   empty = unconstrained/legacy). `Build.platforms = repeated string
platforms = 14;`. `BuildConfig.platforms = repeated string platforms = 5;`.
   `make generate` + commit `api/gen`.
2. `internal/pkgutil/platform`: tiny helpers — `Local() string` (=
   `runtime.GOOS + "/" + runtime.GOARCH`), `Normalize(s)` (lowercases,
   validates `os/arch`, maps common aliases `x86_64→amd64`, `aarch64→arm64`,
   `arm64/v8→arm64`), `Supports(nodeArch string, platforms []string) bool`
   (true when `platforms` is empty OR contains the node's arch).
3. Node boot registration (T-12's `PutNode` at daemon start): set
   `Node.os_arch = platform.Local()` — the ONE place that must always be
   right. Verify join (T-17) already merges the joining node's self-reported
   `os_arch` (it sends its own `platform.Local()` in the join request); if it
   currently only sets a label, set the field too.
4. appconfig: parse `[build] platforms = ["linux/amd64","linux/arm64"]` into
   `BuildConfig.platforms` (normalize each; unknown values = hard error, same
   style as other appconfig validation); absent = empty (cluster-arch default
   resolved later at build time, T-35).
   **Gotchas:** `os_arch` was previously best-effort (a label on some paths) —
   this task makes the FIELD authoritative; keep writing the label too for
   backward-compatible constraint matching, but scheduling reads the field
   (T-88). Snapshot round-trip already covers `Node`/`Release`/`BuildConfig`
   (existing fields) — no state-store change needed, only regenerated messages.
   Do NOT change `EnsureImage`/executor: Docker resolves the arch from the
   manifest list at pull time.
   **Tests:** unit — `platform.Normalize`/`Supports` table (aliases, empty =
   any); appconfig golden with a `platforms` list and a bad-value error; node
   boot registration sets `os_arch` (assert via state after daemon start helper).
   **Acceptance:** `make generate && git diff --exit-code api/gen` after commit;
   `go test ./internal/pkgutil/platform/ ./internal/appconfig/`

### T-88 — Arch-aware placement + release platform resolution ✅ **DONE**

Phase 4 · Depends: T-87, T-24, T-25, T-32 · Size: M
**Files:** `internal/daemon/scheduler/placement.go` (extend, owned by T-24),
`internal/daemon/scheduler/scheduler.go` (extend, owned by T-23),
`internal/daemon/api/deploy.go` (extend, owned by T-25),
`internal/daemon/scheduler/archplacement_test.go`
**Steps:**

1. Placement filter (`placement.go`): add a node filter
   `platform.Supports(node.os_arch, release.platforms)`. Thread the release's
   `platforms` into `Place` — either widen the signature to accept
   `platforms []string` or pass the `*Release`; keep it a pure function.
   A node whose arch is excluded is filtered exactly like an unschedulable
   node (never scored, never picked). If NO node satisfies the platform set,
   emit the same "no node with capacity" style event but with a distinct
   reason (`"no node with a supported architecture (need one of …)"`).
2. Scheduler wiring (`scheduler.go`): the evaluation loop resolves each env's
   active `Release` (it already loads it) and passes `release.platforms` into
   `Place`; green placement in the orchestrator (T-26) uses the deploying
   release's platforms. No behavior change when `platforms` is empty.
3. Release platform resolution at deploy (`deploy.go`, `DeployService.Deploy`):
   - built images: `Release.platforms` is copied from the Build (T-35 wires
     this; here just consume the field — for image-ref deploys the Build is
     absent).
   - image-ref deploys of an image in the EMBEDDED registry: call the
     registry's `ResolveManifest`/index reader (T-32) to read the child
     `platform` descriptors of the manifest/index → set `Release.platforms`.
   - image-ref deploys of an EXTERNAL image (docker hub, ghcr): best-effort
     HEAD/GET the manifest with `Accept: <index media types>` via a tiny
     `internal/daemon/registry/remoteref.go` helper (anonymous or configured
     creds); an index → collect child platforms; a single manifest → that
     one platform (read its config's `architecture`/`os`); on ANY error
     (private/unauthenticated/unreachable) → leave `platforms` empty
     (unconstrained) and emit a debug event — never fail the deploy over
     manifest inspection.
     **Gotchas:** empty `platforms` MUST remain "runs anywhere" so every release
     created before this task (and every image we can't inspect) keeps scheduling
     exactly as today — this is the backward-compat contract; the filter is
     additive tightening, never a new hard requirement. Do not inspect external
     registries on the hot path more than once per deploy (resolve at Deploy time,
     freeze into the Release — the scheduler never re-inspects). A stateful service
     pinned to a volume's node whose arch is unsupported by the release is a
     genuine conflict → surface it as a placement event, do not silently strand.
     **Tests:** unit — placement table: amd64-only release skips arm64 nodes and
     vice-versa, mixed-arch release spreads across both, empty platforms = no
     filter (regression), no-supported-arch → event + zero placements; deploy sets
     `Release.platforms` from a fake registry index reader; external-inspect
     failure → empty platforms, deploy still succeeds.
     **Acceptance:** `go test ./internal/daemon/scheduler/ -run 'TestPlacement|TestArchPlacement'`;
     `go test ./internal/daemon/api/ -run TestDeploy`

---

# Phase 5 — Traffic, TLS, internal DNS, logs, attach (M1 exit)

**Exit criterion:** the E2E smoke test (T-54) is green: source deploy →
HTTPS URL serves → red/green visible → rollback <5s → clean teardown.

### T-39 — Route builder + RouteStream ✅ **DONE**

Phase 5 · Depends: T-26 · Size: L
**Files:** `internal/daemon/scheduler/routes.go`,
`internal/daemon/api/routesvc.go`, `routes_test.go`
**Steps:**

1. Route builder on the leader: watch KindEnvironment/Domain/Assignment/
   Node/ServiceVIP (+ 15s tick), debounce 200ms, build ONE global
   `RouteSnapshot` (version = `state.Version()` at build time):
   - HTTPRoutes: every Domain (+ implicit cluster subdomain
     `<app>-<env>.<cfg.Domain>` when cfg.Domain set) → endpoints = assignments
     of the env's ACTIVE release that are HEALTHY (state RUNNING accepted
     during the first grace minutes — no: healthy only; activator handles
     zero), addr = `containerIP:port` for local… NO: addr must be reachable
     from ANY node → always `nodeMeshIP:meshPort` from mesh_port_bindings
     (127.0.0.1:port single-node). Node-local shortcut is an optimization the
     proxy applies via node_id match (Endpoint.node_id + the proxy knows its
     own).
   - L4Routes from PortSpec.public_l4_port; InternalServices from ServiceVIPs
     (per env: fqdn `<app>.<env>.<project>.internal.`, ports); cert_hosts =
     all route hostnames.
2. `RouteService.WatchRoutes` streams the current snapshot on connect
   (skip if `have_version` matches) + on every rebuild. Node-mTLS only.
3. Client side (`internal/daemon/proxy/routeclient.go` — include here): keeps
   the stream, persists each snapshot to `<data-dir>/proxy/routes.pb`
   (atomic write), loads it at startup BEFORE the first sync
   (quorum-loss autonomy, spec §7); implements `proxy.RouteSource`.
   **Gotchas:** the snapshot is global (same for every node) — build once,
   fan out to all streams; endpoints must exclude assignments desired=STOP;
   route_generation from the Environment gates blue/green: only endpoints of
   active_release_id appear (this IS the traffic switch); deleting the last
   route for a host must propagate (empty snapshot section, not absent stream
   push).
   **Tests:** unit — snapshot correctness across: promote (endpoints swap
   atomically), unhealthy endpoint dropped, domain added, node down;
   routeclient disk round-trip.
   **Acceptance:** `go test ./internal/daemon/scheduler/ -run TestRoutes`

### T-40 — Logstore: segments + follow ✅ **DONE**

Phase 5 · Depends: — · Size: L
**Files:** `internal/daemon/logstore/segmented.go`, `segment.go`,
`segmented_test.go`
**Steps:**

1. Per-stream dir `<data-dir>/logs/<stream>/`: active segment = plain
   append-only file of length-prefixed proto-ish records (varint len +
   time unixnano + flags + line bytes); rotate at 8MB or 1h → compress
   closed segment with zstd (`klauspost/compress`), write sparse index
   (every 64KB: timestamp→offset) as a sidecar.
2. `Append` (buffered writer, fsync-less — logs are best-effort), `Query`
   (binary-search segments by time range using index sidecars, decompress
   forward), `Follow` (Query history then subscribe to an in-memory fan-out
   hub of live appends), `DeleteStream`, retention janitor (size+age caps
   from config).
3. Wire the agent: container Logs channels (from T-15's executor per running
   assignment, `instance/<assignment-id>` streams) and BuildEvents
   (`build/<id>`) → Append. Serve `AgentLocalService.QueryLogs` from it.
   **Gotchas:** stream names come from LogSelector — sanitize into safe dir
   names (ULIDs already safe); the follow hub must drop slow subscribers
   (buffered chan, close on overflow with a "log stream lagged" marker line);
   rotation must be crash-safe (rename tmp on compress complete; on open, a
   leftover uncompressed closed segment is re-compressed).
   **Tests:** unit — append/query round trip across rotation+compression, time
   filtering, follow receives live lines, retention deletes oldest, crash-safe
   recovery (kill between rotate steps simulated by calling internals).
   **Acceptance:** `go test ./internal/daemon/logstore/`

### T-41 — Log fan-out + `zattera logs -f` ✅ **DONE**

Phase 5 · Depends: T-40, T-35 · Size: M
**Files:** `internal/daemon/api/logsvc.go`, `internal/cli/logs.go`,
`logsvc_test.go`
**Steps:**

1. `LogService.Query` on control: resolve the selector to (node, stream)
   pairs via assignments/builds/jobs in state; fan out
   `AgentLocalService.QueryLogs` over the mesh to each node (single-node:
   loopback); k-way merge by timestamp; stream `LogLine`s with app/env names
   filled from state.
2. Follow mode: keep all agent streams open; merge with a small reorder
   buffer (500ms window) — perfect ordering is not promised across nodes.
3. CLI `zattera logs [-f] [--env] [--since 10m] [app]`: colored per-instance
   prefixes (`api-production-1a2b │ line`), `--json` = raw LogLine JSON.
   **Gotchas:** a dead node must not hang the query (per-node dial timeout 3s,
   partial results + warning); heap-based merge must handle streams at
   different rates without unbounded buffering (bounded per-stream lookahead).
   **Tests:** unit — merge ordering with 3 fake agent streams, dead-node
   partial result, since/limit filters.
   **Acceptance:** `go test ./internal/daemon/api/ -run TestLogFanout`

### T-42 — L7 proxy core ✅ **DONE**

Phase 5 · Depends: T-39 · Size: L
**Files:** `internal/daemon/proxy/l7.go`, `lb.go`, `middleware.go`,
`l7_test.go`
**Steps:**

1. `proxy.L7{Source RouteSource}` — an `http.Handler`: match Host (strip
   port) → longest path_prefix → route; pick endpoint via P2C (`lb.go`:
   per-endpoint in-flight counters + healthy filter; prefer node-local
   endpoints when in-flight counts are equal); reverse-proxy via
   `httputil.ReverseProxy` with `Rewrite` (set X-Forwarded-\*), shared
   `http.Transport` with per-host connection pooling.
2. Middleware chain per route flags: HTTPS redirect (from the :80 listener),
   gzip/brotli (skip when Content-Encoding set or websocket), basic auth,
   IP allowlist (parse CIDRs once per snapshot), max body bytes
   (`http.MaxBytesReader`).
3. Listeners: `:80` (ACME HTTP-01 handler mount point T-44 + redirect +
   plain routes) and `:443` TLS (config from T-44). Start on every node
   unless `cfg.Ingress.Disabled`.
4. Metrics counters per env (rps, errors, latency histogram, in-flight) in a
   `proxystats` struct — heartbeat reads it (T-14's ProxySample).
5. WebSocket/HTTP2 pass-through (ReverseProxy handles both; test WS).
   **Gotchas:** 502 with a JSON error body when no healthy endpoint (unless
   scale_to_zero → T-71 activator hooks here — leave the hook point
   `activate func(envID)` nil for now); copy the route pointer once per request
   (snapshot swaps must not race — atomic.Pointer[RouteSnapshot] in the
   source client); in-flight decrement in defer (panics must not leak counters).
   **Tests:** unit — httptest backends: host+path routing, P2C balance
   (rough distribution), health filtering, middleware matrix (auth 401,
   allowlist 403, redirect 308, gzip), websocket echo.
   **Acceptance:** `go test ./internal/daemon/proxy/ -run TestL7`

### T-42-bis — Sticky sessions (cookie affinity) ✅ **DONE**

Phase 5 · Depends: T-42 · Size: S
**Files:** `internal/daemon/proxy/sticky.go`, `internal/daemon/proxy/l7.go`
(extend endpoint selection), `sticky_test.go`
**Steps:**

1. When a route's `Middleware.sticky_sessions` is set, pin a client to one
   endpoint via a cookie (`zt_sticky`): the value is an opaque, stable id per
   endpoint (`stickyID` = fnv32a hex of the endpoint's assignment id, falling
   back to its addr). Extract endpoint selection into `L7.selectEndpoint`:
   - sticky + request carries a `zt_sticky` cookie whose id matches a **healthy**
     current endpoint → reuse it (no P2C, no new cookie).
   - otherwise pick via P2C, and (when sticky) return the chosen endpoint's
     `stickyID` so the handler sets the cookie.
2. Set `Set-Cookie: zt_sticky=<id>; Path=/; HttpOnly; SameSite=Lax` (and
   `Secure` when the request arrived over TLS) BEFORE reverse-proxying, only
   when there is no matching cookie yet (a pinned request re-sends its own).
   **Gotchas:** the sticky target must be re-validated against the CURRENT
   snapshot's endpoints every request — a drained/removed/unhealthy replica falls
   back to P2C and re-pins; never trust the cookie to name an endpoint that is no
   longer in the route; keep the cookie opaque (no raw addr) so it does not leak
   internal topology; non-sticky routes must set no cookie.
   **Tests:** unit — a sticky route pins repeated requests (carrying the returned
   cookie) to the same backend while a non-sticky route spreads; a request whose
   cookie names a now-unhealthy endpoint fails over and re-pins; no `Set-Cookie`
   on non-sticky routes.
   **Acceptance:** `go test ./internal/daemon/proxy/ -run TestSticky`

### T-43 — L4 TCP proxy ✅ **DONE**

Phase 5 · Depends: T-39 · Size: M
**Files:** `internal/daemon/proxy/l4.go`, `l4_test.go`
**Steps:**

1. Reconcile listeners to the snapshot's L4Routes: one `net.Listener` per
   public_port; on accept, pick endpoint (P2C by in-flight), dial with 5s
   timeout, splice both directions (`io.Copy` ×2, close on either EOF),
   count in-flight per endpoint.
2. Half-close correctly (`CloseWrite` on TCPConn when one side EOFs).
   **Gotchas:** port conflicts with platform listeners (80/443/8443/5000) —
   validate at route build time (T-39 emits an event, skips the route);
   listener leaks on snapshot changes (close removed ports promptly, drain
   established conns — do NOT kill active connections on unrelated snapshot
   churn).
   **Tests:** unit — TCP echo through the proxy, balance across 2 backends,
   port add/remove on snapshot swap without dropping the untouched port's
   connections.
   **Acceptance:** `go test ./internal/daemon/proxy/ -run TestL4`

### T-44 — ACME via certmagic + raft storage ✅ **DONE**

Phase 5 · Depends: T-39, T-42 · Size: L
**Files:** `internal/daemon/tlsmgr/tlsmgr.go`, `storage.go`,
`storage_test.go`, `tlsmgr_test.go`
**Steps:**

1. `storage.go`: implement `certmagic.Storage` over the raft KV
   (`certmagic/` prefix): Store/Load/Delete/Exists/List/Stat via
   PutKV/DeleteKV/KV/ListKVPrefix through `applyAnywhere`; Lock/Unlock via
   CAS PutKV with TTL (2m) + poll-retry, per the certmagic contract.
2. `tlsmgr.go`: certmagic config — on-demand issuance with a decision func
   (`hostname ∈ RouteSource.Current().cert_hosts`); HTTP-01 solver mounted
   on the :80 mux (T-42 exposes the mount point); email/staging/disabled
   from config. Dev mode: self-signed CA-issued certs for every hostname on
   demand (no ACME dialing).
3. `GetTLSConfig()` → \*tls.Config with GetCertificate from certmagic; the
   :443 listener consumes it. Only ONE cluster-wide issuer needed — locks
   serialize across proxies through the raft KV.
4. Renewal: certmagic handles it (30d default matches spec).
   **Gotchas:** on-demand MUST have the decision function or you become an
   open cert factory; storage List must support the `recursive` flag semantics
   correctly (certmagic breaks subtly otherwise — copy semantics from
   certmagic's filestorage docs); clock skew: lock TTL entries carry
   `expires_at` — expired locks are stealable (CAS on version).
   **Tests:** unit — storage contract round-trips + lock contention (two
   goroutines, one wins, steal after expiry); tlsmgr with certmagic's internal
   self-signed test CA is overkill — test the decision func and dev-mode cert
   issuance path.
   **Acceptance:** `go test ./internal/daemon/tlsmgr/`

### T-45 — DomainService + cluster subdomains ✅ **DONE**

Phase 5 · Depends: T-39, T-44 · Size: S
**Files:** `internal/daemon/api/domains.go`, `internal/cli/domains.go`,
`domains_test.go`
**Steps:**

1. AddDomain (validate hostname RFC-952ish, unique), ListDomains,
   RemoveDomain, SetMiddleware → commands; cert_status updated by tlsmgr
   events (PENDING→ISSUED via a callback that proposes PutDomain — best
   effort).
2. Route builder already emits implicit cluster subdomains; make AddDomain
   reject hostnames colliding with them.
3. CLI: `domains add api.example.com --app api --env production`,
   `domains ls`, `domains rm`.
   **Tests:** unit — CRUD + collision + middleware set.
   **Acceptance:** `go test ./internal/daemon/api/ -run TestDomains`

### T-46 — Per-(project,env) networks + subnet allocation ✅ **DONE**

Phase 5 · Depends: T-15 · Size: M
**Files:** `internal/daemon/agent/networks.go`, `networks_test.go`; extend
executor
**Steps:**

1. Control allocates subnets: KV-free — `PutNetworkAllocation` from a leader
   helper `AllocateSubnet(projectID, envID, nodeID)` scanning existing
   allocations for the next free `10.201.X.0/24` (X = 0..255 wraps to /16
   pool exhaustion error). Called by the scheduler when placing the first
   assignment of an env on a node.
2. Agent executor: before creating a container, `EnsureNetwork` name
   `zt-<project[:8]>-<env[:8]>-<envID[:6]>` with the allocated subnet; attach
   the container; DNS = the network's gateway IP (resolver binds there,
   T-47).
3. Teardown: last assignment of an env on the node removed → RemoveNetwork +
   `PutNetworkAllocation("")` (control-side janitor in the scheduler).
   **Gotchas:** Docker network names are node-local but subnets are
   CLUSTER-unique (a container's IP must be routable over the mesh later —
   actually cross-node traffic flows via published mesh ports, but unique
   subnets prevent route ambiguity on multi-role nodes; keep unique); network
   create races (two containers same env) — EnsureNetwork is idempotent by
   inspect-first.
   **Tests:** unit — allocation uniqueness/reuse-after-free; executor wires
   network+DNS into the ContainerSpec (fakeruntime asserts).
   **Acceptance:** `go test ./internal/daemon/agent/ -run TestNetworks`

### T-47 — Internal DNS resolver (F26) ✅ **DONE**

Phase 5 · Depends: T-39, T-46 · Size: L
**Files:** `internal/daemon/intdns/resolver.go`, `resolver_test.go`
**Steps:**

1. `miekg/dns` servers bound per gateway IP:53/udp+tcp of every zt-\* network
   on the node (reconciled as networks come and go); the LISTENER address
   determines the (project, env) scope.
2. `*.internal.` queries: `<svc>.<env>.<project>.internal` — resolve ONLY if
   (project, env) matches the listener scope; answer = the service VIP (A
   record, TTL 5) from the RouteSnapshot's InternalServices. NXDOMAIN for
   other projects (isolation), even if they exist.
3. Everything else: forward to upstreams from /etc/resolv.conf (skip
   127.0.0.x loops), 2s timeout, else SERVFAIL.
4. Also answer `<svc>.internal` shorthand within the same env.
   **Gotchas:** binding :53 on the bridge gateway requires the daemon to run as
   root (documented); Docker's embedded DNS is bypassed because containers get
   `--dns <gateway>` (T-46) — do NOT bind 127.0.0.11; case-insensitivity and
   trailing-dot handling in name matching; refuse recursion for .internal
   (authoritative).
   **Tests:** unit — scoped resolution matrix (own env yes, other project
   NXDOMAIN, staging≠production), forwarding fallback with a fake upstream,
   shorthand.
   **Acceptance:** `go test ./internal/daemon/intdns/`

### T-48 — VIP L4 proxy (internal service traffic) ✅ **DONE**

Phase 5 · Depends: T-47, T-43 · Size: M
**Files:** `internal/daemon/intdns/vipproxy.go`, `vipproxy_test.go`;
control-side VIP allocation in `internal/daemon/scheduler/vips.go`
**Steps:**

1. Control: allocate a VIP per environment (`PutServiceVIP`, next free in
   `10.97.0.0/16`) when its first service port appears.
2. Node agent (linux): ensure VIPs exist on a dummy interface `zt-vip`
   (`vishvananda/netlink`, guarded by build tag + runtime OS check); listen
   `VIP:port` per InternalPort, L4-splice to endpoints (reuse T-43's splice +
   P2C; local replicas → containerIP:port, remote → meshIP:published).
3. Reconcile on every snapshot: add/remove VIP addrs and listeners.
   **Gotchas:** binding a VIP requires the addr on an interface FIRST (netlink
   add, then listen); non-linux dev: compile but no-op with a warning; UDP
   ports: v1 = TCP only, log-and-skip UDP InternalPorts (documented limitation).
   **Tests:** unit — reconcile logic against fake netlink/listener interfaces
   (wrap netlink ops in a tiny interface for the test); TCP splice reused from
   T-43 is already tested.
   **Acceptance:** `go test ./internal/daemon/intdns/ -run TestVIP`

### T-49 — Exec/attach, top, fs, port-forward ✅ **DONE**

Phase 5 · Depends: T-35 (agent server), T-13 · Size: L
**Files:** `internal/daemon/api/execsvc.go`,
`internal/daemon/agent/execserver.go`, `internal/cli/attach.go`,
`internal/cli/portforward.go`, `execsvc_test.go`
**Steps:**

1. Agent side: implement AgentLocalService Exec (bidi ↔
   `runtime.Exec` with pipes + resize), Top, ProxyTCP (first frame carries
   dial_addr).
2. Control `ExecService`: resolve instance → node → proxy the bidi stream
   over the mesh to the agent (pure byte relay with a goroutine per
   direction, propagate stream close both ways).
3. `zattera attach <app> [--env] [-- cmd…]`: pick a healthy instance (or
   `--instance`), raw-mode terminal (`golang.org/x/term`), resize on
   SIGWINCH, restore terminal on exit — ALWAYS (defer + signal handling).
4. `zattera top <app>`; `zattera fs ls <app>:<path>` (VolumeService.ListFiles
   naming? NO — container fs: route via CopyFrom-based listing: implement
   `fs ls/cat` over Exec running `ls -la`/`cat`? Decision: use docker's
   archive stat — expose `AgentLocalService.ListVolumeFiles` for volumes
   only; container fs inspection = `zattera fs ls` runs `ls -1ap` via Exec
   (simple, portable). Document that fs is exec-based.)
5. `zattera port-forward <app> <localPort>[:<portName>]`: local listener,
   each conn → ExecService.PortForward → agent ProxyTCP → container.
   **Gotchas:** terminal restore on panic (defer in main path); backpressure on
   the double relay (bounded copies, no unbounded buffering); Exec on
   grpc-gateway is NOT supported (gRPC-only — CLI uses gRPC anyway).
   **Tests:** unit — relay plumbing with an in-process agent+control and
   fakeruntime Exec (echo bytes both ways, exit code propagation); port-forward
   round trip against a local TCP echo behind the fake.
   **Acceptance:** `go test ./internal/daemon/api/ -run TestExec`

### T-50 — Env var injection + config-hash redeploys ✅ **DONE**

Phase 5 · Depends: T-15, T-06 · Size: S
**Files:** `internal/daemon/api/agentsync.go` (extend), `internal/daemon/scheduler/scheduler.go` (extend)
**Steps:**

1. AssignmentSet frames: fill per-assignment `env` map (decrypt sealed vars
   with the Sealer; add `PORT=<first port>`, `ZATTERA_ENV`, `ZATTERA_APP`).
2. Env var change bumps a per-env `var_version` (KV counter) folded into
   config_hash → the NEXT deploy picks it up; document that env changes need
   a deploy (v1 semantics; no hot restart).
   **Tests:** unit — sealed value decrypts into the frame; hash changes when a
   var changes.
   **Acceptance:** `go test ./internal/daemon/api/ -run TestEnvInjection`

### T-51 — `zattera stats` minimal (live from heartbeats) ✅ **DONE**

Phase 5 · Depends: T-14 · Size: S
**Files:** `internal/daemon/api/metricssvc.go`, `internal/cli/stats.go`
**Steps:**

1. `MetricsService.Stats` v1: serve ONLY current values from livestate
   (node cpu/mem/disk, env rps/inflight) — the historical TSDB lands in
   T-59/T-60; return single-point Series so the CLI/API shape is stable.
2. CLI `zattera stats [--nodes|--app]` table.
   **Acceptance:** `go test ./internal/daemon/api/ -run TestStatsLive`

### T-52 — Dev-mode polish for single node ✅ **DONE**

Phase 5 · Depends: T-44, T-42 · Size: S
**Files:** `internal/daemon/daemon.go` (extend), `internal/daemon/devmode.go`
**Steps:**

1. `--dev`: default cluster domain `apps.127.0.0.1.sslip.io` when unset;
   HTTP on `:8080` and HTTPS on `:8443`? NO — API is 8443. Dev listeners:
   http `:8080`, https `:8444`? Conflicts with agent-local. Decision: dev
   ingress http `:8080`, https `:9443`; print all effective URLs at boot.
2. Print a friendly startup block: API endpoint, ingress ports, bootstrap
   token (first boot), CA path — everything the smoke test needs to parse.
   **Gotchas:** keep the printed format stable — T-54 parses it (define
   `DEVBANNER:` prefixed machine-readable lines alongside the pretty block).
   **Acceptance:** manual boot + updated unit snapshot of the banner.

### T-53 — Jobs: one-shot runs (M1 subset) ✅ **DONE**

Phase 5 · Depends: T-23, T-40 · Size: M
**Files:** `internal/daemon/scheduler/jobs.go`, `internal/daemon/api/jobs.go`,
`internal/cli/jobs.go`, `jobs_test.go`
**Steps:**

1. `RunJob` → PutJob(QUEUED); scheduler loop: QUEUED job → assignment with
   `job_id` set, image from the env's active release, command override,
   restart=Never; agent runs it, reports exit; scheduler marks
   SUCCEEDED/FAILED (retries with backoff up to max_retries — re-place, new
   attempt counter).
2. Job logs → `job/<id>` stream; `GetJob/ListJobs/CancelJob` (cancel = stop
   assignment).
3. CLI `zattera jobs run [--env] -- <cmd…>` (waits, streams logs, exits with
   the job's code), `jobs ls`.
   **Gotchas:** job assignments must NOT be picked up by the service replica
   diff in T-23 (filter `job_id == ""` there — go back and assert that filter
   exists, add if missing); exit code propagation through
   AssignmentObserved.exit_code.
   **Tests:** unit — run→succeed, fail→retry×N→FAILED, cancel; evaluator
   ignores job assignments for replica math.
   **Acceptance:** `go test ./internal/daemon/scheduler/ -run TestJobs`

### T-54 — E2E smoke test (M1 exit) ✅ **DONE**

Phase 5 · Depends: ALL P1–P5 · Size: L
**Files:** `test/e2e/smoke_test.go`, `test/e2e/harness.go`, Makefile (verify
`test-e2e` wiring)
**Steps:** (linux CI runner or privileged container locally)

1. Build the binary; start `zattera server --dev --data-dir $TMP --domain
apps.127.0.0.1.sslip.io`; parse DEVBANNER lines for token/ports/CA.
2. `login` (CLI binary, not in-process) → `projects create smoke` → cd
   fixture go-hello → `zattera init --name hello` → `zattera deploy --prod`.
3. Assert stdout contains `Built hello`, `Released v1`, and the URL; poll
   `http://hello-production.apps.127.0.0.1.sslip.io:8080/` (Host routing)
   until the fixture body; assert HTTPS on the dev port with the CA.
4. `zattera logs hello --since 5m` contains the fixture startup line;
   `zattera ps` shows 1 healthy replica.
5. Modify fixture env (`zattera env set FIXTURE_MESSAGE=v2 --env production`)
   - `zattera deploy --prod` (image rebuild not needed — env-only redeploy);
     assert old body until healthy, then new body. `zattera rollback` → old
     body within 5s.
6. `zattera jobs run -- echo done` exits 0.
7. Teardown: SIGTERM daemon; assert no `dev.zattera/managed=true` containers
   remain (`docker ps -a` filter) except none.
   **Gotchas:** sslip.io needs outbound DNS — in CI it resolves; add /etc/hosts
   fallback documentation; every wait is a poll with deadline (≥120s for the
   first build — buildkit cold start + npm-less go build).
   **Acceptance:** `make test-e2e` green on Linux; this closes M1.

---

# Phase 5.1 — Production ingress + TLS (deferred M1 wiring)

**Context:** T-42/T-43/T-44 built the L7/L4 proxy cores, the certmagic ACME
issuer and the raft-backed cert storage; T-39 built the RouteClient. T-54
wired only the _dev_ ingress (in-process RouteBuilder + self-signed CA). These
two tasks complete the _production_ daemon wiring so a non-dev node actually
serves apps on `:80`/`:443` with real certificates, and so the CLI no longer
needs the cluster CA out-of-band.

### T-89 — Production ingress listeners (`:80`/`:443` + ACME) ✅ **DONE**

Phase 5.1 · Depends: T-39, T-42, T-43, T-44 · Size: M
**Files:** `internal/daemon/ingresswiring.go` (extend),
`internal/daemon/daemon.go` (extend), `ingresswiring_test.go`
**Steps:**

1. Generalize `startIngress` to a production mode: source = a `proxy.RouteClient`
   (dials `RouteService` over node mTLS, disk-cached, T-39) instead of the
   in-process RouteBuilder; TLS = `tlsmgr.New` with ACME (raft storage + the
   on-demand DecisionFunc gated on known route hostnames, T-44) instead of the
   dev CA; keep the HTTPS→ HTTPS redirect ON (leave `DisableHTTPSRedirect=false`).
2. Start the L7 handler on `cfg.Ingress.HTTPSListen` (`:443`) under
   `tm.GetTLSConfig()`, and on `cfg.Ingress.HTTPListen` (`:80`) wrapped in
   `tm.HTTP01Handler(l7)` so the ACME HTTP-01 challenge + plaintext redirect
   share the port. Also start the L4 proxy (`proxy.NewL4`) for `public_l4_port`
   passthrough. Skip all of it when `cfg.Ingress.Disabled`.
3. In `daemon.go`, run production ingress on every node (control or worker) that
   is not `--dev` and not `Ingress.Disabled`; dev keeps its existing path. Wire
   the RouteClient dialer over the node's own API/mesh identity, and the CertHost
   source from the current RouteSnapshot's hostnames.
   **Gotchas:** ACME needs `:80`/`:443` publicly reachable + real DNS — cannot be
   verified in CI; unit-test the wiring (listener construction, source/TLS
   selection by mode) with fakes, and gate the ACME dial behind reachability.
   Port conflicts with API/registry — document. Only ONE cluster-wide ACME issuer
   (locks via the raft storage, already in T-44).
   **Tests:** unit — production `startIngress` selects RouteClient + ACME TLS and
   binds both listeners against an injected fake; dev path unchanged.
   **Acceptance:** `go test ./internal/daemon/ -run TestIngress`; manual: a public
   node serves `https://<app>-<env>.<domain>/` with a Let's Encrypt cert.

### T-90 — Public API TLS: ACME for the API + CLI CA trust-on-first-use ✅ **DONE**

Phase 5.1 · Depends: T-44, T-17 · Size: M
**Files:** `internal/daemon/api/server.go` (extend),
`internal/daemon/daemon.go` (extend), `internal/cli/cli.go` (login extend),
`internal/cli/cliconfig/cliconfig.go`, `server_test.go`
**Steps:**

1. API server cert: when a public `api.advertise_addr` hostname + ACME are
   configured (not dev, ACME not disabled), serve the API TLS listener with a
   certificate from the shared `tlsmgr.Manager` (ACME, on-demand for the API
   hostname) instead of the self-signed CA cert. Fall back to the CA server cert
   when ACME is off/unreachable or the endpoint is an IP/loopback.
2. CLI trust-on-first-use: extend `zattera login` with `--ca-pin <sha256>` (and
   a bannered fingerprint at boot). When neither `--ca-cert` nor a public/ACME
   cert is available, dial once with `InsecureSkipVerify`, verify the presented
   chain's CA hash equals the pin (mirrors the T-17 join `caPinCreds`), then
   persist the fetched CA PEM into the CLI context. `--ca-cert` and a
   publicly-trusted cert still work with no pin.
3. Print the CA fingerprint on the dev/first-boot banner so `--ca-pin` is
   copy-pasteable.
   **Gotchas:** the gateway dials the API over loopback — that hop keeps the CA
   cert (don't ACME the loopback SAN). ACME for the API needs the same public
   reachability caveat as T-89.
   **Tests:** unit — login pins + persists the CA from a self-signed server
   (fake), rejects a hash mismatch; API server selects ACME vs CA cert by config.
   **Acceptance:** `go test ./internal/daemon/api/ -run TestServerACME` and
   `go test ./internal/cli/ -run TestLoginPin`.

---

# Phase 6 — M2: HA, mesh phases C/D, metrics, autoscaler, volumes, backup, cron

**Exit criterion:** 3-node control plane survives leader kill with zero
workload impact (chaos suite); volumes snapshot/restore through MinIO;
`zatterad restore` recreates a cluster from S3 on a fresh data dir; cron
jobs fire.

### T-55 — Multi-control-node HA ✅ **DONE** (raft HA core; daemon bring-up → T-55b; mesh HA → T-55c)

Phase 6 · Depends: T-17, T-08 · Size: L
**Done:** the raft HA core — `raftTLSStreamLayer` mTLS transport
(`internal/daemon/raftstore/transport.go`), idempotent `AddVoter`/`RemoveServer`
(GetConfiguration check), `leaderrunner.Run` helper with all leader loops
refactored onto it, `JoinResponse` control-handover fields (roles, data_key,
data_key_version, ca_key_pem, raft_bind_addr) + leader-side `handoverControl`,
`CA.PrivateKeyPEM`, completed control-node removal (raft leave before record
delete). Acceptance test `TestHA` (grow via AddVoter / failover / remove) is
green over real mTLS TCP transports.
**Deferred to T-55b:** the node-side daemon bring-up (a `--control` join
actually starting its own raft + control stack) is blocked on multi-control mesh
addressing (see `meshwiring.go`: mesh is single-hub `10.90.0.1` today). Until
T-55b, a control-role join persists its handover material and runs as a worker
with a clear warning; the leader does NOT auto-AddVoter (would strand a voter
with no live peer).
**Files:** `internal/daemon/api/join.go` (extend), `internal/daemon/daemon.go`
(extend), `test/chaos/ha_test.go`
**Steps:**

1. Join with role `control`: after the normal join, the leader calls
   `AddVoter(nodeID, meshIP:7480)`; the new node starts raft with
   `Bootstrap=false` and the received config; raft transport binds the mesh
   IP with TLS (wrap `raft.NewTCPTransport` in the CA's mTLS via
   `StreamLayer` — implement `raftTLSStreamLayer`).
2. Cluster-key handover: the join response for control nodes includes the
   sealed… NO — plaintext data key travels in the mTLS join response
   (`JoinResponse` additive field `data_key`); document why (auto-unseal
   within a live cluster, spec design §2.10).
3. Leader-only loops (scheduler, orchestrator, dispatcher, janitors) already
   gate on LeaderCh — verify each starts/stops on transitions (add a
   `leaderrunner.Run(store, func(ctx))` helper and refactor callers onto it).
4. Control-node removal: `nodes rm` on a control node → RemoveServer (T-29
   already stubs this — complete it).
   **Gotchas:** raft TLS stream layer must use the NODE cert and verify peer
   URI SANs; AddVoter before the new node's raft is listening → retry window;
   NEVER AddVoter twice for the same id+addr (idempotent check via
   `GetConfiguration`).
   **Tests:** chaos — 3 in-process daemons (real raft TCP on loopback ports,
   mesh disabled): kill leader → new leader elected, API writes keep working
   via leader-forward; remove a follower cleanly.
   **Acceptance:** `go test -tags chaos ./test/chaos/ -run TestHA -timeout 15m`

### T-55b — Daemon join-as-control bring-up ✅ **DONE** (mesh HA completed by T-55c)

Phase 6 · Depends: T-55 · Size: L
**Done:** the daemon path is wired end to end. `runControlPlane` is factored out
of `Run` and shared by the bootstrap and joined paths. `runJoinedControl`
(`daemon.go`) brings a `--control` join up as a full member: mesh spoke →
persist handover CA (`persistHandoverCA`) → `Keyring`/`Sealer` from `data_key` →
raft `Bootstrap=false` on `raftstore.NewTLSTransport` bound to `raft_bind_addr` →
wait for enrollment → run the control stack. Enrollment is safe: the leader
(`api.enrollControlVoter`) probes the joining node's raft address for
reachability BEFORE the idempotent `AddVoter`, so it never strands an
unreachable voter. Unit test `TestEnrollControlVoter`; chaos `TestControlJoin`
(dynamically-joined nodes replicate and one takes over when the bootstrap leader
is killed).
**The three remaining items were completed by T-55c:** joined control nodes come
up as real WireGuard hubs (not spokes), worker↔hub routing fails over between
control nodes, and a joined control+worker uses its join-issued registry
credential. See T-55c for the design and cloud verification.
**Gotchas:** the root CA private key now lives on every control node (chosen
handover design) — treat it as a cluster secret; a joined follower serves its
own API cert from the handover CA; `registerLocalNode` is already done by the
leader's Join handler (`runControlPlane` does not re-register).

### T-55c — Multi-hub mesh + hub/control failover ✅ **DONE**

Phase 6 · Depends: T-55b, T-56 · Size: L
Completes HA on the data plane: makes every control node a WireGuard hub and
gives workers failover across hubs AND across control-plane endpoints, so the
cluster keeps serving through the loss of any control node — not just raft.
**Done:**

1. **Every control node is a hub.** `bringUpControlHub` (`meshwiring.go`) replaces
   `bringUpControlMeshDevice`/`startWorkerMesh` for control nodes — it brings the
   device up on the node's OWN mesh IP (bootstrap `10.90.0.1`, joined `10.90.0.x`),
   enables IP forwarding, and installs the initial control `/32` peers. Used by
   both the bootstrap (`Run`) and joined (`runJoinedControl`) paths. `runJoinedControl`
   no longer comes up as a spoke.
2. **Health-driven hub selection + failover.** `activeHubID` (`meshsvc.go`) picks
   the ALIVE control node with the lowest mesh IP as the hub a worker routes the
   `/16` through; the other control nodes get a warm-standby `/32` with keepalive.
   When the active hub is marked DOWN (gossip/heartbeat → `SetNodeStatus`), the
   `KindNode` change re-pushes every worker a peer set with the `/16` re-pointed to
   the next live hub. Mirrored in `buildInitialPeers`. Selecting by mesh IP keeps
   `10.90.0.1` the default hub (backward-compatible) with failover climbing to `.2`.
3. **Worker control-plane failover.** `controlEndpoints` (`controldialer.go`) is a
   worker's view of reachable control API addresses (seeded at join, refreshed from
   every peer set via `PeerSyncConfig.OnPeers`). Peer sync + route client `pick()`
   (rotate) — they read replicated state, any control node serves them. The
   AgentSync stream `pickLeader()`s: livestate/heartbeats are **leader-memory**
   (`applyAnywhere` drops commands on a follower), so a stream landing on a
   follower would never mark the worker ALIVE. The peer set now carries
   `leader_node_id` (`buildPeerSet` → `leaderNodeID`), so the agent re-targets the
   new leader after an election; leadership changes self-heal via the periodic
   heartbeat-flush `KindNode` re-push. This fixed the first cloud run (workers
   never reached ALIVE because they were rotating onto followers).
4. **Leadership-reactive by construction.** The mesh hub + ingress now come up on
   every control node at boot (not gated on `IsLeader`), so a follower elected
   leader needs no hub bring-up on the `LeaderCh` transition; leader-only loops
   already react via `leaderrunner`. The scale-to-zero activator forwards a wake to
   the leader from any control node's ingress.
5. **Registry self-credential** for a joined control+worker uses the join-issued
   credential (a follower cannot mint its own); the bootstrap leader still mints.
6. **Incremental peer reconcile** (`mesh/device.go` `ApplyPeers`). Cloud runs
   exposed a pre-existing fragility: kernel WG `replace_peers=true` flushes and
   re-handshakes EVERY peer on each push, so a worker joining a multi-control
   cluster reset the control↔control raft tunnels and the leader stepped down
   ("failed to contact quorum"). No prior test hit it (the webapp test has 1
   control; `ha_test` has 0 workers). `ApplyPeers` now diffs against the
   last-applied key set: `replace_peers=false`, update desired peers in place
   (no session reset when endpoint/key are unchanged), and emit an explicit
   `remove` only for peers that vanished. This also makes hub failover itself
   churn-free (flipping a peer's `/16`↔`/32` is an in-place allowed-ips update).
7. **Hub packet forwarding** (`meshwiring.go` `enableIPForward`). Cloud runs
   exposed a second latent gap: the hub set `net.ipv4.ip_forward=1` but never
   added FORWARD rules, and Docker (on every worker-capable node) defaults the
   iptables FORWARD policy to DROP — so worker↔worker packets through the hub were
   silently blackholed. The hub now inserts `FORWARD -i/-o <mesh-iface> ACCEPT` at
   the top of the chain (idempotent). Never caught before because no test drove
   worker↔worker over hub IP-forwarding.
8. **gRPC keepalive on worker↔control streams** (`controldialer.go` +
   `api/server.go`). Cloud runs exposed the last gap: when the join-control node
   (leader + hub) was stopped, its WireGuard device tore down and the workers'
   gRPC streams hung over the dead tunnel for the OS TCP timeout (minutes) — a
   server-stream like peer sync receives but never sends, so it never noticed and
   never reconnected; the workers went DOWN and the `/16` never re-pointed. Worker
   control dials now use a 15s client keepalive (5s timeout, above the server's
   10s `MinTime`), so a dead control node is detected in ~20s → reconnect →
   failover. Added matching server keepalive to reap dead client streams.
   **Files:** `internal/daemon/meshwiring.go`, `internal/daemon/daemon.go`,
   `internal/daemon/api/server.go` (keepalive),
   `internal/daemon/controldialer.go`, `internal/daemon/api/meshsvc.go`,
   `internal/daemon/api/join.go`, `internal/daemon/mesh/peersync.go`,
   `internal/daemon/intdnswiring.go`, `internal/daemon/mesh/device.go`
   (incremental `ApplyPeers`), `internal/daemon/mesh/kernel_linux.go`,
   `api/proto/zattera/cluster/v1/mesh.proto` (`PeerSet.leader_node_id`),
   `test/cloud/multihub_test.go`.
   **Gotchas:** a worker dialing a standby control node needs its SNI to match that
   node's mesh-IP SAN — `controlEndpoints.pick` builds per-target creds. Standby hubs
   must keep a warm keepalive tunnel from NAT'd/hub-routed workers or failover isn't
   instant. Never give two control peers the `/16` — WireGuard routes a prefix to one
   peer only; exactly one active hub carries it.
   **Tests:** unit `TestActiveHubID`, `TestPeerSets` (active hub + failover),
   `TestWatchPeersHubFailover` (status change re-pushes the `/16`),
   `TestBuildPeerSetAdvertisesLeader`, `TestControlEndpoints*` (rotation + leader
   tracking). Chaos `TestHA`/`TestControlJoin` still green.
   **Acceptance (cloud):** `go test -tags cloud ./test/cloud/ -run TestMultiHubFailover`
   — 3 control hubs + 2 hub-routed workers; STOP the bootstrap (leader + active hub +
   join-control node at once); assert the quorum still serves writes, the node is
   marked DOWN, and worker↔worker traffic RECOVERS through a surviving hub.

### T-55d — Follower control+worker agent → leader ✅ **DONE**

Phase 6 · Depends: T-55c · Size: M
Surfaced during T-55c: a control node that also carries the worker role ran its
own node agent over loopback (`localAgentDialer`) to its OWN API. On a FOLLOWER
that API is not the leader, `applyAnywhere` drops agent commands on a follower and
livestate is leader-memory — so a follower control+worker's observed state never
reached the leader, and workloads scheduled onto it never reported healthy (the
deploy orchestrator would stall). Verified GREEN on a real 3× control+worker
Hetzner cluster (`test/cloud/controlworkload_test.go`: deploy a 3-replica app,
assert a healthy replica lands on a FOLLOWER control node).
**Done:**

1. **Client dials the leader.** `controlAgentDialer` (`daemon.go`) replaces
   `localAgentDialer` for a control node's own agent: loopback when leader, else
   the leader's mesh-IP API (`leaderAPIResolver`); re-resolves on every reconnect
   and carries the T-55c keepalive. An election (leader unknown) returns an error
   so it retries rather than dialing a follower.
2. **Server drops non-leader streams.** `SyncServer.Sync` rejects at open and in
   the recv loop when `notLeader()` (`agentsync.go`, nil applier = leader for
   tests). This sheds streams from a leader demoted-but-alive (a re-election that
   client keepalive can't catch since the connection is still live) so they
   reconnect to the new leader — also hardens pure-worker failover (T-55c).
3. **Container host IP fix.** The cloud test exposed a second bug: `agentHostIP`
   returned the constant `10.90.0.1` for EVERY control node, so a joined control
   node bound container ports (and advertised its control gRPC + build source) to
   the bootstrap's IP → "cannot assign requested address". `runControlPlane` now
   uses the node's OWN mesh IP (`hostIP = meshIP`).
   **Files:** `internal/daemon/daemon.go` (`controlAgentDialer`, `hostIP`),
   `internal/daemon/api/agentsync.go` (`notLeader` guard),
   `internal/daemon/api/agentsync_test.go`, `test/cloud/controlworkload_test.go`.
   **Acceptance (cloud):** `go test -tags cloud ./test/cloud/ -run TestControlNodeWorkloads`.

### T-56 — memberlist gossip failure detection ✅ **DONE**

Phase 6 · Depends: T-55, T-19 · Size: M
**Landed:** `internal/daemon/mesh/gossip.go` runs memberlist on control nodes
(mesh IP:7946, WAN tuning, key = sha256 of the cluster CA hash); the shared
decision types live in the dep-free leaf `internal/daemon/nodehealth`
(`Decide` flap guard) so `api` can import them without a cycle (mesh's tests
import api). `LivenessMonitor.WithGossip` feeds the snapshot into the same
SetNodeStatus path — gossip accelerates DOWN and holds a node ALIVE past the
heartbeat deadline; gossip-confirmed death bypasses the post-election grace
window; with no gossip attached the behaviour is byte-identical to before.
**Real-cluster verification (T-55 + T-56): GREEN on Hetzner.**
`test/cloud/ha_test.go` `TestControlHAAndGossip` — a real 3-control quorum forms
and all nodes reach ALIVE (T-55), then killing the bootstrap leader the two
survivors re-elect and keep serving writes (T-55 HA) while the dead leader is
marked DOWN in ~19s — inside the new leader's post-election grace, which only
gossip bypasses (T-56). Getting there took several real-cluster fixes beyond the
in-process work:

- bootstrap node runs raft over the **mTLS transport on its mesh IP** (was
  plain TCP on loopback → a joined node's mTLS listener EOF'd its dials);
- a joining CONTROL node gets **/32 direct peers** to each control node (was
  given overlapping `/16` hub routes, which WireGuard can't program → the 3rd
  control node was unreachable);
- `memberlist.Join` **retries** until a peer is reached (was one-shot → a
  node whose tunnel wasn't up yet stayed invisible to the leader's gossip);
- `serverIPs` uses the node's ACTUAL mesh IP (cert SAN) and
  `leaderAPIResolver` forwards to the leader's mesh IP, so multi-control
  leader-forward verifies + routes over the mesh.
  **Files:** `internal/daemon/mesh/gossip.go`, `gossip_test.go`
  **Steps:**

1. `hashicorp/memberlist` over the mesh (bind mesh IP :7946, LAN config with
   longer timeouts for WAN: SuspicionMult 6, ProbeInterval 2s); join via
   control-node mesh IPs from the peer set; secret key = sha256 of cluster CA
   key hash (gossip encryption).
2. Leader consumes memberlist events (via a control-side subscription
   forwarded over AgentSync? NO — control nodes run memberlist themselves)
   → node suspect/dead → feed the same SetNodeStatus path as T-21 (whichever
   signal is FASTER wins; heartbeats remain the fallback).
   **Gotchas:** memberlist node names = zattera node ids; never let memberlist
   bind a public IP; both detectors racing must not flap status (dead needs
   BOTH gossip-dead AND heartbeat-stale >10s, alive needs either fresh).
   **Tests:** unit — event→status mapping with a fake memberlist; flap guard.
   **Acceptance:** `go test ./internal/daemon/mesh/ -run TestGossip`

### T-57 — meshsock: custom bind + UDP hole punching (Phase C) ✅ **DONE** (real-infra punch → T-57b)

Phase 6 · Depends: T-20 · Size: XL (split if needed)
**Landed:** `internal/daemon/mesh/meshsock/` — a wireguard-go `conn.Bind`
multiplexing WG packets + `0xff`-prefixed HMAC-signed disco frames on one UDP
socket; a per-peer path state machine (home → direct → punched → relay) with
managed per-peer singleton endpoints swapped by an atomic pointer (magicsock
model); control-coordinated simultaneous-open via `MeshService.PunchStream` +
`RequestPunch` (T-18 additive RPCs). Wired into `DeviceManager` (bind + peer
feeding + `nodeID@` endpoints) and the daemon (worker punch client + peer-builder
meshsock pairing). Tests: frame discrimination, path transitions over a
programmable NAT sim (full-cone punch, symmetric→relay, loss→home), and a REAL
wireguard-go tunnel over a hole-punched path. Acceptance
`go test ./internal/daemon/mesh/meshsock/` green.
**Remaining (T-57b):** real-infra hole punching needs each node's reflexive
WG-port endpoint advertised to control (fold the hub's observed per-peer WG
endpoints, or run disco over the meshsock socket). Without it, real NAT'd nodes
fall back to the relay (T-58), which is what `test/cloud/TestMeshsockRelay`
verifies.
**Files:** `internal/daemon/mesh/meshsock/{bind.go,disco.go,path.go}`,
tests alongside
**Steps:**

1. Implement `conn.Bind` (wireguard-go interface): one UDP socket
   multiplexing WG packets and disco frames (discriminate by first byte —
   WG message types are 1..4; disco uses 0xff magic prefix).
2. Per-peer path state machine (`path.go`): candidates = configured
   endpoints + disco-observed; probe with disco ping/pong THROUGH the bind
   socket (source port = WG port — this is what makes punching work);
   coordinate simultaneous-open via control (`MeshService` — add additive
   RPC `RequestPunch(nodeA, nodeB)` that pushes "punch now at t+500ms with
   endpoints […]" over WatchPeers-adjacent stream or a new `PunchStream`).
3. Send path selection: direct (verified) → punched → hub relay (fallback
   stays via control AllowedIPs) — the bind rewrites destination endpoints
   per peer (magicsock model).
4. Keep kernel-WG nodes on phases A/B (no meshsock): peer builder marks
   `meshsock_capable` and only pairs capable nodes for punching.
   **Gotchas:** this is the hardest task in the project — port strictly what's
   needed, no roaming/multi-path; the bind must be lock-light (per-peer atomic
   endpoint pointer); disco frames need HMAC (reuse T-20 keys); punching
   requires BOTH sides sending within the window — the control-coordinated
   timestamp does this; always keep the hub route as final fallback.
   **Tests:** unit — frame discrimination, path preference transitions with a
   fake network (in-memory PacketConns with programmable NAT behavior: full-cone
   and symmetric); integration — two wireguard-go instances with meshsock over
   loopback "NAT" simulator, tunnel ping.
   **Acceptance:** `go test ./internal/daemon/mesh/meshsock/`

### T-58 — TCP relay, DERP-lite (Phase D) ✅ **DONE**

Phase 6 · Depends: T-57 · Size: L
**Landed:** `internal/daemon/mesh/relay/` — an mTLS TCP relay every control node
runs on `:7443` (node-cert auth via URI SAN; frames
`[dstNode(26)][len(u16)][payload]` capped at 2048; per-conn drop-oldest write
queues). meshsock's `relayEndpoint` send path activates after ~10s with no UDP
path; the relay client (fastest-connect + reconnect backoff) injects received
packets into the bind's recv queue. The relay never sees plaintext. Tests:
frame routing between fake clients, drop-on-absent-dst, backpressure drop, and a
REAL wireguard-go tunnel over the relay. Acceptance
`go test ./internal/daemon/mesh/relay/` green. Real-infra check:
`test/cloud/ha_test.go`… `TestMeshsockRelay` (two NAT'd meshsock workers reach
each other only via the relay).

### T-57b — meshsock reflexive-endpoint discovery + real-infra punch

Phase 6 · Depends: T-57 · Size: M
**Why:** hole punching needs each node's reflexive endpoint on its WG/meshsock
source port. Options: (a) the control hub reads its WireGuard device's observed
per-peer endpoint (the worker's NAT-mapped WG addr) and folds it into the
punch-endpoint set; or (b) run the disco echo (T-20) over the meshsock socket so
the reflexive mapping matches the WG port. Then `RequestPunch` returns real
endpoints and NAT'd meshsock workers get a direct punched path instead of the
relay. **Test:** cloud — two full-cone-NAT'd meshsock workers establish a
punched worker↔worker path (assert `direct`/`punched`, not `relay`); block UDP →
verify relay fallback.

### T-57c — meshsock real-infra: hub relayed + slow relay dial ✅ **DONE**

Phase 6 · Depends: T-57 · Size: M
**Symptom (found via `test/cloud/TestMeshsockRelay`):** meshsock workers come up
and punch-coordinate fine, but `TestMeshsockRelay` fails — WireGuard handshakes
fail with `Failed to send handshake initiation: use of closed network
connection`. The bind-close hypothesis in the original write-up was **wrong**:
kept-VM logging of `Bind.Open`/`Close` showed the bind opens once (a single
ephemeral→51820 startup rebind) and then **stays open** — the `net.ErrClosed`
came from the _relay client's_ `Send` (same error string), not the UDP socket.
**Two real root causes, both fixed:**

1. **The hub/control peer was escalated off `PathHome` to the relay.** meshsock
   treated the control peer like any other: no probe pong (control runs plain
   WG, not meshsock) → "unverified" → `markRelay` after `RelayAfter`. But the
   relay _rides_ the hub tunnel, so relaying the hub itself deadlocks. Fix: mark
   the hub-and-spoke control peer `Hub` in `PeerInfo` and pin it to `PathHome` —
   never punch/relay it (`meshsock.PeerInfo.Hub`, `pathManager.evaluatePeer`,
   `meshsockPeers`). Its public endpoint is authoritative; plain-WG handshake is
   the liveness test. Regression test: `TestHubPeerNeverRelays`.
2. **The relay client's first dial hung ~127s.** `relayCli.Run` starts before
   mesh Up and dials the control _mesh_ IP `:7443`; with no per-attempt connect
   timeout, the SYN to the not-yet-reachable mesh IP hung on kernel SYN retries
   (~127s) instead of failing fast and retrying once the hub tunnel was up. Fix:
   `dialTimeout` (8s) wrapping `DialTLS`'s `DialContext`.
   Also: the cloud host image lacked `ping` → `assertMeshReachable`'s ICMP probe
   saw nothing; `Node.InstallDocker` now installs `iputils-ping`. **Acceptance:**
   `test/cloud/TestMeshsockRelay` green on real infra (worker↔worker only via the
   relay).

**old T-58 spec (for reference):**
Phase 6 · Depends: T-57 · Size: L
**Files:** `internal/daemon/mesh/relay/{server.go,client.go}`, tests
**Steps:**

1. Server on every control node: mTLS TCP `:7443`; clients authenticate with
   node certs; frames `[dstNodeID(26B)][len(u16)][payload]`; server relays to
   the dst's connection if present (drop otherwise, UDP semantics).
2. meshsock integration: a `relayEndpoint` implementing the send path when no
   UDP path verifies within 10s; receive side injects relayed payloads into
   the bind's recv queue.
3. Client picks the lowest-RTT control relay (disco RTTs), reconnects with
   backoff, and re-registers.
   **Gotchas:** relayed WG packets are already encrypted — the relay never sees
   plaintext; per-connection write queues with drop-oldest (a slow peer must not
   block the relay); frame size cap 2048.
   **Tests:** unit — relay server frame routing between two fake clients, drop
   on absent dst, backpressure drop; meshsock falls back to relay when UDP
   paths are blocked in the NAT simulator.
   **Acceptance:** `go test ./internal/daemon/mesh/relay/`

### T-59 — Metrics sampler + ring TSDB ✅ **DONE** (proxy env-series feed → T-60)

Phase 6 · Depends: T-13 · Size: L
**Landed:** `internal/daemon/tsdb/ring.go` implements `tsdb.Store` (`RingStore`):
per-`SeriesKey` raw (15s×5760) + downsampled (5m×8640) float32 rings, each
position tagged with its absolute slot number so wrap-around never misreads a
stale slot; downsample-on-write folds each new raw slot into the 5m slot's
running average (per-slot `cnt`); out-of-order samples older than the current
slot are dropped, same-slot re-samples overwrite; `Query` picks raw vs down by
step; 48h GC of idle series; atomic flat-file persistence (`versioned header +
rings`, temp-file+rename) flushed every 60s by a background goroutine and on
`Close`, missing/corrupt file → start empty with a warning. The agent sampler
(`internal/daemon/agent/metrics.go`) runs a 15s loop recording node
cpu/mem/disk/net (gopsutil) and per-instance cpu/mem/net (`Executor.InstanceStats`
→ `runtime.Stats`); it is wired into both node-agent bring-up paths in
`daemon.go` (store at `<data-dir>/metrics/tsdb.bin`). Tests cover round-trip,
sub-window, out-of-order/overwrite, wrap-around, downsample average, resolution
selection, Keys filter, persist/load, corrupt-load, GC, and the sampler across
all three scopes.
**Deferred to T-60:** the proxy env-series feed (`rps`, `latency_p50_ms`,
`latency_p99_ms`, `error_rate`, `inflight`). The sampler already accepts a
`ProxyStats ProxyMetricsFunc` provider and records these series when it is set;
what remains is threading the ingress L7's `proxy.Stats` handle (created inside
`serveIngress`) out to the agent's `Config.ProxyStats`. T-60 owns this — it is
the consumer that fans out to agent TSDBs and already touches the ingress/metrics
surface.
**Original spec below.**
**Files:** `internal/daemon/tsdb/ring.go`, `ring_test.go`,
`internal/daemon/agent/metrics.go`
**Steps:**

1. `ring.go`: implement `tsdb.Store` — per SeriesKey two float32 rings (15s
   ×5760 slots, 5m×8640) with slot timestamps derived from a base index;
   downsample on write (avg into the 5m slot); Query picks resolution by
   step; persistence = flat file (`binary.Write` of a versioned header +
   rings) flushed every 60s and on Close, loaded at start (tolerate
   missing/corrupt = start empty with a warning).
2. Agent sampler loop (15s Clock): node cpu/mem/disk/net (gopsutil) +
   per-instance `runtime.Stats` → Record; proxy stats (T-42's counters) →
   env-scoped series (`rps`, `latency_p50_ms`, `latency_p99_ms`,
   `error_rate`, `inflight`).
   **Gotchas:** ring math off-by-ones (slot = (unixSec/step) % size) — golden
   tests across wrap-around; series cardinality bounded (instances come and go
   — GC series untouched for 48h); float32 precision fine for metrics.
   **Tests:** unit — write/query round trip, wrap-around, downsample, persist/
   load, GC.
   **Acceptance:** `go test ./internal/daemon/tsdb/`

### T-60 — Historical stats API + CLI ✅ **DONE**

Phase 6 · Depends: T-59, T-41 · Size: M
**Landed:**

- **Proxy env-series feed (T-59 deferral):** the ingress L7's `proxy.Stats` is
  surfaced out of `serveIngress` via a `statsSink` callback threaded through
  `startDevIngress`/`startProdIngress`; `runControlPlane` holds it in an
  `atomic.Pointer[proxy.Stats]` and passes `agent.Config.ProxyStats =
proxyStats.Snapshot`, so the sampler now records `rps`/`latency_p50_ms`/
  `latency_p99_ms`/`error_rate`/`inflight` on the ingress node.
- **Agent side:** `agent.StatsServer` (`internal/daemon/agent/statsserver.go`)
  serves `AgentLocalService.Stats` from the node's local ring TSDB — scope filter
  (node → its node series; env/app → env proxy + all instance series; cluster →
  node series), metric filter, `[since,until]` at the resolution nearest
  `step_seconds`. Wired into `LocalServer` + `startAgentLocalServer`.
- **Control side:** `MetricsServer.Stats` routes a query WITH a `since` bound to
  `statsHistory` (`internal/daemon/api/metricshistory.go`): a `StatsDialer`
  (`GRPCStatsDialer`) fans out to the relevant nodes concurrently (3s per-node
  timeout, partial on error) and merges — node/cluster concatenate; env/app fold
  env proxy series + per-instance series (mapped instance→env from state) into
  env-level series, summing `rps`/`inflight`/`memory_bytes`/`net_*` and averaging
  cpu/rates/latencies per timestamp. A query without `since` keeps the live
  (heartbeat) path unchanged.
- **CLI:** `zattera stats` gains `--since`/`--step`/`--node`; historical mode
  renders each series as an eight-level unicode sparkline (`▁▂▃▄▅▆▇█`) with the
  latest value, `--json` returns raw series.
  **Tests:** `TestStatsHistory` (api, acceptance — node/cluster/env/app scopes +
  aggregation + live fallback, backed by real per-node TSDBs), `TestStatsServer*`
  (agent scope/metric filter), `TestSparkline` (cli). Live-path `TestStatsLive`
  still green.
  **Acceptance:** `go test ./internal/daemon/api/ -run TestStatsHistory` ✅
  **Original spec below.**
  **Files:** `internal/daemon/api/metricssvc.go` (extend),
  `internal/cli/stats.go` (extend)
  **Steps:**

1. `Stats` with a time range: fan out to the relevant nodes'
   `AgentLocalService.Stats` (agents serve their local TSDB), merge series
   (concat by scope — node series live on that node; env series merge by
   summing rps across instances / averaging cpu).
2. CLI sparkline rendering (`▁▂▃▅▇` blocks) for `zattera stats --app api
   --since 1h`; `--json` = raw series.
   **Tests:** unit — fan-out merge (sum vs avg per metric), sparkline renderer.
   **Acceptance:** `go test ./internal/daemon/api/ -run TestStatsHistory`

### T-61 — Autoscaler ✅ **DONE**

Phase 6 · Depends: T-59, T-23 · Size: M
**Landed:** `internal/daemon/scheduler/autoscaler.go` — a leader-gated 15s loop
(`leaderrunner.Run`) that, per env with `Autoscale` targets and an active
release (skipping envs a live deployment owns), reads the leader's livestate:
per-instance cpu% and memory% (memory vs the env's `resources.memory_mb` limit)
across the env's running assignments, and per-env RPS from proxy samples.
`desired = ceil(running_replicas × observed / target)` per signal (rps reduces to
`ceil(totalRPS/target)`), max across configured signals, clamped to
`[max(min,1), max]`. Scale-up is immediate (gated only by the cooldown);
scale-down fires only after the load holds below `0.8×target` for 5m (per-env
`lowSince`) and a 3m post-change cooldown. Missing data (no running replicas /
agent gap) or a configured signal with no samples → freeze (no write). Writes
`effective_replicas` via `PutEnvironment` (re-read + clone) and emits an
`autoscale.scaled` event; the scheduler (T-23) converges the count. Hold timers
live in memory and reset each leadership term.
**Wiring (needed for livestate to carry the data):** the agent heartbeat now
attaches `Heartbeat.instances` (per-instance cpu/mem/net) and `Heartbeat.proxy`
(per-env samples). To avoid double-draining the proxy RPS window, the metrics
sampler (T-59) is the SOLE caller of `proxy.Snapshot()` and publishes the latest
instance+proxy samples to the agent (`publishLive`); `heartbeat()` attaches the
published copy. Autoscaler wired into `daemon.go` (`scheduler.NewAutoscaler`).
**Tests:** `TestAutoscaler` — up on cpu spike, clamp to max, scale on rps,
scale-down only after sustained-low + 5m hold, freeze on missing data, cooldown
blocks a second change, no-config no-op (fake clock + scripted livestate).
**Acceptance:** `go test ./internal/daemon/scheduler/ -run TestAutoscaler` ✅
**Original spec below.**
**Files:** `internal/daemon/scheduler/autoscaler.go`, `autoscaler_test.go`
**Steps:**

1. Leader loop (15s Clock): per env with autoscale targets: gather current
   (livestate cpu%/mem% averaged across instances; rps per replica from
   proxy samples) → `desired = ceil(current_replicas × observed/target)`
   (max across configured signals), clamp [min,max].
2. Scale-up immediately; scale-down only after the signal holds below target
   ×0.8 for 5 minutes (track per-env candidate-since in memory) + 3m
   cooldown after any change.
3. Write via `PutEnvironment` mutation of `effective_replicas`; T-23 does
   the rest. Emit events on change.
   **Gotchas:** missing metrics (agent gap) → freeze, never scale on absent
   data; effective_replicas=0 is reserved for scale-to-zero (T-71) — clamp to
   min≥1 here when min>0; leadership change resets in-memory hold timers
   (conservative: restart the 5m window).
   **Tests:** unit — fake clock + scripted livestate: up on cpu spike, down
   after sustained low + cooldown, freeze on missing data, clamping.
   **Acceptance:** `go test ./internal/daemon/scheduler/ -run TestAutoscaler`

### T-62 — Volumes: objects, leases, mounts ✅ **DONE**

Phase 6 · Depends: T-24, T-15 · Size: L
**Landed:**

- **VolumeService CRUD** (`internal/daemon/api/volumes.go`): CreateVolume (pins
  to `node_id` or the least-used ALIVE worker; ACTIVE; DNS-safe unique name per
  env), ListVolumes, DeleteVolume (refuses while mounted — a live lease or a RUN
  instance on the volume's node). Registered in `server.go` + gateway; RBAC
  developer/viewer. Snapshot/Restore/Files stay Unimplemented (T-64/T-65/T-77).
- **Auto-create + pinning** (`internal/daemon/scheduler/volumes.go`):
  `ensureVolumes` (before placement) creates a Volume for each declared mount of
  a stateful service, pinned to the least-used ALIVE worker; `pinnedNodeID`
  (T-24) already restricts placement to `volume.node_id` (verified + tested).
- **NODE_LOST**: `trackVolumeNode` flips a volume to NODE_LOST when its pinned
  node is DOWN (event fired) and back to ACTIVE on recovery. The stateful
  assignment is kept in place (not rescheduled off its data), so no replacement
  is ever created — the no-double-run guarantee.
- **Fencing lease**: `reconcileLeases` (after placement, so a freshly placed
  assignment is leased in the same pass) grants/renews `VolumeLease{node,
assignment, expires: now+60s}` for the holder on a live pinned node, never
  stealing a still-valid lease from another node. The proto gained
  `AssignmentRuntime.volume_lease` (regenerated); `agentsync.buildRuntime`
  attaches the current lease; the agent (`executor.leaseWithholds`) refuses to
  start a stateful+volume container unless the lease names THIS node and THIS
  assignment (reports PENDING, not FAILED, so it starts once the lease lands).
- **Agent mounts** were already in place (executor `EnsureVolume` + `Mounts`).
- **DeleteVolume docker cleanup**: new `AgentLocalService.RemoveVolume` RPC
  (regen) → `LocalServer.RemoveVolume` calls `runtime.RemoveVolume`;
  `VolumeServer.DeleteVolume` best-effort dials the volume's node
  (`GRPCVolumeAgentDialer`, 3s timeout) after the state delete — a down node just
  leaves an orphan, never failing the delete.
- **CLI**: `zattera volume ls/create/rm` (`internal/cli/volumes.go`) over the
  VolumeService client, wired into root.
  **Tests:** `TestVolumeLease` (scheduler: auto-create+pin, lease acquire/renew
  with fake clock, NODE_LOST + no reschedule + lease lapse, recovery),
  `TestLeaseHelpers`; `TestExecutorVolumeFencing`/`TestLeaseWithholds` (agent:
  starts only on a matching lease, withholds on foreign/absent/other-instance
  lease — real fakeruntime container counts); `TestVolumeServer*` (api CRUD +
  refuse-while-mounted); chaos `TestVolumeFencing` (node dies → NODE_LOST, invariant
  held: never a second RUN replica, never migrated off the dead node).
  **Follow-on (still open):** `volume browse`/`cp` read-only file access (T-77) and
  snapshots (T-64/T-65) — the `SnapshotVolume`/`RestoreVolume`/file RPCs stay
  Unimplemented until then.
  **Acceptance:** `go test ./internal/daemon/scheduler/ -run TestVolumeLease` ✅;
  `go test -tags chaos ./test/chaos/` ✅.
  **Original spec below.**
  **Files:** `internal/daemon/api/volumes.go`, `internal/daemon/scheduler/`
  (placement integration), `internal/daemon/agent/volumes.go`, tests
  **Steps:**

1. `VolumeService` CRUD: CreateVolume (node picked by scheduler when empty:
   least-used ALIVE worker), DeleteVolume (refuse while mounted), List.
   Volumes auto-created when a ServiceSpec declares one that doesn't exist
   (scheduler, at placement).
2. Fencing lease: before an assignment with volumes may RUN, the scheduler
   acquires `PutVolumeLease{node, assignment, expires: now+60s}` and RENEWS
   it (leader loop, 20s); the AGENT refuses to start a container whose
   volume lease (sent in the AssignmentSet frame) doesn't name this node —
   THE invariant against double-run (spec §9.1).
3. Placement: stateful+volume → pinned to `volume.node_id` (already in
   T-24's filter — verify + test); volume's node DOWN → volume NODE_LOST,
   service stops (assignment not rescheduled), event fired.
4. Agent: EnsureVolume with managed labels; mounts into ContainerSpec.
   **Gotchas:** lease expiry must be generous vs clock skew (60s TTL, 20s
   renew); NEVER reschedule a stateful assignment while ANY lease for its
   volume names another node; DeleteVolume also removes the docker volume on
   its node (best effort over agent RPC).
   **Tests:** unit — lease renewal/expiry with fake clock; chaos — partition
   the volume's node: old instance keeps lease locally but leader can't renew →
   new placement BLOCKED until lease expiry AND node confirmed dead (both), no
   double-run window (assert via fakeruntime container counts across the whole
   scenario).
   **Acceptance:** `go test ./internal/daemon/scheduler/ -run TestVolumeLease`;
   chaos suite extension green.

### T-63 — Stateful deploy semantics (stop-then-start) ✅ **DONE**

Phase 6 · Depends: T-62, T-26 · Size: M
**Landed:** `internal/daemon/scheduler/stateful.go` — the orchestrator delegates
stateful releases (previously aborted) to `reconcileStateful`, a stop-then-start
machine: `PENDING → [BUILDING] → STOPPING_OLD → STARTING → HEALTHCHECKING →
PROMOTING → SUCCEEDED`. `stopOld` flips the outgoing instance to STOP and waits
until its container is actually gone (`stillRunning` on observed state) — the
single-writer barrier — before `statefulStart` places exactly one new instance on
the volume's pinned node (`Place` pins it; on a first deploy, with no active
release yet, `ensureDeployVolumes` creates the volume on the chosen node first).
`statefulPromote` switches the active release with no drain window (the old
instance is already stopped). Any failure after the old instance stopped —
start failure, unhealthy, or health-deadline timeout — runs `statefulFail`:
reap the new instance, flip the old one back to RUN (best effort), FAILED.
`DEPLOYMENT_PHASE_STOPPING_OLD = 12` added additively (regen); `checkBuild` and
`emitEvent` generalized (next-phase / severity). Maintenance downtime is bracketed
by `deploy.maintenance_start`/`deploy.maintenance_end` events.
**Tests:** `TestStateful` — full stop-then-start walk with a continuous
never-two-RUN assertion after every step, first-deploy volume auto-create,
failure-after-stop restart-old path, and health-timeout restart path; the
red/green `TestDeployment` "stateful releases" case updated to expect the new
STOPPING_OLD route.
**Acceptance:** `go test ./internal/daemon/scheduler/ -run TestStateful` ✅
**Original spec below.**
**Files:** `internal/daemon/scheduler/stateful.go`, `stateful_test.go`
**Steps:**

1. Deployment orchestrator branch for `stateful: true`: phases
   PENDING → STOPPING_OLD (stop blue, wait STOPPED) → STARTING (green on the
   SAME node, same volume) → HEALTHCHECKING → PROMOTING → SUCCEEDED; failure
   after old stopped → RESTART OLD (best effort) then FAILED.
2. Reuse SetDeploymentPhase values (add none — map STOPPING_OLD onto
   PLACING to avoid proto changes? NO: add
   `DEPLOYMENT_PHASE_STOPPING_OLD = 12` additively).
3. Maintenance downtime is expected — emit events around it.
   **Tests:** unit — phase walk, failure-restore path, never two RUN
   assignments for the volume at any step (assert continuously).
   **Acceptance:** `go test ./internal/daemon/scheduler/ -run TestStateful`

### T-64 — Snapshot engine: tar + FastCDC + zstd + AES-GCM + S3 ✅ **DONE**

Phase 6 · Depends: T-13 · Size: L
**Landed:** the `internal/daemon/volumes` package — a content-addressed,
deduplicated snapshot engine. `chunker.go` streams a deterministic tar of the
volume path (sorted walk, zeroed atime/ctime, second-truncated mtime, uid/gid/
mode preserved, PAX) so byte-identical trees tar identically; `snapshot.go`'s
`Engine.Snapshot` feeds it through FastCDC (`jotfs/fastcdc-go`, ~1MB avg) and per
chunk: `sha256(plaintext)` → HEAD-skip if `chunks/<hash>` exists (dedup) → zstd →
AES-GCM (`crypto.go`, 32-byte data key, random 12-byte nonce as the object
header — never hash-derived) → PUT; the ordered chunk list + tar total lands in
an encrypted `manifests/<id>`. `Restore` streams chunks back through a tar
extract (tar-slip guarded); `Prune` refcounts across all manifests and deletes
only orphan chunks. `store.go` defines the `ObjectStore` interface + an in-memory
store for tests; `s3.go` is the `minio-go/v7` `S3Store` (creds from BackupConfig;
1MB objects, no multipart).
**Dep note:** minio-go pinned to **v7.1.0** — v7.2.x pulls a newer
`charmbracelet/x/ansi` that conflicts with the repo's pinned `x/cellbuf` and
breaks the TUI build; 7.1.0 has the same S3 surface and coexists.
**Tests:** `go test ./internal/daemon/volumes/` — crypto round trip (nonce
uniqueness + wrong-key failure), deterministic-tar stability, snapshot→restore
byte-identical, chunk stability + dedup (same data → same chunk set + no new
objects; a 1-byte change → 1–2 new chunks), prune leaves shared chunks;
integration `test/integration/TestSnapshotMinIO` (tag `integration`, real MinIO
container, Docker-gated) does snapshot→restore→byte-identical + prune. Both green.
**Acceptance:** `go test ./internal/daemon/volumes/` ✅;
`go test -tags integration -run TestSnapshotMinIO ./test/integration/` ✅.
**Original spec below.**
**Files:** `internal/daemon/volumes/snapshot.go`, `chunker.go`, `s3.go`,
tests with MinIO integration
**Steps:**

1. Deterministic tar of the volume host path (sorted walk, zeroed
   atime/ctime, uid/gid preserved) streamed into FastCDC (avg 1MB,
   `jotfs/fastcdc-go`) → per chunk: sha256(plaintext) → skip if object
   `chunks/<hash>` exists (HEAD) else zstd → AES-GCM (data key, hash-derived
   nonce is FORBIDDEN — random nonce stored in the object header) → PUT.
2. Manifest JSON (`manifests/<snapshotID>.json`): ordered chunk hashes +
   sizes + tar total + created_at; encrypted too.
3. Restore: read manifest → GET chunks → decrypt → unzstd → sequential tar
   extract into the volume path.
4. Prune: refcount across all manifests (list) → delete orphan chunks
   (`volumes.Prune(ctx)`).
5. S3 client: `minio-go/v7`; creds from BackupConfig (unsealed).
   **Gotchas:** never snapshot a RUNNING db volume without the pre-hook —
   orchestration lives in T-65, engine takes an already-quiesced path; dedup
   correctness depends on deterministic tar (test byte-identical output for
   unchanged dirs); multipart not needed (1MB objects).
   **Tests:** unit — chunking stability (same dir → same chunk set; 1-byte
   change → ≤2 new chunks), crypto round trip. Integration — MinIO container:
   snapshot→wipe→restore→byte-identical dir; prune leaves shared chunks.
   **Acceptance:** `go test ./internal/daemon/volumes/`;
   `go test -tags integration -run TestSnapshotMinIO ./test/integration/`

### T-65 — Volume snapshot orchestration + CLI ✅ **DONE** (file-ops/`cp` → T-77)

Phase 6 · Depends: T-64, T-49 · Size: M
**Landed:**

- **Agent** (`internal/daemon/agent/volumeops.go`): `SnapshotVolume` (optional
  pre-hook via `rt.Exec` in the mounting container → engine snapshot of the
  volume host path → `VolumeOpProgress` stream with the manifest key) and
  `RestoreVolume` (engine restore into the host path), implemented on
  `LocalServer` (it already holds the runtime). The T-64 engine gained a
  `Progress` callback (uploaded bytes).
- **Control** (`internal/daemon/api/volumeops.go`): `SnapshotDispatcher` builds an
  `S3Target` from the unsealed `BackupConfig` creds + cluster data key, dials the
  volume's node, streams the op, and records a `VolumeSnapshot`
  (RUNNING→COMPLETE/FAILED). `VolumeService.SnapshotVolume`/`ListSnapshots`/
  `RestoreSnapshot` (restore refuses while mounted). Wired in `daemon.go` (only
  when unsealed with a backup config).
- **Scheduler** (`internal/daemon/scheduler/snapshots.go`): a leader loop parses
  `SnapshotPolicy.schedule` (robfig/cron), fires a snapshot each due slot
  (baseline = last snapshot / volume creation; per-volume `lastFire` guard), and
  enforces `keep_last` (default 7) — deleting old `VolumeSnapshot` records
  (`DeleteVolumeSnapshot` command, added additively + regen) and pruning their
  chunks via the dispatcher.
- **CLI**: `zattera volume snapshot/snapshots/restore`.
  **Tests:** `TestSnapshotSchedule` (cron due-firing once per slot, manual-only
  no-op, keep_last + default-7 pruning); api `TestSnapshotRPCsRequireDispatcher`,
  `TestRestoreRefusesWhileMounted`, `TestSnapshotDispatcherS3Target` (creds unseal).
  **Deferred to T-77:** the volume **file ops** (`ListVolumeFiles`/`ReadVolumeFile`/
  `WriteVolumeFile`) and `volume cp` — these power the read-only `volume browse`
  that T-77 owns; the RPCs stay Unimplemented until then.
  **Acceptance:** `go test ./internal/daemon/scheduler/ -run TestSnapshotSchedule` ✅
  **Original spec below.**
  **Files:** `internal/daemon/agent/volumeops.go` (AgentLocal Snapshot/Restore/
  ListFiles/Read/Write), `internal/daemon/scheduler/snapshots.go` (schedules),
  `internal/cli/volume.go`
  **Steps:**

1. Agent RPCs from T-35's server: SnapshotVolume (run pre-hook via Exec in
   the mounting container when provided → engine → progress stream),
   RestoreVolume (service must be stopped — control enforces), file ops.
2. Control: SnapshotVolume API → dispatch to the volume's node; scheduled
   snapshots via SnapshotPolicy.schedule (cron parser robfig, leader loop);
   keep_last pruning of manifests + engine prune.
3. CLI: `volume ls/inspect/snapshot/restore`, `volume cp vol:/path ./local`
   (ReadFile stream) and reverse (WriteFile).
   **Gotchas:** restore refuses while leased/mounted (stop the service first —
   print the command to do it); progress streaming keeps the CLI informed
   (bytes done/total).
   **Tests:** unit — schedule loop with fake clock, keep_last pruning; e2e-ish
   integration optional.
   **Acceptance:** `go test ./internal/daemon/scheduler/ -run TestSnapshotSchedule`

### T-66 — Full backup + `zatterad restore` (DR) ✅ **DONE** (schedule/registry/rejoin follow-ups noted)

Phase 6 · Depends: T-64, T-55 · Size: L
**Landed:** `internal/daemon/backup/{backup.go,restore.go}` + `raftstore.ForceSnapshot`.

- **Backup** writes to the S3 object store under `backups/<ts>/`: the raft state
  (`state.SnapshotProto` → marshaled → data-key-sealed → `state.pb.enc`), the CA
  cert+key (data-key-sealed → `ca.pb.enc`, so restored certs stay valid), the
  data key **sealed under a recovery passphrase** (`secrets.SealDataKey` →
  `keys.pb`), and a plaintext `index.json` (pointers + each volume's latest
  completed snapshot manifest key) + a `backups/latest` pointer. `Verify`
  downloads + decrypts the latest state and rebuilds it (weekly restore-test
  primitive).
- **Restore** (`zatterad restore --from s3://… --passphrase-file … --data-dir …`,
  a `daemon.Commands()` subcommand): unseals the data key with the passphrase,
  decrypts state + CA into a FRESH data dir, marks old nodes DOWN (mesh IPs
  preserved), and bootstraps a single-node raft holding the restored state —
  loaded via `RestoreProto` then persisted by applying a `restored-at` marker
  (to advance the applied index past bootstrap) + `ForceSnapshot`, so it survives
  the next `zatterad server` start.
  **Tests:** `internal/daemon/backup` unit (backup→verify round trip, wrong
  passphrase fails, passphrase required); integration `TestDisasterRecovery`
  (MinIO: seed state + a real volume snapshot → backup → restore into a fresh dir →
  reopen raft and assert projects/apps/envs restored + old node DOWN with preserved
  mesh IP + the volume snapshot still restores byte-identical).
  **`BackupService` (added):** `SetBackupConfig` (seals the S3 creds with the data
  key + stores the destination), `TriggerBackup` (runs a full backup now, reusing
  the cluster's existing sealed key material — no fresh passphrase), `ListBackups`
  (records + credential-redacted config); admin-only; `zt backup config/run/ls`.
  This is what makes T-65 snapshots + this backup reachable in a real cluster. The
  DR integration test now drives the wired path (`SetBackupConfig` → `TriggerBackup`
  → restore).
  **Deferred (follow-ups):** **scheduled** backups (a periodic `TriggerBackup`) +
  the weekly `Verify` loop; **registry blob** backup/restore; the live
  worker-**rejoin** volume-data restore choreography (the index records what to
  restore; the `RestoreSnapshot` API + a rejoining worker do the actual data
  restore).
  **Acceptance:** `go test -tags integration -run TestDisasterRecovery ./test/integration/` ✅
  **Original spec below.**
  **Files:** `internal/daemon/backup/{backup.go,restore.go}`, CLI `backup.go`,
  `test/integration/dr_test.go`
  **Steps:**

1. Backup (leader, on schedule/API): raft state snapshot proto (from
   `state.SnapshotProto`) encrypted → `state/<ts>.pb.enc`; registry
   manifests+blobs → same chunk store as volumes; every volume → snapshot
   (T-65 path); sealed key material + a `backup.json` index. PutBackupRecord.
2. `zatterad restore --from s3://bucket/prefix --passphrase-file f`
   (subcommand in internal/daemon): fresh data dir → download latest state
   → unseal data key with passphrase → restore state into a bootstrapped
   single-node raft (new cluster, `--force-new-cluster` semantics: old node
   entries marked DOWN, mesh IPs preserved) → restore registry blobs → mark
   volumes RESTORING; as (new) workers join and claim volumes
   (`RestoreSnapshot` API targeting the new node), scheduler re-places
   everything.
3. Backup verification job (weekly): download latest state backup, decrypt,
   unmarshal, count objects, emit event with result (spec §3.11).
   **Gotchas:** the restored cluster has a NEW raft but OLD node ids — purge
   raft server config to just self; certs remain valid (CA is in the backup —
   include `ca/` key material encrypted!); document RPO explicitly in commands'
   help.
   **Tests:** integration — MinIO: seed cluster with a project+volume, backup,
   restore into a fresh dir, assert state equality (projects/apps/envs) and
   volume snapshot restorable.
   **Acceptance:** `go test -tags integration -run TestDisasterRecovery
./test/integration/ -timeout 20m`

### T-67 — Cron jobs ✅ **DONE**

Phase 6 · Depends: T-53 · Size: M
**Files:** `internal/daemon/scheduler/cron.go`, `cron_test.go`,
`internal/cli/cron.go`
**Steps:**

1. Leader loop: parse every env's `CronSpec`s (robfig/cron/v3 parser, no
   scheduler — compute next-run ourselves on the Clock for testability);
   fire → create a Job (cron_name set) honoring ConcurrencyPolicy (FORBID:
   skip if a run is active; REPLACE: cancel active then run; ALLOW: overlap)
   - per-spec jitter (hash(env+name) % 30s — deterministic).
2. Missed runs on leader failover: on becoming leader compute next-run from
   now (skip missed — document).
3. `zattera cron ls` (next run, last status from job history).
   **Tests:** unit — fake clock walk across schedules, all three policies,
   jitter determinism, failover skip.
   **Acceptance:** `go test ./internal/daemon/scheduler/ -run TestCron`

### T-68 — Quorum-loss autonomy test + chaos expansion ✅ **DONE**

Phase 6 · Depends: T-39, T-55 · Size: M
**Files:** `test/chaos/quorum_test.go`, `test/chaos/relay_test.go`
**Steps:**

1. Quorum loss: 3 controls, kill 2 → API writes fail BUT: proxies keep
   serving the last RouteSnapshot (assert via RouteSource disk cache), agents
   keep containers running (fakeruntime untouched), heartbeats buffer.
   Restore quorum → everything reconciles.
2. Deployment mid-flight during failover already covered (T-30) — add: env
   var change + deploy during a rolling leader restart.
3. Relay failover (after T-58): kill the relay a client uses → traffic moves
   to another control relay within 15s (unit-level with the NAT simulator).
   **Acceptance:** `go test -tags chaos ./test/chaos/ -run TestQuorum -timeout 20m`

---

# Phase 7 — M3: scale-to-zero, serverless, DNS providers, alerts, previews, polish

**Exit criterion:** scale-to-zero wake E2E green; `domains add` automates a
Cloudflare record in a mocked driver test; alerts fire to a webhook;
PR preview environments lifecycle works against simulated webhooks; full CI
matrix green.

### T-69 — Idle tracking + scale to zero ✅ **DONE**

Phase 7 · Depends: T-61, T-42 · Size: M
**Files:** `internal/daemon/scheduler/scaletozero.go`, tests
**Steps:**

1. Envs with `scale_to_zero` + `idle_timeout`: proxy heartbeats carry
   `last_request_at` per env; leader loop: idle beyond timeout →
   `effective_replicas = 0` (allowed here, unlike T-61) → evaluator stops
   replicas. Never for stateful envs (validate at ApplyAppConfig too).
2. Waking is T-70's activator; `effective_replicas=0 → 1` on Activate.
   **Tests:** unit — idle detection with fake clock, stateful rejection.
   **Acceptance:** `go test ./internal/daemon/scheduler/ -run TestScaleToZero`

### T-70 — Activator: hold, wake, flush ✅ **DONE**

Phase 7 · Depends: T-69 · Size: L
**Files:** `internal/daemon/proxy/activator.go`, control
`internal/daemon/api/activator.go`, tests
**Steps:**

1. Proxy: route has scale_to_zero and ZERO healthy endpoints → park the
   request (bounded queue per env, 100 reqs / 60s deadline), call
   `ActivatorService.Activate` (singleflight per env), then wait on route
   updates (RouteSource.Updates) until an endpoint appears → replay parked
   requests in order.
2. Control Activate: singleflight; sets effective_replicas=max(1,min) via
   Apply; idempotent 200.
3. 503 with `Retry-After: 2` when the queue is full; queue drained on route
   update or deadline (504).
   **Gotchas:** parked requests hold client connections — cap body buffering
   (replay needs the body: read up to 10MB into memory, refuse larger with 503
   during cold start); metrics: count cold-start latency (histogram) for
   `zattera stats`.
   **Tests:** unit — park→activate→flush with a scripted RouteSource; queue
   overflow; deadline 504. E2E extension: scale-to-zero fixture env wakes on
   curl (extend T-54's smoke as a separate e2e test file).
   **Acceptance:** `go test ./internal/daemon/proxy/ -run TestActivator`;
   `go test -tags e2e -run TestWake ./test/e2e/`
   **Follow-ups (noted):** (a) the control-node ingress calls Activate in-process,
   so a _follower_ control node returns ErrNotLeader — multi-control wake needs the
   request to reach the leader's ingress; fold into the T-55b multi-control HA work.
   (b) cold-start latency is accumulated on the proxy Activator (`ColdStart()`) but
   not yet surfaced in `zt stats` — small follow-up.

### T-71 — Serverless concurrency autoscaling ✅ **DONE**

Phase 7 · Depends: T-70, T-61 · Size: M
**Files:** `internal/daemon/scheduler/serverless.go`, tests
**Steps:**

1. Envs with `max_concurrency > 0`: autoscaler mode switch — desired =
   ceil(total_inflight / max_concurrency) clamped [0|min, max]; evaluation
   every 5s (tighter loop) using proxy inflight from heartbeats (2s staleness
   accepted).
2. Proxy enforces per-replica cap: endpoints at max_concurrency are skipped
   by P2C; all full → hold like the activator (reuse its queue).
   **Tests:** unit — scaling math table, full-endpoint skip in lb.
   **Acceptance:** `go test ./internal/daemon/scheduler/ -run TestServerless`

### T-72 — DNS provider interface + Cloudflare driver

Phase 7 · Depends: T-45 · Size: M
**Files:** `internal/daemon/dnsproviders/{provider.go,cloudflare.go}`, tests
**Steps:**

1. Interface: `EnsureRecord(ctx, zone, rec Record) error`,
   `DeleteRecord(…)`, `Zones(ctx)`; Record{Type A/AAAA/CNAME, Name, Value,
   TTL, Proxied?}. Config from `DNSProviderConfig` (sealed creds).
2. Cloudflare via raw REST (no SDK — small surface: list zones, upsert
   record); token auth.
3. `domains add --dns` flow: pick the provider managing the matching zone →
   EnsureRecord A/AAAA for every ingress node public IP (or the provided
   `--target`), then the normal domain add.
4. API `PutDNSProvider` + CLI `dns providers add cloudflare --token …`,
   `dns ls`.
   **Gotchas:** idempotent upsert (list-then-update by name+type); never log
   tokens; rate-limit friendly (single upsert per record).
   **Tests:** unit — driver against `httptest` fake CF API (upsert paths),
   zone matching (longest suffix).
   **Acceptance:** `go test ./internal/daemon/dnsproviders/`

### T-73 — DNS drivers: Route53, Hetzner, DigitalOcean

Phase 7 · Depends: T-72 · Size: M
**Files:** `internal/daemon/dnsproviders/{route53.go,hetzner.go,digitalocean.go}`, tests
**Steps:** same interface; Route53 via aws-sdk-go-v2/route53 (heavy but
correct SigV4), Hetzner + DO via raw REST. Fake-API unit tests each.
**Acceptance:** `go test ./internal/daemon/dnsproviders/`

### T-74 — Alert engine + notifiers ✅ **DONE**

Phase 7 · Depends: T-59, T-07 · Size: L
**Files:** `internal/daemon/notify/{engine.go,webhook.go,slack.go,email.go}`,
`internal/daemon/api/alerts.go`, `internal/cli/alerts.go`, tests
**Steps:**

1. Engine (leader loop, 30s): metric rules → evaluate against TSDB/livestate
   (scope resolution), sustained-for tracking; event rules → subscribe
   KindEvent matches. Firing → notification with dedupe (same rule+scope
   silenced 15m) and resolve messages.
2. Notifiers: webhook (JSON POST, optional HMAC header), Slack (incoming
   webhook payload), email (net/smtp + STARTTLS, sealed password).
3. Built-in default rules (created at bootstrap, deletable): deploy.failed,
   node.down, disk>90% sustained 5m, cert.renew_failed, backup.failed.
4. AlertService CRUD + CLI `alerts rules ls/add/rm`, `alerts channels add
   webhook|slack|email`.
   **Gotchas:** notifier failures must not stall the engine (per-channel
   timeout 10s, drop+event); never include secret values in payloads; email is
   the flakiest — always best-effort.
   **Tests:** unit — rule evaluation with fake TSDB (threshold+sustained),
   dedupe window, webhook payload golden + HMAC, event-rule matching.
   **Acceptance:** `go test ./internal/daemon/notify/`
   **Follow-up (noted):** the engine + default rules ship, but only `deploy.failed`
   is emitted today. Emit `node.down` (from the liveness monitor on a durable DOWN
   transition), `backup.failed` (BackupService.TriggerBackup error path), and
   `cert.renew_failed` (ACME renewal failure in tlsmgr) so the remaining built-in
   event rules fire. Small, spread across those subsystems.

### T-75 — Preview environments (PR → preview-\*) ✅ **DONE**

Phase 7 · Depends: T-37, T-45 · Size: M
**Files:** `internal/daemon/github/previews.go`,
`internal/daemon/api/githubpreviews.go`, `internal/daemon/previewwiring.go`, tests
**Done:**

1. `pull_request` webhook events (opened/reopened/synchronize → ensure
   `preview-<n>`; closed → delete). `github.Previews` holds all policy over a
   `PreviewStore` port; the api adapter implements it against state+raft.
2. Env creation clones the service spec **and env vars** from `staging`
   (→ `production` → any non-preview env), type PREVIEW with
   `preview_pr_number` + `preview_expires_at` (7d, extended on every PR event).
3. Build+deploy of the PR head SHA reuses `DeployGit`; the URL is commented on
   the PR once, at creation, via `GitHubApp.CommentPR` (installation token).
4. Janitor: `runPreviewJanitor` sweeps expired previews hourly on the leader
   (`leaderrunner`); deletion cascades to containers via the scheduler's orphan
   reconciler and drops the route + certificate.
5. Cap of 5 concurrent previews per app (LE rate-limit protection); the over-cap
   PR gets a comment explaining why instead of silently getting nothing.
6. Head-SHA dedupe: a synchronize for an already-deployed commit extends the TTL
   but does not rebuild (force-push storms / redeliveries).
7. URL naming verified: env name IS `preview-<n>`, so the T-45 route builder
   yields `<app>-preview-<n>.<domain>` unchanged; `PreviewURL` mirrors it.
   **Tests:** `previews_test.go` — lifecycle open→sync→close, SHA dedupe, cap
   enforcement + slot reuse, TTL janitor on a fake clock, HTTP routing/dedupe/
   disabled. `githubpreviews_test.go` — adapter over a real raft store: spec+var
   cloning, base-env preference order, head-SHA resolution, TTL touch, delete.
   **Acceptance:** `go test ./internal/daemon/github/ -run TestPreviews` ✅

### T-76 — Audit query CLI + events surfacing ✅ **DONE**

Phase 7 · Depends: T-07 · Size: S
**Files:** `internal/cli/audit.go`, `internal/cli/events.go`,
`internal/daemon/api/audit.go`, `api/proto/zattera/v1/api.proto`
**Done:**

1. `zattera audit [--project] [--since] [--method] [--actor] [--limit]`,
   table + `--json`, newest first.
2. `zattera events [-f] [--project] [--kind] [--severity] [--since] [--limit]`;
   follow is a 2s poll loop printing oldest-first, each event exactly once
   (inclusive ms cursor + id dedupe, since ids can share a millisecond).
3. **Deviation from the plan:** the Steps assumed a `ListEvents` RPC existed —
   it did not (only the `Event` message and a store accessor). Added
   `AuditService.ListEvents` + `state.QueryEvents` (newest-first, matching
   `QueryAudit`; the old `ListEvents(limit)` keeps its append-order tail for
   the alert engine, which replays chronologically).
4. `ListEvents` is `reqUser`, not admin-only: the handler scopes non-admins to
   projects they belong to (org owner/admin → cluster-wide). The tier table
   cannot express that, and the RBAC project rewrite is unusable here because
   `project_id=""` legitimately means cluster-wide.
5. `resolveProjectID` in the CLI: audit/event queries are absent from the RBAC
   project table, so the project NAME is resolved to its id client-side —
   without it `--project demo` silently matched nothing.
   **Tests:** `internal/cli/audit_test.go` (all named `TestAudit*` so the
   acceptance command covers events too) — table/json, method/since/kind/severity
   filters, empty result, project scoping incl. cross-project isolation, and a
   follow run asserting once-only delivery, ordering and clean cancel.
   `internal/daemon/api/events_test.go` — scoping contract (admin / member /
   non-member / anonymous) and filters.
   **Acceptance:** `go test ./internal/cli/ -run TestAudit` ✅

### T-77 — `volume browse` TUI (read-only)  ✅ **DONE**

Phase 7 · Depends: T-65 · Size: M (grew to L — see below)
**Files:** `internal/cli/volumebrowse.go`, `internal/daemon/api/volumefiles.go`,
`internal/daemon/agent/volumefiles.go`
**Done:**
1. **Deviation from the plan:** the Steps assumed ListFiles/ReadFile existed.
   They were declared in proto but `Unimplemented` on **both** the control plane
   and the agent, so this task was full-stack, not CLI-only.
2. Agent (`ListVolumeFiles`/`ReadVolumeFile`): reads the volume's host path,
   returns entries dirs-first-then-name (decided server-side so every client
   agrees), streams files in 64KB chunks, refuses directories and non-regular
   files (a FIFO would stream forever).
3. **Two path escapes are blocked**, not one: lexical (`../../etc/shadow`) via
   the now-exported `volumes.SafeJoin`, and a symlink *inside* the volume
   pointing out of it, caught by resolving and re-checking containment. Volume
   contents are workload-written, so the symlink case is attacker-supplied.
4. Control plane proxies to the volume's pinned node, with distinct errors for
   unplaced volume / node down / no dialer. Deliberately allowed while mounted
   (unlike snapshot/restore/delete) — reading a live volume is the point.
5. **Streaming authz:** ReadFile is a server stream and the RBAC interceptor is
   unary-only, so the handler does its own project name→id resolution and
   membership check. Without it ReadFile would be weaker than ListFiles: any
   authenticated user could read any project's volume files by guessing a path.
   Added ListFiles/ReadFile to the RBAC table (ROLE_VIEWER) for the unary leg.
6. Entry cap of 5000 with `truncated` on the response (new proto field) — a
   silently capped listing reads as a complete directory.
7. Agent request fields changed from `volume_docker_name` to
   `environment_id`+`volume_name`, matching `AgentRemoveVolumeRequest` ("the
   agent derives the docker volume name") so the naming rule lives in one place.
8. TUI: arrows (+jk), enter/backspace, `d` download with a progress bar (size
   comes from the listing; ReadFile carries no total), `r` refresh, `q` quit,
   window scrolling, human-readable sizes, middle-truncated long names. No
   delete or upload keys. Downloads use the **base name only** — a volume path
   must never decide where a file lands locally — and a failed transfer removes
   the partial file. bubbletea is imported in this one file; +270KB (+0.55%).
**Tests:** `internal/cli/volumebrowse_test.go` (8 tests, model driven through a
fake volumeFS — navigation, download incl. progress and cleanup-on-failure,
directory refusal, view contents, fatal-vs-recoverable load errors, scrolling,
name truncation); `internal/daemon/agent/volumefiles_test.go` (listing order,
cap+truncated, **lexical and symlink traversal**, chunking, refusals);
`internal/daemon/api/volumefiles_test.go` (mapping, streaming, **authorization
incl. the non-member and anonymous cases on both RPCs**, unavailability).
**Acceptance:** `go test ./internal/cli/ -run TestVolumeBrowse` ✅
**Not done (deliberate):** `volume cp` (the write path) stays out — T-77 is
read-only by design. `WriteFile`/`WriteVolumeFile` remain Unimplemented.

### T-78 — CLI polish pass

Phase 7 · Depends: all CLI tasks · Size: M
**Files:** across `internal/cli/`; `docs/reference/errors.md`
**Steps:**

1. Shell completions (`zattera completion bash|zsh|fish` — cobra built-in,
   verify names); `--watch` on ps/nodes/stats (2s redraw).
2. Error catalog: map common gRPC codes+messages to actionable text +
   docs link (`errors.md` with anchors); a central `cli.RenderError`.
3. Spinners on every >500ms operation (deploy/build/logs attach already;
   add to volume ops, state apply); consistent `--project/--app/--env`
   resolution everywhere (one helper, one error text).
4. `zattera doctor`: checks server reachability, version skew (client vs
   server), docker on the node (via an API health field).
   **Acceptance:** `go test ./internal/cli/`; manual sweep of `--help` texts.

### T-79 — Docs quickstart + CI-tested install script ✅ **DONE**

Phase 7 · Depends: T-54 · Size: M
**Files:** `docs/getting-started/quickstart.md`, `scripts/install.sh`,
`test/e2e/quickstart_test.go`, `.github/workflows/release.yml`
**Steps:**

1. `install.sh`: detect OS/arch, download the right release binary (or use
   a local build via `ZATTERA_LOCAL_BIN` for CI), install to
   /usr/local/bin, `--join`/`--token` flags write a config and systemd unit
   (linux) — keep it POSIX sh.
2. Quickstart doc: the real 5-minute path (install → deploy go app → HTTPS
   URL), every command copy-pasteable; CI test executes the doc's commands
   (extract fenced blocks marked `<!-- ci-verify -->`) against a dev server.
3. `release.yml`: tag → `make cross` → GitHub release with checksums.
   **Acceptance:** `go test -tags e2e -run TestQuickstart ./test/e2e/`

### T-80 — Full verification sweep + M3 sign-off

Phase 7 · Depends: everything · Size: M
**Files:** none new (fixes only), `docs/operations/production-checklist.md`
**Steps:**

1. Run the entire matrix locally: `make lint test test-integration test-e2e
test-chaos` + `make cross` + binary-size assertion; fix all failures.
2. Sweep TODOs: every `TODO(T-xx)` left in code must reference a REAL future
   task (M4/M5) or be resolved; grep and clean.
3. Write the production checklist (3 control nodes, backup config, alert
   channels, TLS email, capacity headroom) from what actually shipped.
4. Update README status section: pre-alpha → alpha, features checklist with
   what's real.
   **Acceptance:** full CI green on a PR titled "M3 complete"; zero unreferenced
   TODOs (`grep -rn "TODO(" internal/ cmd/ pkg/ | grep -v "TODO(T-"` empty and
   every `TODO(T-xx)` points to M4/M5 backlog entries below).

---

# Phase 8 — F27: node autoprovisioning (provider drivers, Hetzner Cloud)

**Exit criterion:** with a configured `burst-eu` Hetzner pool, saturating the
cluster (pending replicas) makes the provisioner create a Hetzner server that
cloud-init-joins as a worker within minutes; sustained idle drains and
destroys it; `max`/budget rails hold; every provision/destroy is audited and
eventable. All of it verified against a fake Hetzner API (real-account
integration test optional and skipped by default).

Scope note: **driver interface + Hetzner Cloud driver only.** DigitalOcean/
AWS/Scaleway remain backlog. The core provisioner must never contain
provider-specific logic (spec §3.14).

### T-81 — NodePool model: protos, API, CLI

Phase 8 · Depends: T-12 · Size: M
**Files:** `api/proto/zattera/v1/provision.proto` (new),
`api/proto/zattera/cluster/v1/fsm.proto` (additive),
`api/proto/zattera/v1/node.proto` (additive), `api/gen` (regenerate),
`internal/state/accessors_infra.go` (extend), `internal/daemon/raftstore/apply.go`
(extend), `internal/daemon/api/pools.go`, `internal/cli/pools.go`, tests
**Steps:**

1. `provision.proto`: `NodePool{Meta, name, provider ("hetzner"), region,
server_type, min, max, cooldown (Duration), labels map, budget_monthly_eur
(double), dry_run bool, credential_id, disabled}` — mirrors spec §3.14's
   TOML; `CloudCredential{Meta, provider, name, token EncryptedValue}`;
   `ProvisionedMachine{Meta, pool_id, node_id, provider_machine_id,
provider_status, hourly_price_eur, created_at, phase enum
(CREATING/JOINING/ACTIVE/DRAINING/DESTROYING/FAILED)}`.
2. `fsm.proto` (NEW oneof numbers 260-269 — additive, never renumber):
   `PutNodePool`, `DeleteByID delete_node_pool`, `PutCloudCredential`,
   `DeleteByID delete_cloud_credential`, `PutProvisionedMachine`,
   `DeleteByID delete_provisioned_machine`. `node.proto`: add
   `string pool_id = 16` to `Node` (additive).
3. State store: three new collections with the standard Put/Delete/Get/List
   accessors (follow the existing pattern exactly: clone-on-read, touch with
   new Kinds `KindNodePool`, `KindCloudCredential`, `KindProvisionedMachine`)
   - `MachinesByPool(poolID)` linear filter; extend `Snapshot` proto (new
     field numbers 40-42) + `SnapshotProto`/`RestoreProto`/`resetLocked`.
4. FSM dispatch: six new cases in `apply.go` (one-liners).
5. API: new `ProvisionService` in `api.proto` (additive service): PutPool/
   ListPools/DeletePool, PutCredential/ListCredentials/DeleteCredential
   (token sealed server-side from a plaintext request field, admin-only in
   the T-04 table + T-05 rbac), ListMachines. REST annotations under
   `/v1/node-pools`, `/v1/cloud-credentials`.
6. CLI: `zattera pools ls/set/rm` (`pools set burst-eu --provider hetzner
   --region fsn1 --type cpx31 --min 0 --max 10 --cooldown 20m
   --budget-eur 150 [--dry-run]`), `zattera pools machines`,
   `zattera cloud-credentials add hetzner --token …`.
   **Gotchas:** run `make generate` and commit `api/gen`; snapshot round-trip
   test MUST be extended (internal/state) or restore silently drops the new
   collections; `min > 0` pools provision even when idle — validate `min ≤ max`,
   `max ≤ 50` hard cap; deleting a pool with live machines → refuse unless
   `--force` (machines then become unmanaged, warn loudly).
   **Tests:** unit — state accessors + snapshot round trip with the new
   collections; API CRUD + rbac (admin-only); credential token never returned
   unredacted.
   **Acceptance:** `make generate && git diff --exit-code api/gen` after commit;
   `go test ./internal/state/ ./internal/daemon/api/ -run
'TestSnapshot|TestPools'`

### T-82 — Provider driver interface + fake driver

Phase 8 · Depends: T-81 · Size: M
**Files:** `internal/daemon/provision/driver.go`, `fake.go`, `driver_test.go`
**Steps:**

1. The FROZEN interface (spec §3.14 — keep it minimal, provider-agnostic):
   ```go
   type MachineSpec struct {
       Name        string            // zt-<pool>-<ulid[:8]>
       Region      string
       ServerType  string
       CloudInit   string            // user-data (join script)
       Labels      map[string]string // provider-side labels for List
   }
   type Machine struct {
       ProviderID     string
       Name           string
       Status         string // normalized: "creating"|"running"|"deleting"|"unknown"
       PublicIPv4     string
       HourlyPriceEUR float64
       Labels         map[string]string
   }
   type Driver interface {
       Create(ctx context.Context, spec MachineSpec) (Machine, error)
       Destroy(ctx context.Context, providerID string) error       // idempotent: absent = success
       Get(ctx context.Context, providerID string) (Machine, error) // ErrMachineNotFound
       List(ctx context.Context, labelSelector map[string]string) ([]Machine, error)
       // PriceEURPerHour returns the hourly price for a server type in a
       // region (budget rail); 0 with nil error = unknown, rail falls back
       // to the price recorded at Create time.
       PriceEURPerHour(ctx context.Context, region, serverType string) (float64, error)
   }
   var ErrMachineNotFound = errors.New("provision: machine not found")
   ```
2. Registry: `provision.OpenDriver(provider string, cred *zatterav1.CloudCredential,
sealer secrets.Sealer) (Driver, error)` — switch on provider name;
   compiled-in drivers only (no plugins, spec §3.14).
3. `fake.go`: in-memory Driver for tests — scriptable latency, create
   failures, machines that never reach "running", quota errors; exposes
   `Machines()` snapshot for assertions (mirror the fakeruntime style).
   **Gotchas:** the interface is consumed by the provisioner loop ONLY — no
   provider types may leak upward; Destroy must be idempotent (the reconciler
   retries); normalize provider statuses in the driver, never in the core.
   **Tests:** unit — fake driver contract self-test (create→get→list→destroy,
   not-found semantics) so every real driver can reuse the same contract test
   via a shared `RunDriverContractTest(t, driver)` helper — write that helper
   here.
   **Acceptance:** `go test ./internal/daemon/provision/`

### T-83 — Hetzner Cloud driver

Phase 8 · Depends: T-82 · Size: M
**Files:** `internal/daemon/provision/hetzner.go`, `hetzner_test.go`
**Steps:**

1. Raw REST client against `https://api.hetzner.cloud/v1` (no SDK — surface
   is 4 endpoints; follow the dnsproviders/cloudflare.go pattern): Bearer
   token from the sealed credential; base URL injectable for tests.
2. `Create`: `POST /servers` with `{name, server_type, image:
"debian-12", location: spec.Region, user_data: spec.CloudInit, labels,
public_net: {enable_ipv4: true, enable_ipv6: false}}`; map response
   (`server.id` → ProviderID as decimal string, `server.public_net.ipv4.ip`,
   `server.status`); price from the create response
   (`server_type.prices[location].price_hourly.gross`) recorded into
   `HourlyPriceEUR`.
3. `Get`/`List` (`GET /servers/{id}`, `GET /servers?label_selector=k==v`
   comma-joined), `Destroy` (`DELETE /servers/{id}`; 404 → nil),
   `PriceEURPerHour` (`GET /server_types?name=…`, match location).
4. Status normalization: `initializing|starting → creating`,
   `running → running`, `deleting → deleting`, else `unknown`.
5. Rate-limit handling: on 429 honor `Retry-After` once, then error out (the
   reconciler retries next tick — never sleep-loop inside the driver).
   **Gotchas:** Hetzner label values are constrained (`[a-z0-9A-Z._-]`, ≤63) —
   sanitize pool names; server names must be RFC-1123 (lowercase, ≤63);
   `user_data` max 32KiB — the cloud-init template (T-84) must stay small;
   prices are strings in the API — parse as float carefully, `gross` not `net`;
   NEVER log the token (redact the Authorization header in any error paths).
   **Tests:** unit — `httptest` fake Hetzner API implementing the 4 endpoints
   (record requests, replay canned JSON from testdata/): run the T-82 contract
   test against it + assert request bodies (user_data passthrough, label
   selector encoding, 429 retry, 404-destroy idempotency). Optional real-API
   integration test behind `HCLOUD_TOKEN` env: `t.Skip` when unset — creates and
   destroys one cpx11, guarded by a `-run TestHetznerReal` name nobody types by
   accident.
   **Acceptance:** `go test ./internal/daemon/provision/ -run TestHetzner`

### T-84 — Provisioner: scale-up loop + cloud-init join

Phase 8 · Depends: T-83, T-17, T-29 · Size: L
**Files:** `internal/daemon/provision/provisioner.go`, `cloudinit.go`,
`pending.go`, `provisioner_test.go`; small extension in
`internal/daemon/scheduler/scheduler.go` (pending-placement signal)
**Steps:**

1. Pending signal (`pending.go` + scheduler extension): when T-23's
   evaluation cannot place replicas, record `{envID, count, constraints,
since}` in livestate (leader memory; cleared when placement succeeds).
   Expose `PendingPlacements()` to the provisioner. Also compute pool-wide
   utilization: sum of reservations / sum of ALIVE worker capacity.
2. Provisioner loop (leader-only via `leaderrunner`, 30s Clock tick):
   scale-up when, for ≥3 consecutive ticks: (a) pending placements exist
   whose `constraints` are satisfiable by a pool's labels+region, or (b)
   utilization > 85% and some pool has headroom. Pick the matching pool with
   the lowest hourly price.
3. Rails BEFORE any Create (evaluate in this order, emit a distinct event on
   each refusal): pool disabled → skip; live+creating machines ≥ pool.max →
   skip; projected monthly cost (Σ hourly_price × 730 over the pool's
   non-destroyed machines + the candidate's price) > budget_monthly_eur →
   skip; `dry_run` → emit `provision.dryrun` event with the full decision and
   skip.
4. Create path: mint a **single-use join token** (reuse T-12's creation,
   TTL 30m, roles [worker]) → render cloud-init (`cloudinit.go`: `#cloud-config`
   with a `runcmd` that installs Docker if absent, downloads the zattera
   binary — URL from config `provision.binary_url`, default the GitHub
   release for the running version — and runs `zattera server --join
<public-api-addr> --token <token>` with labels
   `autoprovisioned=true,pool=<name>,provider=hetzner,region=<r>` via a
   written config file) → `driver.Create` → `PutProvisionedMachine{CREATING}`
   - audit entry (actor `system:provisioner`) + `provision.created` event.
5. Machine reconciliation (same loop): CREATING machines → poll
   `driver.Get`; provider "running" → JOINING; a Node appears with matching
   pool label + join within 15m → link (`PutNode` with `pool_id`, machine →
   ACTIVE); timeout (15m from create) → destroy + FAILED + event
   `provision.join_timeout` (the single-use token is burned — expected).
   Machines in provider but not in state (orphans, e.g. leader died between
   Create and Put) → adopt by `List(labels)` at loop start, or destroy if
   unknown pool.
   **Gotchas:** the join address in cloud-init must be a PUBLIC control-plane
   address (`cfg.API.AdvertiseAddr` — validate it's set for pools to work,
   refuse `pools set` otherwise with a clear error); never store the join token
   in state beyond its hash (existing T-12 semantics); Create-then-crash is THE
   correctness hazard — the orphan adoption via provider labels
   (`zattera-cluster=<cluster-id>`, `zattera-pool=<name>`) makes the loop
   self-healing, so tag every machine at Create; all durable transitions via
   Apply, poll state ephemeral in livestate; failure to provision must degrade
   gracefully — pending replicas just wait (spec §3.14), no crash, no tight
   retry (min 5m backoff per pool after a Create error).
   **Tests:** unit (fake driver + fake clock + simcluster-style state): pending
   → create after 3 ticks; token minted single-use; rails: max, budget
   (projected math), dry-run event; join-timeout destroy; orphan adoption;
   Create error → backoff, no machine storm.
   **Acceptance:** `go test ./internal/daemon/provision/ -run TestScaleUp`

### T-85 — Scale-down: cooldown drain + destroy, rails, alerts

Phase 8 · Depends: T-84 · Size: M
**Files:** `internal/daemon/provision/scaledown.go`, `scaledown_test.go`;
default alert rules in `internal/daemon/notify/` (extend T-74's built-ins)
**Steps:**

1. Same leader loop: scale-down candidate when for the whole `cooldown`
   window (per pool, track low-watermark since-timestamps in livestate):
   utilization < 50% AND no pending placements AND pool has more than `min`
   ACTIVE machines.
2. Candidate selection: the ACTIVE autoprovisioned machine whose node has
   the fewest RUN assignments; **ineligible**: nodes with stateful/pinned
   volumes (any Volume.node_id == node), nodes not owned by a pool
   (manually joined nodes are NEVER touched — assert `pool_id != ""`).
3. Sequence (resumable via machine phase): machine → DRAINING +
   `DrainNode` (T-29 path, migrates stateless replicas) → node DRAINED →
   `RemoveNode` → `driver.Destroy` → DESTROYING → provider confirms gone →
   delete machine record + audit + `provision.destroyed` event. One
   scale-down in flight per pool at a time.
4. Drain stuck >30m → abort scale-down (node back to schedulable, machine
   ACTIVE, event `provision.drain_aborted`) — capacity crunches mid-drain
   must self-cancel.
5. Alerts: add built-in default rules `provision.join_timeout`,
   `provision.budget_exceeded`, `provision.drain_aborted` → default channel
   wiring like T-74's built-ins.
   **Gotchas:** leader failover mid-sequence: every step is re-derivable from
   `ProvisionedMachine.phase` + node status — write the resume switch first,
   then the happy path; never Destroy before RemoveNode succeeds (a destroyed
   machine with a live raft/node entry leaves a ghost DOWN node); cooldown
   timers live in leader memory — restart the window on failover
   (conservative, same as T-61).
   **Tests:** unit — cooldown window with fake clock; candidate excludes
   volume-pinned and manual nodes; full sequence walk incl. resume-from-phase
   after simulated failover; drain-stuck abort; min floor respected.
   **Acceptance:** `go test ./internal/daemon/provision/ -run TestScaleDown`

### T-86 — Provisioning verification sweep + docs

Phase 8 · Depends: T-84, T-85 · Size: M
**Files:** `test/chaos/provision_test.go`,
`docs/guides/node-autoprovisioning.md`, `paas-specification.md` (§3.14 +
roadmap touch-up), `internal/cli/pools.go` (status polish)
**Steps:**

1. Chaos scenario (fake driver, simcluster 3 controls + fake worker agents):
   saturate → machine created and "joins" (test injects the node) → work
   places → idle → cooldown → drain → destroy; kill the leader once during
   scale-up and once during scale-down — end state converges with zero
   orphan machines and zero ghost nodes (assert via fake driver + state).
2. Budget storm test: pool max 10, budget allows 2 → exactly 2 created,
   `provision.budget_exceeded` event emitted once per window (not per tick).
3. `zattera pools ls` shows live columns (machines active/creating, projected
   €/month, last action); `zattera pools machines` phases.
4. Docs page: pool setup walkthrough (credential → pool → watch it scale),
   rails explanation, the "manually joined nodes are never touched"
   guarantee, cost caveats. Spec: update §3.14 heading from "(F27, future)"
   to reflect Hetzner-first availability; move the remaining providers note
   to the roadmap.
   **Acceptance:** `go test -tags chaos ./test/chaos/ -run TestProvision
-timeout 20m`; docs build (plain markdown, no tooling yet); spec diff
   reviewed in the PR.

---

# Phase 9 — Follow-ups

### T-91 — Never lose short-lived job logs (harvest-on-exit)

Phase 9 · Depends: T-53, T-40 · Size: S
**Problem:** job logs are captured only by `agent.LogCapture`, which polls every
3s and starts a follower _only_ for containers observed `Running`
(`internal/daemon/agent/logcapture.go`). A one-shot job (T-53) whose whole
lifetime falls inside a 3s poll gap is never seen running, so no follower ever
starts; the scheduler then reaps the assignment and the executor
force-`RemoveContainer`s it (`internal/daemon/agent/executor.go` `stopAndRemove`)
without reading its output. Result: sub-~3s / sub-second job logs are lost
non-deterministically. Builds don't have this (they stream `BuildEvent`s
synchronously); jobs must not either. Invariant: **a job's stdout/stderr is
never lost, regardless of how briefly it runs.**
**Files:** `internal/daemon/agent/executor.go`,
`internal/daemon/agent/logcapture.go` (or a small helper),
`internal/daemon/agent/executor_test.go`
**Steps:**

1. Harvest-on-exit: before the executor removes a container that carries a
   `job_id` (in `stopAndRemove`, and on the exit path in `pollLiveness` once a
   job container is observed STOPPED/FAILED), do a synchronous non-follow read
   `rt.Logs(ctx, id, LogsOptions{Follow:false})` and `store.Append` every entry
   to the assignment's stream. Docker retains the json-file until removal, so
   this captures the full output even when no follower ran. Guard against
   double-capture when a follower _did_ run (dedupe by (time,stderr,line) tail,
   or only harvest when no follower was ever registered for the assignment —
   ask LogCapture).
2. Make harvest idempotent + ordered: harvested entries must land before the
   container is removed; never block the reconcile loop for more than a bounded
   read timeout (e.g. 2s) — on timeout, log a warning and proceed with removal
   (best-effort, matching the logstore's fsync-less contract).
3. Optional hardening (do only if step 1 proves racy): start the follower
   event-driven in `bringUp` at container creation instead of relying on the 3s
   poll, so the running window is never missed for services either.
   **Gotchas:** don't harvest twice (follower + harvest) — pick a single owner per
   assignment; the stream key is the **assignment id**, not `job/<id>` (the
   `job/<id>` convention in `logstore/store.go` is documented but unused — either
   wire it or fix the comment); force-remove must not race the harvest read
   (harvest, then remove); keep it best-effort so a hung `docker logs` never
   wedges the executor.
   **Tests:** unit — a fake runtime whose job container exits before any
   LogCapture poll could observe it `Running`; assert every emitted line is present
   in the logstore after reap. Add a sub-second job case and a
   job-that-outlives-a-poll case (must not duplicate lines).
   **Acceptance:** `go test ./internal/daemon/agent/ -run TestJobLog`

---

### T-96 — `zt nodes label`: set node labels from the CLI

Phase 9 · Depends: T-12 · Size: XS
**Problem:** placement constraints (`[env.<name>.placement]` in `zattera.toml`,
`appconfig.go`) match node labels, and `SetNodeLabels` is implemented and
admin-gated (`internal/daemon/api/nodes.go`, `PUT /v1/nodes/{id}/labels`) — but
nothing in `internal/cli/nodes.go` calls it. Nodes only ever have the two labels
they self-assign at boot (`zattera.dev/os-arch`, `builder=true`), so the
documented "pin this environment to `region=eu`" workflow is unreachable
without hand-rolling an API call. Either the feature is complete or it isn't;
right now the server half ships without a client.
**Files:** `internal/cli/nodes.go`, `internal/cli/nodes_test.go`,
`docs/setup/nodes.md`, `docs/cli/reference.md`
**Steps:**

1. `zt nodes label <name> key=value [key=value…]` — resolve the name via
   `resolveNodeID`, merge onto the node's existing labels (read-modify-write via
   `GetNode`), call `SetNodeLabels`. `key-` removes a key, mirroring kubectl.
2. `--overwrite` to replace the whole set rather than merge; without it,
   refuse to change an existing key so a typo can't silently repoint placement.
3. Reject the reserved `zattera.dev/` prefix — those are node-asserted facts,
   and letting an operator overwrite `os-arch` would misplace multi-arch images.
   `builder` stays writable (opting a worker out of builds is legitimate).
4. Print the resulting label set; honor `--json` like the other node commands.

**Gotchas:** `SetNodeLabelsRequest` also carries `schedulable`, which
cordon/uncordon own — send the node's current value or you will silently
uncordon a cordoned node. Labels are the scheduler's input, so a placement
constraint that matches nothing must fail the deploy loudly rather than park
replicas forever (check what the scheduler does today before shipping).
**Tests:** unit — merge vs `--overwrite`, `key-` removal, reserved-prefix
refusal, existing-key refusal without `--overwrite`, and that `schedulable` is
preserved for a cordoned node.
**Acceptance:** `go test ./internal/cli/ -run 'TestParseLabelArgs|TestMergeLabels'`
**Docs:** remove the "custom labels are API-only" callout from
`docs/setup/nodes.md` and document the command there and in the CLI reference.

**DONE** — `zt nodes label <name> KEY=VALUE|KEY- [--overwrite]` in
`internal/cli/nodes.go`. Merge by default, `--overwrite` to change an existing
key, `KEY-` to remove, `zattera.dev/*` refused on both set and remove (also
under `--overwrite`), `builder` left writable, `--json` emits the resulting map.
The schedulable gotcha was real: `SetNodeLabels` assigns `n.Schedulable =
req.GetSchedulable()` unconditionally, so the command echoes the node's current
value — verified on a dev cluster that a cordoned node stays `ALIVE,CORDONED`
across a labeling call.

**Correction to this task's premise:** step 4 claimed an unsatisfiable
constraint parks replicas forever with no error. It does not — `Place` already
returns "only 0 of N replicas placeable (constraints/capacity)" at the end.
What was actually wrong is that the message could not distinguish a label typo
from a full cluster. `placement.go` now tracks `labelRejected` alongside
`archRejected` and returns `no node matches constraints region=eu`; the generic
capacity error is unchanged, and a test asserts a matching-but-full node does
**not** produce the constraint message.
**Tests:** `internal/cli/nodes_test.go` (arg parsing incl. duplicate-key and
`KEY=` empty-value, merge/overwrite/remove, reserved-prefix refusal, current-map
not mutated, idempotent re-set allowed);
`internal/daemon/scheduler/placement_test.go`
(`TestUnsatisfiablePlacementConstraintErrors`,
`TestPlacementConstraintSatisfiedByFullNode`).

---

### T-97 — Node os-arch must describe the container runtime, not the daemon binary

Phase 9 · Depends: T-87, T-88 · Size: S
**Problem:** `registerLocalNode` and the join path both set `OsArch:
platform.Local()` (`internal/daemon/daemon.go`, `internal/daemon/join.go`),
which is the *daemon binary's* `runtime.GOOS/GOARCH`. Containers do not run in
the daemon — they run in Docker. On macOS the daemon is `darwin/arm64` while
Docker Desktop runs `linux/arm64`, so since arch-aware placement (T-88) landed,
**`--dev` on macOS cannot deploy anything at all**: every release resolves to
`linux/*` platforms, `platform.Supports("darwin/arm64", …)` is false for all of
them, and placement fails with "no node with a supported architecture (need one
of linux/arm64)". Reproduced on a dev cluster with both a prebuilt image and a
locally built one. This breaks the Local dev mode path in
`docs/getting-started/quickstart.md`, which is the documented first-run
experience. Linux nodes are unaffected (daemon and containers share a kernel),
which is why CI and the cloud tests never saw it.
**Files:** `internal/daemon/nodeinfo/nodeinfo.go` (or a new helper),
`internal/daemon/daemon.go`, `internal/daemon/join.go`,
`internal/daemon/runtime/` (runtime info accessor), tests alongside
**Steps:**

1. Ask the container runtime for its platform — Docker's `/info` reports
   `OSType` and `Architecture` (`docker version --format
   '{{.Server.Os}}/{{.Server.Arch}}'` shows `linux/arm64` on macOS). Normalize
   through the existing `platform` alias table (`aarch64` → `arm64`, …).
2. Use that for the node's `OsArch` **and** the `zattera.dev/os-arch` label, in
   both the bootstrap path and the join path, so a node advertises what it can
   actually execute.
3. Fall back to `platform.Local()` when the runtime is unreachable, and log at
   WARN when the two disagree — a silent mismatch is what made this hard to see.
4. Re-check `platform.Supports` callers for anywhere else that conflates the two.

**Gotchas:** the value is written at registration, so an existing dev cluster
keeps the stale `darwin/*` until the node re-registers — make registration
idempotently refresh it rather than only setting it on first boot. Don't break
the multi-arch scheduling tests, which construct nodes with explicit `OsArch`.
**Tests:** unit — a fake runtime reporting `linux/aarch64` yields `linux/arm64`;
runtime-unreachable falls back to `platform.Local()`; a mismatch logs. Add a
placement regression: a node whose runtime is `linux/arm64` accepts a
`linux/arm64` release even when the daemon is `darwin/arm64`.
**Acceptance:** `go test ./internal/daemon/... -run 'OsArch|Platform'`, plus
`zt deploy --image nginx:alpine --prod` succeeding against `zattera server
--dev` on macOS.

**DONE** — `runtime.EnginePlatform` / `(*Docker).Platform` query the engine's
`Info` (`OSType/Architecture`); `nodeOsArch` (internal/daemon/osarch.go)
normalizes through `platform.Normalize`, falls back to `platform.Local()` with
a WARN when the engine is unreachable or unparseable, and logs the
darwin-vs-linux divergence at INFO. Wired into both `registerLocalNode` and the
join request. Verified on macOS: the node now advertises
`zattera.dev/os-arch=linux/arm64` and `zt deploy --image nginx:alpine --prod`
releases healthy on a `--dev` node (it failed placement before).
**Beyond the plan:** re-registration preservation. `PutNode` is a wholesale
replace and `registerLocalNode` re-runs on every daemon restart, so refreshing
os-arch idempotently (the task's gotcha) would have kept wiping operator state:
custom labels (T-96) and the cordon flag (`Schedulable: true` unconditional).
Re-register now carries over non-self-asserted labels and the schedulable flag
while refreshing os-arch/capacity/version.
**Found while verifying (filed, not fixed):** T-98 — the health monitor
resolves its probe target once at registration and never re-inspects, so a
stateful second deploy on macOS deterministically times out (`could not
resolve a probe target; skipping` every tick). The failure path itself behaved
as documented: old instance restarted, `deploy.rolled_back` emitted.
**Tests:** `internal/daemon/osarch_test.go` (aarch64 normalization,
unreachable fallback + WARN, unparseable fallback + WARN, placement regression
with engine arch vs binary arch); `internal/daemon/register_test.go` (os-arch
field + label from the engine; re-register preserves custom labels, cordon and
created-at while refreshing stale os-arch).

---

### T-98 — Health monitor never re-resolves a probe target (stateful dev deploys always fail)

Phase 9 · Depends: T-63, T-97 · Size: S
**Problem:** on macOS (`useHostPort`, `health.go`) the health monitor resolves
its probe target from the container's published host port ONCE, at monitor
registration (`health.go` ~260–293). If the inspect happens before Docker has
reported the dynamic binding (`HostPort: 0` until after Start, `executor.go:417`),
resolution fails and the monitor logs `health: could not resolve a probe
target; skipping` **on every tick forever** — it never re-inspects. The
instance stays RUNNING, never HEALTHY, and the deploy times out. Reproduced
deterministically (2/2) with a stateful `redis` second deploy on a `--dev`
macOS node after T-97: `deploy.maintenance_start` fires, green starts and its
port IS published (visible in `docker ps`), but the monitor skips 150+ probes
and `statefulHealth` fails the deploy — which then correctly restarts the old
instance (`deploy.rolled_back`). First deploys are less exposed (slower pull
widens the gap) but the race is generic; Linux is unaffected only because it
probes the container IP, which exists from creation.
**Files:** `internal/daemon/agent/health.go`, `internal/daemon/agent/health_test.go`
**Steps:**

1. Make target resolution retryable: when `probeTarget` returns ok=false,
   don't bake the failure in — re-inspect the container on each probe tick
   (or until first success) instead of resolving once at registration.
2. Keep the WARN, but log it once per state change, not per tick (152
   identical lines in one deploy is noise that buries the signal).
3. Regression test with a fake runtime whose ContainerState reports no port
   binding on the first inspect and the binding on the second: the monitor
   must go HEALTHY, not skip forever.

**Gotchas:** don't re-inspect on every tick after a successful resolve (extra
Docker load for nothing); the EXEC probe path doesn't need a target — leave it
alone. The monitor's mutex is held around registration (`m.mu`), keep the
re-resolve outside it.
**Acceptance:** `go test ./internal/daemon/agent/ -run TestHealth`, plus a
stateful image bump on a macOS `--dev` node reaching `deploy.maintenance_end`.

**DONE — with a corrected diagnosis.** The task's premise ("resolves once,
never re-inspects") was wrong: `Ensure` never registers a monitor on a failed
resolve, so every reconcile tick already retried it. Instrumenting the repro
showed the green container was **crash-looping** — the "second deploy" test
image (`redis:7.2-alpine`) cannot read the RDB written by `redis:7-alpine`
(which is 7.4), so the container died in Docker's restart backoff. The real
bug: Docker keeps `State.Running=true` while `Restarting`, with no ports
published, so `pollLiveness` reported the instance RUNNING — the deployment
advanced to HEALTHCHECKING and hung until the deadline, and the health prober
warned every tick against a container that publishes nothing.
Fixes:
- `ContainerState.Restarting` mapped from Docker inspect; `pollLiveness`
  checks it BEFORE `Running` and reports FAILED with "crash-looping: last
  exit code N" — a crash-looping green now fails the deploy in seconds
  (measured 6s vs the 3-minute deadline) and the stateful path restarts the
  previous instance immediately.
- The unresolvable-target warn logs once per episode (tracked per assignment,
  cleared on resolve/remove) instead of 150+ identical lines per deploy.
- `Executor.fail` now logs every attempt: the observed message is reaped with
  the assignment, so the node log is where a failed deploy's real reason
  survives. (This logging is how the crash loop — and a stale-network overlap
  masking it — was found at all.)
**Not done:** dev-mode probes were healthy in all observed cases once the
container actually ran; no change to probe timing.
**Tests:** `TestPollLivenessCrashLoop` (Restarting→FAILED with exit code,
recovery→RUNNING again); `TestEnsureUnresolvableTargetRetriesAndWarnsOnce`
(no monitor while unresolvable, one warn per episode, resolves after recovery,
new episode warns again); fakeruntime gained `SetRestarting` mimicking
Docker's Running=true/no-ports backoff state. Verified live on macOS `--dev`:
crash-looping bump fails in ~6s with `deploy.rolled_back` and the old
instance back healthy; a good bump still promotes.

---

### T-99 — A declared port block without `container_port` must be rejected

Phase 9 · Depends: T-31 · Size: XS
**Problem:** `ports()` in `internal/appconfig/appconfig.go` defaults the whole
`[[env.<name>.ports]]` array to `http/8080` when it is absent, but applies **no
default to an incomplete entry**: a block that omits `container_port` yields
`ContainerPort: 0`. Nothing rejects it. Verified end to end on a dev cluster —
`zt apply` prints `✓ Applied`, the deploy then starts a container with nothing
routable, the health probe cannot resolve a target, and the deploy dies at the
health deadline with `green instances did not become healthy in time`. The user
gets a timeout, minutes later, for a one-line typo. Every other required field
in this file (`[app] name`, volume `name`/`mount_path`, cron `schedule`) fails
fast at parse time; this one should too.
**Files:** `internal/appconfig/appconfig.go`, `internal/appconfig/appconfig_test.go`,
`docs/deploy/zattera-toml.md`
**Steps:**

1. In `ports()`, return an error when a declared entry has
   `container_port == 0`: `env.<name>.ports[<i>]: container_port is required`.
   Match the existing message style (path + field + reason).
2. Keep the absent-array default (`http/8080`) exactly as is — that is the
   documented convenience and is widely relied on.
3. Consider the same treatment for a port whose name duplicates another in the
   same environment (routing keys `mesh_port_bindings` by name, so duplicates
   silently collide — check before deciding).

**Gotchas:** `ports()` currently has no error return; threading one changes
`serviceSpec`'s signature usage. `defaultServiceSpec` in
`internal/daemon/api/apps.go` constructs ports directly and must keep working.
Don't reject `container_port == 0` on specs arriving over the API from older
clients unless you also bump validation there deliberately.
**Tests:** parse-level — a declared block without `container_port` errors and
names the env + index; the absent array still yields `http/8080`; an explicit
`container_port` is untouched.
**Acceptance:** `go test ./internal/appconfig/ -run TestPorts`
**Docs:** replace the "no default — required" note in
`docs/deploy/zattera-toml.md` with the fail-fast rule once this lands.

---

### T-100 — Build dispatch ignores cordon (and always picks the same node)

Phase 9 · Depends: T-35, T-95 · Size: S
**Problem:** `pickBuilder` (`internal/daemon/scheduler/builds.go`) selects the
lowest-id node with `status == ALIVE` and `labels["builder"] == "true"`. It
never consults `Schedulable`, so a **cordoned node still receives builds** —
directly contradicting what cordon promises ("stop scheduling new work on a
node") and what `zt cluster upgrade` relies on: the upgrade cordons a node
before swapping its binary, and a build dispatched into that window runs
against a daemon that is about to restart. The same function also makes build
placement degenerate: lowest-id-wins means one machine runs every build, so
adding workers never adds build capacity, and a busy builder is never relieved.
**Files:** `internal/daemon/scheduler/builds.go`, `internal/daemon/scheduler/builds_test.go`,
`docs/deploy/builds.md`, `docs/setup/nodes.md`
**Steps:**

1. Skip unschedulable nodes in `pickBuilder`: require
   `n.GetSchedulable()` alongside ALIVE + `builder=true`. A cordoned node must
   not be handed new builds.
2. If no schedulable builder exists, leave the build QUEUED (the dispatcher
   already retries) rather than failing it — cordoning the only builder should
   delay a build, not break the deploy. Emit an event once so it is visible.
3. Replace lowest-id with a load-aware pick: fewest in-flight builds first,
   then lowest id as the deterministic tie-break. Keep it deterministic —
   the dispatcher runs on the leader and must not flap between ticks.
4. Consider (do not implement blindly) whether the layer-cache locality of
   sticky placement outweighs spreading; if it does, document that choice
   instead of changing step 3.

**Gotchas:** the drain path already stops workloads but builds are not
assignments, so a draining node needs the same exclusion. Don't break the
single-node/dev case, where the only node is both control and builder — it
stays schedulable, so it keeps working. `cluster upgrade` cordons then
uncordons; make sure a build queued during the cordon window is picked up
after uncordon without operator action.
**Tests:** unit — a cordoned builder is skipped; with no schedulable builder
the build stays QUEUED and is dispatched after uncordon; with two idle
builders the one with fewer in-flight builds wins, ties broken by id.
**Acceptance:** `go test ./internal/daemon/scheduler/ -run TestBuildDispatch`
**Docs:** drop the "cordoning a node does not stop builds landing on it" and
"builds are not spread across nodes" notes from `docs/deploy/builds.md` once
this lands.

**DONE** — `pickBuilder` now requires `Schedulable` alongside ALIVE +
`builder=true`, and orders candidates by RUNNING builds on each node (counted
from replicated state, so the view survives a failover) with the node id as
tie-break.
**Decision on step 4:** kept cache locality *and* got spreading, because the
(in-flight, id) order gives both — with every builder idle the tie-break picks
the same node every time, so its layer cache stays warm; work only spills once
the preferred node is actually building. No separate stickiness mechanism was
needed.
A build with no schedulable builder stays QUEUED and emits
`build.waiting_for_builder` **once per build** (the dispatcher retries every
15s; without the dedupe a long cordon would append an event per tick — the
T-98 lesson). The node-log warning is deduped the same way. Verified live:
deploying against a cordoned single-node cluster left the build queued with
buildkitd never starting, one event emitted, and `nodes uncordon` alone let
the build run to completion with no redeploy.
**Fixture correction:** `seedBuilder` never set `Schedulable`, so every builds
test ran against a node that production would never produce
(`registerLocalNode` always sets it). Two existing tests failed on the new
rule; the fixture was fixed to mirror production rather than loosening the
rule.
**Tests:** `TestBuildDispatchSkipsCordonedBuilder` (queued + no dial + event,
then dispatches after uncordon — confirmed to FAIL without the schedulable
check), `TestBuildDispatchPrefersIdleBuilder` (idle→sticky, busy→spill,
equal→deterministic tie-break).

---

### T-101 — Registry pull-through between control nodes

Phase 9 · Depends: T-32, T-55 · Size: M
**Problem:** registry blobs are node-local by design (`internal/daemon/registry`,
"NOT in raft"), which is the right storage decision — layers do not belong in a
consensus log, and it keeps a 5 GB image costing 5 GB once rather than once per
control node. But nothing reconciles *which* control node holds a blob with
*which* one a node asks:

- `registryClientAddr` returns each node's **own** address
  (`internal/daemon/daemon.go`).
- The build dispatcher runs on the **leader** and pushes to the leader's
  registry (`BuildDispatcherConfig.RegistryAddr`).
- The join server hands a joining node the registry address of **whichever
  control node served the join** (`internal/daemon/api/join.go`), and the node
  persists it in `mesh.json` and never refreshes it.

So on a multi-control cluster a worker can be pointed at a control node that
never received the push, and leadership changes scatter images across nodes over
time. Losing a control node makes the images only it held unpullable — raft
quorum keeps the control plane alive, not the image store. Single-control
clusters are unaffected, which is why this has not bitten yet.

**Decision (agreed with the user):** keep blobs node-local and make any control
node able to *serve* any blob by fetching it from a peer on demand.
Storage stays ~1×; the cost is one extra hop on a cold pull. This is the
default, always-on behaviour, with no configuration. The optional durable
backend is T-102 and layers on top.

**Files:** `internal/daemon/registry/{httpapi,blobstore,pullthrough}.go` (new),
`internal/daemon/registrywiring.go`, `internal/daemon/registry/*_test.go`,
`docs/deploy/builds.md`,
`docs/contributing/architecture-decision-records/0005-registry-blob-locality.md` (new)
**Steps:**

1. **Peer resolution.** Add a `PeerSource` the registry can ask for the other
   control nodes' registry addresses — derived from state
   (`ListNodes` → control role → mesh IP + registry port), never a static list,
   so it follows joins and removals. Exclude self.
2. **Blob pull-through.** In `Handler.blob`, when `store.Stat(dgst)` returns
   `ErrBlobUnknown`, try each peer in turn: `HEAD /v2/<repo>/blobs/<dgst>`, then
   `GET` and stream it into the local `BlobStore.Write` (which already
   digest-verifies and commits atomically) while serving the same bytes to the
   client. Serve from local storage on the retry path so a second pull is local.
   Cache the fetched blob — the node that pulled it now has it, which is what
   makes repeated pulls cheap and gradually heals the cluster.
3. **Manifest pull-through.** Same treatment in `Handler.manifest`: a manifest
   is a blob plus tag/refcount bookkeeping, so fetch it, then register it via
   `Manifests` so the local refcount graph and GC stay correct. **Do not** blindly
   copy tags — fetch by digest, and only bind a tag locally when the request was
   by tag and the peer's tag resolves to that digest.
4. **Auth between control nodes.** Peer fetches must authenticate as this node
   (`node-<id>` credential, already minted at join and validated by
   `registryAuthenticator`), over the mesh with the cluster CA. Never anonymous,
   never a user PAT.
5. **Bounded and safe.** Per-peer timeout, a total deadline shorter than the
   Docker pull timeout, and a concurrency cap so one cold image cannot open a
   fetch per layer per peer. Failure to reach a peer must fall through to the
   next and finally return the normal OCI `BLOB_UNKNOWN`, never a 500.
6. **Do not** fetch through to external registries (Docker Hub et al.) — this is
   strictly intra-cluster. That is a separate feature with different security
   properties.
7. Write **ADR-0005** recording why blobs stay node-local (consensus log vs
   layer size, storage amplification) and why pull-through was chosen over
   replicate-all / always-address-the-leader.

**Gotchas:** the fetch path must not deadlock the write path — `BlobStore.Write`
streams to a temp file and renames, so serving while writing needs either a
tee into the response or a write-then-serve-local (prefer the simpler
write-then-serve unless a cold pull's latency proves unacceptable). A blob
being uploaded concurrently by a push must not be half-served: only serve after
commit. GC (`refDec`) must not delete a blob another node is mid-fetch — check
whether the existing refcount covers a pulled-through blob with no local
manifest yet, and give it a local reference if not. Dev mode is single-node:
pull-through must be a no-op with zero peers, not an error.
**Tests:** unit — a two-registry harness where node B serves a blob node A
lacks: A's GET succeeds, A now `Has` the blob, a second GET never touches B;
digest mismatch from a peer is rejected and not committed; no peers → clean
`BLOB_UNKNOWN`; peer unreachable → next peer tried; manifest pull-through
registers refcounts so a later GC does not orphan blobs. Integration in
`test/cloud` if a multi-control rig is available: build on the leader, pull
from a worker joined via a follower.
**Acceptance:** `go test ./internal/daemon/registry/ -run 'PullThrough|Blob'`,
plus a 3-control cloud cluster where an image built on the leader pulls
successfully through a follower's registry.
**Docs:** replace the "Blobs are node-local, not replicated" warning in
`docs/deploy/builds.md` with the real behaviour: still one copy, any control
node can serve it, and the durability caveat that remains until T-102.

**DONE** — `internal/daemon/registry/pullthrough.go`: a `Fetcher` with a
state-derived `PeerSource` (control nodes, ALIVE, excluding self — resolved per
call so it follows joins/removals), wired into the blob and manifest miss paths
in `httpapi.go` and enabled from `daemon.go` with the node's existing
`node-<id>` registry credential over the cluster CA.
Blobs commit before serving (`BlobStore.Write` digest-verifies and renames
atomically), so a failed transfer never half-serves; a peer whose bytes do not
match the requested digest is dropped, not committed. Manifests fetch their
children and blobs first, then go through `PutManifest`, so the refcount graph
stays complete; a tag is bound locally only when the request was by tag.
Concurrent requests for one digest collapse via single-flight, plus a
concurrency cap, per-peer HEAD probe, and per-attempt/total deadlines.
**Beyond the plan:** the write path needed no tee — committing then serving
locally was both simpler and safer than streaming while writing, so the gotcha
about serving mid-write resolved by construction.
**GC gotcha checked, not just assumed:** a pulled-through blob carries no
refcount until a manifest references it. Today's GC only sweeps from manifest
teardown, so it is not treated as garbage — verified by test. ADR-0005 records
that any future mark-and-sweep over the blob directory must account for it.
**Not done:** the 3-control cloud verification in the acceptance criteria.
Local dev is single-node, so the multi-node path is covered by the two-registry
harness (real HTTP handlers, real auth) rather than real infra; run the cloud
rig before trusting it in production.
**Tests:** `internal/daemon/registry/pullthrough_test.go` — fetch+commit+serve,
second pull never touches the peer, zero peers is a clean 404 (single-node
no-op), unreachable peer falls through to the next, digest mismatch commits
nothing under either digest, manifest pull-through brings config+layers and
registers the graph, 8 concurrent pulls collapse to one fetch, an
authenticated peer rejects anonymous and accepts the node credential, and an
unrelated sweep does not delete a pulled-through blob.
Also verified on a dev cluster that the single-node path is untouched: build,
push and pull succeed with zero peers resolved and no pull-through log lines.

### T-102 — Optional S3-backed registry blob store

Phase 9 · Depends: T-101, T-64 · Size: M
**Problem:** with T-101 any control node can *serve* any blob, but every blob
still exists only on the node(s) that happened to fetch it. Losing a control
node's disk still loses images, and backups (`internal/daemon/backup`) cover
state, CA and volume snapshot refs — **not** registry blobs. Clusters that want
real image durability should be able to put blobs in the same object store they
already use for volume snapshots and backups.

**Decision (agreed with the user):** an **optional, opt-in** backend a cluster
can adopt later — the default stays local disk + pull-through, and enabling S3
must be an upgrade path, not a migration cliff.

**Files:** `internal/daemon/registry/blobstore.go` (extract an interface),
`internal/daemon/registry/s3blobs.go` (new), `internal/config/config.go`,
`internal/daemon/registrywiring.go`, `docs/setup/configuration.md`,
`docs/deploy/builds.md`
**Steps:**

1. Extract the blob backend behind an interface (`Has/Stat/Open/Write/Delete`
   — the methods `httpapi.go` and `Manifests` already use), with the current
   on-disk store as the default implementation. No behaviour change.
2. Add an S3-backed implementation reusing `volumes.ObjectStore` /
   `volumes.NewS3Store` — the S3 client, config shape and credentials handling
   already exist for snapshots; do not write a second one. Key blobs by digest
   (`blobs/sha256/<hex>`) so the store stays content-addressed.
3. Config: `[registry] backend = "local" | "s3"` (default `local`) plus the S3
   connection settings, mirroring how backups are configured. Sealed
   credentials, same as the backup destination.
4. **Local read cache.** An S3 backend must still cache blobs on local disk
   after first fetch — pulling multi-hundred-MB layers from S3 on every
   container start is not acceptable. Bound the cache and evict by LRU.
5. **Adoption path.** Switching an existing cluster from `local` to `s3` must
   upload the blobs it already has rather than orphaning them: a one-shot
   migration (`zt registry migrate`, or automatic on first boot with the new
   backend) that walks the local store and puts anything missing. Idempotent and
   resumable — it moves gigabytes.
6. Decide and document what GC means with a shared backend: refcounts are
   currently per-node bbolt, but with one shared bucket two nodes must not
   independently delete a shared blob. Either move the refcount graph to raft
   (small, unlike blobs) or make S3-mode GC leader-only. **Ask before
   choosing** — it changes the consistency story.

**Gotchas:** S3 is not POSIX — `Open` returning an `*os.File` is a leaky
signature, so the interface extraction in step 1 must return `io.ReadSeekCloser`
(`http.ServeContent` needs seeking, or the handler switches to a ranged read).
Multipart upload is required for large layers. Do not let an S3 outage take the
whole registry down for images already cached locally. Air-gapped clusters must
keep working on the default backend — this feature must never become required.
**Tests:** the interface's contract suite run against both backends; migration
is idempotent and resumable; a cached blob is served without an S3 round-trip;
S3 unreachable serves cached blobs and fails cleanly for the rest.
**Acceptance:** `go test ./internal/daemon/registry/ -run 'Backend|S3'`, plus a
cluster switched from local to s3 that still pulls every pre-existing image.
**Docs:** `[registry] backend` in the configuration reference; a "durable image
storage" section in `docs/deploy/builds.md` explaining the trade (default =
zero dependencies, opt-in = survives losing a control node).

---

### T-103 — `zt env set --from-file` (load a .env) and round-trippable `env pull`

Phase 9 · Depends: T-12 · Size: S
**Problem:** `zt env set` accepts only positional `KEY=VALUE` args
(`internal/cli/env.go`), so uploading a `.env` is left to the shell — and the
shell cannot do dotenv semantics. Verified on a dev cluster:

- `zt env set $(cat .env | xargs)` fails on `GREETING="hello world"` with
  `invalid KEY=VALUE: "world"`, because xargs splits on spaces.
- Splitting on newlines instead works, but passes lines through literally:
  `QUOTED="hello world"` is stored **with the quotes**, `export FOO=bar`
  becomes a key literally named `export FOO`, and inline `# comments` survive
  into the value. All silent — the command reports success.
- Multi-line values (PEM keys) cannot be expressed at all, and `zt env pull`
  prints them across lines, so its output does not round-trip back into
  `env set`.

Secrets that are silently wrong are worse than an error, and "paste your
`.env`" is the single most common way a real app's config arrives.
**Files:** `internal/cli/env.go`, `internal/cli/env_test.go`,
`docs/deploy/environment-variables.md`, `docs/cli/reference.md`
**Steps:**

1. `zt env set --from-file <path>` (and `-` for stdin, so `cat .env | zt env
   set --from-file -` works). Combinable with positional `KEY=VALUE` args,
   which win on conflict — the explicit argument beats the file.
2. Parse dotenv properly in one small, tested helper: skip blank lines and
   `#` comments, strip an optional leading `export `, trim surrounding single
   or double quotes, honour `\n`/`\"` escapes inside double quotes, keep `=`
   inside values, and reject a line with no `=` naming the file and line
   number. Do **not** do variable interpolation (`${FOO}`) — silently
   expanding one secret into another is a footgun; reject it or leave it
   literal, and document which.
3. Multi-line values: support the common `KEY="line1\nline2"` escaped form.
   Decide whether to also support real embedded newlines between quotes —
   only if the parser stays simple.
4. `--dry-run` printing the keys that would be set (count + names, never
   values) so an operator can check a file before it reaches the cluster.
5. Make `zt env pull --reveal` round-trip: quote values that contain spaces,
   quotes, `#` or newlines, so `zt env pull --reveal > .env` followed by
   `zt env set --from-file .env` is lossless. Add a test that asserts exactly
   that round-trip.
6. Consider `--replace` (delete keys absent from the file) — useful for
   "make the environment match this file", dangerous by default, so opt-in
   only. **Ask before adding.**

**Gotchas:** never echo values in success output or errors — the existing
`Set %d variable(s)` phrasing is right, keep it. A file with CRLF line endings
must not leave `\r` at the end of every value (Windows users, and it is
invisible when it breaks). Reading from stdin conflicts with `--json`
consumers, so keep stdout clean. The env family requires `--app` (it does not
read `zattera.toml`); do not change that here.
**Tests:** parser table — comments, blanks, `export `, single/double quotes,
escapes, `=` in value, empty value, CRLF, missing `=` errors with line number,
`${VAR}` handling; precedence of positional args over file entries;
`--dry-run` prints names and not values; pull→set round-trip preserves spaces,
quotes and newlines exactly.
**Acceptance:** `go test ./internal/cli/ -run TestEnvFile`
**Docs:** replace the shell-loop section in
`docs/deploy/environment-variables.md` (and its "the shell is not a .env
parser" warning) with the real command, and add the flag to the CLI reference.

**DONE** — `zt env set --from-file <path|-> [--dry-run]`, parser in
`internal/cli/dotenv.go` (deliberately separate from the command so it is
testable on its own). Positional args are applied after the file, so an
explicit argument overrides a file entry. `--dry-run` prints key names and
never values. `env pull --reveal` now quotes values through `quoteEnvValue`,
making `pull > .env` → `set --from-file .env` lossless.
**Decisions taken (step 2/6 asked):** `${VAR}` is **not** interpolated — the
text is stored verbatim, because silently expanding one secret into another
is the kind of bug you find in production. `--replace` was **not** added: it
is destructive, nothing needed it yet, and "delete everything absent from
this file" deserves its own explicit design rather than a flag bolted on here.
**Parser contract:** blank/`#` lines skipped, optional `export ` stripped,
surrounding single or double quotes removed, `\n \r \t \\ \"` unescaped inside
double quotes only (single quotes literal, POSIX-style), trailing ` #` comment
dropped on unquoted values only (so `https://x#frag` survives), first `=`
splits, CRLF tolerated, 4 MB line cap for escaped PEM bodies. A line that is
neither blank, comment, nor `KEY=VALUE` errors with file and line number
instead of being skipped.
**Tests:** `internal/cli/dotenv_test.go` — the exact cases the shell mangled
(quotes, `export `, trailing comments, `=` in value, `#` in URL), escapes and
multi-line values, CRLF, four error cases each asserting the file:line is
named, no interpolation, later-duplicate-wins, plus a round-trip property test
over spaces/quotes/newlines/tabs/backslashes/`${}`.
**Verified live** on a dev cluster with the same `.env` that broke the shell
earlier: `--dry-run` listed 6 names and no values, the real run stored
`hello world` unquoted, `EXPORTED` (not `export EXPORTED`), `value` without
its trailing comment, and a genuinely multi-line `TLS_KEY`; stdin form worked;
an argument overrode the file; and `pull --reveal` from one app piped into
another app produced a byte-identical `diff`.

---

### T-104 — A hostname should be shareable across apps by path prefix

Phase 9 · Depends: T-44 · Size: S
**Problem:** `AddDomain` rejects a hostname that already exists
(`s.store.DomainByHostname(host)` in `internal/daemon/api/domains.go`), and the
state index `domainsByHostname` is keyed by hostname alone. So `--path` can
only narrow which requests reach the ONE environment that owns the hostname —
the documented use case ("different apps can share one hostname") is
impossible. Verified: adding `shop.example.com --path /admin` then
`shop.example.com` fails with `hostname "shop.example.com" is already in use`.
The data plane is already there: `pickRoute` (`internal/daemon/proxy/l7.go`)
matches every route for a hostname and takes the **longest** path prefix, so
`/` → web and `/api` → api would work the moment the API allowed it. This is
the natural way to put a frontend and an API on one domain without a separate
reverse proxy.
**Files:** `internal/daemon/api/domains.go`, `internal/state/accessors_infra.go`,
`internal/state/state.go` (index), `docs/deploy/custom-domains.md`
**Steps:**

1. Key the uniqueness check on **(hostname, path_prefix)** rather than
   hostname: adding the same hostname with a different prefix succeeds, the
   same pair still conflicts with `AlreadyExists`.
2. Update the `domainsByHostname` index to hold the set of domains per
   hostname (callers that want "the domain for this host" need to say which
   path, or get all of them). Audit every caller — cert issuance and
   `collidesWithClusterDomain` both look hostnames up.
3. One certificate per hostname regardless of how many path routes share it:
   the ACME manager keys on hostname, so it must not issue twice or fight
   itself when two environments own different prefixes of one host.
4. `zt domains ls` must show the prefix (it already prints `host/prefix`) and
   `zt domains rm` must take the prefix to disambiguate — removing
   `shop.example.com` when two routes exist has to be unambiguous, not
   "delete the first one".

**Gotchas:** cross-project sharing is a policy question, not just a uniqueness
one — letting project A claim `/api` on project B's hostname is a
privilege-escalation shaped hole. **Ask before allowing it**; the safe default
is to require the same project. Certificate ownership follows the hostname, so
whoever adds the first route effectively controls issuance. `path_prefix`
normalization matters (`/admin` vs `/admin/`) or the same prefix will be
addable twice.
**Tests:** same host + different prefixes both add and route (longest wins);
same host + same prefix conflicts; `rm` with a prefix removes only that route;
one certificate covers all routes of a hostname; cross-project attempt is
rejected (or allowed, per the decision above) with a test either way.
**Acceptance:** `go test ./internal/daemon/api/ -run TestDomain`
**Docs:** replace the "One hostname, one environment" warning in
`docs/deploy/custom-domains.md` with the working pattern.

**DONE** — uniqueness is now on the **(hostname, path_prefix)** pair.
`domainsByHostname` became `hostname → set of domain ids`, with
`DomainsByHostname` (longest prefix first) and `DomainByRoute` alongside the
existing single-domain lookup, which now documents that it is only for the
per-hostname ACME policy. `normalizePathPrefix` canonicalizes `/api`, `api`
and `/api/` onto one route so the same prefix cannot be registered twice.
**Decision on the flagged policy question:** cross-project sharing is
**refused** (`PermissionDenied`). The certificate is per-hostname, so letting
project B claim a path on project A's host would ride on — and could disrupt —
A's certificate. Same project, any number of apps and environments, is
supported. This is the conservative direction and can be loosened later
without breaking anyone.
**Certificates:** `SetCertStatus` writes to every route of a hostname, since
one certificate covers them all — otherwise siblings would sit at `pending`
forever once the first route issued.
**CLI:** `domains rm` took a bare hostname and deleted the first match, which
is exactly the "delete whichever came first" hazard. It now accepts the
`host/prefix` form `ls` prints, a bare hostname (resolving to the `/` route),
or a domain id; when every route is prefixed and the argument is bare it
lists the candidates instead of guessing.
**Correction found while testing:** the task assumed a bare hostname is always
ambiguous when several routes exist. It is not — the `/` route prints as a
bare hostname in `ls`, so that string is an exact match. Ambiguity only arises
when no `/` route exists. The test was rewritten to the real semantics rather
than forcing the code to match the assumption.
**Tests:** `internal/daemon/api/domains_test.go` (two apps sharing a host,
exact-pair conflict, prefix normalization, independent removal, cross-project
refusal, cert status covering every route); `internal/cli/domains_test.go`
(`matchDomainRoute`: bare host → `/` route, `host/prefix` exact, domain id,
ambiguous-when-all-prefixed lists options, unknown).
**Verified live** on a dev cluster with two nginx apps made distinguishable:
`GET /` returned `WEB-ROOT` and `GET /api/` returned `API-BACKEND` on the same
hostname — the case that failed outright before. Removing `/api` left the root
route serving, and a bare `rm` against two prefixed routes printed the
disambiguation error.

### T-105 — `zt domains` flags for route middleware

Phase 9 · Depends: T-44 · Size: S
**Problem:** `Middleware` (`api/proto/zattera/v1/domain.proto`) is enforced by
the proxy today — basic auth, IP allowlist, max body size, compression, sticky
sessions, HTTPS redirect (`internal/daemon/proxy/l7.go`) — and `SetMiddleware`
is implemented and RBAC-gated. But `internal/cli/domains.go` exposes no flag
for any of it, so a working feature is reachable only by calling the API by
hand. Basic auth on a staging domain and an IP allowlist on an admin path are
exactly the things an operator reaches for on day one. Same server-without-a-
client shape as T-96 (node labels).
**Files:** `internal/cli/domains.go`, `internal/cli/domains_test.go`,
`docs/deploy/custom-domains.md`, `docs/cli/reference.md`
**Steps:**

1. `zt domains set <hostname> [--path P]` with `--basic-auth user:password`
   (hash client-side — the plaintext password must never reach the API or the
   audit log), `--ip-allow CIDR` (repeatable), `--max-body 10MB`,
   `--compress=false`, `--sticky`, `--redirect-https=false`.
2. Read-modify-write: `SetMiddleware` replaces the whole message, so unset
   flags must preserve their current values or every call silently clears the
   others.
3. `zt domains ls` should show which middleware is active (a compact column,
   never the password hash).
4. Decide how to clear a value (`--ip-allow=""`? a `--clear-ip-allow`?) and
   keep it consistent with `zt nodes label`'s `KEY-` removal style.

**Gotchas:** bcrypt/argon2id hashing belongs client-side; if it is done
server-side the plaintext travels in the request and lands in the audit log.
`max_body_bytes` is a uint64 of bytes — accept human sizes (`10MB`) but store
bytes. Enabling basic auth on a route that scale-to-zero wakes must not break
the wake path (the probe request also passes through middleware — check).
**Tests:** each flag maps to the right proto field; unset flags are preserved
across calls; the password is hashed before it leaves the CLI (assert the
request carries no plaintext); `ls` never prints the hash.
**Acceptance:** `go test ./internal/cli/ -run TestDomainsMiddleware`
**Docs:** replace the "API-only" note in the middleware table in
`docs/deploy/custom-domains.md` with the real commands.

---

### T-106 — `port-forward` fails in dev mode, and reports success either way

Phase 9 · Depends: T-51 · Size: S
**Problem:** `resolvePortTarget` (`internal/daemon/api/execsvc.go`) skips every
candidate whose node has no mesh IP and dials `node.MeshIp:hostPort`. In
`--dev` the mesh is disabled, so `mesh_ip` is empty on the only node and the
RPC can never resolve a target: **`zt port-forward` is dead in dev mode**,
which is the environment the quickstart tells a new user to start in.
Reproduced: listener binds, `curl` returns `000`, nothing is logged.
Worse, the CLI prints `✓ Forwarding 127.0.0.1:18082 → site (port http)`
**before** any byte flows, so an unresolvable target still looks like success —
in dev *and* in production. A user sees a green check and a connection that
silently hangs.
**Files:** `internal/daemon/api/execsvc.go`, `internal/cli/portforward.go`,
tests alongside
**Steps:**

1. Dev/mesh-disabled path: when a node has no mesh IP, fall back to the
   published host port on the node's own address (the executor already
   reports `mesh_port_bindings`, and dev publishes on `127.0.0.1`). The health
   prober solved the same problem with `useHostPort` — reuse that idea rather
   than inventing a second one.
2. Fail loudly instead of hanging: `resolvePortTarget` returning `Unavailable`
   must reach the user as an error, not a success line followed by a dead
   socket.
3. Move the CLI's `✓ Forwarding` line **after** the first successful dial (or
   print `listening…` first and confirm on connect), so the check mark means
   "this works", not "a socket is open".

**Gotchas:** don't regress the production path, where dialing the mesh IP is
correct and the host port may not be published on a routable interface. A
single-node prod cluster (mesh enabled, one node) must keep working. The
stream is bidirectional — surfacing the resolve error means propagating it
before the copy loops start.
**Tests:** resolve returns the host-port target when the node has no mesh IP
and the mesh target when it does; a failed resolve surfaces as a CLI error
with a non-zero exit; no success line is printed before the first dial.
**Acceptance:** `go test ./internal/daemon/api/ -run TestPortForward`, plus
`zt port-forward` actually serving traffic against `zattera server --dev`.

### T-107 — Per-client rate limiting at the ingress ✅ **DONE**

Phase 9 · Depends: T-19 · Size: S
**Problem:** the L7 proxy had no rate limiting of any kind, so an app exposed
to the internet had no built-in defence against a single client hammering it.
**Files:** `api/proto/zattera/v1/service.proto`,
`api/proto/zattera/cluster/v1/routes.proto`, `internal/appconfig/appconfig.go`,
`internal/daemon/proxy/ratelimit.go` (new), `internal/daemon/proxy/l7.go`,
`internal/daemon/scheduler/routes.go`
**Done:**

1. `RateLimit{requests_per_second, burst}` on **ServiceSpec**, not on
   `Middleware`. Two reasons: `Middleware` is unreachable from `zattera.toml`
   (TOML `domains` are never applied — see T-108), and `RouteBuilder` emits the
   implicit `<app>-<env>.<domain>` route with no `Domain` behind it, so
   domain-level config would leave that internet-exposed hostname unprotected.
2. `[env.<name>.rate_limit]` in `zattera.toml`; absent = off. `burst` defaults
   to `requests_per_second` and may not be lower (a burst under the sustained
   rate can never refill, silently capping throughput below the stated limit).
3. `rateLimiter` — per-key token buckets over the injected `clock.Clock`, keyed
   `environment_id|client_ip`. Bounded at 32k keys with an idle sweep plus
   arbitrary-eviction fallback; eviction fails open (a dropped bucket restarts
   full) rather than falsely throttling.
4. Checked in `ServeHTTP` after the IP allowlist and **before** basic auth and
   endpoint selection, so credential guessing is throttled too and shed
   requests never occupy a backend slot. Over-limit = `429` + `Retry-After`.

**Gotchas:** the limiter is deliberately node-local — no gossip, no shared
counters — so the cluster-wide ceiling is `rps × ingress nodes` while per-client
enforcement stays exact under DNS pinning. Client identity is `RemoteAddr`
only; trusting `X-Forwarded-For` would let any caller mint unlimited buckets.
Documented in both `docs/deploy/zattera-toml.md` and `docs/networking/ingress.md`.
**Tests:** `internal/daemon/proxy/ratelimit_test.go` (bucket refill/cap/
independence/eviction, 429 + backend-never-reached, off-by-default, per-env
isolation), `TestParseRateLimit` + validation cases, and
`TestRoutesRateLimitOnBothRouteKinds` pinning point 1.
**Acceptance:** `go test ./internal/daemon/proxy/ ./internal/appconfig/
./internal/daemon/scheduler/`

### T-108 — `zattera.toml` `domains` are parsed and then dropped

Phase 9 · Depends: T-11 · Size: S
**Problem:** `appconfig` parses `env.<name>.domains` into `AppConfig.Domains`,
and nothing ever applies it — `internal/cli/apply.go` carries a
`TODO(T-40): apply ac.Domains via DomainService`, but T-40 is the logstore task
and has been done for a long time, so the reference is stale and the work is
unowned. A user who declares `domains = ["api.example.com"]` in `zattera.toml`
gets silence: no domain, no error. Found while implementing T-107.
**Files:** `internal/cli/apply.go`, `internal/cli/deploy.go`,
`internal/appconfig/appconfig.go`
**Steps:**

1. Apply `ac.Domains` through `DomainService` on `zt apply`/`zt deploy`:
   create domains that are absent, and decide explicitly whether a domain
   removed from the TOML is deleted or left alone (declarative would say
   delete; that destroys certs, so it likely needs a confirmation or a flag).
2. Fix the stale `TODO(T-40)` reference either way.
3. Consider whether `Middleware` (basic auth, IP allowlist, body limits)
   should become TOML-settable in the same pass — today it is API/CLI-only,
   which is why T-107 put the rate limit on `ServiceSpec` instead.

**Gotchas:** domains are project-scoped while `ApplyAppConfig` is app-scoped;
a hostname may already belong to another environment, which must be a clear
error rather than a silent steal. Deleting a domain drops its certificate.
**Tests:** apply with a new domain creates it; re-apply is idempotent; a
hostname owned by another env fails with a useful message.
**Acceptance:** `go test ./internal/cli/ ./internal/daemon/api/ -run Domain`

---

# Backlog (M4/M5 — do not implement now)

- **M4:** SSO/OIDC login; wildcard certs via DNS-01 (libdns providers);
  browser-based CLI login; Prometheus `/metrics` endpoint; external log
  sinks (Loki/S3); GeoDNS guidance docs; sticky-session refinements;
  pause/unpause pre-warming experiments.
- **M5 (F27) remainder:** the driver interface + Hetzner Cloud driver ship in
  Phase 8 (T-81..T-86). Remaining backlog: DigitalOcean, AWS and Scaleway
  drivers (implement against T-82's `RunDriverContractTest`); per-pool
  provider quota hints; spot/preemptible instance support.

### T-92 — Audit/event retention on object storage ✅ **DONE**

Phase 9 · Depends: T-66, T-76 · Size: M
**Problem:** audit entries and events live in capped rings in replicated state
(50k / 10k), so on a busy cluster the trail holds days, not years — `zt audit`
cannot answer "who deleted this app in April". The ring cap is correct (raft
state must stay bounded); what was missing is a durable copy outside it.
**Files:** `internal/daemon/archive/` (new), `internal/daemon/archivewiring.go`,
`internal/daemon/api/audit.go`, `internal/cli/audit.go`, `internal/cli/events.go`
**Done:**

1. Opt-in via `BackupConfig.archive` (`zt backup config --archive`) — reuses the
   backup destination and its sealed credentials; no second config or key path.
2. Leader-gated sweeper (5m) copies both rings out as gzipped NDJSON sealed with
   the cluster data key, keyed
   `<stream>/<YYYY-MM-DD>/<startMs>-<endMs>-<ulid>.ndjson.gz.enc` so a
   time-scoped read skips objects without fetching them.
3. Resume cursor in the replicated KV (`archive/cursor/<stream>`) is a
   millisecond watermark **plus the ids already written at that millisecond** — a
   bare watermark is either lossy (exclusive) or duplicating (inclusive), and
   audit ids are minted before the raft round trip so they are not a strict
   watermark either. A 2-minute settle lag keeps in-flight records out of a
   batch whose cursor would then skip their siblings.
4. Cursor advances only after the object is durable: a crash re-archives a batch
   rather than losing it, and the reader dedupes by id.
5. Read-back: `include_archive` on QueryAudit/ListEvents merges archive with the
   ring server-side (dedupe by id, newest-first, limit applied after the merge)
   and reports `from_archive`. The CLI (`--archive`) therefore needs no bucket
   credentials, and non-admin project scoping applies to archived records
   because the same filter runs over both.
6. Nothing is ever deleted — object lifetime is the bucket's lifecycle policy.
   **Tests:** `internal/daemon/archive/archive_test.go` — round trip, cursor
   resume (no loss/no duplication), same-millisecond boundary, settle lag,
   archiving-off no-op, key-range skipping (asserts objects outside the window are
   never fetched), and wrong-key-fails-to-decrypt.
   `internal/daemon/api/auditarchive_test.go` — merge semantics, overlap dedupe,
   filters over archived records, limit-after-merge, and the degraded path where
   `include_archive` is set but no archive is configured.
   **Acceptance:** `go test ./internal/daemon/archive/ ./internal/daemon/api/ -run 'TestArchive|TestQueryAuditIncludeArchive|TestIncludeArchive'` ✅
   **Not done (deliberate):** the archive is write-and-read-back only — no
   compaction of small objects, and a `--archive` query lists the stream prefix
   each time. Both are fine at the object counts an hourly-ish sweep produces;
   revisit if a cluster archives for years.

### T-93 — Trustworthy per-node version reporting  ✅ **DONE**
Phase 9 · Depends: T-14 · Size: S
**Problem:** `Node.binary_version` exists but is not reliable, so there is no
answer to "what version is each node on" — the prerequisite for any upgrade
orchestration. Three gaps: `registerLocalNode` (`daemon.go`) builds the whole
Node record at boot **without** `BinaryVersion`, so the bootstrap control node
never has one and a rejoining node overwrites its own with empty;
`AgentHello.binary_version` is received in `agentsync.go` and only **logged**,
never folded into state; and `zt nodes ls` has no version column.
**Files:** `internal/daemon/daemon.go`, `internal/daemon/api/agentsync.go`,
`internal/cli/nodes.go`
**Steps:**
1. Set `BinaryVersion: version.Version` in `registerLocalNode`.
2. On `AgentHello`, if the reported version differs from the stored Node
   record, apply a `PutNode` with it — this is what makes the value refresh
   after an upgrade without a rejoin.
3. `zt nodes ls`: add a VERSION column; mark rows that differ from the majority
   so skew is visible at a glance.
4. Helper `state.ClusterMinVersion()` (and max) for the upgrade planner and for
   `zt doctor` (T-78).
**Gotchas:** the AgentHello write must be conditional, not every reconnect — an
unconditional PutNode per stream open is a raft write amplifier. Version strings
are `git describe` output (`v0.3.1-4-gabc` / `dev`); compare with a real semver
parse and treat unparseable as "unknown", never as "older".
**Done:** all four steps. `version.Parse/Compare/Older` handle git-describe
output (`v0.3.1-4-gabc`, `-dirty`, `dev`); an unparseable version is never
ordered, so a planner can never silently decide such a node is old (or current).
`state.ClusterVersionRange` returns min/max plus an unknown flag.
**Tests:** `internal/pkgutil/version/compare_test.go` (parse shapes, ordering
incl. the ahead-counter and numeric 0.10 > 0.9, unknown-never-ordered);
`TestNodeVersionRecording` (write once on change, never on re-report, ignore
empty/unknown node); `TestClusterVersionRange`.
**Acceptance:** `go test ./internal/daemon/api/ -run TestNodeVersion` ✅

### T-94 — Cordon/uncordon (return a drained node to service)  ✅ **DONE**
Phase 9 · Depends: T-19 · Size: S
**Problem:** `DrainNode` flips a node to DRAINING/DRAINED and nothing can flip it
back. Liveness deliberately skips those states (`liveness.go`), so today a
drained node only returns to service by **restarting its daemon**, which
re-applies `PutNode` with ALIVE+schedulable. That is an accidental mechanism, and
an upgrade loop that cordons each node needs an explicit way out.
**Files:** `api/proto/zattera/v1/api.proto`, `internal/daemon/api/nodes.go`,
`internal/cli/nodes.go`, `internal/daemon/api/policy.go`
**Steps:**
1. `UncordonNode` RPC (admin): DRAINING/DRAINED → ALIVE, `schedulable=true`.
   Refuse on a DOWN node — liveness owns that transition.
2. `CordonNode`: `schedulable=false` **without** draining. This is the primitive
   T-95 actually needs (see its Gotchas): it stops new placements while leaving
   running containers alone.
3. CLI `zt nodes cordon|uncordon <name>`; show a cordoned-but-alive node
   distinctly in `nodes ls` (it is ALIVE with schedulable=false).
**Done:** `CordonNode`/`UncordonNode` RPCs (admin), `zt nodes cordon|uncordon`,
and a `STATUS` column that renders a cordoned node as `ALIVE,CORDONED` — an
otherwise-normal-looking node quietly receiving no work is exactly the state
that wastes an afternoon. Cordon keeps the node ALIVE and leaves a drain in
progress alone; uncordon refuses a DOWN node, since liveness owns that
transition. No scheduler change was needed: the placement filter is already
`ALIVE && Schedulable`.
**Tests:** `TestCordonUncordon` — cordon does not disturb a running assignment
(that is drain's job), uncordon recovers a DRAINED node, cordon does not
override a drain, uncordon of a DOWN node is refused, unknown node is NotFound.
**Acceptance:** `go test ./internal/daemon/api/ -run TestCordon` ✅

### T-95 — `zattera cluster upgrade` (rolling, minimal-downtime)  ✅ **DONE**
Phase 9 · Depends: T-93, T-94, T-54 · Size: L
**Goal:** one command brings every node to the same version, in a safe order,
without taking workloads down: `zt cluster upgrade [--version vX.Y.Z]`.

**The key fact this design rests on:** the agent's executor returns on
`ctx.Done()` and **never stops containers** (`agent/executor.go`) — workloads are
docker-managed and survive a `zatterad` restart. So a binary upgrade does **not**
need to drain a node, and must not: `reconcileDrains` hard-stops stateful
workloads (node-pinned volumes can't move), which would turn a zero-downtime
binary swap into a database outage on every stateful node. Cordon, restart,
uncordon — never drain.

**What actually blinks** during a node's restart is that node's in-process
ingress proxy and its agent stream, i.e. seconds, and only for traffic landing
on that node. Zero ingress downtime therefore requires ≥2 ingress nodes; with
one, the command must say so up front rather than pretend.

**Files:** `internal/cli/clusterupgrade.go`, `internal/daemon/api/upgrade.go`,
`internal/daemon/agent/selfupgrade.go`,
`api/proto/zattera/cluster/v1/agent.proto`, `api/proto/zattera/v1/api.proto`
**Steps:**
1. **Plan/preflight** (`UpgradePlan` RPC, admin): resolve the target version
   (default: latest release), confirm an asset exists for every distinct
   `Node.os_arch`, list each node's current version (T-93), and refuse to start
   if any node is DOWN or the cluster has no quorum margin (a 3-node control
   plane can lose exactly one node at a time). `--dry-run` prints the plan and
   the expected ingress impact, and exits.
2. **Order: workers → control followers → leader last**, one node at a time.
   The FSM is additive-only and `raftstore/apply.go` surfaces an unknown
   mutation as an error *without halting the node* — so a new leader proposing a
   new mutation type silently diverges old followers' state. An old leader only
   ever proposes mutations that newer followers understand, so upgrading the
   leader last is the safe direction. Transfer leadership explicitly before
   upgrading it (raft `LeadershipTransfer`) instead of forcing an election.
3. **Per node:** cordon (T-94) → send an upgrade instruction → wait for the node
   to come back reporting the new version (T-93) → health gate → uncordon →
   next. Any failure aborts the run, leaves the remaining nodes untouched, and
   reports exactly which node is stuck and how to recover.
4. **Agent self-upgrade** (new `ControlMessage` arm — the oneof is ready for an
   additive field 3): the control plane sends `{url, sha256, version}`; the agent
   downloads, **verifies the checksum before touching anything**, writes
   `zattera.new`, keeps the running binary as `zattera.prev`, atomically
   renames, and re-execs (or `systemctl restart` when running under the unit —
   `nodecmd.go` already owns that unit). Report progress/result on a new
   `AgentMessage` arm.
5. **Rollback:** `--rollback` restores `zattera.prev` node-by-node in reverse
   order. `install/install.sh` currently does an atomic `mv` with no backup —
   add the `.prev` retention there too so a manually-installed node can also
   roll back.
6. Docs: the `upgrades.md` page the spec already anticipates.

**Gotchas:**
- **This is a remote-code-execution primitive by design.** Admin-only, the
  checksum must be verified before execution and must come from the control
  plane rather than from the download host, and the agent must refuse any base
  URL outside a configured allowlist (default: the official release host). Say
  this plainly in the docs rather than hiding it.
- Do **not** reuse drain (see above). Cordon is the primitive.
- Air-gapped clusters: `--from-file` / a control-plane-served artifact, so the
  nodes never need egress. Can be a follow-up, but design the instruction so the
  URL is not assumed public.
- A node that never comes back must not hang the run forever — bounded wait,
  then abort with the node left cordoned (visible, not silently degraded).
- The upgrading node is often the one running the CLI's own API connection.
  Upgrading the leader tears down the client stream; the CLI must reconnect
  through the control-endpoint rotation (T-55c) rather than reporting failure.
- Version skew is expected *during* the run; `zt doctor`/CLI skew warnings must
  not fire spuriously while an upgrade is in progress (record an in-progress
  marker in the KV).
**Tests:** unit — plan ordering (workers/followers/leader-last) over a fake
cluster incl. a single-node cluster; abort-on-failure leaves later nodes
untouched; checksum mismatch refuses to swap and reports; rollback ordering.
Integration — simcluster upgrade with a fake artifact server, asserting
containers keep running across the restart and assignments reconcile.
Cloud (`test/cloud/upgrade_test.go`) — real 3-node upgrade, asserting an app
stays reachable throughout and every node reports the new version.
**Acceptance:** `go test ./internal/cli/ -run TestClusterUpgrade` ✅

**Implemented as:**
- `internal/daemon/upgrade` resolves a version to per-arch asset URL + SHA-256
  from the release checksum manifest, caching so one run pins one release (a
  retag mid-rollout cannot split the cluster). `""` follows the /latest
  redirect to learn the concrete tag, which is what makes "already up to date"
  honest.
- `NodeService.UpgradePlan` orders workers → followers → **leader last** and
  returns preflight warnings (DOWN nodes, missing arch assets, single-ingress
  blip, unknown versions). `UpgradeNode` performs one step.
- `AgentLocalService.UpgradeBinary` on the node: verify digest → keep
  `zattera.prev` → atomic swap → restart (systemd unit if present, else re-exec).
- `zt cluster upgrade [--version] [--dry-run] [--yes]` drives the rollout,
  tolerating the connection drop when it upgrades the node serving its own API
  call (the version check, not the RPC result, decides success).
- `install/install.sh` now also keeps `zattera.prev`.
**Deviation:** the plan suggested a new `ControlMessage` arm on the AgentSync
stream. Used `AgentLocalService` instead — it is the established control→node
RPC direction (exec, logs, volume ops all use it), needs no new ack arm, and
keeps the assignment stream contract untouched.
**Not done:** `--rollback` (the `.prev` binary is retained, so re-running with
the older `--version` is the current path); air-gapped `--from-file`; the
cloud test. The in-progress KV marker for suppressing skew warnings was not
needed — `nodes ls` marks skew but nothing errors on it.
**Tests:** `internal/daemon/upgrade/release_test.go` (manifest parsing, latest
redirect, caching, asset→os/arch mapping); `TestUpgradePlanOrder` (leader last)
plus skip/warn and per-node cases; `internal/daemon/agent/selfupgrade_test.go`
(**checksum mismatch installs nothing and does not restart**, foreign URL
refused, missing checksum refused, download failure leaves the node untouched,
`.prev` retained, allowlist prefix cannot be fooled by a sibling path);
`internal/cli/clusterupgrade_test.go` (plan rendering, leader marking,
up-to-date filtering, `--yes` gating).

# Dependency quick-reference

```
P1: T-01→T-02→T-04→T-05→T-06→T-07/T-08 · T-03 · T-09 · T-10(04-06,09) · T-11 · T-12
P2: T-13 · T-14(02,12)→T-15→T-16 · T-17(01,12)→T-19(18)→T-20 · T-18 · T-21(14) · T-22(17,19)
P3: T-23(15)→T-26(16,25) · T-24 · T-25(06,09) · T-27 · T-28(25,10) · T-29(23) · T-30(26,29)
P4: T-31(02)→T-32→T-33(13)→T-34 · T-35(33,25)→T-36(28) · T-37(35) · T-38(32,26)
    T-87 · T-88(87,24,25,32)   # multi-arch: protos+node arch, arch-aware scheduling
P5: T-39(26)→T-42→T-43/T-44→T-45 · T-40→T-41(35) · T-46(15)→T-47→T-48 ·
    T-49(35,13) · T-50 · T-51 · T-52 · T-53(23,40) · T-54(ALL)
P6: T-55(17,08)→T-56 · T-57(20)→T-58 · T-59(13)→T-60(41)/T-61(23) ·
    T-62(24,15)→T-63(26) · T-64(13)→T-65(49)→T-66(55) · T-67(53) · T-68(39,55)
P7: T-69(61,42)→T-70→T-71 · T-72(45)→T-73 · T-74(59,07) · T-75(37,45) ·
    T-76 · T-77(65) · T-78 · T-79(54) · T-80(all)
P8: T-81(12)→T-82→T-83 · T-84(83,17,29)→T-85(84) · T-86(84,85)
P9: T-91(53,40) · T-92(66,76) · T-93(14) · T-94(19) · T-95(93,94,54) · T-96(12) · T-97(87,88) · T-98(63,97) · T-99(31) · T-100(35,95) · T-101(32,55)→T-102(101,64) · T-103(12) · T-104(44) · T-105(44) · T-106(51) · T-107(19) · T-108(11)
```
