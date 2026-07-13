# Contributing to Zattera

## Dev setup

Requirements: Go ≥ 1.24, Docker (Desktop on macOS), make.

```bash
make tools        # install pinned buf/protoc plugins/golangci-lint into ./bin
make generate     # regenerate protobuf/gRPC code (commit the result)
make build        # full binary → bin/zattera
make test         # unit tests (fast, no Docker, includes simcluster)
make test-integration  # needs a running Docker daemon
make test-e2e     # full single-node smoke test
make test-chaos   # slow fault-injection suite (no Docker)
```

Run a dev server: `bin/zattera server --dev --data-dir /tmp/zattera-dev`.

## Ground rules

- **Protos are frozen contracts**: never change or reuse a field number; new
  fields/mutations are additive. CI runs `buf breaking` against `main`.
- **All state mutations go through the Raft FSM** (`internal/daemon/raftstore`);
  never mutate `internal/state` outside an apply handler.
- **Only `internal/daemon/runtime` may import the Docker SDK.** Everything
  else uses the `ContainerRuntime` interface.
- **Time in control loops goes through `pkgutil/clock.Clock`** so tests can
  drive it deterministically.
- CLI code never imports `internal/daemon` and vice versa (ADR-0002); the
  shared surface is `pkg/apiclient` + generated protos.
- Architecture decisions are recorded in
  `docs/contributing/architecture-decision-records/`.

## macOS caveat

WireGuard TUN, netlink VIPs and the internal DNS listener are Linux-only
features. Their integration tests run inside privileged Linux containers
(works under Docker Desktop); `make test` stays fully native.

## Implementation plan

The dependency-ordered implementation plan lives in [TASKS.md](./TASKS.md).
Pick the first unclaimed task whose dependencies are done.
