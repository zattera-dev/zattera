---
title: Remote debugging
description: Shell into instances, inspect processes and files, and forward ports — over the API, no SSH.
---

# Remote debugging

Debug running instances from your laptop with no SSH keys, no bastion, no node access — everything tunnels through the authenticated API and the mesh, and every call is subject to [RBAC](access-control).

## How to use

```bash
zt attach api                        # interactive /bin/sh in a healthy instance
zt attach api -- printenv            # run a one-off command (exit code propagates)
zt top api                           # process table
zt fs ls api:/app/config             # list a directory
zt fs cat api:/app/config/prod.yaml  # print a file
zt port-forward api 5432:db          # 127.0.0.1:5432 → the instance's "db" port
```

All of these target the first **healthy** instance by default; narrow with `--env` or pin an exact one with `--instance <id>` (ids from `zt ps`). `attach` allocates a TTY when it's interactive (disable with `--no-tty`), forwards terminal resizes, and restores your terminal on exit.

`port-forward` is the practical way to reach a database from your laptop: it listens on `127.0.0.1:<localPort>` and tunnels each connection to the named service port.

## How it works

The CLI never talks to nodes directly. A debug command hits the public API, which proxies the stream over the **mTLS mesh** to the agent on the instance's node; the agent execs into the container (or splices a TCP connection, for `port-forward`). `fs ls`/`fs cat` are deliberately thin wrappers over exec (`ls -1ap`, `cat`) — no separate file-transfer protocol, same audit trail as any exec.

Remote exit codes come back through the tunnel: `zt attach api -- ./healthcheck.sh && echo ok` behaves exactly like running it locally, and failed [jobs](jobs) exit your shell with the job's own code.
