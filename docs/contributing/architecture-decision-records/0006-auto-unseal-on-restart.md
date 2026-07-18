---
title: "ADR-0006: Auto-unseal on restart"
description: Why the cluster data key may be cached on local disk by default, reversing the original memory-only rule.
---

# ADR-0006: Auto-unseal on restart

**Status:** accepted · **Date:** 2026-07-18 · **Tasks:** T-111, T-112

## Context

The cluster data key encrypts secrets at rest: environment variables, alert
channel credentials, backup S3 credentials, archived audit objects. The original
rule was absolute — `Keyring` "holds the cluster data key in memory only… it is
never written to disk."

That rule had an unstated consequence nobody had exercised. The key enters a
process's memory in exactly two places: `Bootstrap` on a cluster's very first
start, and the control-node join handover. `Bootstrap` is idempotent and returns
no keyring once the cluster exists, so **every restart came up sealed, with no
way back**. There was no `unseal` command; the recovery passphrase was only
usable by `zattera restore`, which requires an empty data directory.

Verified on a live single-node cluster: create an alert channel (succeeds),
restart the daemon against the same data dir, repeat the command —
`cluster key is not unsealed; cannot store channel secrets`. Permanently.

The damage was silent. A sealed node kept serving, so alerting simply stopped
notifying, `zt env set` started failing, and backups stopped running, with no
log line saying why. For a product whose premise is a single binary that an
operator does not babysit, "works until the first reboot" is not a viable
default.

## Decision

Cache the data key at `<data-dir>/node/data.key`, mode `0600`, and read it back
at startup. Recovery order: local file → control peer over mTLS → stay sealed
and log loudly. Operators who want the old guarantee set `sealed_at_rest = true`
and unseal manually with `zt unseal --passphrase-file`.

The peer fetch is a new mTLS `KeyService.FetchDataKey`, not a reuse of
`JoinService.Join`: join tokens are single-use, and joining mints mesh IPs,
certificates and registry credentials as side effects that must not repeat on
every reboot. The handler additionally requires the calling node to hold the
control role, so a stolen worker certificate does not become a path to the key.

## Why this trade is acceptable

The memory-only rule protected less than it appeared to. The **cluster CA
private key is already stored unencrypted in the data directory** — it has to
be, so a control node can sign node certificates after a restart. An attacker
with the disk therefore already holds the credential needed to mint a node
certificate, present it to a peer, and be handed the data key. Writing the data
key next to the CA key changes the cost of that attack from "a few steps" to
"none"; it does not change who can succeed at it.

What sealing genuinely protects is **backups**: objects in S3 are wrapped by the
recovery passphrase, and nothing in this decision touches that. Off-host
material stays protected by something that is not on the host.

Against that we weighed silent, permanent loss of alerting, secret writes and
backups after any reboot — a failure mode that removes the monitoring you would
use to notice it.

## Consequences

- A reboot returns a node to full function with no operator action.
- The data key is on disk by default. Documented plainly in
  [Sealing & unsealing](../../operations/sealing) rather than implied.
- We deliberately do **not** wrap the file with a host-local key. Anything able
  to unwrap it would live on the same disk, so wrapping would add obscurity
  while suggesting a guarantee that does not exist.
- `sealed_at_rest = true` preserves the original posture for operators who want
  it, at the cost of manual unsealing on cold start.
- A future KMS/TPM-backed wrap is compatible with this layout: it replaces how
  `data.key` is protected without changing the recovery order.
