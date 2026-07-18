---
title: Metrics & alerts
description: Live and historical metrics, plus alert rules firing to webhook/Slack/email channels.
---

# Metrics & alerts

## Metrics

Every node runs an embedded ring TSDB — a metrics sampler records node CPU/memory/disk/net, per-instance CPU/memory/network, and per-env proxy series (RPS, in-flight, error rate, p50/p99 latency) every 15s into a two-resolution ring buffer (15s for 24h, 5m for 30d) that survives restarts (`<data-dir>/metrics/tsdb.bin`).

- **Current values** — `zattera stats` (per node) or `zattera stats --app NAME` (per env) reads the latest heartbeat.
- **History** — `zattera stats --since 1h [--step 5m]` reads the TSDB: the control plane fans the query out to each relevant node's local store and merges the series (node series per node; env series sum RPS/in-flight and average CPU/latency across instances). The CLI renders each series as a sparkline; `--json` returns the raw points. Scope it with `--node ID`, `--app NAME`, or leave it cluster-wide.

**Under the hood:** the store is `internal/daemon/tsdb` (`tsdb.Store` — per-series float32 rings, downsample-on-write, 48h GC of idle series, flat-file persistence). The sampler is `internal/daemon/agent/metrics.go`; each node serves its history over `AgentLocalService.Stats` (`internal/daemon/agent/statsserver.go`), and the control plane fans out and merges in `internal/daemon/api/metricshistory.go`. See [Logs](logs) for log tailing and retention.

## Alerts

The leader runs an alert engine that evaluates **rules** and delivers **notifications** to channels. A rule is either a **metric threshold** or an **event match**.

```bash
# Metric rule: fire when any node's disk is over 90% for 5 minutes.
zt alerts rules add disk-full --metric disk_percent --scope cluster --op '>' --threshold 90 --for 5m --channel <id>

# Event rule: fire on every failed deploy.
zt alerts rules add deploys --event deploy.failed --channel <id>

zt alerts rules ls
```

- **Metrics** — `cpu_percent`, `memory_percent`, `disk_percent` (scope `node:<id>` or `cluster`), and `error_rate`, `rps`, `inflight`, `latency_p99_ms` (scope `env:<id>`). A metric rule fires only after the condition has held for `--for`, and **resolves** with a follow-up notification when it clears. Missing data freezes the rule (never fires on absent metrics).
- **Events** — any cluster event kind, e.g. `deploy.failed`, `node.down`, `backup.failed`, `cert.renew_failed`.
- **Dedupe** — a firing rule is silenced for 15 minutes before it re-alerts, per rule + scope.
- **Built-in rules** — a fresh cluster ships with deletable defaults (`deploy-failed`, `node-down`, `cert-renew-failed`, `backup-failed`, and `disk-full` >90% for 5m). Attach a channel to start receiving them.

### Limits worth knowing

- **Delivery needs an unsealed node.** Channel credentials are sealed with the cluster data key, so a sealed node evaluates rules but cannot send. Nodes recover the key automatically at startup; if one reports sealed, see [Sealing & unsealing](sealing).
- **`cert.renew_failed` is reported by the cluster leader.** A renewal that fails on a non-leader node is written to that node's log but does not currently reach the event log, so no rule matches it. Renewal failures are persistent — the next attempt on the leader surfaces it — but the first notification can be delayed.
- **`cert.renew_failed` covers renewals, not first issuance.** A domain that never obtained a certificate at all is a different condition; check `zt domains ls` for its cert status.

### Channels

```bash
zt alerts channels add webhook ops --url https://hooks.example.com/x --secret <hmac-key>
zt alerts channels add slack team --slack-url https://hooks.slack.com/services/...
zt alerts channels add telegram oncall --bot-token <bot-token> --chat-id <chat-id>
zt alerts channels add email oncall --to oncall@example.com --from alerts@example.com \
  --smtp-host smtp.example.com --smtp-user alerts --smtp-pass <pw> --starttls
zt alerts channels ls   # secrets are redacted
```

Supported channel types are **webhook**, **slack**, **telegram**, and **email**. For Telegram, create a bot with [@BotFather](https://t.me/BotFather) for the token and use the target chat/channel id (a group id is negative, e.g. `-1001…`); the bot must be a member of that chat.

Channel secrets (webhook HMAC key, Slack URL, Telegram bot token, SMTP password) are **sealed with the cluster data key** server-side and never returned by the API. Webhook payloads are JSON and, when a signing key is set, carry an `X-Zattera-Signature: sha256=…` HMAC of the body. A slow or failing channel never stalls evaluation (10s per-channel timeout, and the failure itself is recorded as an event); email is treated as best-effort.
