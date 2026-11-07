---
title: Contracts
tags: [docs, architecture]
updated: 2026-11-07
---

# Contracts

The source of truth is `packages/engine/src/schema.ts` (zod 4). JSON Schemas are generated into `contracts/` for non-TS consumers. CI fails if they are stale (`npm run schema:check -w @kafkasim/engine`).

| File | Shape | Producers | Consumers |
|---|---|---|---|
| `manifest.schema.json` | Scenario: regions, latency, clusters (controllers, brokers, config), topics (replicas, observers, mirror), producers, consumers, timed actions | humans, `contracts/scenarios/*.json`, bridge (`manifest_from`) | engine, harness |
| `event.schema.json` | `SimEvent` union | engine, bridge, harness | web reducer, calibration |
| `event-log.schema.json` | `{schemaVersion, source: sim\|recording\|live, manifest, events}` | engine (`scripts/run.ts`, Export log), bridge `--record`, harness | Replay mode, calibration |
| `bridge-frame.schema.json` | WS frames: `hello` (manifest), `events`, `error` | bridge | Live mode |

## Rules

- `schemaVersion` is `1`. Breaking changes bump it, and the web app rejects unknown versions.
- Node ids match `[A-Za-z0-9_.-]+`. Topic-partition keys are `<cluster>/<topic>/<partition>`.
- `t` is milliseconds from the start of the run (sim), the start of observation (bridge), or producer start (harness).
- Events must be state-complete. The view is rebuilt only from `NODE_STATUS`, `BROKER_FENCE`, `CONTROLLER_*`, `LEADER_CHANGE`, `ISR_CHANGE`, `LEO`, `HW`, `TRUNCATE`, client events and `NETWORK`. `MSG` is cosmetic.
- Cross-language check: CI runs the Python bridge test, which writes frames and a recording, then validates them with the zod schemas (`scripts/validate.ts`).

## Manifest example

```json
{
  "schemaVersion": 1,
  "name": "My scenario",
  "regions": [{ "id": "dc1" }],
  "clusters": [{ "id": "main", "controllers": [{ "id": "c1", "region": "dc1" }], "brokers": [{ "id": "b1", "region": "dc1" }, { "id": "b2", "region": "dc1" }], "config": { "minInsyncReplicas": 2 } }],
  "topics": [{ "cluster": "main", "name": "orders", "partitions": 1, "replicas": [["b1", "b2"]] }],
  "producers": [{ "id": "app", "cluster": "main", "region": "dc1", "topic": "orders", "acks": "all", "ratePerSec": 20 }],
  "actions": [{ "type": "KILL_NODE", "at": 10000, "node": "b1" }]
}
```
