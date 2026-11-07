---
title: kafkasim MVP spec
tags: [docs, plans, specs]
status: active
updated: 2026-11-07
---

# kafkasim MVP spec

Replaces the Gemini draft (`architecture_specification.md` and the `KafkaSimulator.jsx` zip).

## Goal

A GitHub Pages site where an engineer can watch, scrub and break a Kafka cluster, and trust what they see because it is calibrated against real Kafka.

## Scope (MVP)

1. **Contracts:** manifest, SimEvent, EventLog and bridge frames in zod, emitted as JSON Schema.
2. **Engine:** deterministic DES with the behaviours in [engine-model](../../architecture/engine-model.md). Nine built-in scenarios: broker failure, acks=1 vs all, min.insync, zombie leader, unclean election, stretch partition, Cluster Linking DR [CP], MM2 DR, MRC observers [CP].
3. **Web:** canvas topology with message particles, timeline with markers, scrubbing, speed 0.05–8×, click-to-inject faults (plus a keyboard path), partitions table (leader, epoch, ISR, LEO vs HW), telemetry, event log with click-to-seek, manifest editor, export log.
4. **Replay:** load an EventLog file or a bundled recording.
5. **Live:** `kafkasim-bridge` (AdminClient → WS). Read-only.
6. **Harness:** real Kafka 4.2 compose, fault driver, recorder, outcome calibration in CI.

## Out of scope (backlog)

Consumer-group rebalancing, transactions, retention and compaction, keyed partitioning, latency shaping in the harness, multi-cluster calibration, KIP-714.

## Acceptance

- [x] `npm test`: engine behaviour suite (35) + web unit tests green.
- [x] bridge + harness pytest green; bridge output validates against the zod schemas.
- [x] Pages build; Simulate, Replay and Live verified headless (Playwright), no console errors.
- [x] Calibration job green on GitHub Actions for gating outcomes (2026-11-07).

## Changes vs the Gemini draft

| Draft said | Now |
|---|---|
| Two different manifest schemas | one zod schema, generated JSON Schema |
| MirrorMaker 2 "deprecated" | wrong for Apache Kafka; MM2 modelled, Cluster Linking labelled Confluent-only |
| Spring WebFlux, `Sinks.Many()` | Python bridge (the API is `Sinks.many()` anyway) |
| WebGL, quadtree culling, transferable arrays | Canvas 2D retained layout; <100 nodes don't need it |
| Hard-coded canvas demo | real state machine + reducer + keyframed timeline |
| Observers and promotion as Kafka features | Confluent MRC only, labelled |
