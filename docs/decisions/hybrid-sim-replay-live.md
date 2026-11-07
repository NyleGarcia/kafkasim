---
title: "Decision: hybrid simulator + recorded replays + optional live bridge"
tags: [docs, decisions]
updated: 2026-11-07
---

# Decision: hybrid simulator + recorded replays + optional live bridge

**Date:** 2026-11-07 · **Status:** accepted

## Context

The first draft (a Gemini-generated spec and prototype) mixed two incompatible manifest schemas. It proposed a Spring WebFlux backend and WebGL with quadtree culling, and it called MirrorMaker 2 "deprecated" (false for Apache Kafka). The prototype was a hard-coded canvas animation with no state machine. Requirements from the owner:
- runs on GitHub Pages (static)
- simulates Kafka *faithfully*
- can optionally attach to a real cluster, which needs its own component

## Options

| | Fidelity | Static hosting | Effort |
|---|---|---|---|
| A. Pure in-browser model | only what we encode | ✅ | low |
| B. Real Kafka + fault injection only | real | ❌ (needs Docker) | medium |
| **C. Model in browser, calibrated against recorded real runs** | real outcomes, deterministic replay | ✅ | medium |

## Decision

**C.**
- The deterministic TypeScript engine is the product on Pages.
- A Docker harness runs the same manifests on real Kafka 4.2, records EventLogs, and gates outcome-level agreement in CI.
- Recordings ship as static Replay files.
- Live mode is a separate local Python bridge that streams the same contract.

## Consequences

- Every simulator rule needs a test, and the outcome-level rules are also checked against real Kafka.
- Python backend (not Spring): it matches the owner's other stacks (uv) and the confluent-kafka AdminClient covers what the bridge needs. KIP-714 client telemetry would need a JVM plugin. Deferred.
- Confluent-only features (Cluster Linking, MRC observers) are modelled but labelled. They are not calibrated, because that needs a multi-broker Confluent Platform licence.
