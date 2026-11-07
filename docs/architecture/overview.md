---
title: Architecture overview
tags: [docs, architecture]
updated: 2026-11-07
---

# Architecture overview

kafkasim is a static web app (GitHub Pages) with three event sources that share one contract:

```
                       contracts/*.schema.json   (emitted from packages/engine/src/schema.ts, zod)
                                │
   ┌────────────────────────────┼─────────────────────────────────┐
   │                            │                                 │
Simulate                     Replay                             Live
packages/engine           EventLog JSON file               bridge/ (Python, uv)
deterministic DES     (harness recording or            AdminClient poll → SimEvents
in a Web Worker        "Export log" from Simulate)      → WebSocket ws://localhost:8765/events
   │                            │                                 │
   └──────────────► SimEvent[] ─┴─► reduceEvent → ViewState ◄─────┘
                                      │
                         apps/web: canvas topology, timeline,
                         partitions, telemetry, event log
```

## Pieces

| Path | What | Tech |
|---|---|---|
| `packages/engine` | Schema/contracts, simulator, view reducer, scenarios | TypeScript, zod 4, vitest |
| `apps/web` | UI, GitHub Pages build | React 19, Vite 8, Tailwind 4, shadcn/ui + 21st.dev components |
| `bridge/` | Live mode + recorder: real cluster → SimEvents | Python 3.12, confluent-kafka, websockets |
| `harness/` | Real Kafka 4.2 in Docker, fault driver, calibration | Python, docker compose |
| `contracts/` | Generated JSON Schemas + scenario JSON | generated (`npm run schema -w @kafkasim/engine`) |

## Key decisions

- **One event contract for all three sources.** The renderer only ever reduces `SimEvent`s, so a real-Kafka recording replays exactly like a simulation. See [decision](../decisions/hybrid-sim-replay-live.md).
- **Engine off the main thread.** The worker simulates a whole run (tens of ms) and precomputes view keyframes every 500 ms. Scrubbing costs one keyframe clone plus at most 500 ms of reduction.
- **Fault injection = deterministic re-run** with extra actions. No mutable engine state lives in the UI.
- **Canvas 2D, retained layout.** Layout is recomputed only on resize or manifest change. Each frame paints the view at `t`, with messages in flight interpolated along arcs. LAN hops (1 ms) get a minimum visual flight time, so they stay visible at any speed.
- **No WebGL or quadtree.** Scenarios have fewer than 100 nodes. Canvas 2D is enough and keeps the code small.
- **Accessibility.** The canvas has a keyboard-accessible twin (the Nodes panel with fault menus). Partition rows are focusable and highlight replicas on the canvas.

## Data flow in Simulate mode

1. `App` → `SimRunner.run({manifest, extraActions})` → worker.
2. Worker: `parseManifest` (zod + cross-reference checks) → `simulate` → `buildTimeline` (state events, MSG list, keyframes).
3. `usePlayback` advances `tRef` every animation frame; panels re-render at about 12 Hz.
4. Click a node, or use the Nodes panel → `FaultOption` → appended to `extraActions` at the current `t` → re-run (step 1).

See [engine-model.md](engine-model.md) for simulation semantics and [contracts.md](contracts.md) for the schemas.
