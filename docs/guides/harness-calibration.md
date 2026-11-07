---
title: Harness and calibration (real Kafka vs simulator)
tags: [docs, guides]
updated: 2026-11-07
---

# Harness and calibration (real Kafka vs simulator)

`harness/` runs a scenario manifest against **real Apache Kafka 4.2 (KRaft) in Docker**. It applies the same timed faults, records an EventLog and compares *outcomes* with the simulator. This keeps the simulator honest.

```bash
cd harness
uv run kafkasim-harness record single-cluster-broker-failure      # → apps/web/public/replays/<slug>.real.json
uv run kafkasim-harness calibrate                                 # all calibrated scenarios, exit 1 on gating mismatch
uv run kafkasim-harness calibrate --only min-insync-replicas-not-enough-replicas
```

Requires Docker with compose v2, Node (to run the sim headless) and about 3 GB RAM (3 controllers + 3 brokers).

## How a run works

1. `compose.plan`: manifest → `compose.json`. Image `apache/kafka:4.2.0`, separate controllers and brokers, and cluster config mapped to broker/controller configs (`min.insync.replicas`, `unclean.leader.election.enable`, `replica.lag.time.max.ms`, `broker.session.timeout.ms`, …). `auto.leader.rebalance.enable=false`, to match the sim.
2. Topics are created with the manifest's replica assignment.
3. At t=0, producers start (confluent-kafka, the same acks/linger/idempotence settings), the bridge observer polls every 500 ms, and the fault driver applies actions:
   - `KILL_NODE` → `docker kill -s SIGKILL`
   - `RESTART_NODE` → `docker start`
   - `PAUSE_NODE` / `RESUME_NODE` → `docker pause` / `unpause` (cgroup freezer, about the same as SIGSTOP)
   - `PARTITION` / `HEAL` → `docker network disconnect` / `connect` (whole-node isolation only)
4. After the run, every topic is read back. Acked ids missing from the log become `DATA_LOSS`, the same oracle the simulator uses.

## What is compared (`signature.py`)

Timing differs between a model and a cluster; outcomes should not.

| Outcome | Meaning |
|---|---|
| `lost_acked_records` | any acknowledged record missing at the end |
| `not_enough_replicas_seen` | acks=all hit `NOT_ENOUGH_REPLICAS` |
| `leadership_left_failed_node` | partitions led by a killed/paused node got a new leader within 20s |
| `isr_complete_at_end` | every ISR is back to its initial replica set |
| `writes_resumed` | acks in the last 20% of the run |

Each scenario lists its **gating** outcomes in `CALIBRATED` (`harness/src/kafkasim_harness/__init__.py`). The others are reported but don't fail the run. The nightly CI job writes a table to the job summary and uploads the recordings, which also become the bundled Replay entries.

## Limits (v1)

- Single-cluster scenarios only. No latency shaping (tc/netem needs `NET_ADMIN`), so WAN scenarios are not calibrated yet.
- Node-level isolation only. Region groups are resolved to their nodes.
