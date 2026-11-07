---
title: Changelog
tags: [docs]
updated: 2026-11-07
---

# Changelog

## Unreleased

- Kubernetes / CFK platform layer:
  - StatefulSet pods on worker nodes per AZ, with zone-locked or ephemeral PVCs and schedulable/capacity per worker.
  - kubelet restarts with CrashLoopBackOff; pod delete with controlled shutdown; stuck `Unknown` pods on failed nodes; AZ outage.
  - Rack awareness = zone; Confluent Self-Balancing broker-failure healing (reassignment).
  - 4 scenarios, 10 tests. Builder platform panel and k8s faults; canvas shows workers and pod phases.

- Scenario builder: visual editor for regions and latency, clusters (KRaft config, controllers and brokers per region), multiple topics (RF or per-partition placement, observers, MM2 / Cluster Linking mirrors), producers, consumers and timed faults. It has a live preview, validation through the engine parser, and JSON as an advanced tab.
- Calibration green on real Kafka 4.2 (broker failure, min.insync, zombie leader). Real-Kafka recordings now ship in Replay.
- Engine fidelity: Kafka default timeouts, connection reset on SIGKILL, KIP-794 adaptive partitioning, acking of committed idempotent duplicates.

## 0.1.0 — 2026-11-07

- Engine: deterministic KRaft/replication model.
  - Produce path: acks, min.insync, idempotence, KIP-580 backoff, KIP-480/794 partitioner.
  - Replication: long-poll fetch, HW/ISR with strict AlterPartition.
  - KRaft: quorum with pre-vote and check-quorum; fencing; clean and unclean election; leader-epoch truncation; zombie leaders.
  - Cross-cluster: Cluster Linking [CP], MRC observers [CP], MM2.
- 9 scenarios; 35 behaviour tests.
- Web: Simulate / Replay / Live; canvas topology, timeline, partitions, telemetry, event log, fault injection, manifest editor.
- Bridge: AdminClient → SimEvents over WebSocket, `--record`.
- Harness: real Kafka 4.2 in Docker, fault driver, read-back loss oracle, outcome calibration.
