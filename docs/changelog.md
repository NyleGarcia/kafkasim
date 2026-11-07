---
title: Changelog
tags: [docs]
updated: 2026-11-07
---

# Changelog

## Unreleased

- Rack awareness per cluster (per AZ / per region / off) for VMs and Kubernetes. Confluent Replica Placement Policies (`confluent.placement.constraints`) per topic, with a builder editor and a live assignment preview. The MRC scenario now uses an RPP.
- Up to 64 controllers, brokers or workers per location, with typeable counts. The canvas uses a fixed-pitch wrapping grid, so adding a node never moves existing ones. The builder tables have fixed columns.
- Followers long-poll from t=0, which removes a startup latency artifact.

- Regions with availability zones:
  - Three latency tiers (same AZ / cross-AZ / cross-region).
  - AZ- or region-wide outages and partitions; rack placement per AZ.
  - Canvas region bands with AZ columns; builder zone editing (split a region into AZs, add/rename/remove AZs).
  - New multi-region × 3-AZ CFK scenario with region outage and Cluster Linking failover.

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
