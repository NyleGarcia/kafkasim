---
title: Changelog
tags: [docs]
updated: 2026-11-07
---

# Changelog

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
