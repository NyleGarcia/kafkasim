---
title: "Engine model: what the simulator models, and how faithfully"
tags: [docs, architecture]
updated: 2026-11-07
---

# Engine model: what the simulator models, and how faithfully

The engine (`packages/engine/src/engine.ts`) is a deterministic discrete-event model of Apache Kafka 4.x in KRaft mode, plus two Confluent Platform features that are labelled as such. This note lists what it models, the rules it follows, and where it deliberately simplifies. Every rule here is pinned by a test in `packages/engine/test/behaviour.test.ts`. The outcome-level rules are also checked against real Kafka by the calibration harness (see [harness-calibration.md](../guides/harness-calibration.md)).

## Execution model

- Single event queue ordered by `(time, sequence)`, seeded PRNG (`manifest.seed`). The same manifest gives a byte-identical event log.
- Interactive fault injection re-runs the simulation with the extra action appended. Determinism guarantees the prefix before the injection point is identical (tested).
- Every node (controller, broker, client, MM2 worker) owns its timers. A dead node's timers are discarded (an incarnation counter), and a paused node's timers and inbound messages queue until it resumes.
- Network: per-region latency matrix (`latency.links`, else `interRegionMs`, `intraRegionMs`), ±jitter. A partition is a set of groups; messages across groups are dropped at send *and* at arrival.

## Modelled behaviour

| Area | Rule | Kafka source of truth |
|---|---|---|
| Produce | `acks=0` acks at send; `acks=1` after leader append; `acks=all` once HW ≥ batch end | `Partition.appendRecordsToLeader` |
| min.insync | `acks=all` with ISR < `min.insync.replicas` → `NOT_ENOUGH_REPLICAS`, no append | same |
| Idempotence | leader dedupes `(producerId, seq)`; a duplicate that is already committed is acked straight away | KIP-98 |
| Retries | `retry.backoff.ms` doubling to 1s (KIP-580); `request.timeout.ms` 30s; `delivery.timeout.ms` 120s | producer defaults |
| Connection reset | SIGKILL of a broker → peers get `DISCONNECTED` after ~1 RTT. SIGSTOP and partitions give no reset, so requests wait `request.timeout.ms` | TCP semantics |
| Partitioner | sticky (KIP-480), switches on batch completion. Adaptive (KIP-794): the next partition is the least-loaded one | `BuiltInPartitioner` |
| Batching | `linger.ms`; with a request in flight (`max.in.flight=1`) the open batch keeps filling | accumulator |
| Replication | followers long-poll fetch (`replica.fetch.wait.max.ms`); the leader answers as soon as data arrives; follower LEO is reported on its next fetch | `ReplicaFetcherThread` |
| HW | `min(LEO of ISR)`, never decreases on the leader | `Partition.maybeIncrementLeaderHW` |
| ISR shrink | follower not caught up for `replica.lag.time.max.ms` (using `lastCaughtUpTime` semantics) | `Partition.getOutOfSyncReplicas` |
| ISR expand | follower LEO ≥ HW **and** ≥ start offset of the current leader epoch | `isFollowerInSync` |
| Strict ISR | leader changes ISR only via AlterPartition to the controller; stale partition epochs are rejected | KIP-497 |
| KRaft quorum | leader heartbeats; followers elect after `quorum.fetch.timeout` + random `election.timeout`; **pre-vote** (KIP-996); check-quorum resignation | KIP-595, KIP-996 |
| Fencing | broker heartbeats; no heartbeat for `broker.session.timeout.ms` → fenced → leaderships moved, removed from ISRs (except the last member) | KIP-631 |
| Restart | new incarnation → fenced → re-registers → unfenced, then catches up | KIP-631 |
| Leader election | clean: **first live ISR member in assignment order**. Unclean (if enabled): first live replica, ISR reset to `[leader]` | `PartitionLeaderElectionAlgorithms` |
| Leader epochs | each record carries its epoch; a follower whose last epoch diverges truncates to the leader's end offset for that epoch | KIP-101 / KIP-279 |
| Zombie leader | a paused leader keeps its stale view; on wake it processes buffered requests *then* new metadata, so acks=1 writes it accepted are truncated | — |
| Consumers | read ≤ HW from the leader; out-of-range position → `auto.offset.reset=latest` | fetch path |
| Data-loss oracle | acked record id missing from the committed leader's log → `DATA_LOSS` | — |
| Cluster Linking *(Confluent)* | destination mirror leader long-polls the source leader for records ≤ source HW; offsets preserved; mirror read-only until `FAILOVER` | CP docs |
| MRC observers *(Confluent)* | observers replicate but are never in the ISR unless `observerPromotionPolicy=under-min-isr` and ISR < min.insync; demoted when sync replicas return | CP docs |
| MirrorMaker 2 *(Apache)* | Connect worker consumes ≤ HW, produces with acks=all; offsets not preserved | KIP-382 |

## Kubernetes / CFK platform (`cluster.platform`)

Optional per cluster. Spec: [k8s-cfk](../plans/specs/k8s-cfk.md). A manifest region is a Kubernetes zone. Each controller and broker is a StatefulSet pod placed on a worker node.

| Behaviour | Rule |
|---|---|
| Initial placement | each pod goes to the first schedulable worker in its zone with free capacity; a zone that can't host its pods is a manifest error |
| Container crash (`CONTAINER_CRASH`, or `KILL_NODE` on a pod) | kubelet restarts in place: immediately the first time, then `crashBackoffBaseMs × 2^(n-2)` capped at `crashBackoffMaxMs` (CrashLoopBackOff), reset after 10 min of running. The PVC stays attached, so the log survives |
| Startup | `podStartupMs` before the broker process starts and re-registers (fenced → caught up → unfenced) |
| Pod delete / drain | SIGTERM → **controlled shutdown** (the controller fences the broker and moves its leaderships first) → exit after `terminationGraceMs` → StatefulSet recreates → scheduler |
| Scheduling | Ready + schedulable worker, in the PVC's zone, with free capacity. Otherwise `Pending` with the reason |
| Worker failure | processes die at once; NotReady after `nodeNotReadyAfterMs`; pods go `Unknown` and are **not replaced** (StatefulSet at-most-one semantics) until `K8S_NODE_RECOVER` or `K8S_NODE_DELETE` |
| Zone outage | every worker in the zone fails, and the zone is network-partitioned from the others |
| Storage | `zoneLocked` (default): the pod can only run in its PVC zone. `ephemeral`: a recreated pod starts with an empty log and re-replicates |
| Rack awareness | on: `broker.rack = zone` (zone-interleaved placement); off: placement ignores zones |
| Self-Balancing *(Confluent)* | a broker fenced longer than `brokerFailureThresholdMs` gets its replicas reassigned to healthy, least-loaded brokers, preferring uncovered zones. Add replica → wait for the ISR → drop the dead replica (`REPLICAS_CHANGE`) |

Simplifications:
- The pod's zone never changes, even with ephemeral storage.
- No PodDisruptionBudgets, resource limits or OOM kills.
- No taint-based eviction timers beyond NotReady.
- No Strimzi.
- The CFK operator's own reconcile loop is folded into StatefulSet behaviour.
- Not calibrated against a real Kubernetes cluster yet (backlog: kind-based harness).

## Deliberate simplifications

Fix these when calibration shows they matter.

- `batch.size` is counted in records, not bytes. `max.in.flight.requests.per.connection` is fixed at 1 per partition.
- One fetch request per (follower, partition), not one per broker pair. Message counts are higher than real; outcomes are unaffected.
- Records have no size, so throughput limits, quotas, disk and page cache are not modelled. Logs survive SIGKILL (Kafka relies on replication, not fsync).
- The controller "metadata log" is a single shared object, written only by a controller that holds a live quorum. Raft log replication between controllers is not modelled; election safety is.
- Brokers learn about changes by controller push (with latency) and by heartbeat responses, not by fetching `__cluster_metadata`.
- Clients find the leader through a metadata request to a reachable broker, which answers from its own possibly stale view. Producers cache the leader until an error.
- Cluster Linking and MM2 look up the source leader from committed metadata (no separate client metadata cache).
- Consumer groups are static (partitions split by sorted member id). Rebalancing (classic and KIP-848), offset commits, transactions, retention, compaction and keyed partitioning are not modelled yet. See [backlog](../plans/next/backlog.md).
- No preferred-leader rebalancing (`auto.leader.rebalance.enable` is off in the harness too).
- No ELR (KIP-966). An offline partition keeps its last leader in the ISR and recovers when that broker returns.

## Known behaviours worth knowing (not bugs)

- **WAN followers rejoin the ISR only during a lull.** Under a steady trickle faster than the follower RTT, the follower's fetch offset is always about one record behind HW. Real Kafka has the same rule (`followerLEO >= HW`). The stretch scenario uses `linger.ms=100`.
- **A partitioned leader stalls its partition for `request.timeout.ms`.** No TCP reset arrives, so the in-flight request times out (30s). KIP-794 then moves new records to healthy partitions.

## Reference simulators

Logic only. [monedula.dev/kafka-simulator](https://monedula.dev/kafka-simulator/) publishes a useful [fidelity page](https://monedula.dev/flock/docs/kafka-simulator/concepts/fidelity/). Where this engine differs on purpose:
- We hold batches to a leaderless partition until `delivery.timeout.ms`, as Kafka does.
- We elect the first live ISR member, as Kafka does (monedula elects the highest LEO).
- We model network partitions at the message level (latency and drops), not as truncate-to-HW.
