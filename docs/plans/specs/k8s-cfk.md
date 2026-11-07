---
title: Kubernetes / CFK platform layer
tags: [docs, plans, specs]
status: done
updated: 2026-11-07
---

# Kubernetes / CFK platform layer

## Goal

Simulate Kafka run by **Confluent for Kubernetes (CFK)**. What matters is how Kubernetes scheduling and storage turn a broker failure into a short blip, a long outage, or a permanent loss of redundancy, and how CFK and Self-Balancing heal it.

## Model

A cluster can carry an optional `platform` block. Without it, a cluster runs on bare metal or VMs, as today.

| Concept | Rule |
|---|---|
| Zone | a manifest region = a Kubernetes zone (`topology.kubernetes.io/zone`) |
| Worker node | `{id, zone, schedulable, capacity}`. Capacity = Kafka pods it can host (one per node with anti-affinity = 1) |
| Pod | one per controller and broker (StatefulSet ordinal). Phases: `Pending → ContainerCreating → Running`, `Terminating`, `CrashLoopBackOff` |
| PVC | `zoneLocked` (EBS/PD-style RWO). Bound to the zone of first scheduling; the pod can only ever run in that zone. `ephemeral` storage: the log is wiped when the pod moves |
| Scheduling | a pod goes to a Ready, schedulable worker in an allowed zone (PVC zone if bound), with free capacity and no other Kafka pod (anti-affinity) |
| Container crash | kubelet restarts in place after backoff (`10s × 2^n`, capped at 5 min) → CrashLoopBackOff on repeats |
| Pod deleted (drain or manual) | StatefulSet recreates it → reschedule → startup |
| Worker failure | pods die; the node is NotReady after `nodeNotReadyAfterMs` (40s). **StatefulSet pods on an unreachable node stay Terminating** (no replacement) until the node object is deleted or the pod is force-deleted. Real behaviour: StatefulSets guarantee at most one pod per ordinal |
| Startup | `podStartupMs` (image, JVM, log recovery) before the broker process runs; then it registers like `RESTART_NODE` |
| Rack awareness | on: `broker.rack = zone`, so replica placement is zone-interleaved. Off: every broker is in the same rack, and placement ignores zones |
| CFK Self-Balancing (Confluent) | if a broker stays fenced for `selfBalancing.brokerFailureThresholdMs` (default 1h), move each of its replicas to a live broker, rack-aware. Add the new replica, wait for it to join the ISR, then drop the dead one. Restores the replication factor without the dead zone |

## Faults (manifest actions)

`K8S_NODE_FAIL {node}`, `K8S_NODE_RECOVER {node}`, `K8S_NODE_DELETE {node}`, `K8S_CORDON {node|zone}`, `K8S_UNCORDON {node|zone}`, `POD_DELETE {pod}`, `CONTAINER_CRASH {pod}`, `ZONE_OUTAGE {zone}` (all workers in the zone fail, and the zone is partitioned), `ZONE_RECOVER {zone}`.

## Events

- `K8S_NODE {node, zone, ready, schedulable}`
- `POD {pod, phase, worker|null, reason}`
- `REPLICAS_CHANGE {tp, replicas, observers}`: reassignment, used by the view

## Scenarios

1. **CFK: AZ outage with zone-locked PVCs.** The az1 brokers can't be rescheduled; RF=3 across zones keeps partitions online but under-replicated; Self-Balancing restores RF in az2 and az3.
2. **CFK: container crash self-heal.** Kubelet restart, rejoin the ISR, then CrashLoopBackOff on a repeat.
3. **Rack awareness off.** The same AZ outage takes partitions offline.
4. **Only some AZs schedulable.** Draining a node in a cordoned or full zone leaves a broker Pending: the PVC is zone-locked and the zone has no room.

## Builder

Cluster card → Platform: VM / Kubernetes (CFK). Settings: workers per zone (with schedulable toggle and capacity), storage, rack awareness, timings, Self-Balancing. Faults tab: the new actions.

## Out of scope

Strimzi; PodDisruptionBudgets and rolling-restart orchestration; resource limits and OOM; tiered storage.
