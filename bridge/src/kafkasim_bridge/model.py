"""Pure, Kafka-free core: snapshot diffing → SimEvents. Unit-tested without a broker."""

from __future__ import annotations

import re
from dataclasses import dataclass, field

SCHEMA_VERSION = 1


def node_id(broker: int) -> str:
    return f"b{broker}"


def tp_key(cluster: str, topic: str, partition: int) -> str:
    return f"{cluster}/{topic}/{partition}"


def safe_id(raw: str) -> str:
    """Manifest ids must match [A-Za-z0-9_.-]+."""
    return re.sub(r"[^A-Za-z0-9_.-]", "-", raw) or "x"


@dataclass(frozen=True)
class PartitionState:
    leader: int | None
    replicas: tuple[int, ...]
    isr: tuple[int, ...]
    hw: int
    epoch: int


@dataclass
class Snapshot:
    cluster: str
    brokers: dict[int, str]  # broker id → rack/region
    partitions: dict[tuple[str, int], PartitionState] = field(default_factory=dict)
    controller: int | None = None


def diff(prev: Snapshot | None, cur: Snapshot, t: float) -> list[dict]:
    """Events that turn `prev` into `cur`. `prev=None` emits the full initial state."""
    out: list[dict] = []
    c = cur.cluster
    prev_brokers = set(prev.brokers) if prev else set(cur.brokers)
    for b in sorted(prev_brokers - set(cur.brokers)):
        # absent from metadata = fenced/unregistered; we cannot tell dead from partitioned
        out.append(
            {"t": t, "type": "BROKER_FENCE", "cluster": c, "node": node_id(b), "fenced": True, "reason": "absent from cluster metadata"}
        )
    for b in sorted(set(cur.brokers) - prev_brokers):
        out.append({"t": t, "type": "BROKER_FENCE", "cluster": c, "node": node_id(b), "fenced": False, "reason": "registered"})
    if cur.controller is not None and (prev is None or prev.controller != cur.controller):
        out.append({"t": t, "type": "NOTE", "level": "info", "message": f"describeCluster reports controller {cur.controller}"})
    for (topic, p), s in sorted(cur.partitions.items()):
        key = tp_key(c, topic, p)
        old = prev.partitions.get((topic, p)) if prev else None
        if old is None or old.leader != s.leader or old.epoch != s.epoch:
            out.append(
                {
                    "t": t,
                    "type": "LEADER_CHANGE",
                    "tp": key,
                    "leader": node_id(s.leader) if s.leader is not None else None,
                    "epoch": max(s.epoch, 0),
                    "unclean": False,
                }
            )
        if old is None or old.isr != s.isr:
            out.append(
                {"t": t, "type": "ISR_CHANGE", "tp": key, "isr": [node_id(b) for b in s.isr], "reason": "observed" if old else "created"}
            )
        if old is None or old.hw != s.hw:
            out.append({"t": t, "type": "HW", "tp": key, "hw": s.hw})
            if s.leader is not None:
                # leader LEO ≥ HW; AdminClient can't see it, so report HW as the floor
                out.append({"t": t, "type": "LEO", "tp": key, "node": node_id(s.leader), "leo": s.hw})
    return out


def manifest_from(snap: Snapshot, name: str | None = None) -> dict:
    """Synthesize a manifest (contracts/manifest.schema.json) describing the observed cluster."""
    regions = sorted({r for r in snap.brokers.values()}) or ["default"]
    region_of = lambda b: snap.brokers.get(b, regions[0])  # noqa: E731
    topics: dict[str, list[tuple[int, PartitionState]]] = {}
    for (topic, p), s in snap.partitions.items():
        topics.setdefault(topic, []).append((p, s))
    all_brokers = sorted(set(snap.brokers) | {b for s in snap.partitions.values() for b in s.replicas})
    return {
        "schemaVersion": SCHEMA_VERSION,
        "name": name or f"Live: {snap.cluster}",
        "description": "Observed via kafkasim-bridge (AdminClient polling). Per-follower LEO and client traffic are not visible from the AdminClient; leaders show HW as LEO.",
        "durationMs": 1,
        "regions": [{"id": safe_id(r), "name": r} for r in regions],
        "clusters": [
            {
                "id": snap.cluster,
                "controllers": [{"id": "kraft", "region": safe_id(regions[0])}],
                "brokers": [{"id": node_id(b), "region": safe_id(region_of(b))} for b in all_brokers],
            }
        ],
        "topics": [
            {
                "cluster": snap.cluster,
                "name": topic,
                "partitions": len(parts),
                "replicas": [[node_id(b) for b in s.replicas] for _, s in sorted(parts)],
            }
            for topic, parts in sorted(topics.items())
        ],
    }
