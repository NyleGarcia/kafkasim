import json
from pathlib import Path

from kafkasim_bridge.model import PartitionState, Snapshot, diff, manifest_from

CONTRACTS = Path(__file__).resolve().parents[2] / "contracts"


def snap(**parts) -> Snapshot:
    return Snapshot(cluster="c", brokers={1: "east", 2: "east", 3: "west"}, partitions=parts)


def test_initial_diff_emits_full_state():
    s = snap(**{"orders": None})
    s.partitions = {("orders", 0): PartitionState(1, (1, 2, 3), (1, 2, 3), 10, 0)}
    evs = diff(None, s, 0)
    types = [e["type"] for e in evs]
    assert types == ["LEADER_CHANGE", "ISR_CHANGE", "HW", "LEO"]
    assert evs[0]["tp"] == "c/orders/0" and evs[0]["leader"] == "b1"


def test_leader_isr_and_fence_changes():
    a = Snapshot("c", {1: "east", 2: "east", 3: "west"}, {("t", 0): PartitionState(1, (1, 2, 3), (1, 2, 3), 5, 0)})
    b = Snapshot("c", {2: "east", 3: "west"}, {("t", 0): PartitionState(2, (1, 2, 3), (2, 3), 5, 1)})
    evs = diff(a, b, 1000)
    assert {"type": "BROKER_FENCE", "node": "b1", "fenced": True}.items() <= evs[0].items()
    lc = next(e for e in evs if e["type"] == "LEADER_CHANGE")
    assert lc["leader"] == "b2" and lc["epoch"] == 1
    assert next(e for e in evs if e["type"] == "ISR_CHANGE")["isr"] == ["b2", "b3"]
    assert not any(e["type"] == "HW" for e in evs)


def test_offline_partition_has_null_leader():
    a = Snapshot("c", {1: "r"}, {("t", 0): PartitionState(1, (1,), (1,), 0, 0)})
    b = Snapshot("c", {}, {("t", 0): PartitionState(None, (1,), (1,), 0, 0)})
    lc = next(e for e in diff(a, b, 1) if e["type"] == "LEADER_CHANGE")
    assert lc["leader"] is None


def test_manifest_matches_contract_shape():
    s = Snapshot(
        "c",
        {1: "east", 2: "west"},
        {("orders", 0): PartitionState(1, (1, 2), (1, 2), 0, 0), ("orders", 1): PartitionState(2, (2, 1), (2, 1), 0, 0)},
    )
    m = manifest_from(s)
    schema = json.loads((CONTRACTS / "manifest.schema.json").read_text())
    for key in schema["required"]:
        assert key in m
    assert m["topics"][0]["replicas"] == [["b1", "b2"], ["b2", "b1"]]
    assert {r["id"] for r in m["regions"]} == {"east", "west"}
