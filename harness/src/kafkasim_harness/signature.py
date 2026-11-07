"""Outcome signature of an EventLog — what calibration compares between sim and real.

Timing differs between a model and a real cluster; *outcomes* should not:
was acknowledged data lost, did leadership leave a failed broker, did
acks=all hit NOT_ENOUGH_REPLICAS, did the ISR heal, did writes resume.
"""

from __future__ import annotations


def signature(log: dict) -> dict:
    m = log["manifest"]
    evs = sorted(log["events"], key=lambda e: e["t"])
    end = max(m.get("durationMs", 0), evs[-1]["t"] if evs else 0)
    failed = [a for a in m.get("actions", []) if a["type"] in ("KILL_NODE", "PAUSE_NODE")]
    moved = {}
    for a in failed:
        # partitions it led right before the failure: did leadership move within 20s?
        last = {}
        for e in evs:
            if e["type"] == "LEADER_CHANGE" and e["t"] <= a["at"]:
                last[e["tp"]] = e["leader"]
        led = {tp for tp, leader in last.items() if leader == a["node"]}
        moved[a["node"]] = (
            all(
                any(
                    e["type"] == "LEADER_CHANGE" and e["tp"] == tp and a["at"] < e["t"] <= a["at"] + 20_000 and e["leader"] != a["node"]
                    for e in evs
                )
                for tp in led
            )
            if led
            else None
        )
    # replica set = first observed ISR (all replicas at creation); final ISR must contain it again
    first: dict[str, set[str]] = {}
    isr: dict[str, list[str]] = {}
    for e in evs:
        if e["type"] == "ISR_CHANGE":
            first.setdefault(e["tp"], set(e["isr"]))
            isr[e["tp"]] = e["isr"]
    tail = [e for e in evs if e["type"] == "PRODUCE_ACK" and e["t"] > end * 0.8]
    # producer-side view (independent of the observer): longest gap between acks per acks mode.
    # librdkafka retries NOT_ENOUGH_REPLICAS internally, so a stall is the observable symptom.
    acks_mode = {p["id"]: str(p.get("acks", "all")) for p in m.get("producers", [])}

    def max_gap(mode: str) -> float | None:
        ids = {p for p, a in acks_mode.items() if a == mode}
        if not ids:
            return None
        ts = [0.0, *[e["t"] for e in evs if e["type"] == "PRODUCE_ACK" and e["producer"] in ids], end]
        ts.sort()
        return max(b - a for a, b in zip(ts, ts[1:], strict=False))

    gap_all, gap_one = max_gap("all"), max_gap("1")
    return {
        "lost_acked_records": sum(e["count"] for e in evs if e["type"] == "DATA_LOSS") > 0,
        "not_enough_replicas_seen": any(e["type"] == "PRODUCE_ERROR" and "NOT_ENOUGH_REPLICAS" in e["error"] for e in evs),
        "leadership_left_failed_node": moved,
        "isr_complete_at_end": all(set(v) >= first[tp] for tp, v in isr.items()) if isr else None,
        "writes_resumed": len(tail) > 0,
        "acks_all_stalled_5s": None if gap_all is None else gap_all > 5_000,
        "acks_1_stalled_5s": None if gap_one is None else gap_one > 5_000,
    }


def compare(sim: dict, real: dict, gate: list[str]) -> tuple[bool, list[str]]:
    ok = True
    rows = []
    for k in sim:
        same = sim[k] == real.get(k)
        gating = k in gate
        if gating and not same:
            ok = False
        rows.append(f"| {k} | {sim[k]} | {real.get(k)} | {'✅' if same else ('❌' if gating else '⚠️')} |")
    return ok, rows
