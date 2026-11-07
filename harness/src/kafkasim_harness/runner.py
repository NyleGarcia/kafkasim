"""Run one scenario manifest against real Kafka; write an EventLog (contracts/event-log.schema.json).

Timeline: t=0 is when producers start (cluster already up, topics created).
Events come from three places: the bridge observer (metadata/ISR/HW), the
producers' delivery reports (PRODUCE_ACK/ERROR), and the fault driver (ACTION,
NODE_STATUS). After the run every topic is read back to compute DATA_LOSS
(acked ids missing from the log) — the same oracle the simulator uses.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from kafkasim_bridge.kafka import KafkaObserver
from kafkasim_bridge.model import Snapshot, diff

from . import docker
from .compose import ComposePlan, plan

log = logging.getLogger("kafkasim.harness")


class Recorder:
    def __init__(self) -> None:
        self.t0 = time.monotonic()
        self.events: list[dict] = []
        self.lock = threading.Lock()

    def now(self) -> float:
        return round((time.monotonic() - self.t0) * 1000, 3)

    def emit(self, e: dict) -> None:
        with self.lock:
            self.events.append(e)


def wait_ready(bootstrap: str, brokers: int, timeout: float = 120) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        try:
            if len(admin.describe_cluster(request_timeout=5).result().nodes) >= brokers:
                return
        except Exception:
            pass
        time.sleep(1)
    raise TimeoutError("cluster did not come up")


def new_topics(p: ComposePlan, manifest: dict) -> list[NewTopic]:
    ids = {v: k for k, v in p.names.items()}
    brokers = [b["id"] for b in manifest["clusters"][0]["brokers"]]
    out = []
    for t in manifest.get("topics", []):
        n = t.get("partitions", 1)
        if t.get("replicas"):
            assign = [[ids[b] for b in r] for r in t["replicas"]]
        else:
            rf = t.get("replicationFactor") or min(3, len(brokers))
            assign = [[ids[brokers[(i + k) % len(brokers)]] for k in range(rf)] for i in range(n)]
        cfg = {}
        if t.get("minInsyncReplicas"):
            cfg["min.insync.replicas"] = str(t["minInsyncReplicas"])
        # confluent-kafka 2.16: num_partitions must equal len(assignment), replication_factor must be -1
        # (its docstring still says -1 for both; create_topics rejects that)
        out.append(NewTopic(t["name"], num_partitions=len(assign), replication_factor=-1, replica_assignment=assign, config=cfg))
    return out


def create_topics(p: ComposePlan, manifest: dict) -> None:
    admin = AdminClient({"bootstrap.servers": p.bootstrap})
    for name, fut in admin.create_topics(new_topics(p, manifest), request_timeout=30).items():
        fut.result()
        log.info("created topic %s", name)


class Remap:
    """Bridge speaks `b<kafka id>` and the real cluster id; rewrite to manifest names."""

    def __init__(self, p: ComposePlan, cluster: str):
        self.nodes = {f"b{k}": v for k, v in p.names.items()}
        self.cluster = cluster

    def tp(self, key: str) -> str:
        _, topic_part = key.split("/", 1)
        return f"{self.cluster}/{topic_part}"

    def __call__(self, e: dict) -> dict:
        e = dict(e)
        for k in ("node", "leader"):
            if isinstance(e.get(k), str):
                e[k] = self.nodes.get(e[k], e[k])
        if "isr" in e:
            e["isr"] = [self.nodes.get(x, x) for x in e["isr"]]
        if "tp" in e:
            e["tp"] = self.tp(e["tp"])
        if "cluster" in e:
            e["cluster"] = self.cluster
        return e


def run_scenario(manifest: dict, out: Path, workdir: Path, keep: bool = False) -> dict:
    p = plan(manifest, workdir)
    cluster = manifest["clusters"][0]["id"]
    rec = Recorder()
    remap = Remap(p, cluster)
    docker.down(p)
    docker.up(p)
    try:
        wait_ready(p.bootstrap, len(manifest["clusters"][0]["brokers"]))
        create_topics(p, manifest)
        time.sleep(2)  # let leaders settle before t=0
        rec.t0 = time.monotonic()
        stop = threading.Event()
        threads = [threading.Thread(target=observe, args=(p, rec, remap, stop), daemon=True)]
        producers = {pr["id"]: ProducerLoop(pr, p.bootstrap, rec, cluster) for pr in manifest.get("producers", [])}
        threads += [threading.Thread(target=pl.run, args=(stop,), daemon=True) for pl in producers.values()]
        threads.append(threading.Thread(target=drive_faults, args=(p, manifest, rec, producers, stop), daemon=True))
        for th in threads:
            th.start()
        duration = manifest.get("durationMs", 60_000) / 1000
        stop.wait(duration)
        stop.set()
        for pl in producers.values():
            pl.flush()
        for th in threads:
            th.join(timeout=15)
        # heal anything left cut off so the read-back can reach every replica
        docker.Faults(p, manifest).apply({"type": "HEAL"})
        for cn in p.containers.values():
            docker.sh("docker", "unpause", cn, check=False)
        loss_check(p, manifest, producers, rec, cluster)
    finally:
        if not keep:
            docker.down(p)
    log_doc = {"schemaVersion": 1, "source": "recording", "manifest": manifest, "events": sorted(rec.events, key=lambda e: e["t"])}
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(log_doc))
    return log_doc


def observe(p: ComposePlan, rec: Recorder, remap: Remap, stop: threading.Event) -> None:
    obs = KafkaObserver({"bootstrap.servers": p.bootstrap}, timeout=3)
    prev: Snapshot | None = None
    while not stop.is_set():
        try:
            snap = obs.snapshot()
            for e in diff(prev, snap, rec.now()):
                if prev is None and e["type"] in ("LEADER_CHANGE", "ISR_CHANGE"):
                    e = e | {"t": 0}
                rec.emit(remap(e))
            prev = snap
        except Exception as e:  # no broker answered metadata at all: keep polling
            log.warning("observe: %s", e)
        stop.wait(0.5)


class ProducerLoop:
    def __init__(self, pr: dict, bootstrap: str, rec: Recorder, cluster: str):
        self.pr = pr
        self.rec = rec
        self.cluster = cluster
        acks = pr.get("acks", "all")
        self.p = Producer(
            {
                "bootstrap.servers": bootstrap,
                "acks": str(acks),
                "linger.ms": pr.get("lingerMs", 5),
                "batch.num.messages": pr.get("batchSize", 16),
                "enable.idempotence": bool(pr.get("enableIdempotence", True)) and acks == "all",
                "request.timeout.ms": pr.get("requestTimeoutMs", 30_000),
                "delivery.timeout.ms": pr.get("deliveryTimeoutMs", 120_000),
                "retry.backoff.ms": pr.get("retryBackoffMs", 100),
            }
        )
        self.active = pr.get("startMs", 0) == 0
        self.acked: dict[str, set[str]] = {}
        self.sent: dict[str, float] = {}

    def on_delivery(self, err, msg) -> None:
        rid = msg.value().decode()
        t = self.rec.now()
        tp = f"{self.cluster}/{msg.topic()}/{msg.partition()}"
        if err is None:
            self.acked.setdefault(tp, set()).add(rid)
            self.rec.emit(
                {
                    "t": t,
                    "type": "PRODUCE_ACK",
                    "producer": self.pr["id"],
                    "tp": tp,
                    "count": 1,
                    "latencyMs": round(t - self.sent.pop(rid, t), 3),
                }
            )
        else:
            name = err.name() if err.code() != KafkaError._MSG_TIMED_OUT else "DELIVERY_TIMEOUT"
            self.rec.emit(
                {"t": t, "type": "PRODUCE_ERROR", "producer": self.pr["id"], "tp": tp, "count": 1, "error": name, "retriable": False}
            )

    def run(self, stop: threading.Event) -> None:
        gap = 1 / self.pr.get("ratePerSec", 10)
        seq = 0
        nxt = time.monotonic()
        while not stop.is_set():
            if self.active:
                rid = f"{self.pr['id']}:{seq}"
                seq += 1
                self.sent[rid] = self.rec.now()
                try:
                    self.p.produce(self.pr["topic"], value=rid.encode(), on_delivery=self.on_delivery)
                except BufferError:
                    pass
            self.p.poll(0)
            nxt += gap
            stop.wait(max(0, nxt - time.monotonic()))

    def flush(self) -> None:
        self.p.flush(30)


def drive_faults(p: ComposePlan, manifest: dict, rec: Recorder, producers: dict, stop: threading.Event) -> None:
    faults = docker.Faults(p, manifest)
    status = {"KILL_NODE": "dead", "RESTART_NODE": "running", "PAUSE_NODE": "paused", "RESUME_NODE": "running"}
    for a in sorted(manifest.get("actions", []), key=lambda a: a["at"]):
        delay = a["at"] / 1000 - (time.monotonic() - rec.t0)
        if stop.wait(max(0, delay)):
            return
        t = rec.now()
        try:
            faults.apply(a)
        except Exception as e:
            rec.emit({"t": t, "type": "NOTE", "level": "error", "message": f"harness could not apply {a['type']}: {e}"})
            continue
        rec.emit({"t": t, "type": "ACTION", "action": a["type"], "detail": json.dumps(a)})
        if a["type"] in status:
            rec.emit({"t": t, "type": "NODE_STATUS", "node": a["node"], "status": status[a["type"]], "reason": "harness"})
        if a["type"] in ("STOP_PRODUCER", "START_PRODUCER") and a["producer"] in producers:
            producers[a["producer"]].active = a["type"] == "START_PRODUCER"


def watermark(c: Consumer, tp: TopicPartition, tries: int = 15) -> int | None:
    """High watermark asked from the broker (not the local cache); None if the partition stays unavailable."""
    for _ in range(tries):
        try:
            return c.get_watermark_offsets(tp, timeout=5, cached=False)[1]
        except Exception:
            time.sleep(2)
    return None


def loss_check(p: ComposePlan, manifest: dict, producers: dict, rec: Recorder, cluster: str) -> None:
    """Read every topic back; acked ids missing anywhere in the partition = DATA_LOSS."""
    present: dict[str, set[str]] = {}
    unreadable: set[str] = set()
    c = Consumer(
        {
            "bootstrap.servers": p.bootstrap,
            "group.id": "kafkasim-harness-check",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    try:
        for t in manifest.get("topics", []):
            ends = {}
            for i in range(t.get("partitions", 1)):
                hi = watermark(c, TopicPartition(t["name"], i))
                if hi is None:
                    rec.emit(
                        {
                            "t": rec.now(),
                            "type": "NOTE",
                            "level": "warn",
                            "message": f"read-back: {t['name']}/{i} unavailable (offline?); loss not assessed",
                        }
                    )
                    unreadable.add(f"{cluster}/{t['name']}/{i}")
                else:
                    ends[i] = hi
            parts = [TopicPartition(t["name"], i, 0) for i in ends]
            c.assign(parts)
            remaining = {k for k, v in ends.items() if v > 0}
            deadline = time.monotonic() + 30
            while remaining and time.monotonic() < deadline:
                for m in c.consume(500, timeout=1):
                    if m.error():
                        continue
                    present.setdefault(f"{cluster}/{m.topic()}/{m.partition()}", set()).add(m.value().decode())
                    if m.offset() + 1 >= ends[m.partition()]:
                        remaining.discard(m.partition())
    finally:
        c.close()
    t = rec.now()
    for pl in producers.values():
        for tp, ids in pl.acked.items():
            if tp in unreadable:
                continue
            lost = len(ids - present.get(tp, set()))
            if lost:
                rec.emit({"t": t, "type": "DATA_LOSS", "tp": tp, "count": lost, "reason": "acked but absent on read-back"})
