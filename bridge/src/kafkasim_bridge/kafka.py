"""AdminClient polling → Snapshot. The only module that talks to Kafka.

Every call is isolated: during a failure (dead leader, partitioned broker) one
request timing out must not blind the whole snapshot — that's exactly when the
user wants to see what changes.
"""

from __future__ import annotations

import logging
import re

from confluent_kafka import TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec

from .model import PartitionState, Snapshot, safe_id

log = logging.getLogger("kafkasim.bridge")


class KafkaObserver:
    def __init__(self, conf: dict, topic_filter: str | None = None, timeout: float = 5.0):
        self.admin = AdminClient(conf)
        self.topic_re = re.compile(topic_filter) if topic_filter else None
        self.timeout = timeout
        self.cluster = "live"
        self.last_hw: dict[tuple[str, int], tuple[int, int]] = {}
        self.last_brokers: dict[int, str] = {}

    def snapshot(self) -> Snapshot:
        md = self.admin.list_topics(timeout=self.timeout)  # required; raises if no broker answers
        try:
            dc = self.admin.describe_cluster(request_timeout=self.timeout).result()
            self.cluster = safe_id(dc.cluster_id or "live")[:40]
            brokers = {n.id: (n.rack or "default") for n in dc.nodes}
        except Exception as e:
            log.warning("describe_cluster failed (%s); using metadata broker list", e)
            brokers = {b: self.last_brokers.get(b, "default") for b in md.brokers}
        self.last_brokers = brokers
        parts: dict[tuple[str, int], tuple] = {}
        for name, tm in md.topics.items():
            if name.startswith("__") or (self.topic_re and not self.topic_re.search(name)):
                continue
            for pid, pm in tm.partitions.items():
                parts[(name, pid)] = (pm.leader if pm.leader >= 0 else None, tuple(pm.replicas), tuple(pm.isrs))
        req = {TopicPartition(t, p): OffsetSpec.latest() for (t, p), v in parts.items() if v[0] is not None}
        if req:
            try:
                futs = self.admin.list_offsets(req, request_timeout=min(self.timeout, 2.0))
            except Exception as e:
                log.warning("list_offsets failed: %s", e)
                futs = {}
            for tp, fut in futs.items():
                try:
                    r = fut.result()
                    self.last_hw[(tp.topic, tp.partition)] = (r.offset, r.leader_epoch if r.leader_epoch is not None else -1)
                except Exception:  # leader dead/moving: keep last known HW
                    pass
        state = {}
        for k, v in parts.items():
            hw, epoch = self.last_hw.get(k, (0, -1))
            state[k] = PartitionState(leader=v[0], replicas=v[1], isr=v[2], hw=hw, epoch=epoch)
        return Snapshot(cluster=self.cluster, brokers=brokers, partitions=state, controller=None)
