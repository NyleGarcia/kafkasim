"""AdminClient polling → Snapshot. The only module that talks to Kafka."""

from __future__ import annotations

import re

from confluent_kafka import TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec

from .model import PartitionState, Snapshot, safe_id


class KafkaObserver:
    def __init__(self, conf: dict, topic_filter: str | None = None, timeout: float = 5.0):
        self.admin = AdminClient(conf)
        self.topic_re = re.compile(topic_filter) if topic_filter else None
        self.timeout = timeout
        self.cluster = "live"

    def snapshot(self) -> Snapshot:
        dc = self.admin.describe_cluster(request_timeout=self.timeout).result()
        self.cluster = safe_id(dc.cluster_id or "live")[:40]
        brokers = {n.id: (n.rack or "default") for n in dc.nodes}
        md = self.admin.list_topics(timeout=self.timeout)
        parts: dict[tuple[str, int], tuple] = {}
        for name, tm in md.topics.items():
            if name.startswith("__") or (self.topic_re and not self.topic_re.search(name)):
                continue
            for pid, pm in tm.partitions.items():
                parts[(name, pid)] = (pm.leader if pm.leader >= 0 else None, tuple(pm.replicas), tuple(pm.isrs))
        hw: dict[tuple[str, int], tuple[int, int]] = {}
        if parts:
            req = {TopicPartition(t, p): OffsetSpec.latest() for (t, p), v in parts.items() if v[0] is not None}
            if req:
                for tp, fut in self.admin.list_offsets(req, request_timeout=self.timeout).items():
                    try:
                        r = fut.result()
                        hw[(tp.topic, tp.partition)] = (r.offset, r.leader_epoch if r.leader_epoch is not None else -1)
                    except Exception:  # offline partition / leader moving
                        pass
        state = {
            k: PartitionState(leader=v[0], replicas=v[1], isr=v[2], hw=hw.get(k, (0, -1))[0], epoch=hw.get(k, (0, -1))[1])
            for k, v in parts.items()
        }
        ctl = dc.controller.id if dc.controller is not None else None
        return Snapshot(cluster=self.cluster, brokers=brokers, partitions=state, controller=ctl)
