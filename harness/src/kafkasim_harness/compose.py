"""Manifest → docker compose project for a single KRaft cluster (apache/kafka image).

Compose accepts JSON (YAML 1.2 superset), so no YAML dependency is needed.
Node ids: controllers 1..n, brokers 101..; `names` maps id → manifest node id.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

IMAGE = "apache/kafka:4.2.0"
CLUSTER_ID = "a2Fma2FzaW0tY2FsaWJyYXQ"  # fixed base64 uuid-ish: reproducible formats
HOST_PORT_BASE = 29092


@dataclass
class ComposePlan:
    project: str
    path: Path
    names: dict[int, str]  # kafka node id → manifest id
    containers: dict[str, str]  # manifest id → container name
    bootstrap: str


def plan(manifest: dict, workdir: Path, project: str = "kafkasim") -> ComposePlan:
    clusters = manifest["clusters"]
    if len(clusters) != 1:
        raise ValueError("harness v1 runs single-cluster scenarios only")
    c = clusters[0]
    cfg = {
        "replicaLagTimeMaxMs": 30_000,
        "minInsyncReplicas": 1,
        "uncleanLeaderElection": False,
        "brokerHeartbeatIntervalMs": 2_000,
        "brokerSessionTimeoutMs": 9_000,
        "replicaFetchWaitMaxMs": 500,
    } | c.get("config", {})
    ctl = c["controllers"]
    brk = c["brokers"]
    voters = ",".join(f"{i + 1}@{x['id']}:9093" for i, x in enumerate(ctl))
    names: dict[int, str] = {}
    containers: dict[str, str] = {}
    services: dict[str, dict] = {}
    common = {
        "CLUSTER_ID": CLUSTER_ID,
        "KAFKA_CONTROLLER_QUORUM_VOTERS": voters,
        "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,HOST:PLAINTEXT",
    }
    for i, x in enumerate(ctl):
        nid = i + 1
        names[nid] = x["id"]
        containers[x["id"]] = f"{project}-{x['id']}"
        services[x["id"]] = {
            "image": IMAGE,
            "container_name": containers[x["id"]],
            "hostname": x["id"],
            "environment": common
            | {
                "KAFKA_NODE_ID": str(nid),
                "KAFKA_PROCESS_ROLES": "controller",
                "KAFKA_LISTENERS": "CONTROLLER://:9093",
                "KAFKA_BROKER_SESSION_TIMEOUT_MS": str(cfg["brokerSessionTimeoutMs"]),
            },
        }
    for i, x in enumerate(brk):
        nid = 101 + i
        port = HOST_PORT_BASE + i
        names[nid] = x["id"]
        containers[x["id"]] = f"{project}-{x['id']}"
        services[x["id"]] = {
            "image": IMAGE,
            "container_name": containers[x["id"]],
            "hostname": x["id"],
            "ports": [f"{port}:{port}"],
            "depends_on": [y["id"] for y in ctl],
            "environment": common
            | {
                "KAFKA_NODE_ID": str(nid),
                "KAFKA_PROCESS_ROLES": "broker",
                "KAFKA_BROKER_RACK": x.get("rack") or x["region"],
                "KAFKA_LISTENERS": f"PLAINTEXT://:19092,HOST://:{port}",
                "KAFKA_ADVERTISED_LISTENERS": f"PLAINTEXT://{x['id']}:19092,HOST://localhost:{port}",
                "KAFKA_INTER_BROKER_LISTENER_NAME": "PLAINTEXT",
                "KAFKA_MIN_INSYNC_REPLICAS": str(cfg["minInsyncReplicas"]),
                "KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE": str(cfg["uncleanLeaderElection"]).lower(),
                "KAFKA_REPLICA_LAG_TIME_MAX_MS": str(cfg["replicaLagTimeMaxMs"]),
                "KAFKA_BROKER_HEARTBEAT_INTERVAL_MS": str(cfg["brokerHeartbeatIntervalMs"]),
                "KAFKA_REPLICA_FETCH_WAIT_MAX_MS": str(cfg["replicaFetchWaitMaxMs"]),
                # sim has no preferred-leader rebalancing; keep real Kafka comparable
                "KAFKA_AUTO_LEADER_REBALANCE_ENABLE": "false",
                "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": str(min(3, len(brk))),
                "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": str(min(3, len(brk))),
                "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
                "KAFKA_AUTO_CREATE_TOPICS_ENABLE": "false",
            },
        }
    doc = {"name": project, "services": services}
    workdir.mkdir(parents=True, exist_ok=True)
    path = workdir / "compose.json"
    path.write_text(json.dumps(doc, indent=2))
    bootstrap = ",".join(f"localhost:{HOST_PORT_BASE + i}" for i in range(len(brk)))
    return ComposePlan(project=project, path=path, names=names, containers=containers, bootstrap=bootstrap)
