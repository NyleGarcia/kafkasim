"""Fault injection through the docker CLI (kill/start/pause/unpause/network)."""

from __future__ import annotations

import logging
import subprocess

from .compose import ComposePlan

log = logging.getLogger("kafkasim.harness")


def sh(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    log.debug("$ %s", " ".join(args))
    return subprocess.run(args, check=check, capture_output=True, text=True)


def up(p: ComposePlan) -> None:
    sh("docker", "compose", "-f", str(p.path), "up", "-d", "--quiet-pull")


def down(p: ComposePlan) -> None:
    sh("docker", "compose", "-f", str(p.path), "down", "-v", "--remove-orphans", check=False)


def network(p: ComposePlan) -> str:
    return f"{p.project}_default"


class Faults:
    """Maps manifest actions onto containers. Unsupported actions raise."""

    def __init__(self, p: ComposePlan, manifest: dict):
        self.p = p
        self.manifest = manifest
        self.disconnected: set[str] = set()

    def nodes_in(self, ids: list[str]) -> set[str]:
        c = self.manifest["clusters"][0]
        out = set()
        for x in ids:
            if x in self.p.containers:
                out.add(x)
            for n in c["controllers"] + c["brokers"]:
                if n["region"] == x:
                    out.add(n["id"])
        return out

    def apply(self, a: dict) -> None:
        t = a["type"]
        cn = self.p.containers.get(a.get("node", ""))
        if t == "KILL_NODE":
            sh("docker", "kill", "--signal", "SIGKILL", cn)
        elif t == "RESTART_NODE":
            sh("docker", "start", cn)
        elif t == "PAUSE_NODE":
            sh("docker", "pause", cn)
        elif t == "RESUME_NODE":
            sh("docker", "unpause", cn)
        elif t == "PARTITION":
            # v1: every node outside group 0 is cut off from the docker network entirely
            cut = set().union(*(self.nodes_in(g) for g in a["groups"][1:])) - self.nodes_in(a["groups"][0])
            for n in sorted(cut):
                sh("docker", "network", "disconnect", network(self.p), self.p.containers[n])
                self.disconnected.add(n)
        elif t == "HEAL":
            for n in sorted(self.disconnected):
                sh("docker", "network", "connect", "--alias", n, network(self.p), self.p.containers[n], check=False)
            self.disconnected.clear()
        elif t in ("STOP_PRODUCER", "START_PRODUCER"):
            pass  # handled by the runner
        else:
            raise NotImplementedError(f"harness cannot apply {t} yet")
