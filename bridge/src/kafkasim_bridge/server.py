"""Poll loop + WebSocket fan-out + optional recording (EventLog JSON)."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Protocol

from websockets.asyncio.server import ServerConnection, serve

from .model import SCHEMA_VERSION, Snapshot, diff, manifest_from

log = logging.getLogger("kafkasim.bridge")


class Observer(Protocol):
    def snapshot(self) -> Snapshot: ...


class Bridge:
    """Keeps the full event history so late WebSocket joiners get a complete replay."""

    def __init__(self, observer: Observer, interval: float = 1.0, name: str = "kafkasim-bridge"):
        self.observer = observer
        self.interval = interval
        self.name = name
        self.t0 = time.monotonic()
        self.prev: Snapshot | None = None
        self.manifest: dict | None = None
        self.events: list[dict] = []
        self.clients: set[ServerConnection] = set()

    def now_ms(self) -> float:
        return round((time.monotonic() - self.t0) * 1000, 3)

    def tick(self) -> list[dict]:
        snap = self.observer.snapshot()
        t = self.now_ms()
        if self.manifest is None or (self.prev and set(snap.partitions) != set(self.prev.partitions)):
            # topology changed (topic created/deleted): new manifest; clients get a fresh hello
            self.manifest = manifest_from(snap)
            self.prev = None
            self.events = []
        evs = diff(self.prev, snap, t)
        self.prev = snap
        self.events.extend(evs)
        return evs

    def hello(self) -> str:
        return json.dumps({"kind": "hello", "schemaVersion": SCHEMA_VERSION, "bridge": self.name, "manifest": self.manifest})

    async def handler(self, ws: ServerConnection) -> None:
        if self.manifest is None:
            await ws.send(json.dumps({"kind": "error", "message": "bridge has not observed the cluster yet"}))
        else:
            await ws.send(self.hello())
            await ws.send(json.dumps({"kind": "events", "events": self.events}))
        self.clients.add(ws)
        try:
            await ws.wait_closed()
        finally:
            self.clients.discard(ws)

    async def broadcast(self, frame: str) -> None:
        for ws in list(self.clients):
            try:
                await ws.send(frame)
            except Exception:  # closed mid-send
                self.clients.discard(ws)

    def event_log(self) -> dict:
        m = dict(self.manifest or {})
        m["durationMs"] = max(1, int(self.events[-1]["t"]) + 1) if self.events else 1
        return {"schemaVersion": SCHEMA_VERSION, "source": "recording", "manifest": m, "events": self.events}

    async def run(self, host: str, port: int, record: Path | None, stop: asyncio.Event | None = None) -> None:
        stop = stop or asyncio.Event()
        async with serve(self.handler, host, port, process_request=_only_events_path):
            log.info("serving ws://%s:%d/events", host, port)
            while not stop.is_set():
                before = self.manifest
                try:
                    evs = await asyncio.to_thread(self.tick)
                except Exception as e:  # cluster unreachable: keep serving, report
                    log.warning("poll failed: %s", e)
                    await self.broadcast(json.dumps({"kind": "error", "message": f"poll failed: {e}"}))
                    evs = []
                if self.manifest is not before:
                    await self.broadcast(self.hello())
                if evs:
                    await self.broadcast(json.dumps({"kind": "events", "events": evs}))
                if record:
                    record.write_text(json.dumps(self.event_log()))
                try:
                    await asyncio.wait_for(stop.wait(), timeout=self.interval)
                except TimeoutError:
                    pass


def _only_events_path(connection, request):
    if request.path.rstrip("/") not in ("/events", ""):
        return connection.respond(404, "use /events\n")
    return None
