import asyncio
import json
import os
from pathlib import Path

from websockets.asyncio.client import connect

from kafkasim_bridge.model import PartitionState, Snapshot
from kafkasim_bridge.server import Bridge


class FakeObserver:
    """Leader moves 1 → 2 on the second poll."""

    def __init__(self):
        self.n = 0

    def snapshot(self) -> Snapshot:
        self.n += 1
        leader, isr = (1, (1, 2)) if self.n == 1 else (2, (2,))
        return Snapshot("c", {1: "r", 2: "r"}, {("t", 0): PartitionState(leader, (1, 2), isr, self.n * 10, self.n - 1)})


def test_hello_backlog_then_stream(tmp_path):
    async def go():
        stop = asyncio.Event()
        bridge = Bridge(FakeObserver(), interval=0.05)
        rec = tmp_path / "rec.json"
        task = asyncio.create_task(bridge.run("127.0.0.1", 18765, rec, stop))
        await asyncio.sleep(0.1)
        frames = []
        async with connect("ws://127.0.0.1:18765/events") as ws:
            for _ in range(3):
                frames.append(json.loads(await asyncio.wait_for(ws.recv(), 2)))
        stop.set()
        await task
        return frames, json.loads(rec.read_text())

    frames, log = asyncio.run(go())
    assert frames[0]["kind"] == "hello" and frames[0]["manifest"]["clusters"][0]["id"] == "c"
    assert frames[1]["kind"] == "events"
    all_events = [e for f in frames[1:] for e in f["events"]]
    leaders = [e["leader"] for e in all_events if e["type"] == "LEADER_CHANGE"]
    assert leaders[:2] == ["b1", "b2"]
    assert log["source"] == "recording" and log["events"]
    # cross-language contract check: CI validates these with the TS zod schemas
    if out := os.environ.get("KAFKASIM_CONTRACT_OUT"):
        Path(out).mkdir(parents=True, exist_ok=True)
        (Path(out) / "bridge-frames.json").write_text(json.dumps(frames))
        (Path(out) / "bridge-recording.json").write_text(json.dumps(log))
