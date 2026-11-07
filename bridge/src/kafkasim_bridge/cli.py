"""kafkasim-bridge CLI."""

from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(
        prog="kafkasim-bridge", description="Stream a real Kafka cluster to the kafkasim UI (live mode) or record it for replay."
    )
    ap.add_argument("--bootstrap", "-b", default="localhost:9092", help="bootstrap.servers")
    ap.add_argument("--host", default="127.0.0.1", help="WebSocket bind host (default loopback only)")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--interval", type=float, default=1.0, help="AdminClient poll interval, seconds")
    ap.add_argument("--topics", help="regex filter for topic names")
    ap.add_argument("--record", type=Path, help="also write an EventLog JSON (Replay mode) to this file")
    ap.add_argument(
        "-X",
        dest="conf",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="extra librdkafka config, e.g. -X security.protocol=SASL_SSL",
    )
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    from .kafka import KafkaObserver
    from .server import Bridge

    conf = {"bootstrap.servers": args.bootstrap}
    for kv in args.conf:
        k, _, v = kv.partition("=")
        conf[k] = v
    bridge = Bridge(KafkaObserver(conf, args.topics), interval=args.interval)
    try:
        asyncio.run(bridge.run(args.host, args.port, args.record))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
