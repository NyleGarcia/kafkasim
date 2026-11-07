---
title: "Live mode: kafkasim-bridge"
tags: [docs, guides]
updated: 2026-11-07
---

# Live mode: kafkasim-bridge

A browser cannot open raw TCP connections, so it cannot speak the Kafka protocol. Live mode therefore needs a small helper process running near the cluster.

```bash
# from a clone
cd bridge && uv run kafkasim-bridge --bootstrap localhost:9092
# or without cloning
uvx --from "git+https://github.com/NyleGarcia/kafkasim#subdirectory=bridge" kafkasim-bridge --bootstrap localhost:9092
```

Then open the site → **Live** → Connect (`ws://localhost:8765/events`). Add `?bridge=ws://host:port/events` to the URL to preset another address.

| Flag | Default | |
|---|---|---|
| `--bootstrap` | `localhost:9092` | `bootstrap.servers` |
| `--host` / `--port` | `127.0.0.1` / `8765` | WebSocket bind. Loopback by default; expose deliberately |
| `--interval` | `1.0` s | AdminClient poll interval |
| `--topics` | all non-internal | regex filter |
| `--record FILE` | — | also write an EventLog for Replay mode |
| `-X key=value` | — | extra librdkafka config (SASL/SSL, …) |

## What it observes

- `describeCluster`: registered brokers (and their racks, which become regions). A broker missing from it is reported as fenced; dead and partitioned cannot be told apart.
- `listTopics`: leader, replicas, ISR per partition.
- `listOffsets(latest)`: HW and leader epoch.

It cannot see per-follower LEO, client traffic or KRaft voter state. The UI shows the leader's LEO as HW. Live mode is read-only; use the [harness](harness-calibration.md) to inject faults into real Kafka.

## Browser notes

- Chrome and Firefox allow an https page to open `ws://localhost`. Chrome 142+ may ask for local network access permission first.
- Safari blocks it. Use another browser, or put the bridge behind TLS (`wss://`).
