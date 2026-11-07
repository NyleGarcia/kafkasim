# kafkasim-bridge

Streams a real Kafka cluster to the kafkasim UI (Live mode) or records it for Replay. See [docs/guides/live-bridge.md](../docs/guides/live-bridge.md).

```bash
uv run kafkasim-bridge --bootstrap localhost:9092 [--record out.json]
```
