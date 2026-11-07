# kafkasim

**See what Kafka actually does when things break.** A deterministic, in-browser simulator of Apache Kafka 4.x (KRaft) replication, failure and disaster recovery. It is checked against real Kafka in CI.

**▶ [nylegarcia.github.io/kafkasim](https://nylegarcia.github.io/kafkasim/)**

- **Simulate.** Fourteen scenarios (broker failure, acks=1 vs acks=all, `min.insync.replicas`, zombie leader, unclean election, stretch-cluster partition, MirrorMaker 2 DR, plus Confluent Cluster Linking, MRC observers and four CFK-on-Kubernetes scenarios, labelled). Build your own in the visual **scenario builder**: regions, clusters, topics, clients, Kubernetes and timed faults, no JSON needed. Watch every produce, fetch, heartbeat and vote move between nodes. Scrub the timeline, or click any broker to kill, pause or isolate it at that moment.
- **Replay.** Load an event log recorded from real Kafka (or exported from a simulation). Same renderer, same schema.
- **Live.** Run `kafkasim-bridge` next to your cluster and watch its leaders, ISRs and high watermarks change in real time.

## What it models

Produce path:
- acks 0/1/all and `min.insync.replicas`
- idempotent retries and KIP-580 backoff
- sticky + adaptive partitioning (KIP-480/794)

Replication and failure:
- long-poll follower fetch, LEO/HW, ISR shrink/expand through the controller (strict ISR)
- KRaft quorum with pre-vote and check-quorum (KIP-996), broker fencing
- clean and unclean leader election, leader-epoch truncation, zombie leaders
- consumers read up to the HW

Cross-cluster:
- Cluster Linking and MRC observers *(Confluent Platform)*
- MirrorMaker 2 *(Apache)*

Topology: regions containing availability zones. Latency has three tiers (same AZ, cross-AZ, cross-region), and an outage or partition can hit one AZ or a whole region.

On Kubernetes (Confluent for Kubernetes):
- StatefulSet pods, zone-locked PVCs, and worker nodes that are schedulable only in some AZs
- kubelet self-healing (CrashLoopBackOff), drains with controlled shutdown, stuck pods on failed nodes, AZ outages
- rack awareness from the zone, and Self-Balancing healing

Full rules and deliberate simplifications: [docs/architecture/engine-model.md](docs/architecture/engine-model.md).

## How it stays honest

`harness/` runs the same scenario manifests on **real Apache Kafka 4.2 in Docker**. It applies the same faults (`docker kill`, `pause`, network disconnect), reads every topic back to find lost acknowledged records, and compares outcomes with the simulator: data lost? leadership moved? NOT_ENOUGH_REPLICAS? ISR healed? A nightly CI job fails on any gating mismatch. Details: [docs/guides/harness-calibration.md](docs/guides/harness-calibration.md).

## Repo layout

| Path | |
|---|---|
| `packages/engine` | contracts (zod), simulator, view reducer, scenarios, behaviour tests |
| `apps/web` | React 19 + Vite 8 + Tailwind 4 UI, shadcn/ui and adapted [21st.dev](https://21st.dev) components |
| `bridge/` | `kafkasim-bridge`: live mode and recorder (Python, uv) |
| `harness/` | real-Kafka runner and calibration (Python, Docker) |
| `contracts/` | generated JSON Schemas + scenario JSON |
| `docs/` | architecture, guides, decisions, plans (Obsidian-compatible vault) |

## Develop

```bash
npm ci
npm run dev                       # http://localhost:5173/kafkasim/
npm test                          # engine behaviour suite + web unit tests
npm run schema -w @kafkasim/engine  # regenerate contracts/ after editing schema.ts

cd bridge && uv run pytest        # bridge
cd harness && uv run kafkasim-harness calibrate   # needs Docker
```

Write your own scenario: **Edit** in the UI, or add JSON matching [`contracts/manifest.schema.json`](contracts/manifest.schema.json).

## Inspiration

[monedula.dev/kafka-simulator](https://monedula.dev/kafka-simulator/) is a great, broad Kafka simulator and its published fidelity notes were a useful logic reference. kafkasim exists because I needed something different: a static GitHub Pages app, message-level network and failure timing, deterministic fault injection mid-run, real-Kafka calibration and replays, and a live bridge to my own clusters. [kafkaisfun.dev](https://kafkaisfun.dev/simulate) was also a logic reference. No code or UI was taken from either.

UI components adapted from 21st.dev: *Log Viewer* (Hirael, MIT, Mohammad Shehadeh), *Status Dot* (iconiq), *Number Ticker* (fancycomponents). Credits are in each file's header.
