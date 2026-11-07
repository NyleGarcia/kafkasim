---
title: Next
tags: [docs, plans, next]
updated: 2026-11-07
---

# Next

- [ ] Consumer groups: classic eager/cooperative rebalance + KIP-848, offset commits, lag per group
- [ ] Keyed records (murmur2 partitioning) and per-key ordering view
- [ ] Harness latency shaping (tc netem with NET_ADMIN) → calibrate stretch and acks=1 scenarios
- [ ] Producer `max.in.flight` > 1 with idempotent ordering
- [ ] Shareable links (manifest + injected actions in the URL hash)
- [ ] Code-split zod/engine out of the main bundle (current ~700 kB)
- [ ] Calibrate the Kubernetes/CFK layer against a real cluster (kind + CFK in CI, or a zima namespace)
- [ ] Strimzi operator mode; PodDisruptionBudgets and rolling restarts
