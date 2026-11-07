/**
 * Kafka behaviour suite. Each test pins one documented Kafka property against
 * the engine. When the calibration harness records real-Kafka traces, the same
 * assertions run against those recordings (harness/calibrate.py).
 */
import { describe, expect, it } from 'vitest';
import { buildTimeline, parseManifest, placementFor, rackIds, scenarios, simulate, viewAt, type ManifestInput, type SimEvent } from '../src/index.ts';

const byName = (prefix: string) => {
  const s = scenarios.find((x) => x.name.startsWith(prefix));
  if (!s) throw new Error(`no scenario ${prefix}`);
  return structuredClone(s) as ManifestInput;
};
const run = (input: ManifestInput) => {
  const m = parseManifest(input);
  return { m, ...simulate(m) };
};
const of = <T extends SimEvent['type']>(evs: SimEvent[], type: T) => evs.filter((e): e is Extract<SimEvent, { type: T }> => e.type === type);
const sum = (evs: { count: number }[]) => evs.reduce((a, e) => a + e.count, 0);

describe('manifest validation', () => {
  it('rejects dangling references with every problem listed', () => {
    const bad = byName('Single cluster');
    bad.producers![0]!.topic = 'nope';
    bad.actions = [{ type: 'KILL_NODE', at: 1, node: 'ghost' }];
    expect(() => parseManifest(bad)).toThrow(/unknown topic main\/nope[\s\S]*unknown node "ghost"/);
  });
  it('rejects Confluent features on an apache cluster', () => {
    const bad = byName('MRC observers');
    bad.clusters[0]!.flavor = 'apache';
    expect(() => parseManifest(bad)).toThrow(/require a confluent-flavor cluster/);
  });
  it('every built-in scenario parses', () => {
    for (const s of scenarios) expect(() => parseManifest(s)).not.toThrow();
  });
});

describe('determinism', () => {
  it('same manifest + seed → identical event log', () => {
    const a = run(byName('Stretch cluster'));
    const b = run(byName('Stretch cluster'));
    expect(JSON.stringify(a.events)).toBe(JSON.stringify(b.events));
  });
  it('injected action leaves the prefix before it untouched', () => {
    const m = parseManifest(byName('Single cluster'));
    const base = simulate(m).events;
    const inj = simulate(m, { extraActions: [{ type: 'KILL_NODE', at: 5_000, node: 'b2' }] }).events;
    const before = (evs: SimEvent[]) => JSON.stringify(evs.filter((e) => e.t < 5_000));
    expect(before(inj)).toBe(before(base));
    expect(inj.some((e) => e.type === 'NODE_STATUS' && e.node === 'b2' && e.status === 'dead')).toBe(true);
  });
});

describe('producer partitioner', () => {
  it('adaptive sticky partitioner (KIP-794) never starves a partition, but favours the less-loaded one', () => {
    const r = run(byName('Stretch cluster'));
    const per = (tp: string, t0: number, t1: number) => sum(of(r.events, 'PRODUCE_ACK').filter((e) => e.tp === tp && e.t >= t0 && e.t < t1));
    const a = per('stretch/trades/0', 0, 15_000); // led by e1, same region as producer
    const b = per('stretch/trades/1', 0, 15_000); // led by w1, across the WAN
    expect(Math.min(a, b) / Math.max(a, b)).toBeGreaterThan(0.3);
    expect(a).toBeGreaterThan(b);
    // while p1's leader is unreachable its queue backs up → new records go to p0
    expect(per('stretch/trades/0', 25_000, 35_000)).toBeGreaterThan(250);
  });
});

describe('idempotent producer', () => {
  it('a retried batch that is already committed is acked (not stuck until delivery timeout)', () => {
    // stretch: p1 leader w1 is cut off mid-request; the batch was already replicated, new leader e2 dedupes it
    const r = run(byName('Stretch cluster'));
    // in-flight request to cut-off leader w1 (no TCP reset) waits request.timeout.ms=30s, then retries to e2
    const p1 = of(r.events, 'PRODUCE_ACK').filter((e) => e.tp === 'stretch/trades/1' && e.t > 44_000 && e.t < 50_000);
    expect(sum(p1)).toBeGreaterThan(100);
    expect(of(r.events, 'PRODUCE_ERROR').filter((e) => e.error === 'DELIVERY_TIMEOUT')).toHaveLength(0);
  });
});

describe('broker failure (acks=all, RF=3, min.insync=2)', () => {
  const r = run(byName('Single cluster'));
  it('fences the dead broker after broker.session.timeout.ms, not before', () => {
    const fence = of(r.events, 'BROKER_FENCE').find((e) => e.node === 'b1' && e.fenced)!;
    // timeout counts from the last heartbeat (≤ 2s before the kill)
    expect(fence.t).toBeGreaterThanOrEqual(15_000 + 9_000 - 2_000);
    expect(fence.t).toBeLessThan(15_000 + 9_000 + 1_500);
  });
  it('moves leadership off b1 with a bumped epoch', () => {
    const lc = of(r.events, 'LEADER_CHANGE').filter((e) => e.t > 15_000 && e.tp === 'main/orders/0');
    expect(lc[0]!.leader).not.toBe('b1');
    expect(lc[0]!.epoch).toBe(1);
  });
  it('loses no acknowledged record', () => {
    expect(of(r.events, 'DATA_LOSS')).toHaveLength(0);
  });
  it('b1 rejoins the ISR after restart', () => {
    const tl = buildTimeline(r.m, r.events, r.endT);
    const end = viewAt(tl, r.endT);
    for (const p of Object.values(end.partitions)) expect(p.isr).toContain('b1');
  });
  it('consumers only read up to the high watermark (never more than acked)', () => {
    const consumed = sum(of(r.events, 'CONSUME'));
    const acked = sum(of(r.events, 'PRODUCE_ACK'));
    expect(consumed).toBeLessThanOrEqual(acked + 50); // + in-flight acks
    expect(consumed).toBeGreaterThan(acked * 0.9);
  });
});

describe('acks=1 vs acks=all', () => {
  const r = run(byName('acks=1 vs acks=all'));
  it('acks=1 topic loses acknowledged records on leader crash', () => {
    expect(sum(of(r.events, 'DATA_LOSS').filter((e) => e.tp === 'main/clicks/0'))).toBeGreaterThan(0);
  });
  it('acks=all topic loses nothing', () => {
    expect(of(r.events, 'DATA_LOSS').filter((e) => e.tp === 'main/payments/0')).toHaveLength(0);
  });
  it('acks=all costs at least one follower round trip (~2×25ms)', () => {
    const lat = (p: string) => {
      const xs = of(r.events, 'PRODUCE_ACK').filter((e) => e.producer === p && e.t < 9_000).map((e) => e.latencyMs).sort((a, b) => a - b);
      return xs[Math.floor(xs.length / 2)]!;
    };
    expect(lat('pay')).toBeGreaterThan(45);
    expect(lat('web')).toBeLessThan(lat('pay'));
  });
});

describe('min.insync.replicas', () => {
  const r = run(byName('min.insync.replicas'));
  it('acks=all gets NOT_ENOUGH_REPLICAS once ISR < 2', () => {
    const shrink = of(r.events, 'ISR_CHANGE').find((e) => e.tp === 'main/ledger/0' && e.isr.length === 1)!;
    expect(shrink).toBeDefined();
    expect(of(r.events, 'PRODUCE_ERROR').some((e) => e.producer === 'bank' && e.error === 'NOT_ENOUGH_REPLICAS' && e.t > shrink.t)).toBe(true);
  });
  it('acks=1 keeps writing while ISR = 1', () => {
    const acks = of(r.events, 'PRODUCE_ACK').filter((e) => e.producer === 'agent' && e.t > 22_000 && e.t < 28_000);
    expect(sum(acks)).toBeGreaterThan(50);
  });
  it('acks=all recovers once a replica returns', () => {
    expect(of(r.events, 'PRODUCE_ACK').some((e) => e.producer === 'bank' && e.t > 32_000)).toBe(true);
  });
});

describe('zombie leader', () => {
  const r = run(byName('Zombie leader'));
  it('new leader elected while b1 is paused', () => {
    const lc = of(r.events, 'LEADER_CHANGE').find((e) => e.t > 8_000)!;
    expect(lc.leader).not.toBe('b1');
    expect(lc.t).toBeLessThan(23_000);
  });
  it('woken zombie truncates its divergent suffix', () => {
    expect(of(r.events, 'TRUNCATE').some((e) => e.node === 'b1' && e.t >= 23_000)).toBe(true);
  });
  it('acks=1 writes accepted by the zombie are lost', () => {
    expect(sum(of(r.events, 'DATA_LOSS'))).toBeGreaterThan(0);
  });
});

describe('unclean leader election', () => {
  it('enabled: out-of-sync replica becomes leader, acked data lost', () => {
    const r = run(byName('Unclean'));
    expect(of(r.events, 'LEADER_CHANGE').some((e) => e.unclean && e.leader === 'b2')).toBe(true);
    expect(sum(of(r.events, 'DATA_LOSS'))).toBeGreaterThan(0);
  });
  it('disabled: partition goes offline, nothing lost', () => {
    const s = byName('Unclean');
    s.clusters[0]!.config!.uncleanLeaderElection = false;
    const r = run(s);
    expect(of(r.events, 'LEADER_CHANGE').some((e) => e.leader === null)).toBe(true);
    expect(of(r.events, 'DATA_LOSS')).toHaveLength(0);
  });
});

describe('stretch cluster partition', () => {
  const r = run(byName('Stretch cluster'));
  it('majority side keeps the controller; no controller flaps on heal (pre-vote)', () => {
    expect(of(r.events, 'CONTROLLER_LOST')).toHaveLength(0);
  });
  it('east keeps acknowledging on the east-led partition once west leaves the ISR', () => {
    expect(sum(of(r.events, 'PRODUCE_ACK').filter((e) => e.tp === 'stretch/trades/0' && e.t > 25_000 && e.t < 35_000))).toBeGreaterThan(100);
  });
  it('a killed broker resets connections: producer fails fast instead of waiting request.timeout.ms', () => {
    const s = run(byName('Single cluster'));
    const first = of(s.events, 'PRODUCE_ERROR').find((e) => e.t > 15_000)!;
    expect(first.error).toBe('DISCONNECTED');
    expect(first.t - 15_000).toBeLessThan(50);
  });
  it('west brokers are fenced, then unfenced after heal', () => {
    const f = of(r.events, 'BROKER_FENCE').filter((e) => e.node === 'w1');
    expect(f.map((e) => e.fenced)).toEqual([true, false]);
    expect(f[1]!.t).toBeGreaterThan(35_000);
  });
  it('acks=all latency includes the WAN round trip (west in ISR before partition)', () => {
    const xs = of(r.events, 'PRODUCE_ACK').filter((e) => e.t < 14_000).map((e) => e.latencyMs).sort((a, b) => a - b);
    expect(xs[Math.floor(xs.length / 2)]!).toBeGreaterThan(70);
  });
  it('all replicas back in ISR at end; no loss', () => {
    const end = viewAt(buildTimeline(r.m, r.events, r.endT), r.endT);
    for (const p of Object.values(end.partitions)) expect(p.isr.sort()).toEqual(['e1', 'e2', 'w1', 'w2']);
    expect(of(r.events, 'DATA_LOSS')).toHaveLength(0);
  });
});

describe('cross-cluster DR', () => {
  const cl = run(byName('Cluster Linking'));
  const mm2 = run(byName('MirrorMaker 2'));
  const rpo = (evs: SimEvent[]) => of(evs, 'MIRROR_PROMOTED').reduce((a, e) => a + e.rpoRecords, 0);
  it('cluster link mirror offsets equal source offsets (same LEO pre-failure)', () => {
    const end = viewAt(buildTimeline(cl.m, cl.events, cl.endT), 19_900);
    const src = end.partitions['primary/orders/0']!;
    const dst = end.partitions['dr/orders/0']!;
    expect(Math.abs((dst.leo[dst.leader!] ?? 0) - src.hw)).toBeLessThan(10);
  });
  it('both promote with a small, bounded RPO', () => {
    expect(of(cl.events, 'MIRROR_PROMOTED')).toHaveLength(2);
    expect(rpo(cl.events)).toBeLessThan(25);
    expect(rpo(mm2.events)).toBeLessThan(60);
  });
  it('mirror topic is read-only before promotion', () => {
    const s = byName('Cluster Linking');
    s.producers!.push({ id: 'rogue', cluster: 'dr', region: 'west', topic: 'orders', acks: 'all', ratePerSec: 5 });
    const r = run(s);
    expect(of(r.events, 'PRODUCE_ERROR').some((e) => e.producer === 'rogue' && e.error === 'READ_ONLY_MIRROR')).toBe(true);
  });
});

describe('MRC observers', () => {
  const r = run(byName('MRC observers'));
  it('observers are outside the ISR in steady state', () => {
    const end = viewAt(buildTimeline(r.m, r.events, r.endT), 11_000);
    expect(end.partitions['mrc/payments/0']!.isr.sort()).toEqual(['e1', 'e2']);
  });
  it('promotes an observer when ISR < min.insync, then writes resume', () => {
    const promo = of(r.events, 'ISR_CHANGE').find((e) => e.reason?.includes('observer promotion'))!;
    expect(promo).toBeDefined();
    expect(promo.isr.some((x) => x.startsWith('w'))).toBe(true);
    expect(sum(of(r.events, 'PRODUCE_ACK').filter((e) => e.t > promo.t + 1_000 && e.t < 32_000))).toBeGreaterThan(100);
  });
  it('demotes observers once sync replicas are back', () => {
    const end = viewAt(buildTimeline(r.m, r.events, r.endT), r.endT);
    expect(end.partitions['mrc/payments/0']!.isr.sort()).toEqual(['e1', 'e2']);
  });
});

describe('Kubernetes / CFK platform', () => {
  const pods = (evs: SimEvent[], pod: string) => of(evs, 'POD').filter((e) => e.pod === pod);

  it('rejects CFK on an apache-flavor cluster and zones that cannot host their pods', () => {
    const bad = byName('CFK: broker container crash');
    bad.clusters[0]!.flavor = 'apache';
    expect(() => parseManifest(bad)).toThrow(/CFK runs Confluent Server/);
    const full = byName('CFK: broker container crash');
    full.clusters[0]!.platform!.workers = full.clusters[0]!.platform!.workers.filter((w) => w.zone !== 'az3');
    expect(() => parseManifest(full)).toThrow(/no schedulable worker with free capacity in zone az3/);
  });

  it('rack awareness: RF=3 placement puts one replica per zone', () => {
    const m = parseManifest(byName('CFK: AZ outage'));
    const zone = (b: string) => m.clusters[0]!.brokers.find((x) => x.id === b)!.region;
    const v = viewAt(buildTimeline(m, simulate(m, { untilMs: 1 }).events, 1), 0);
    for (const p of Object.values(v.partitions)) expect(new Set(p.replicas.map(zone)).size).toBe(3);
  });

  it('container crash: kubelet restarts in place, then CrashLoopBackOff with doubling back-off; log survives', () => {
    const r = run(byName('CFK: broker container crash'));
    const ph = pods(r.events, 'b1').map((e) => e.phase);
    expect(ph.filter((p) => p === 'CrashLoopBackOff')).toHaveLength(2);
    const back = pods(r.events, 'b1').filter((e) => e.phase === 'CrashLoopBackOff').map((e) => e.reason);
    expect(back).toEqual(['restart 2: back-off 10s', 'restart 3: back-off 20s']);
    expect(of(r.events, 'TRUNCATE').filter((e) => e.node === 'b1' && e.to === 0)).toHaveLength(0); // PVC kept the log
    // back after the 2nd restart (Running at 80s) and in sync before the 3rd crash at 85s
    const mid = viewAt(buildTimeline(r.m, r.events, r.endT), 84_000);
    const hosted = Object.values(mid.partitions).filter((p) => p.replicas.includes('b1'));
    expect(hosted.length).toBeGreaterThan(0);
    for (const p of hosted) expect(p.isr).toContain('b1');
    expect(of(r.events, 'DATA_LOSS')).toHaveLength(0);
  });

  it('worker failure: StatefulSet pods stay stuck (Unknown), never rescheduled while the node is unreachable', () => {
    const r = run(byName('CFK: AZ outage'));
    const b1 = pods(r.events, 'b1');
    expect(b1.find((e) => e.phase === 'Unknown')!.t).toBeCloseTo(10_000 + 40_000, -2); // nodeNotReadyAfterMs
    expect(b1.filter((e) => e.t > 10_000 && e.t < 120_000).some((e) => e.phase === 'Pending' || e.phase === 'ContainerCreating')).toBe(false);
  });

  it('AZ outage with rack awareness: partitions stay online; Self-Balancing restores RF without the dead zone', () => {
    const r = run(byName('CFK: AZ outage'));
    expect(of(r.events, 'LEADER_CHANGE').filter((e) => e.t > 0 && e.leader === null)).toHaveLength(0);
    const tl = buildTimeline(r.m, r.events, r.endT);
    const healed = viewAt(tl, 110_000);
    for (const p of Object.values(healed.partitions)) {
      expect(p.replicas).toHaveLength(3);
      expect(p.replicas.some((b) => b === 'b1' || b === 'b2')).toBe(false);
      expect(p.isr).toHaveLength(3);
    }
    expect(sum(of(r.events, 'PRODUCE_ACK').filter((e) => e.t > 30_000 && e.t < 110_000))).toBeGreaterThan(2_500);
    expect(of(r.events, 'DATA_LOSS')).toHaveLength(0);
  });

  it('rack awareness off: the same outage takes co-located partitions offline', () => {
    const r = run(byName('Kubernetes: rack awareness off'));
    expect(of(r.events, 'LEADER_CHANGE').some((e) => e.leader === null)).toBe(true);
    const on = byName('Kubernetes: rack awareness off');
    on.clusters[0]!.rack = 'zone';
    expect(of(run(on).events, 'LEADER_CHANGE').some((e) => e.leader === null)).toBe(false);
  });

  it('drain into a full zone: controlled shutdown (no lost acks), pod Pending on its zone-locked PVC until uncordon', () => {
    const r = run(byName('Kubernetes: drain'));
    expect(of(r.events, 'BROKER_FENCE').find((e) => e.node === 'b5')!.reason).toBe('controlled shutdown');
    const pend = pods(r.events, 'b5').filter((e) => e.phase === 'Pending');
    expect(pend.at(-1)!.reason).toMatch(/PVC is bound to az3/);
    const sched = pods(r.events, 'b5').find((e) => e.phase === 'ContainerCreating')!;
    expect(sched.t).toBeGreaterThanOrEqual(60_000);
    expect(of(r.events, 'PRODUCE_ERROR').filter((e) => !e.retriable)).toHaveLength(0);
    expect(of(r.events, 'DATA_LOSS')).toHaveLength(0);
  });

  it('KILL_NODE on a pod is a container crash: Kubernetes heals it', () => {
    const s = byName('CFK: broker container crash');
    s.actions = [{ type: 'KILL_NODE', at: 10_000, node: 'b3' }];
    const r = run(s);
    expect(pods(r.events, 'b3').some((e) => e.phase === 'Running' && e.t > 10_000)).toBe(true);
    expect(of(r.events, 'NODE_STATUS').filter((e) => e.node === 'b3').map((e) => e.status)).toEqual(['dead', 'running']);
  });

  it('ephemeral storage: a rescheduled pod starts empty and re-replicates', () => {
    const s = byName('Kubernetes: drain');
    s.clusters[0]!.platform!.storage = 'ephemeral';
    const r = run(s);
    expect(of(r.events, 'TRUNCATE').some((e) => e.node === 'b5' && e.to === 0)).toBe(true);
    const end = viewAt(buildTimeline(r.m, r.events, r.endT), r.endT);
    expect(end.partitions['cfk/payments/0']!.isr).toContain('b5');
  });
});

describe('regions with availability zones', () => {
  const multi = () => byName('Multi-region CFK');

  it('nodes must be placed in a zone when their region has zones', () => {
    const bad = multi();
    bad.clusters[0]!.brokers[0]!.region = 'us-east-1';
    expect(() => parseManifest(bad)).toThrow(/region "us-east-1" has zones; place it in one of use1-az1/);
    const dup = multi();
    dup.regions[1]!.zones![0]!.id = 'use1-az1';
    expect(() => parseManifest(dup)).toThrow(/duplicate region\/zone id "use1-az1"/);
  });

  it('three latency tiers: same AZ < cross-AZ < cross-region', () => {
    const r = run(multi());
    const loc = new Map<string, string>();
    for (const c of r.m.clusters) for (const n of [...c.controllers, ...c.brokers]) loc.set(n.id, n.region);
    for (const p of [...r.m.producers, ...r.m.consumers]) loc.set(p.id, p.region);
    const regionOf = (l: string) => (l.startsWith('use1') ? 'east' : 'west');
    const tiers = { same: [] as number[], az: [] as number[], region: [] as number[] };
    for (const e of of(r.events, 'MSG')) {
      const a = loc.get(e.from), b = loc.get(e.to);
      if (!a || !b) continue;
      (a === b ? tiers.same : regionOf(a) === regionOf(b) ? tiers.az : tiers.region).push(e.arriveT - e.t);
    }
    const max = (xs: number[]) => Math.max(...xs);
    const min = (xs: number[]) => Math.min(...xs);
    expect(tiers.same.length && tiers.az.length && tiers.region.length).toBeTruthy();
    expect(max(tiers.same)).toBeLessThan(min(tiers.az)); // 0.4ms ±10% vs 1.2ms ±10%
    expect(max(tiers.az)).toBeLessThan(min(tiers.region)); // vs 65ms
    expect(min(tiers.region)).toBeGreaterThan(55);
  });

  it('AZ outage inside a region keeps serving (rack-aware RF=3); region outage takes the whole region down', () => {
    const r = run(multi());
    expect(sum(of(r.events, 'PRODUCE_ACK').filter((e) => e.t > 15_000 && e.t < 40_000))).toBeGreaterThan(1_000);
    const net = of(r.events, 'NETWORK').find((e) => e.t === 40_000)!;
    expect(net.groups![1]!.sort()).toEqual(['use1-az1', 'use1-az2', 'use1-az3']);
    const promoted = of(r.events, 'MIRROR_PROMOTED');
    expect(promoted).toHaveLength(3);
    expect(promoted.reduce((a, e) => a + e.rpoRecords, 0)).toBeLessThan(20);
  });

  it('zoneless regions still work as single locations (backward compatible)', () => {
    for (const s of scenarios.filter((x) => !x.regions.some((r) => r.zones))) expect(() => parseManifest(s)).not.toThrow();
  });
});

describe('rack awareness modes + Replica Placement Policies (RPP)', () => {
  const regionsOf = (m: ReturnType<typeof parseManifest>) => m;

  it('RPP places sync replicas and observers by rack (MRC scenario)', () => {
    const m = parseManifest(byName('MRC observers'));
    const pl = placementFor(m.clusters[0]!, m.topics[0]!, regionsOf(m));
    expect(pl.replicas[0]!.sort()).toEqual(['e1', 'e2']);
    expect(pl.observers[0]!.sort()).toEqual(['w1', 'w2']);
  });

  it('rack = region: racks are regions even when brokers sit in AZs; RPP counts per region', () => {
    const s = byName('Multi-region CFK');
    // one stretched cluster over both regions' AZs, racks by region
    const east = s.clusters[0]!;
    const west = s.clusters[1]!;
    east.brokers = [...east.brokers, ...west.brokers];
    east.controllers = [...east.controllers, ...west.controllers.slice(0, 2)];
    east.platform!.workers = [...east.platform!.workers, ...west.platform!.workers];
    east.rack = 'region';
    s.clusters = [east];
    s.topics = [{ cluster: 'east', name: 'orders', partitions: 4, placement: { replicas: [{ count: 3, rack: 'us-east-1' }], observers: [{ count: 2, rack: 'us-west-2' }] } }];
    s.producers = [{ id: 'shop', cluster: 'east', region: 'use1-az2', topic: 'orders', acks: 'all', ratePerSec: 20 }];
    s.consumers = [];
    s.actions = [];
    const m = parseManifest(s);
    const c = m.clusters[0]!;
    expect(rackIds(c, m)).toEqual(['us-east-1', 'us-west-2']);
    const pl = placementFor(c, m.topics[0]!, m);
    for (let p = 0; p < 4; p++) {
      expect(pl.replicas[p]!.every((b) => b.startsWith('e'))).toBe(true);
      expect(pl.replicas[p]).toHaveLength(3);
      expect(pl.observers[p]!.every((b) => b.startsWith('w'))).toBe(true);
    }
    // rotation spreads leaders across the region's brokers
    expect(new Set(pl.replicas.map((r) => r[0])).size).toBeGreaterThan(1);
    const r = simulate(m);
    expect(of(r.events, 'PRODUCE_ACK').length).toBeGreaterThan(0);
  });

  it('rack = zone vs region changes automatic placement', () => {
    const base = byName('Multi-region CFK');
    const m1 = parseManifest(base);
    const zonePl = placementFor(m1.clusters[0]!, m1.topics[0]!, m1).replicas[0]!;
    const zoneOf = (b: string) => m1.clusters[0]!.brokers.find((x) => x.id === b)!.region;
    expect(new Set(zonePl.map(zoneOf)).size).toBe(3); // one per AZ
    base.clusters[0]!.rack = 'region';
    const m2 = parseManifest(base);
    expect(rackIds(m2.clusters[0]!, m2)).toEqual(['us-east-1']); // single rack → no zone spreading guaranteed
  });

  it('RPP validation: unknown rack, too few brokers, mixed with RF, apache flavor, rack none', () => {
    const t = (patch: (s: ManifestInput) => void) => {
      const s = byName('MRC observers');
      patch(s);
      return () => parseManifest(s);
    };
    expect(t((s) => (s.topics![0]!.placement = { replicas: [{ count: 1, rack: 'mars' }] }))).toThrow(/no broker has rack "mars"/);
    expect(t((s) => (s.topics![0]!.placement = { replicas: [{ count: 3, rack: 'east' }] }))).toThrow(/rack "east" needs 3 brokers, has 2/);
    expect(t((s) => (s.topics![0]!.replicationFactor = 2))).toThrow(/replace replicationFactor\/replicas\/observers/);
    expect(t((s) => (s.clusters[0]!.flavor = 'apache'))).toThrow(/require a confluent-flavor cluster/);
    expect(t((s) => (s.clusters[0]!.rack = 'none'))).toThrow(/need rack awareness/);
  });
});
