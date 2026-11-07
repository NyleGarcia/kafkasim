/**
 * View model: pure reduction of a SimEvent stream. Same code path for
 * sim, recorded replays and live bridge streams — the renderer never
 * touches engine internals.
 */
import type { Manifest, SimEvent } from './schema.ts';
import { initialPlacement, placementFor, tpKey } from './schema.ts';

export interface NodeView {
  id: string;
  kind: 'broker' | 'controller' | 'producer' | 'consumer' | 'mm2';
  cluster: string;
  region: string;
  status: 'running' | 'dead' | 'paused';
  fenced: boolean;
  activeController: boolean;
  /** Kubernetes clusters only */
  pod?: { phase: PodPhase; worker: string | null; reason?: string };
}

export type PodPhase = Extract<SimEvent, { type: 'POD' }>['phase'];

export interface WorkerView {
  id: string;
  cluster: string;
  zone: string;
  ready: boolean;
  schedulable: boolean;
}

export interface PartitionView {
  tp: string;
  cluster: string;
  topic: string;
  partition: number;
  replicas: string[];
  observers: string[];
  leader: string | null;
  epoch: number;
  isr: string[];
  hw: number;
  leo: Record<string, number>;
  mirrorOf?: string;
  mirrorType?: 'cluster_link' | 'mm2';
  mirrorLag: number;
  promoted: boolean;
  lost: number;
}

export interface ClientView {
  id: string;
  acked: number;
  /** records that failed permanently */
  errors: number;
  /** retriable errors seen (each retry counted) */
  retries: number;
  lastError?: string;
  /** recent latencies (ms), capped ring */
  latencies: number[];
  consumed: number;
  lag: number;
}

export interface ViewState {
  t: number;
  nodes: Record<string, NodeView>;
  workers: Record<string, WorkerView>;
  partitions: Record<string, PartitionView>;
  clients: Record<string, ClientView>;
  controllers: Record<string, { node: string | null; epoch: number }>;
  partitioned: boolean;
  groups: string[][] | null;
  totals: { acked: number; produceErrors: number; lost: number; consumed: number; rpo: number };
  /** index of next event to apply (into the event array) */
  cursor: number;
}

const LAT_RING = 64;

export function initialView(m: Manifest): ViewState {
  const nodes: Record<string, NodeView> = {};
  const add = (id: string, kind: NodeView['kind'], cluster: string, region: string) =>
    (nodes[id] = { id, kind, cluster, region, status: 'running', fenced: false, activeController: false });
  const controllers: ViewState['controllers'] = {};
  const workers: Record<string, WorkerView> = {};
  for (const c of m.clusters) {
    c.controllers.forEach((x, i) => {
      add(x.id, 'controller', c.id, x.region);
      if (i === 0) nodes[x.id]!.activeController = true;
    });
    for (const b of c.brokers) add(b.id, 'broker', c.id, b.region);
    controllers[c.id] = { node: c.controllers[0]!.id, epoch: 1 };
    if (c.platform) {
      for (const w of c.platform.workers) workers[w.id] = { id: w.id, cluster: c.id, zone: w.zone, ready: true, schedulable: w.schedulable };
      for (const [pod, worker] of initialPlacement(c)) nodes[pod]!.pod = { phase: 'Running', worker };
    }
  }
  const clients: Record<string, ClientView> = {};
  const client = (id: string) => (clients[id] = { id, acked: 0, errors: 0, retries: 0, latencies: [], consumed: 0, lag: 0 });
  for (const p of m.producers) {
    add(p.id, 'producer', p.cluster, p.region);
    client(p.id);
  }
  for (const c of m.consumers) {
    add(c.id, 'consumer', c.cluster, c.region);
    client(c.id);
  }
  const partitions: Record<string, PartitionView> = {};
  for (const t of m.topics) {
    const c = m.clusters.find((x) => x.id === t.cluster)!;
    if (t.mirror?.type === 'mm2') {
      const id = `mm2-${t.cluster}-${t.name}`;
      add(id, 'mm2', t.cluster, t.mirror.workerRegion ?? c.brokers[0]!.region);
      client(id);
    }
    const placement = placementFor(c, t, m);
    for (let i = 0; i < t.partitions; i++) {
      const replicas = placement.replicas[i]!;
      const tp = tpKey(t.cluster, t.name, i);
      partitions[tp] = {
        tp,
        cluster: t.cluster,
        topic: t.name,
        partition: i,
        replicas,
        observers: placement.observers[i]!,
        leader: replicas[0]!,
        epoch: 0,
        isr: [...replicas],
        hw: 0,
        leo: Object.fromEntries([...replicas, ...placement.observers[i]!].map((b) => [b, 0])),
        ...(t.mirror ? { mirrorOf: tpKey(t.mirror.sourceCluster, t.mirror.sourceTopic, i), mirrorType: t.mirror.type } : {}),
        mirrorLag: 0,
        promoted: false,
        lost: 0,
      };
    }
  }
  return {
    t: 0,
    nodes,
    workers,
    partitions,
    clients,
    controllers,
    partitioned: false,
    groups: null,
    totals: { acked: 0, produceErrors: 0, lost: 0, consumed: 0, rpo: 0 },
    cursor: 0,
  };
}

/** Mutating reducer (callers clone keyframes). */
export function reduceEvent(s: ViewState, e: SimEvent): void {
  s.t = e.t;
  switch (e.type) {
    case 'NODE_STATUS': {
      const n = s.nodes[e.node];
      if (n) n.status = e.status;
      break;
    }
    case 'BROKER_FENCE': {
      const n = s.nodes[e.node];
      if (n) n.fenced = e.fenced;
      break;
    }
    case 'CONTROLLER_ELECTED':
      for (const n of Object.values(s.nodes)) if (n.cluster === e.cluster && n.kind === 'controller') n.activeController = n.id === e.node;
      s.controllers[e.cluster] = { node: e.node, epoch: e.epoch };
      break;
    case 'CONTROLLER_LOST': {
      const n = s.nodes[e.node];
      if (n) n.activeController = false;
      if (s.controllers[e.cluster]?.node === e.node) s.controllers[e.cluster] = { node: null, epoch: s.controllers[e.cluster]!.epoch };
      break;
    }
    case 'LEADER_CHANGE': {
      const p = s.partitions[e.tp];
      if (p) {
        p.leader = e.leader;
        p.epoch = e.epoch;
      }
      break;
    }
    case 'ISR_CHANGE': {
      const p = s.partitions[e.tp];
      if (p) p.isr = [...e.isr];
      break;
    }
    case 'LEO': {
      const p = s.partitions[e.tp];
      if (p) p.leo[e.node] = e.leo;
      break;
    }
    case 'HW': {
      const p = s.partitions[e.tp];
      if (p) p.hw = e.hw;
      break;
    }
    case 'TRUNCATE': {
      const p = s.partitions[e.tp];
      if (p) {
        p.leo[e.node] = e.to;
        if (p.hw > e.to && p.leader === e.node) p.hw = e.to;
      }
      break;
    }
    case 'PRODUCE_ACK': {
      const c = s.clients[e.producer];
      if (c) {
        c.acked += e.count;
        c.latencies.push(e.latencyMs);
        if (c.latencies.length > LAT_RING) c.latencies.shift();
      }
      s.totals.acked += e.count;
      break;
    }
    case 'PRODUCE_ERROR': {
      const c = s.clients[e.producer];
      if (c) {
        if (e.retriable) c.retries += 1;
        else c.errors += e.count;
        c.lastError = e.error;
      }
      if (!e.retriable) s.totals.produceErrors += e.count;
      break;
    }
    case 'CONSUME': {
      const c = s.clients[e.consumer];
      if (c) {
        c.consumed += e.count;
        c.lag = e.lag;
      }
      s.totals.consumed += e.count;
      break;
    }
    case 'DATA_LOSS': {
      const p = s.partitions[e.tp];
      if (p) p.lost += e.count;
      s.totals.lost += e.count;
      break;
    }
    case 'MIRROR_LAG': {
      const p = s.partitions[e.tp];
      if (p) p.mirrorLag = e.lag;
      break;
    }
    case 'MIRROR_PROMOTED': {
      const p = s.partitions[e.tp];
      if (p) p.promoted = true;
      s.totals.rpo += e.rpoRecords;
      break;
    }
    case 'NETWORK':
      s.partitioned = e.partitioned;
      s.groups = e.groups ?? null;
      break;
    case 'K8S_NODE': {
      const w = s.workers[e.node];
      if (w) {
        w.ready = e.ready;
        w.schedulable = e.schedulable;
      }
      break;
    }
    case 'POD': {
      const n = s.nodes[e.pod];
      if (n) n.pod = { phase: e.phase, worker: e.worker, ...(e.reason ? { reason: e.reason } : {}) };
      break;
    }
    case 'REPLICAS_CHANGE': {
      const p = s.partitions[e.tp];
      if (p) {
        p.replicas = [...e.replicas];
        p.observers = [...e.observers];
        for (const r of [...e.replicas, ...e.observers]) p.leo[r] ??= 0;
        for (const r of Object.keys(p.leo)) if (!e.replicas.includes(r) && !e.observers.includes(r)) delete p.leo[r];
      }
      break;
    }
    case 'ACTION':
    case 'NOTE':
    case 'MSG':
      break;
  }
}

export interface Timeline {
  events: SimEvent[];
  /** events without MSG, for fast reduction */
  stateEvents: SimEvent[];
  msgs: Extract<SimEvent, { type: 'MSG' }>[];
  keyframes: ViewState[];
  keyframeEveryMs: number;
  endT: number;
}

/** Precompute keyframes so scrubbing to any t costs ≤ one keyframe interval of reduction. */
export function buildTimeline(m: Manifest, events: SimEvent[], endT: number, keyframeEveryMs = 500): Timeline {
  const stateEvents = events.filter((e) => e.type !== 'MSG');
  const msgs = events.filter((e): e is Extract<SimEvent, { type: 'MSG' }> => e.type === 'MSG');
  const keyframes: ViewState[] = [];
  const s = initialView(m);
  let i = 0;
  for (let t = 0; t <= endT; t += keyframeEveryMs) {
    while (i < stateEvents.length && stateEvents[i]!.t <= t) reduceEvent(s, stateEvents[i++]!);
    s.t = t;
    s.cursor = i;
    keyframes.push(structuredClone(s));
  }
  return { events, stateEvents, msgs, keyframes, keyframeEveryMs, endT };
}

export function viewAt(tl: Timeline, t: number): ViewState {
  const k = Math.max(0, Math.min(tl.keyframes.length - 1, Math.floor(t / tl.keyframeEveryMs)));
  const s = structuredClone(tl.keyframes[k]!);
  let i = s.cursor;
  while (i < tl.stateEvents.length && tl.stateEvents[i]!.t <= t) reduceEvent(s, tl.stateEvents[i++]!);
  s.t = t;
  s.cursor = i;
  return s;
}

/** Messages in flight at time t (binary search on send time; bounded window). */
export function messagesInFlight(tl: Timeline, t: number, maxFlightMs = 1_000) {
  const msgs = tl.msgs;
  let lo = 0,
    hi = msgs.length;
  const start = t - maxFlightMs;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (msgs[mid]!.t < start) lo = mid + 1;
    else hi = mid;
  }
  const out: typeof msgs = [];
  for (let i = lo; i < msgs.length && msgs[i]!.t <= t; i++) {
    const m = msgs[i]!;
    if (m.arriveT >= t || (m.dropped && t - m.t < 150)) out.push(m);
  }
  return out;
}

/** Events (excluding MSG/LEO/HW noise) in [t0, t1] for the log panel. */
export function notableEvents(tl: Timeline, t0: number, t1: number, limit = 200) {
  const noisy = new Set(['LEO', 'HW', 'CONSUME', 'PRODUCE_ACK', 'MIRROR_LAG']);
  const out: SimEvent[] = [];
  for (const e of tl.stateEvents) {
    if (e.t > t1) break;
    if (e.t >= t0 && !noisy.has(e.type)) out.push(e);
  }
  return out.slice(-limit);
}
