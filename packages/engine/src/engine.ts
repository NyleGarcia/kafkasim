/**
 * Deterministic discrete-event model of Apache Kafka (KRaft) + Confluent extensions.
 *
 * Fidelity goals (each covered by test/behaviour.test.ts):
 *  - produce path: acks 0/1/all, min.insync.replicas, NOT_ENOUGH_REPLICAS, linger/batch, idempotent retries
 *  - replication: follower fetch, LEO/HW, ISR shrink (replica.lag.time.max.ms) / expand, strict ISR via controller
 *  - KRaft: quorum election + check-quorum resign, broker heartbeats, fencing, leader election (clean/unclean)
 *  - leader epochs + divergence truncation, zombie (paused) leaders, acked-record loss detection
 *  - consumers read ≤ HW; lag
 *  - Confluent: Cluster Linking (offset-preserving mirror), MRC observers + under-min-isr promotion
 *  - Apache: MirrorMaker 2 (consume → produce, offsets translated)
 *
 * Simplifications are listed in docs/architecture/engine-model.md.
 */
import type { ActionT, ClusterT, ConsumerT, Manifest, MsgKind, ProducerT, SimEvent, TopicT } from './schema.ts';
import { expandLocation, initialPlacement, locationsOf, placementFor, tpKey } from './schema.ts';
import { EventQueue, rng, type Rng } from './util.ts';

type Kind = 'broker' | 'controller' | 'producer' | 'consumer' | 'mm2';
type Status = 'running' | 'dead' | 'paused';

interface NodeRt {
  id: string;
  kind: Kind;
  cluster: string;
  region: string;
  status: Status;
  incarnation: number;
  /** Work that arrived while paused (SIGSTOP keeps sockets: TCP buffers it). */
  pending: (() => void)[];
}

interface Rec {
  offset: number;
  epoch: number;
  id: string;
}

interface PartMeta {
  leader: string | null;
  epoch: number;
  isr: string[];
  /** bumps on any ISR/leader change (AlterPartition fencing). */
  pEpoch: number;
}

interface FollowerState {
  leo: number;
  lastCaughtUpT: number;
  lastFetchT: number;
  leaderLeoAtLastFetch: number;
}

interface PendingAck {
  endOffset: number;
  resolve: (err?: string) => void;
}

interface ReplicaRt {
  log: Rec[];
  hw: number;
  view: PartMeta | null;
  // leader-only
  followers: Map<string, FollowerState>;
  pendingAcks: PendingAck[];
  alterInFlight: boolean;
  /** producerId → highest appended seq (idempotence). */
  seqs: Map<string, { seq: number; offset: number }>;
  /** long-poll follower fetches waiting for new data (replica.fetch.wait.max.ms). */
  parked: Map<string, () => void>;
  /** long-poll cross-cluster link fetches waiting for HW to advance. */
  parkedHw: Map<string, () => void>;
  // follower-only
  fetchInFlight: number; // timestamp of in-flight fetch or -1
}

interface Partition {
  key: string;
  cluster: string;
  topic: string;
  index: number;
  replicas: string[];
  observers: string[];
  minIsr: number;
  promotion: 'none' | 'under-min-isr';
  /** committed metadata (the KRaft metadata log). */
  meta: PartMeta;
  rep: Map<string, ReplicaRt>;
  /** record ids acked to some client. */
  acked: Set<string>;
  lostReported: Set<string>;
  mirror?: {
    type: 'cluster_link' | 'mm2';
    sourceKey: string;
    active: boolean;
    fetchIntervalMs: number;
    workerId?: string;
  };
}

interface QuorumNode {
  epoch: number;
  votedFor: string | null;
  role: 'leader' | 'follower' | 'candidate';
  lastContact: number;
  votes: Set<string>;
  lastAck: Map<string, number>;
}

interface ClusterRt {
  def: ClusterT;
  active: string | null;
  epoch: number;
  quorum: Map<string, QuorumNode>;
  lastHb: Map<string, number>;
  incarnations: Map<string, number>;
  fenced: Set<string>;
  fencedAt: Map<string, number>;
  /** brokers in controlled shutdown: stay fenced until they re-register (new incarnation) */
  shuttingDown: Set<string>;
  /** brokers Self-Balancing already started healing away from */
  healed: Set<string>;
  /** tp → reassignment in progress (Self-Balancing) */
  moving: Map<string, { from: string; to: string }>;
}

type PodPhase = 'Pending' | 'ContainerCreating' | 'Running' | 'Terminating' | 'CrashLoopBackOff' | 'Unknown';

interface WorkerRt {
  id: string;
  cluster: string;
  zone: string;
  /** hardware/kubelet alive */
  alive: boolean;
  /** API view (NotReady only after node-monitor-grace-period) */
  ready: boolean;
  deleted: boolean;
  schedulable: boolean;
  capacity: number;
  pods: Set<string>;
}

interface PodRt {
  id: string;
  cluster: string;
  phase: PodPhase;
  worker: string | null;
  /** zone the PVC is bound to (zoneLocked storage) */
  pvcZone: string;
  restarts: number;
  lastCrashT: number;
  /** bumps on every new pod object (StatefulSet recreate) — stale timers check it */
  gen: number;
}

interface Batch {
  tp: string;
  ids: string[];
  seq: number;
  createdT: number;
  firstSendT: number;
  attempt: number;
}

interface ProducerRt {
  def: ProducerT;
  active: boolean;
  seq: number;
  stickyPartition: number;
  accum: Map<string, Batch>;
  queue: Map<string, Batch[]>;
  inFlight: Map<string, { batch: Batch; reqId: number; target: string; fail: (err: string) => void }>;
  leaders: Map<string, string | null>;
  metadataInFlight: boolean;
  reqId: number;
  partitions: string[];
}

interface ConsumerRt {
  def: ConsumerT;
  position: Map<string, number>;
  leaders: Map<string, string | null>;
  inFlight: Set<string>;
  assigned: string[];
}

interface Mm2Rt {
  id: string;
  dest: Partition[];
  position: Map<string, number>; // per source tp
  inFlight: Set<string>;
  seq: number;
}

const META_PUSH: MsgKind = 'metadata';
const CONTROLLER_TICK_MS = 500;
const ISR_CHECK_MS = 1_000;
const SBC_TICK_MS = 5_000;
/** kubelet resets CrashLoopBackOff after the container runs this long */
const CRASH_RESET_MS = 600_000;

export interface SimOptions {
  /** Extra actions injected interactively (merged with manifest.actions). */
  extraActions?: ActionT[];
  untilMs?: number;
  /** Skip MSG events (headless runs / calibration). */
  omitMessages?: boolean;
}

export interface SimResult {
  events: SimEvent[];
  endT: number;
  stats: { eventsProcessed: number; wallMs: number };
}

export function simulate(manifest: Manifest, opts: SimOptions = {}): SimResult {
  const sim = new Sim(manifest, opts);
  return sim.run();
}

class Sim {
  private now = 0;
  private q = new EventQueue<{ node: string | null; inc: number; fn: () => void }>();
  private out: SimEvent[] = [];
  private r: Rng;
  private nodes = new Map<string, NodeRt>();
  private clusters = new Map<string, ClusterRt>();
  private parts = new Map<string, Partition>();
  private producers = new Map<string, ProducerRt>();
  private consumers = new Map<string, ConsumerRt>();
  private mm2 = new Map<string, Mm2Rt>();
  private workers = new Map<string, WorkerRt>();
  private pods = new Map<string, PodRt>();
  private latency = new Map<string, number>();
  /** location (zone) → region */
  private locRegion = new Map<string, string>();
  private groups: string[][] | null = null;
  private processed = 0;

  private m: Manifest;
  private opts: SimOptions;

  constructor(m: Manifest, opts: SimOptions) {
    this.m = m;
    this.opts = opts;
    this.r = rng(m.seed);
    for (const l of locationsOf(m)) this.locRegion.set(l.id, l.region);
    for (const l of m.latency.links) this.latency.set(this.linkKey(l.a, l.b), l.ms);
  }

  // ------------------------------------------------------------ infra

  private emit(e: SimEvent) {
    this.out.push(e);
  }
  private note(level: 'info' | 'warn' | 'error', message: string, node?: string) {
    this.emit({ t: this.now, type: 'NOTE', level, message, ...(node ? { node } : {}) });
  }

  /** Schedule work owned by `node` (skipped if node died/restarted, deferred if paused). */
  private at(t: number, node: string | null, fn: () => void) {
    const inc = node ? this.nodes.get(node)!.incarnation : 0;
    this.q.push(Math.max(t, this.now), { node, inc, fn });
  }
  private after(ms: number, node: string | null, fn: () => void) {
    this.at(this.now + ms, node, fn);
  }
  /** Periodic work. First run: random within the period (jitter), or `firstMs` if given. */
  private every(ms: number, node: string, fn: () => void, jitterFirst = true, firstMs?: number) {
    const tick = () => {
      fn();
      this.after(ms, node, tick);
    };
    this.after(firstMs ?? (jitterFirst ? this.r.between(0, ms) : ms), node, tick);
  }

  private linkKey(a: string, b: string) {
    return a < b ? `${a}|${b}` : `${b}|${a}`;
  }
  /** a, b = locations (zone ids, or zoneless region ids). */
  private regionLatency(a: string, b: string) {
    if (a === b) return this.m.latency.intraRegionMs;
    const direct = this.latency.get(this.linkKey(a, b));
    if (direct !== undefined) return direct;
    const ra = this.locRegion.get(a) ?? a;
    const rb = this.locRegion.get(b) ?? b;
    if (ra === rb) return this.m.latency.interZoneMs;
    return this.latency.get(this.linkKey(ra, rb)) ?? this.m.latency.interRegionMs;
  }
  private rtt(a: string, b: string) {
    return 2 * this.regionLatency(this.nodes.get(a)!.region, this.nodes.get(b)!.region);
  }
  private groupOf(id: string) {
    if (!this.groups) return 0;
    const loc = this.nodes.get(id)?.region;
    const region = loc ? this.locRegion.get(loc) : undefined;
    // most specific wins: node id, then its zone, then its region
    for (const key of [id, loc, region]) {
      if (!key) continue;
      for (let i = 0; i < this.groups.length; i++) if (this.groups[i]!.includes(key)) return i;
    }
    return 0;
  }
  private reachable(a: string, b: string) {
    return this.groupOf(a) === this.groupOf(b);
  }

  /** Send a message. Delivery dropped if partitioned (at send or arrival) or target dead. */
  private send(from: string, to: string, kind: MsgKind, onArrive: () => void, records = 0) {
    const src = this.nodes.get(from)!;
    const dst = this.nodes.get(to);
    if (!dst || src.status !== 'running') return;
    const base = this.regionLatency(src.region, dst.region);
    const j = this.m.latency.jitter;
    const lat = Math.max(0.05, base * (1 + this.r.between(-j, j)));
    const arriveT = this.now + lat;
    const ok = this.reachable(from, to);
    if (!this.opts.omitMessages)
      this.emit({ t: this.now, type: 'MSG', from, to, kind, arriveT, dropped: !ok, records });
    if (!ok) return;
    this.q.push(arriveT, {
      node: null,
      inc: 0,
      fn: () => {
        if (!this.reachable(from, to)) return;
        this.deliver(to, onArrive);
      },
    });
  }
  private deliver(to: string, fn: () => void) {
    const n = this.nodes.get(to)!;
    if (n.status === 'dead') return;
    if (n.status === 'paused') n.pending.push(fn);
    else fn();
  }

  // ------------------------------------------------------------ setup

  private addNode(id: string, kind: Kind, cluster: string, region: string) {
    this.nodes.set(id, { id, kind, cluster, region, status: 'running', incarnation: 0, pending: [] });
  }

  private setup() {
    const m = this.m;
    for (const c of m.clusters) {
      const crt: ClusterRt = {
        def: c,
        active: c.controllers[0]!.id,
        epoch: 1,
        quorum: new Map(),
        lastHb: new Map(),
        incarnations: new Map(),
        fenced: new Set(),
        fencedAt: new Map(),
        shuttingDown: new Set(),
        healed: new Set(),
        moving: new Map(),
      };
      this.clusters.set(c.id, crt);
      for (const ctl of c.controllers) {
        this.addNode(ctl.id, 'controller', c.id, ctl.region);
        crt.quorum.set(ctl.id, {
          epoch: 1,
          votedFor: c.controllers[0]!.id,
          role: ctl.id === crt.active ? 'leader' : 'follower',
          lastContact: 0,
          votes: new Set(),
          lastAck: new Map(c.controllers.map((x) => [x.id, 0])),
        });
      }
      for (const b of c.brokers) {
        this.addNode(b.id, 'broker', c.id, b.region);
        crt.lastHb.set(b.id, 0);
        crt.incarnations.set(b.id, 0);
      }
      if (c.platform) this.setupK8s(c);
    }
    // topics (sources before mirrors so mirror can look up source)
    const ordered = [...m.topics].sort((a, b) => Number(!!a.mirror) - Number(!!b.mirror));
    for (const t of ordered) this.createTopic(t);

    for (const p of m.producers) {
      this.addNode(p.id, 'producer', p.cluster, p.region);
      const partitions = [...this.parts.values()].filter((x) => x.cluster === p.cluster && x.topic === p.topic).map((x) => x.key);
      this.producers.set(p.id, {
        def: p,
        active: p.startMs === 0,
        seq: 0,
        stickyPartition: Math.floor(this.r.next() * partitions.length),
        accum: new Map(),
        queue: new Map(partitions.map((k) => [k, []])),
        inFlight: new Map(),
        leaders: new Map(partitions.map((k) => [k, this.parts.get(k)!.meta.leader])),
        metadataInFlight: false,
        reqId: 0,
        partitions,
      });
    }
    const groups = new Map<string, ConsumerT[]>();
    for (const c of m.consumers) {
      this.addNode(c.id, 'consumer', c.cluster, c.region);
      const g = `${c.cluster}/${c.topic}/${c.group}`;
      groups.set(g, [...(groups.get(g) ?? []), c]);
    }
    for (const members of groups.values()) {
      const sorted = [...members].sort((a, b) => a.id.localeCompare(b.id));
      const tps = [...this.parts.values()].filter((x) => x.cluster === sorted[0]!.cluster && x.topic === sorted[0]!.topic).map((x) => x.key);
      sorted.forEach((c, i) => {
        const assigned = tps.filter((_, j) => j % sorted.length === i);
        this.consumers.set(c.id, {
          def: c,
          position: new Map(assigned.map((k) => [k, 0])),
          leaders: new Map(assigned.map((k) => [k, this.parts.get(k)!.meta.leader])),
          inFlight: new Set(),
          assigned,
        });
      });
    }
  }

  private createTopic(t: TopicT) {
    const c = this.clusters.get(t.cluster)!;
    const brokers = c.def.brokers;
    const placement = placementFor(c.def, t, this.m);
    const mirrorSrc = t.mirror ? tpKey(t.mirror.sourceCluster, t.mirror.sourceTopic, 0).slice(0, -2) : undefined;
    let workerId: string | undefined;
    if (t.mirror?.type === 'mm2') {
      workerId = `mm2-${t.cluster}-${t.name}`;
      const region = t.mirror.workerRegion ?? brokers[0]!.region;
      this.addNode(workerId, 'mm2', t.cluster, region);
      this.mm2.set(workerId, { id: workerId, dest: [], position: new Map(), inFlight: new Set(), seq: 0 });
    }
    for (let i = 0; i < t.partitions; i++) {
      const replicas = placement.replicas[i]!;
      const observers = placement.observers[i]!;
      const key = tpKey(t.cluster, t.name, i);
      const meta: PartMeta = { leader: replicas[0]!, epoch: 0, isr: [...replicas], pEpoch: 0 };
      const p: Partition = {
        key,
        cluster: t.cluster,
        topic: t.name,
        index: i,
        replicas,
        observers,
        minIsr: t.minInsyncReplicas ?? c.def.config.minInsyncReplicas,
        promotion: t.observerPromotionPolicy,
        meta,
        rep: new Map(),
        acked: new Set(),
        lostReported: new Set(),
      };
      if (t.mirror) {
        p.mirror = {
          type: t.mirror.type,
          sourceKey: `${mirrorSrc}/${i}`,
          active: true,
          fetchIntervalMs: t.mirror.fetchIntervalMs,
          ...(workerId ? { workerId } : {}),
        };
        if (workerId) this.mm2.get(workerId)!.dest.push(p);
      }
      for (const b of [...replicas, ...observers]) p.rep.set(b, this.newReplica({ ...meta, isr: [...meta.isr] }));
      this.parts.set(key, p);
      this.emit({ t: 0, type: 'LEADER_CHANGE', tp: key, leader: meta.leader, epoch: 0, unclean: false });
      this.emit({ t: 0, type: 'ISR_CHANGE', tp: key, isr: [...meta.isr], reason: 'created' });
    }
  }

  private newReplica(view: PartMeta | null): ReplicaRt {
    return {
      log: [],
      hw: 0,
      view,
      followers: new Map(),
      pendingAcks: [],
      alterInFlight: false,
      seqs: new Map(),
      parked: new Map(),
      parkedHw: new Map(),
      fetchInFlight: -1,
    };
  }

  // ------------------------------------------------------------ run loop

  run(): SimResult {
    const wall = performance.now();
    this.setup();
    for (const [id, n] of this.nodes) this.startNode(id, n.kind, true);
    for (const p of this.parts.values()) {
      const leader = p.meta.leader!;
      const rep = p.rep.get(leader)!;
      for (const f of [...p.replicas, ...p.observers]) if (f !== leader) rep.followers.set(f, { leo: 0, lastCaughtUpT: 0, lastFetchT: 0, leaderLeoAtLastFetch: 0 });
    }
    for (const p of this.m.producers) {
      if (p.startMs > 0) this.at(p.startMs, null, () => this.applyAction({ type: 'START_PRODUCER', at: p.startMs, producer: p.id }));
      if (p.stopMs !== undefined) this.at(p.stopMs, null, () => this.applyAction({ type: 'STOP_PRODUCER', at: p.stopMs!, producer: p.id }));
    }
    const actions = [...this.m.actions, ...(this.opts.extraActions ?? [])].sort((a, b) => a.at - b.at);
    for (const a of actions) this.at(a.at, null, () => this.applyAction(a));
    for (const c of this.clusters.values()) {
      if (!c.def.platform?.selfBalancing.enabled) continue;
      const tick = () => {
        this.selfBalancingTick(c);
        this.at(this.now + SBC_TICK_MS, null, tick);
      };
      this.at(SBC_TICK_MS, null, tick);
    }

    const until = this.opts.untilMs ?? this.m.durationMs;
    for (;;) {
      const t = this.q.peekT();
      if (t === undefined || t > until) break;
      const item = this.q.pop()!;
      this.now = item.t;
      const { node, inc, fn } = item.v;
      if (node) {
        const n = this.nodes.get(node)!;
        if (n.status === 'dead' || n.incarnation !== inc) continue;
        if (n.status === 'paused') {
          n.pending.push(fn);
          continue;
        }
      }
      this.processed++;
      fn();
    }
    this.now = until;
    for (const p of this.parts.values()) this.checkLoss(p, 'end of run');
    this.out.sort((a, b) => a.t - b.t);
    return { events: this.out, endT: until, stats: { eventsProcessed: this.processed, wallMs: performance.now() - wall } };
  }

  private startNode(id: string, kind: Kind, initial: boolean) {
    switch (kind) {
      case 'controller':
        this.every(CONTROLLER_TICK_MS / 2, id, () => this.quorumTick(id));
        this.every(CONTROLLER_TICK_MS, id, () => this.controllerTick(id));
        break;
      case 'broker': {
        const cfg = this.clusters.get(this.nodes.get(id)!.cluster)!.def.config;
        this.every(cfg.brokerHeartbeatIntervalMs, id, () => this.brokerHeartbeat(id), true);
        if (!initial) this.after(0, id, () => this.brokerHeartbeat(id));
        // one loop per broker over the partitions it hosts *now* (reassignment adds/removes replicas).
        // A running cluster's followers are already long-polling at t=0, so start within a few ms.
        this.every(cfg.replicaFetchWaitMaxMs, id, () => {
          for (const p of this.parts.values()) {
            if (!p.rep.has(id)) continue;
            this.followerFetch(p, id);
            if (p.mirror?.type === 'cluster_link') this.linkFetch(p, id);
          }
        }, true, this.r.between(0, 5));
        this.every(ISR_CHECK_MS, id, () => {
          for (const p of this.parts.values()) if (p.rep.has(id)) this.isrCheck(p, id);
        });
        break;
      }
      case 'producer': {
        const pr = this.producers.get(id)!;
        this.every(1000 / pr.def.ratePerSec, id, () => this.produceRecord(pr), false);
        break;
      }
      case 'consumer': {
        const c = this.consumers.get(id)!;
        this.every(c.def.pollIntervalMs, id, () => this.consumerPoll(c));
        break;
      }
      case 'mm2': {
        const w = this.mm2.get(id)!;
        const ms = w.dest[0]?.mirror?.fetchIntervalMs ?? 500;
        this.every(ms, id, () => this.mm2Tick(w));
        break;
      }
    }
  }

  // ------------------------------------------------------------ actions / faults

  private applyAction(a: ActionT) {
    const detail = JSON.stringify(a);
    this.emit({ t: this.now, type: 'ACTION', action: a.type, detail });
    switch (a.type) {
      case 'KILL_NODE':
        // on Kubernetes a killed broker process is a container crash: kubelet restarts it
        if (this.pods.has(a.node)) this.containerCrash(a.node);
        else this.killNode(a.node, 'SIGKILL');
        break;
      case 'RESTART_NODE':
        // pods are restarted by kubelet / the StatefulSet, never by hand
        if (this.pods.has(a.node)) this.note('info', `${a.node} is a Kubernetes pod: kubelet restarts it (RESTART_NODE ignored)`, a.node);
        else this.restartNode(a.node, 'restart');
        break;
      case 'K8S_NODE_FAIL':
        this.workerFail(a.worker);
        break;
      case 'K8S_NODE_RECOVER':
        this.workerRecover(a.worker);
        break;
      case 'K8S_NODE_DELETE':
        this.workerDelete(a.worker);
        break;
      case 'K8S_CORDON':
      case 'K8S_UNCORDON': {
        const zones = new Set(expandLocation(this.m, a.target));
        for (const w of this.workers.values())
          if (w.id === a.target || zones.has(w.zone)) {
            w.schedulable = a.type === 'K8S_UNCORDON';
            this.emitWorker(w);
          }
        this.scheduleAll();
        break;
      }
      case 'POD_DELETE':
        this.podDelete(a.pod);
        break;
      case 'CONTAINER_CRASH':
        this.containerCrash(a.pod);
        break;
      case 'ZONE_OUTAGE': {
        // a zone, or a whole region (all of its zones)
        const out = expandLocation(this.m, a.zone);
        for (const w of this.workers.values()) if (out.includes(w.zone)) this.workerFail(w.id);
        // VM clusters: everything placed there loses power
        for (const n of this.nodes.values()) if (out.includes(n.region) && !this.pods.has(n.id) && n.kind !== 'producer' && n.kind !== 'consumer') this.killNode(n.id, `${a.zone} outage`);
        const others = locationsOf(this.m)
          .map((l) => l.id)
          .filter((l) => !out.includes(l));
        if (others.length) {
          this.groups = [others, out];
          this.emit({ t: this.now, type: 'NETWORK', partitioned: true, groups: this.groups });
        }
        break;
      }
      case 'ZONE_RECOVER':
        this.groups = null;
        this.emit({ t: this.now, type: 'NETWORK', partitioned: false });
        const back = expandLocation(this.m, a.zone);
        for (const w of this.workers.values()) if (back.includes(w.zone)) this.workerRecover(w.id);
        for (const n of this.nodes.values()) if (back.includes(n.region) && !this.pods.has(n.id) && n.status === 'dead') this.restartNode(n.id, `${a.zone} recovered`);
        break;
      case 'PAUSE_NODE': {
        const n = this.nodes.get(a.node)!;
        if (n.status !== 'running') return;
        n.status = 'paused';
        this.emit({ t: this.now, type: 'NODE_STATUS', node: n.id, status: 'paused', reason: 'SIGSTOP' });
        break;
      }
      case 'RESUME_NODE': {
        const n = this.nodes.get(a.node)!;
        if (n.status !== 'paused') return;
        n.status = 'running';
        this.emit({ t: this.now, type: 'NODE_STATUS', node: n.id, status: 'running', reason: 'SIGCONT' });
        const work = n.pending;
        n.pending = [];
        const inc = n.incarnation;
        for (const fn of work) this.q.push(this.now, { node: n.id, inc, fn });
        break;
      }
      case 'PARTITION':
        this.groups = a.groups.map((g) => [...g]);
        this.emit({ t: this.now, type: 'NETWORK', partitioned: true, groups: this.groups });
        break;
      case 'HEAL':
        this.groups = null;
        this.emit({ t: this.now, type: 'NETWORK', partitioned: false });
        break;
      case 'SET_LATENCY':
        this.latency.set(this.linkKey(a.a, a.b), a.ms);
        break;
      case 'FAILOVER':
        this.failover(a.cluster, a.topic);
        break;
      case 'STOP_PRODUCER': {
        const p = this.producers.get(a.producer);
        if (p) p.active = false;
        break;
      }
      case 'START_PRODUCER': {
        const p = this.producers.get(a.producer);
        if (p) p.active = true;
        break;
      }
    }
  }

  private killNode(id: string, reason: string) {
    const n = this.nodes.get(id)!;
    if (n.status === 'dead') return;
    n.status = 'dead';
    n.incarnation++;
    n.pending = [];
    this.emit({ t: this.now, type: 'NODE_STATUS', node: n.id, status: 'dead', reason });
    this.onKilled(n);
  }

  private restartNode(id: string, reason: string) {
    const n = this.nodes.get(id)!;
    if (n.status !== 'dead') return;
    n.status = 'running';
    n.incarnation++;
    this.emit({ t: this.now, type: 'NODE_STATUS', node: n.id, status: 'running', reason });
    this.startNode(n.id, n.kind, false);
  }

  private onKilled(n: NodeRt) {
    if (n.kind === 'broker') {
      // SIGKILL closes the process's sockets: peers see a reset after ~1 RTT
      for (const pr of this.producers.values())
        for (const f of pr.inFlight.values())
          if (f.target === n.id) this.after(this.rtt(pr.def.id, n.id), pr.def.id, () => f.fail('DISCONNECTED'));
      for (const p of this.parts.values()) {
        const rep = p.rep.get(n.id);
        if (!rep) continue;
        // process memory lost; log on disk survives
        for (const pa of rep.pendingAcks) pa.resolve('DISCONNECTED');
        rep.pendingAcks = [];
        rep.followers.clear();
        rep.parked.clear();
        rep.parkedHw.clear();
        rep.view = null;
        rep.alterInFlight = false;
        rep.fetchInFlight = -1;
      }
    } else if (n.kind === 'controller') {
      const c = this.clusters.get(n.cluster)!;
      const qn = c.quorum.get(n.id)!;
      qn.role = 'follower';
      qn.votes.clear();
      if (c.active === n.id) {
        c.active = null;
        this.emit({ t: this.now, type: 'CONTROLLER_LOST', cluster: c.def.id, node: n.id });
      }
    } else if (n.kind === 'producer') {
      const p = this.producers.get(n.id)!;
      p.accum.clear();
      for (const q of p.queue.values()) q.length = 0;
      p.inFlight.clear();
      p.metadataInFlight = false;
    } else if (n.kind === 'consumer') {
      this.consumers.get(n.id)!.inFlight.clear();
    } else if (n.kind === 'mm2') {
      this.mm2.get(n.id)!.inFlight.clear();
    }
  }

  // ------------------------------------------------------------ KRaft quorum

  private quorumTick(id: string) {
    const c = this.clusters.get(this.nodes.get(id)!.cluster)!;
    const qn = c.quorum.get(id)!;
    const cfg = c.def.config;
    const voters = c.def.controllers.map((x) => x.id);
    const majority = Math.floor(voters.length / 2) + 1;
    if (qn.role === 'leader') {
      for (const v of voters) {
        if (v === id) continue;
        const epoch = qn.epoch;
        this.send(id, v, 'vote', () => {
          const fq = c.quorum.get(v)!;
          if (epoch < fq.epoch) {
            // stale leader learns of newer epoch
            this.send(v, id, 'vote', () => this.stepDown(c, id, fq.epoch));
            return;
          }
          if (epoch > fq.epoch) {
            fq.epoch = epoch;
            fq.votedFor = id;
          }
          fq.role = 'follower';
          fq.lastContact = this.now;
          this.send(v, id, 'vote', () => qn.lastAck.set(v, this.now));
        });
      }
      // check-quorum: resign if majority unheard for fetch timeout
      const heard = voters.filter((v) => v === id || this.now - (qn.lastAck.get(v) ?? -Infinity) <= cfg.quorumFetchTimeoutMs * 1.5).length;
      if (heard < majority && this.now > cfg.quorumFetchTimeoutMs * 1.5) this.stepDown(c, id, qn.epoch, 'lost quorum (check-quorum)');
      return;
    }
    const timeout = cfg.quorumFetchTimeoutMs + this.r.between(0, cfg.quorumElectionTimeoutMs);
    if (this.now - qn.lastContact < timeout) return;
    qn.lastContact = this.now; // reset election timer
    // KIP-996 pre-vote: only bump the epoch if a majority would vote for us.
    // Stops a partitioned controller from inflating epochs and deposing a healthy leader on heal.
    this.requestVotes(c, id, qn.epoch + 1, true, majority);
    if (majority === 1) this.becomeActive(c, id);
  }

  private requestVotes(c: ClusterRt, id: string, epoch: number, prevote: boolean, majority: number) {
    const qn = c.quorum.get(id)!;
    const cfg = c.def.config;
    if (!prevote) {
      qn.role = 'candidate';
      qn.epoch = epoch;
      qn.votedFor = id;
    }
    const votes = new Set([id]);
    qn.votes = votes;
    for (const v of c.def.controllers.map((x) => x.id)) {
      if (v === id) continue;
      this.send(id, v, 'vote', () => {
        const vq = c.quorum.get(v)!;
        const leaderRecentlySeen = vq.role === 'leader' || (vq.role === 'follower' && this.now - vq.lastContact < cfg.quorumFetchTimeoutMs);
        let granted = epoch > vq.epoch && !leaderRecentlySeen;
        if (granted && !prevote) {
          vq.epoch = epoch;
          vq.votedFor = id;
          vq.role = 'follower';
          vq.lastContact = this.now;
        }
        if (prevote && vq.votedFor !== null && vq.epoch >= epoch) granted = false;
        this.send(v, id, 'vote', () => {
          if (!granted || qn.votes !== votes || qn.role === 'leader') return;
          if (!prevote && (qn.role !== 'candidate' || qn.epoch !== epoch)) return;
          votes.add(v);
          if (votes.size < majority) return;
          if (prevote) {
            qn.votes = new Set();
            this.requestVotes(c, id, Math.max(epoch, qn.epoch + 1), false, majority);
          } else this.becomeActive(c, id);
        });
      });
    }
  }

  private stepDown(c: ClusterRt, id: string, epoch: number, why = 'saw higher epoch') {
    const qn = c.quorum.get(id)!;
    if (qn.role !== 'leader') return;
    qn.role = 'follower';
    qn.epoch = Math.max(qn.epoch, epoch);
    qn.lastContact = this.now;
    if (c.active === id) {
      c.active = null;
      this.emit({ t: this.now, type: 'CONTROLLER_LOST', cluster: c.def.id, node: id });
      this.note('warn', `controller ${id} resigned: ${why}`, id);
    }
  }

  private becomeActive(c: ClusterRt, id: string) {
    const qn = c.quorum.get(id)!;
    if (c.active && c.active !== id) {
      const old = c.quorum.get(c.active)!;
      if (old.epoch < qn.epoch) {
        // old leader does not know yet (zombie / partitioned); committed role moves on.
        this.emit({ t: this.now, type: 'CONTROLLER_LOST', cluster: c.def.id, node: c.active });
      }
    }
    qn.role = 'leader';
    c.active = id;
    c.epoch = qn.epoch;
    for (const v of c.def.controllers) qn.lastAck.set(v.id, this.now);
    // grace: new controller restarts broker session timers
    for (const b of c.def.brokers) c.lastHb.set(b.id, Math.max(c.lastHb.get(b.id) ?? 0, this.now));
    this.emit({ t: this.now, type: 'CONTROLLER_ELECTED', cluster: c.def.id, node: id, epoch: qn.epoch });
  }

  /** Is `id` the committed active controller with a live quorum? */
  private isActive(c: ClusterRt, id: string) {
    return c.active === id && c.quorum.get(id)!.role === 'leader' && c.quorum.get(id)!.epoch === c.epoch;
  }

  private controllerTick(id: string) {
    const c = this.clusters.get(this.nodes.get(id)!.cluster)!;
    if (!this.isActive(c, id)) return;
    const cfg = c.def.config;
    for (const b of c.def.brokers) {
      if (c.fenced.has(b.id)) continue;
      if (this.now - (c.lastHb.get(b.id) ?? 0) > cfg.brokerSessionTimeoutMs) this.fence(c, b.id, 'session timeout');
    }
  }

  // ------------------------------------------------------------ broker ↔ controller

  private brokerHeartbeat(id: string) {
    const n = this.nodes.get(id)!;
    const c = this.clusters.get(n.cluster)!;
    const ctl = c.active;
    if (!ctl) return;
    const inc = n.incarnation;
    this.send(id, ctl, 'heartbeat', () => {
      if (!this.isActive(c, ctl)) return;
      const prevInc = c.incarnations.get(id);
      c.lastHb.set(id, this.now);
      if (prevInc !== inc) {
        c.incarnations.set(id, inc);
        c.shuttingDown.delete(id);
        if (!c.fenced.has(id)) this.fence(c, id, 'new incarnation (restart)');
      }
      // full metadata in response; restarted broker rebuilds its view
      const snapshot = this.metaSnapshot(c.def.id, id);
      this.send(ctl, id, 'heartbeat', () => {
        for (const [key, meta] of snapshot) this.applyMetadata(this.parts.get(key)!, id, meta);
        if (c.fenced.has(id) && !c.shuttingDown.has(id) && this.isActive(c, ctl) && this.nodes.get(id)!.incarnation === inc) this.unfence(c, id);
      });
    });
  }

  private metaSnapshot(cluster: string, broker: string): [string, PartMeta][] {
    const out: [string, PartMeta][] = [];
    for (const p of this.parts.values())
      if (p.cluster === cluster && p.rep.has(broker)) out.push([p.key, { ...p.meta, isr: [...p.meta.isr] }]);
    return out;
  }

  private fence(c: ClusterRt, b: string, reason: string) {
    c.fenced.add(b);
    if (!c.fencedAt.has(b)) c.fencedAt.set(b, this.now);
    this.emit({ t: this.now, type: 'BROKER_FENCE', cluster: c.def.id, node: b, fenced: true, reason });
    for (const p of this.parts.values()) {
      if (p.cluster !== c.def.id || !p.rep.has(b)) continue;
      if (p.meta.leader === b) this.electLeader(c, p, `leader ${b} fenced`);
      else if (p.meta.isr.includes(b) && p.meta.isr.length > 1) this.commit(c, p, { isr: p.meta.isr.filter((x) => x !== b) }, `${b} fenced`);
    }
  }

  private unfence(c: ClusterRt, b: string) {
    c.fenced.delete(b);
    c.fencedAt.delete(b);
    this.emit({ t: this.now, type: 'BROKER_FENCE', cluster: c.def.id, node: b, fenced: false, reason: 'heartbeat ok' });
    for (const p of this.parts.values()) if (p.cluster === c.def.id && p.meta.leader === null && p.rep.has(b)) this.electLeader(c, p, `${b} back`);
  }

  private electLeader(c: ClusterRt, p: Partition, reason: string) {
    const live = (x: string) => !c.fenced.has(x);
    const order = [...p.replicas, ...p.observers];
    let leader = order.find((x) => p.meta.isr.includes(x) && live(x)) ?? null;
    let unclean = false;
    if (!leader && c.def.config.uncleanLeaderElection) {
      leader = order.find(live) ?? null;
      unclean = !!leader;
    }
    if (leader === p.meta.leader) return;
    const isr = leader ? (unclean ? [leader] : p.meta.isr.filter(live)) : p.meta.isr.filter((x) => x === p.meta.leader || live(x));
    this.commit(c, p, { leader, isr, epoch: p.meta.epoch + 1 }, reason, unclean);
    if (!leader) this.note('error', `${p.key} OFFLINE — no eligible replica in ISR (${reason})`);
    else if (unclean) this.note('error', `${p.key} UNCLEAN election → ${leader}: acked data may be lost`);
  }

  /** Commit a partition change to the metadata log and push it to replicas. */
  private commit(c: ClusterRt, p: Partition, change: Partial<PartMeta>, reason: string, unclean = false) {
    const prev = p.meta;
    p.meta = { ...prev, ...change, pEpoch: prev.pEpoch + 1 };
    if (change.epoch !== undefined && change.epoch !== prev.epoch)
      this.emit({ t: this.now, type: 'LEADER_CHANGE', tp: p.key, leader: p.meta.leader, epoch: p.meta.epoch, unclean });
    if (change.isr && change.isr.join() !== prev.isr.join()) this.emit({ t: this.now, type: 'ISR_CHANGE', tp: p.key, isr: [...p.meta.isr], reason });
    const ctl = c.active!;
    const snap = { ...p.meta, isr: [...p.meta.isr] };
    for (const b of p.rep.keys()) {
      if (c.fenced.has(b) && b !== prev.leader) continue;
      this.send(ctl, b, META_PUSH, () => this.applyMetadata(p, b, snap));
    }
  }

  private applyMetadata(p: Partition, b: string, meta: PartMeta) {
    const rep = p.rep.get(b);
    if (!rep) return; // replica reassigned away
    const prev = rep.view;
    if (prev && (meta.epoch < prev.epoch || (meta.epoch === prev.epoch && meta.pEpoch <= prev.pEpoch))) return;
    rep.view = meta;
    const wasLeader = prev?.leader === b;
    const isLeader = meta.leader === b;
    if (isLeader && (!wasLeader || prev!.epoch !== meta.epoch)) this.becomeLeader(p, b);
    else if (!isLeader && wasLeader) {
      this.flushParked(p, b);
      for (const pa of rep.pendingAcks) pa.resolve('NOT_LEADER_OR_FOLLOWER');
      rep.pendingAcks = [];
      rep.followers.clear();
      rep.alterInFlight = false;
    } else if (isLeader) {
      rep.alterInFlight = false;
      this.advanceHw(p, b);
    }
  }

  private becomeLeader(p: Partition, b: string) {
    const rep = p.rep.get(b);
    if (!rep) return; // replica reassigned away
    rep.followers.clear();
    for (const f of [...p.replicas, ...p.observers])
      if (f !== b) rep.followers.set(f, { leo: 0, lastCaughtUpT: this.now, lastFetchT: this.now, leaderLeoAtLastFetch: rep.log.length });
    rep.alterInFlight = false;
    rep.seqs = rebuildSeqs(rep.log);
    this.checkLoss(p, `new leader ${b} (epoch ${rep.view!.epoch})`);
  }

  // ------------------------------------------------------------ replication

  private followerFetch(p: Partition, b: string) {
    const rep = p.rep.get(b);
    if (!rep) return; // replica reassigned away
    const view = rep.view;
    if (!view || !view.leader || view.leader === b) return;
    if (rep.fetchInFlight >= 0 && this.now - rep.fetchInFlight < 3_000) return;
    rep.fetchInFlight = this.now;
    const leader = view.leader;
    const fetchOffset = rep.log.length;
    const lastEpoch = rep.log.at(-1)?.epoch ?? -1;
    const epoch = view.epoch;
    this.send(b, leader, 'fetch', () => {
      const lrep = p.rep.get(leader);
      const reply = (fn: () => void, records = 0) => this.send(leader, b, 'fetch_resp', fn, records);
      if (!lrep || lrep.view?.leader !== leader) return reply(() => (rep.fetchInFlight = -1));
      if (lrep.view.epoch !== epoch) return reply(() => (rep.fetchInFlight = -1));
      // divergence check (KIP-595 style lastFetchedEpoch)
      const end = endOffsetForEpoch(lrep.log, lastEpoch);
      if (fetchOffset > end) {
        return reply(() => {
          rep.fetchInFlight = -1;
          this.truncate(p, b, end, `diverged at epoch ${lastEpoch}`);
          this.after(0, b, () => this.followerFetch(p, b));
        });
      }
      const fs = lrep.followers.get(b);
      if (fs) {
        fs.leo = fetchOffset;
        if (fetchOffset >= lrep.log.length) fs.lastCaughtUpT = this.now;
        else if (fetchOffset >= fs.leaderLeoAtLastFetch) fs.lastCaughtUpT = fs.lastFetchT;
        fs.lastFetchT = this.now;
        fs.leaderLeoAtLastFetch = lrep.log.length;
      }
      this.advanceHw(p, leader);
      this.maybeExpandIsr(p, leader, b);
      const respond = () => {
        const recs = lrep.log.slice(fetchOffset, fetchOffset + 2000);
        const hw = lrep.hw;
        reply(() => {
          rep.fetchInFlight = -1;
          if (rep.view?.epoch !== epoch || rep.view.leader !== leader) return;
          if (rep.log.length !== fetchOffset) return; // stale response
          if (recs.length) {
            rep.log.push(...recs.map((r) => ({ ...r })));
            this.emit({ t: this.now, type: 'LEO', tp: p.key, node: b, leo: rep.log.length });
          }
          rep.hw = Math.min(hw, rep.log.length);
          // next fetch immediately: carries our new LEO → leader advances HW
          this.followerFetch(p, b);
        }, recs.length);
      };
      if (fetchOffset < lrep.log.length) return respond();
      // long-poll: park until new data or replica.fetch.wait.max.ms
      lrep.parked.set(b, respond);
      const wait = this.clusters.get(p.cluster)!.def.config.replicaFetchWaitMaxMs;
      this.after(wait, leader, () => {
        if (lrep.parked.get(b) !== respond) return;
        lrep.parked.delete(b);
        respond();
      });
    });
  }

  /** New data on leader → answer parked long-poll fetches. */
  private flushParked(p: Partition, leader: string) {
    const rep = p.rep.get(leader)!;
    if (!rep.parked.size) return;
    const fns = [...rep.parked.values()];
    rep.parked.clear();
    for (const fn of fns) fn();
  }

  private truncate(p: Partition, b: string, to: number, why: string) {
    const rep = p.rep.get(b);
    if (!rep) return; // replica reassigned away
    if (rep.log.length <= to) return;
    const from = rep.log.length;
    rep.log.length = to;
    rep.hw = Math.min(rep.hw, to);
    this.emit({ t: this.now, type: 'TRUNCATE', tp: p.key, node: b, from, to });
    this.emit({ t: this.now, type: 'LEO', tp: p.key, node: b, leo: to });
    this.note('warn', `${b} truncated ${p.key} ${from}→${to} (${why})`, b);
    this.checkLoss(p, `truncation on ${b}`);
  }

  private advanceHw(p: Partition, leader: string) {
    const rep = p.rep.get(leader)!;
    const view = rep.view!;
    if (view.leader !== leader) return;
    let hw = rep.log.length;
    for (const m of view.isr) if (m !== leader) hw = Math.min(hw, rep.followers.get(m)?.leo ?? 0);
    if (hw > rep.hw) {
      rep.hw = hw;
      this.emit({ t: this.now, type: 'HW', tp: p.key, hw });
      if (rep.parkedHw.size) {
        const fns = [...rep.parkedHw.values()];
        rep.parkedHw.clear();
        for (const fn of fns) fn();
      }
    }
    // resolve even without an HW move: an idempotent duplicate may already be committed
    if (rep.pendingAcks.some((a) => a.endOffset <= rep.hw)) {
      const ready = rep.pendingAcks.filter((a) => a.endOffset <= rep.hw);
      rep.pendingAcks = rep.pendingAcks.filter((a) => a.endOffset > rep.hw);
      for (const a of ready) a.resolve();
    }
  }

  private isrCheck(p: Partition, b: string) {
    const rep = p.rep.get(b);
    if (!rep) return; // replica reassigned away
    const view = rep.view;
    if (!view || view.leader !== b || rep.alterInFlight) return;
    const lag = this.clusters.get(p.cluster)!.def.config.replicaLagTimeMaxMs;
    const out = view.isr.filter((m) => m !== b && this.now - (rep.followers.get(m)?.lastCaughtUpT ?? 0) > lag);
    let isr = view.isr.filter((m) => !out.includes(m));
    // MRC observer demotion: enough sync replicas again → drop promoted observers
    const syncInIsr = isr.filter((m) => p.replicas.includes(m)).length;
    if (p.promotion === 'under-min-isr' && syncInIsr >= p.minIsr) isr = isr.filter((m) => p.replicas.includes(m));
    if (isr.join() !== view.isr.join())
      this.alterPartition(p, b, isr, out.length ? `${out.join(',')} lagging > ${lag}ms` : 'observers demoted');
    else this.maybePromoteObservers(p, b);
  }

  private maybeExpandIsr(p: Partition, leader: string, f: string) {
    const rep = p.rep.get(leader)!;
    const view = rep.view!;
    if (rep.alterInFlight || view.isr.includes(f)) return;
    if (!p.replicas.includes(f)) return this.maybePromoteObservers(p, leader);
    const fs = rep.followers.get(f);
    const epochStart = rep.log.findIndex((r) => r.epoch === view.epoch);
    const inEpoch = epochStart < 0 || (fs?.leo ?? 0) >= epochStart;
    if (fs && fs.leo >= rep.hw && inEpoch) this.alterPartition(p, leader, [...view.isr, f], `${f} caught up`);
  }

  private maybePromoteObservers(p: Partition, leader: string) {
    if (p.promotion !== 'under-min-isr') return;
    const rep = p.rep.get(leader)!;
    const view = rep.view!;
    if (rep.alterInFlight || view.isr.length >= p.minIsr) return;
    const cands = p.observers.filter((o) => !view.isr.includes(o) && (rep.followers.get(o)?.leo ?? 0) >= rep.hw);
    if (cands.length) this.alterPartition(p, leader, [...view.isr, ...cands.slice(0, p.minIsr - view.isr.length)], `observer promotion (ISR < min.insync=${p.minIsr})`);
  }

  /** AlterPartition: leader → controller; controller rejects stale epochs (strict ISR). */
  private alterPartition(p: Partition, leader: string, isr: string[], reason: string) {
    const rep = p.rep.get(leader)!;
    const c = this.clusters.get(p.cluster)!;
    const ctl = c.active;
    if (!ctl) return;
    rep.alterInFlight = true;
    const { epoch, pEpoch } = rep.view!;
    this.send(leader, ctl, 'metadata', () => {
      if (!this.isActive(c, ctl)) return;
      if (p.meta.leader !== leader || p.meta.epoch !== epoch || p.meta.pEpoch !== pEpoch) {
        this.send(ctl, leader, 'metadata', () => (rep.alterInFlight = false));
        return;
      }
      this.commit(c, p, { isr: isr.filter((x) => !c.fenced.has(x) || x === leader) }, reason);
    });
    this.after(5_000, leader, () => (rep.alterInFlight = false));
  }

  // ------------------------------------------------------------ produce path

  private produceRecord(pr: ProducerRt) {
    if (!pr.active || !pr.partitions.length) return;
    const tp = pr.partitions[pr.stickyPartition % pr.partitions.length]!;
    let b = pr.accum.get(tp);
    if (!b) {
      b = { tp, ids: [], seq: pr.seq, createdT: this.now, firstSendT: -1, attempt: 0 };
      pr.accum.set(tp, b);
      const batch = b;
      this.after(pr.def.lingerMs, pr.def.id, () => {
        // linger elapsed: ready — but with a request in flight (max.in.flight=1) keep filling it
        if (pr.accum.get(tp) === batch && !pr.inFlight.has(tp)) this.sealBatch(pr, tp);
      });
    }
    b.ids.push(`${pr.def.id}:${pr.seq++}`);
    if (b.ids.length >= pr.def.batchSize) this.sealBatch(pr, tp);
  }

  /**
   * KIP-794 adaptive partitioning (default on in Kafka ≥3.3): the next sticky
   * partition favours partitions whose leader queue is short, so one slow or
   * unreachable leader does not absorb its round-robin share. Kafka picks at
   * random, weighted by inverse queue size; we take the least-loaded one
   * (ties broken round-robin) so runs stay deterministic.
   */
  private nextSticky(pr: ProducerRt) {
    const n = pr.partitions.length;
    let best = -1;
    let bestLoad = Infinity;
    for (let k = 1; k <= n; k++) {
      const i = (pr.stickyPartition + k) % n;
      const tp = pr.partitions[i]!;
      const load = pr.queue.get(tp)!.reduce((a, b) => a + b.ids.length, 0) + (pr.inFlight.get(tp)?.batch.ids.length ?? 0) + (pr.accum.get(tp)?.ids.length ?? 0);
      if (load < bestLoad) {
        bestLoad = load;
        best = i;
      }
    }
    pr.stickyPartition = best;
  }

  private sealBatch(pr: ProducerRt, tp: string) {
    const b = pr.accum.get(tp);
    if (!b) return;
    pr.accum.delete(tp);
    this.nextSticky(pr); // KIP-480 sticky partitioner: switch on batch completion
    pr.queue.get(tp)!.push(b);
    this.drain(pr, tp);
  }

  private drain(pr: ProducerRt, tp: string): void {
    if (pr.inFlight.has(tp)) return;
    const open = pr.accum.get(tp);
    if (!pr.queue.get(tp)!.length && open && this.now - open.createdT >= pr.def.lingerMs) {
      pr.accum.delete(tp);
      pr.queue.get(tp)!.push(open);
      this.nextSticky(pr); // batch completed → sticky partitioner moves on (KIP-480)
    }
    const b = pr.queue.get(tp)![0];
    if (!b) return;
    if (b.firstSendT < 0) b.firstSendT = this.now;
    if (this.now - b.createdT > pr.def.deliveryTimeoutMs) {
      pr.queue.get(tp)!.shift();
      this.emit({ t: this.now, type: 'PRODUCE_ERROR', producer: pr.def.id, tp, count: b.ids.length, error: 'DELIVERY_TIMEOUT', retriable: false });
      return this.drain(pr, tp);
    }
    const leader = pr.leaders.get(tp);
    if (!leader) {
      this.refreshMetadata(pr);
      this.after(pr.def.retryBackoffMs, pr.def.id, () => this.drain(pr, tp));
      return;
    }
    const reqId = ++pr.reqId;
    b.attempt++;
    const acks = pr.def.acks;
    const p = this.parts.get(tp)!;
    const done = (err?: string) => {
      const cur = pr.inFlight.get(tp);
      if (!cur || cur.reqId !== reqId) return; // timed out already
      pr.inFlight.delete(tp);
      if (!err) {
        pr.queue.get(tp)!.shift();
        for (const id of b.ids) p.acked.add(id);
        this.emit({ t: this.now, type: 'PRODUCE_ACK', producer: pr.def.id, tp, count: b.ids.length, latencyMs: this.now - b.createdT });
        return this.drain(pr, tp);
      }
      if (err === 'READ_ONLY_MIRROR' || err === 'INVALID_REQUIRED_ACKS') {
        pr.queue.get(tp)!.shift();
        this.emit({ t: this.now, type: 'PRODUCE_ERROR', producer: pr.def.id, tp, count: b.ids.length, error: err, retriable: false });
        return this.drain(pr, tp);
      }
      this.emit({ t: this.now, type: 'PRODUCE_ERROR', producer: pr.def.id, tp, count: b.ids.length, error: err, retriable: true });
      if (err === 'NOT_LEADER_OR_FOLLOWER' || err === 'REQUEST_TIMEOUT' || err === 'DISCONNECTED') {
        pr.leaders.set(tp, null);
        this.refreshMetadata(pr);
      }
      if (!pr.def.enableIdempotence) {
        // non-idempotent retry = new sequence → duplicates possible
        b.seq = pr.seq;
        pr.seq += b.ids.length;
      }
      // KIP-580 exponential backoff: retry.backoff.ms doubling up to retry.backoff.max.ms (1s)
      const backoff = Math.min(1_000, pr.def.retryBackoffMs * 2 ** Math.max(0, b.attempt - 1));
      this.after(backoff, pr.def.id, () => this.drain(pr, tp));
    };
    pr.inFlight.set(tp, { batch: b, reqId, target: leader, fail: done });
    this.after(pr.def.requestTimeoutMs, pr.def.id, () => done('REQUEST_TIMEOUT'));
    if (this.nodes.get(leader)!.status === 'dead') {
      // crashed host: connection refused/reset comes back after ~1 RTT, not after request.timeout.ms
      this.after(this.rtt(pr.def.id, leader), pr.def.id, () => done('DISCONNECTED'));
      return;
    }
    const ids = [...b.ids];
    const seq = b.seq;
    this.send(pr.def.id, leader, 'produce', () => this.handleProduce(p, leader, pr.def.id, seq, ids, acks, (err) => {
      if (acks === 0) return;
      this.send(leader, pr.def.id, 'produce_resp', () => done(err));
    }), ids.length);
    if (acks === 0) done();
  }

  private handleProduce(p: Partition, b: string, producer: string, seq: number, ids: string[], acks: 0 | 1 | 'all', reply: (err?: string) => void) {
    const rep = p.rep.get(b);
    if (!rep) return reply('NOT_LEADER_OR_FOLLOWER'); // replica moved away (reassignment)
    const view = rep.view;
    if (!view || view.leader !== b) return reply('NOT_LEADER_OR_FOLLOWER');
    if (p.mirror?.active && p.mirror.type === 'cluster_link') return reply('READ_ONLY_MIRROR');
    if (acks === 'all' && view.isr.length < p.minIsr) return reply('NOT_ENOUGH_REPLICAS');
    const last = rep.seqs.get(producer);
    let endOffset: number;
    if (last && seq <= last.seq) {
      endOffset = last.offset + 1; // duplicate (idempotent retry): already appended
    } else {
      for (let i = 0; i < ids.length; i++) rep.log.push({ offset: rep.log.length, epoch: view.epoch, id: ids[i]! });
      endOffset = rep.log.length;
      rep.seqs.set(producer, { seq: seq + ids.length - 1, offset: endOffset - 1 });
      this.emit({ t: this.now, type: 'LEO', tp: p.key, node: b, leo: rep.log.length });
      this.flushParked(p, b);
    }
    if (acks !== 'all') {
      this.advanceHw(p, b);
      return reply();
    }
    rep.pendingAcks.push({ endOffset, resolve: reply });
    this.advanceHw(p, b);
  }

  private refreshMetadata(client: ProducerRt | ConsumerRt) {
    const id = client.def.id;
    if ('metadataInFlight' in client) {
      if (client.metadataInFlight) return;
      client.metadataInFlight = true;
      this.after(1_000, id, () => (client.metadataInFlight = false));
    }
    const c = this.clusters.get(client.def.cluster)!;
    const region = this.nodes.get(id)!.region;
    const brokers = c.def.brokers.filter((b) => this.nodes.get(b.id)!.status === 'running');
    if (!brokers.length) return;
    const local = brokers.filter((b) => b.region === region);
    const target = this.r.pick(local.length ? local : brokers).id;
    this.send(id, target, 'metadata', () => {
      const view = new Map<string, string | null>();
      for (const tp of client.leaders.keys()) {
        const p = this.parts.get(tp)!;
        // broker answers from its own (possibly stale) view; non-replicas know committed state
        const v = p.rep.get(target)?.view ?? p.meta;
        view.set(tp, v.leader);
      }
      this.send(target, id, 'metadata', () => {
        for (const [tp, l] of view) client.leaders.set(tp, l);
        if ('metadataInFlight' in client) client.metadataInFlight = false;
      });
    });
  }

  // ------------------------------------------------------------ consume path

  private consumerPoll(c: ConsumerRt) {
    for (const tp of c.assigned) {
      if (c.inFlight.has(tp)) continue;
      const leader = c.leaders.get(tp);
      if (!leader) {
        this.refreshMetadata(c);
        continue;
      }
      c.inFlight.add(tp);
      const pos = c.position.get(tp)!;
      const p = this.parts.get(tp)!;
      const id = c.def.id;
      this.after(5_000, id, () => c.inFlight.delete(tp));
      this.send(id, leader, 'consume', () => {
        const rep = p.rep.get(leader);
        if (!rep || rep.view?.leader !== leader)
          return this.send(leader, id, 'fetch_resp', () => {
            c.inFlight.delete(tp);
            c.leaders.set(tp, null);
          });
        const hw = rep.hw;
        let newPos = pos;
        let reset = false;
        if (pos > rep.log.length) {
          newPos = rep.log.length; // OFFSET_OUT_OF_RANGE → auto.offset.reset=latest
          reset = true;
        }
        const n = Math.max(0, Math.min(hw, newPos + c.def.maxPollRecords) - newPos);
        this.send(leader, id, 'fetch_resp', () => {
          c.inFlight.delete(tp);
          if (reset) this.note('warn', `${id} offset ${pos} out of range on ${tp}; reset to ${newPos}`, id);
          c.position.set(tp, newPos + n);
          if (n || reset) this.emit({ t: this.now, type: 'CONSUME', consumer: id, tp, count: n, position: newPos + n, lag: Math.max(0, hw - newPos - n) });
        }, n);
      });
    }
  }

  // ------------------------------------------------------------ cross-cluster

  /** Cluster Linking: mirror partition leader fetches from source leader across WAN, offsets preserved. */
  private linkFetch(p: Partition, b: string) {
    const rep = p.rep.get(b);
    if (!rep) return; // replica reassigned away
    if (!p.mirror?.active || rep.view?.leader !== b || rep.fetchInFlight >= 0) return;
    const src = this.parts.get(p.mirror.sourceKey)!;
    const srcLeader = src.meta.leader;
    if (!srcLeader) return;
    rep.fetchInFlight = this.now;
    const from = rep.log.length;
    this.after(5_000, b, () => (rep.fetchInFlight = -1));
    this.send(b, srcLeader, 'link_fetch', () => {
      const srep = src.rep.get(srcLeader);
      if (!srep || srep.view?.leader !== srcLeader) return this.send(srcLeader, b, 'fetch_resp', () => (rep.fetchInFlight = -1));
      const respond = () => {
        const recs = srep.log.slice(from, Math.min(srep.hw, from + 2000));
        const srcHw = srep.hw;
        this.send(srcLeader, b, 'fetch_resp', () => {
          rep.fetchInFlight = -1;
          if (!p.mirror?.active || rep.view?.leader !== b || rep.log.length !== from) return;
          const epoch = rep.view.epoch;
          for (const r of recs) rep.log.push({ offset: r.offset, epoch, id: r.id });
          if (recs.length) {
            this.emit({ t: this.now, type: 'LEO', tp: p.key, node: b, leo: rep.log.length });
            this.flushParked(p, b);
            this.advanceHw(p, b);
          }
          this.emit({ t: this.now, type: 'MIRROR_LAG', tp: p.key, sourceTp: src.key, lag: Math.max(0, srcHw - rep.log.length) });
          this.linkFetch(p, b); // fetcher thread loops immediately
        }, recs.length);
      };
      if (from < srep.hw) return respond();
      // long-poll on the source leader until HW moves (mirror only copies committed data)
      srep.parkedHw.set(b, respond);
      this.after(p.mirror!.fetchIntervalMs, srcLeader, () => {
        if (srep.parkedHw.get(b) !== respond) return;
        srep.parkedHw.delete(b);
        respond();
      });
    });
  }

  /** MirrorMaker 2: Connect worker consumes source ≤ HW, produces to dest (acks=all). */
  private mm2Tick(w: Mm2Rt) {
    for (const p of w.dest) {
      if (!p.mirror?.active || w.inFlight.has(p.key)) continue;
      const src = this.parts.get(p.mirror.sourceKey)!;
      const srcLeader = src.meta.leader;
      const dstLeader = p.meta.leader;
      if (!srcLeader || !dstLeader) continue;
      w.inFlight.add(p.key);
      this.after(10_000, w.id, () => w.inFlight.delete(p.key));
      const pos = w.position.get(src.key) ?? 0;
      this.send(w.id, srcLeader, 'consume', () => {
        const srep = src.rep.get(srcLeader);
        const recs = srep?.view?.leader === srcLeader ? srep.log.slice(pos, Math.min(srep.hw, pos + 500)) : [];
        const srcHw = srep?.hw ?? 0;
        this.send(srcLeader, w.id, 'fetch_resp', () => {
          if (!recs.length) {
            w.inFlight.delete(p.key);
            this.emit({ t: this.now, type: 'MIRROR_LAG', tp: p.key, sourceTp: src.key, lag: Math.max(0, srcHw - pos) });
            return;
          }
          const ids = recs.map((r) => r.id);
          const seq = w.seq;
          w.seq += ids.length;
          this.send(w.id, dstLeader, 'produce', () => this.handleProduce(p, dstLeader, w.id, seq, ids, 'all', (err) => {
            this.send(dstLeader, w.id, 'produce_resp', () => {
              w.inFlight.delete(p.key);
              if (err) return;
              for (const id of ids) p.acked.add(id);
              w.position.set(src.key, pos + ids.length);
              this.emit({ t: this.now, type: 'MIRROR_LAG', tp: p.key, sourceTp: src.key, lag: Math.max(0, srcHw - pos - ids.length) });
            });
          }), ids.length);
        }, recs.length);
      });
    }
  }

  private failover(cluster: string, topic: string) {
    for (const p of this.parts.values()) {
      if (p.cluster !== cluster || p.topic !== topic || !p.mirror) continue;
      p.mirror.active = false;
      const src = this.parts.get(p.mirror.sourceKey)!;
      const leaderLog = p.meta.leader ? p.rep.get(p.meta.leader)!.log : [];
      const have = new Set(leaderLog.map((r) => r.id));
      let rpo = 0;
      for (const id of src.acked) if (!have.has(id)) rpo++;
      this.emit({ t: this.now, type: 'MIRROR_PROMOTED', tp: p.key, rpoRecords: rpo });
      if (rpo) this.note('error', `${p.key} promoted with ${rpo} acked source records missing (RPO)`);
      else this.note('info', `${p.key} promoted, no acked records missing`);
    }
  }

  // ------------------------------------------------------------ Kubernetes (CFK)

  private setupK8s(c: ClusterT) {
    const pf = c.platform!;
    for (const w of pf.workers)
      this.workers.set(w.id, { id: w.id, cluster: c.id, zone: w.zone, alive: true, ready: true, deleted: false, schedulable: w.schedulable, capacity: w.capacity, pods: new Set() });
    const place = initialPlacement(c);
    for (const n of [...c.controllers, ...c.brokers]) {
      const wid = place.get(n.id)!;
      this.workers.get(wid)!.pods.add(n.id);
      this.pods.set(n.id, { id: n.id, cluster: c.id, phase: 'Running', worker: wid, pvcZone: n.region, restarts: 0, lastCrashT: -Infinity, gen: 0 });
    }
    for (const w of this.workers.values()) if (w.cluster === c.id) this.emitWorker(w);
    for (const pod of this.pods.values()) if (pod.cluster === c.id) this.emitPod(pod, 'scheduled');
  }

  private platformOf(cluster: string) {
    return this.clusters.get(cluster)!.def.platform!;
  }

  private emitWorker(w: WorkerRt) {
    this.emit({ t: this.now, type: 'K8S_NODE', node: w.id, zone: w.zone, ready: w.ready && !w.deleted, schedulable: w.schedulable && !w.deleted });
  }

  private emitPod(pod: PodRt, reason?: string) {
    this.emit({ t: this.now, type: 'POD', pod: pod.id, phase: pod.phase, worker: pod.worker, ...(reason ? { reason } : {}) });
  }

  private setPhase(pod: PodRt, phase: PodPhase, reason?: string) {
    pod.phase = phase;
    this.emitPod(pod, reason);
  }

  private workerFail(wid: string) {
    const w = this.workers.get(wid)!;
    if (!w.alive) return;
    w.alive = false;
    for (const id of w.pods) this.killNode(id, `worker ${w.id} failed`);
    const grace = this.platformOf(w.cluster).nodeNotReadyAfterMs;
    this.after(grace, null, () => {
      if (w.alive || w.deleted) return;
      w.ready = false;
      this.emitWorker(w);
      for (const id of w.pods) {
        const pod = this.pods.get(id)!;
        // StatefulSet guarantees ≤1 pod per ordinal: it won't replace a pod it can't confirm dead
        this.setPhase(pod, 'Unknown', `node ${w.id} unreachable; StatefulSet won't recreate it until the node is deleted or the pod force-deleted`);
      }
      this.note('warn', `worker ${w.id} NotReady; ${w.pods.size} Kafka pod(s) stuck on it`);
    });
  }

  private workerRecover(wid: string) {
    const w = this.workers.get(wid)!;
    if (w.alive || w.deleted) return;
    w.alive = true;
    w.ready = true;
    this.emitWorker(w);
    for (const id of w.pods) this.startPod(this.pods.get(id)!, `worker ${w.id} back; kubelet restarts the container`);
    this.scheduleAll();
  }

  private workerDelete(wid: string) {
    const w = this.workers.get(wid)!;
    if (w.deleted) return;
    for (const id of [...w.pods]) this.killNode(id, `worker ${w.id} deleted`);
    w.alive = false;
    w.ready = false;
    w.deleted = true;
    this.emitWorker(w);
    for (const id of [...w.pods]) this.recreatePod(this.pods.get(id)!, `node ${w.id} deleted`);
  }

  /** kubectl delete pod / drain: SIGTERM → controlled shutdown → exit → StatefulSet recreates it. */
  private podDelete(id: string) {
    const pod = this.pods.get(id)!;
    if (pod.phase === 'Terminating') return;
    const running = this.nodes.get(id)!.status === 'running';
    if (!running) return this.recreatePod(pod, 'pod deleted');
    this.setPhase(pod, 'Terminating', 'SIGTERM');
    const n = this.nodes.get(id)!;
    if (n.kind === 'broker') this.controlledShutdown(id);
    const gen = pod.gen;
    this.after(this.platformOf(pod.cluster).terminationGraceMs, null, () => {
      if (pod.gen !== gen) return;
      this.killNode(id, 'terminated (pod deleted)');
      this.recreatePod(pod, 'pod deleted');
    });
  }

  /** Broker asks the controller to move its leaderships away before exiting (no unavailability). */
  private controlledShutdown(id: string) {
    const c = this.clusters.get(this.nodes.get(id)!.cluster)!;
    const ctl = c.active;
    if (!ctl) return;
    this.send(id, ctl, 'heartbeat', () => {
      if (!this.isActive(c, ctl)) return;
      c.shuttingDown.add(id);
      if (!c.fenced.has(id)) this.fence(c, id, 'controlled shutdown');
    });
  }

  private containerCrash(id: string) {
    const pod = this.pods.get(id)!;
    if (this.nodes.get(id)!.status === 'dead' || pod.phase !== 'Running') return;
    this.killNode(id, 'container crashed');
    const pf = this.platformOf(pod.cluster);
    if (this.now - pod.lastCrashT > CRASH_RESET_MS) pod.restarts = 0;
    pod.restarts++;
    pod.lastCrashT = this.now;
    const delay = pod.restarts === 1 ? 0 : Math.min(pf.crashBackoffMaxMs, pf.crashBackoffBaseMs * 2 ** (pod.restarts - 2));
    const gen = pod.gen;
    if (delay > 0) this.setPhase(pod, 'CrashLoopBackOff', `restart ${pod.restarts}: back-off ${Math.round(delay / 1000)}s`);
    this.after(delay, null, () => {
      if (pod.gen !== gen) return;
      const w = pod.worker ? this.workers.get(pod.worker) : undefined;
      if (w?.alive) this.startPod(pod, `kubelet restart #${pod.restarts}`);
    });
  }

  /** ContainerCreating → (podStartupMs) → Running + broker process starts. */
  private startPod(pod: PodRt, reason: string) {
    this.setPhase(pod, 'ContainerCreating', reason);
    const gen = pod.gen;
    this.after(this.platformOf(pod.cluster).podStartupMs, null, () => {
      const w = pod.worker ? this.workers.get(pod.worker) : undefined;
      if (pod.gen !== gen || pod.phase !== 'ContainerCreating' || !w?.alive) return;
      this.setPhase(pod, 'Running');
      this.restartNode(pod.id, 'pod started');
    });
  }

  /** StatefulSet recreates the pod object → Pending → scheduler. */
  private recreatePod(pod: PodRt, reason: string) {
    if (pod.worker) this.workers.get(pod.worker)!.pods.delete(pod.id);
    pod.worker = null;
    pod.gen++;
    pod.restarts = 0;
    this.setPhase(pod, 'Pending', reason);
    this.scheduleAll();
  }

  private scheduleAll() {
    for (const pod of [...this.pods.values()].sort((a, b) => a.id.localeCompare(b.id))) {
      if (pod.phase !== 'Pending' || pod.worker) continue;
      const pf = this.platformOf(pod.cluster);
      const w = [...this.workers.values()].find((w) => w.cluster === pod.cluster && w.alive && w.ready && !w.deleted && w.schedulable && w.zone === pod.pvcZone && w.pods.size < w.capacity);
      if (!w) {
        const why = pf.storage === 'zoneLocked' ? `PVC is bound to ${pod.pvcZone}: no schedulable worker with room there` : `no schedulable worker with room in ${pod.pvcZone}`;
        this.emitPod(pod, why);
        continue;
      }
      w.pods.add(pod.id);
      pod.worker = w.id;
      if (pf.storage === 'ephemeral') this.wipe(pod.id);
      this.startPod(pod, `scheduled on ${w.id}`);
    }
  }

  /** Ephemeral storage: a new pod starts with an empty log and re-replicates everything. */
  private wipe(id: string) {
    for (const p of this.parts.values()) {
      const rep = p.rep.get(id);
      if (!rep || !rep.log.length) continue;
      this.truncate(p, id, 0, 'new pod, ephemeral storage');
      rep.seqs.clear();
    }
  }

  // ------------------------------------------------------------ Confluent Self-Balancing

  /** Heal: move replicas off brokers that stayed fenced past the threshold (rack/zone-aware). */
  private selfBalancingTick(c: ClusterRt) {
    if (!c.active || !this.isActive(c, c.active)) return;
    const pf = c.def.platform!;
    const zone = (b: string) => this.nodes.get(b)!.region;
    for (const [b, since] of c.fencedAt) {
      if (c.healed.has(b) || this.now - since < pf.selfBalancing.brokerFailureThresholdMs) continue;
      c.healed.add(b);
      this.note('warn', `Self-Balancing: ${b} failed for ${Math.round((this.now - since) / 1000)}s; moving its replicas`, b);
      for (const p of this.parts.values()) {
        if (p.cluster !== c.def.id || !p.replicas.includes(b) || c.moving.has(p.key)) continue;
        const live = p.replicas.filter((r) => r !== b);
        const usedZones = new Set(live.map(zone));
        const load = (x: string) => [...this.parts.values()].filter((q) => q.replicas.includes(x)).length;
        const cands = c.def.brokers
          .map((x) => x.id)
          .filter((x) => !p.replicas.includes(x) && !p.observers.includes(x) && !c.fenced.has(x) && this.nodes.get(x)!.status === 'running')
          .sort((x, y) => Number(usedZones.has(zone(x))) - Number(usedZones.has(zone(y))) || load(x) - load(y) || x.localeCompare(y));
        const to = cands[0];
        if (!to) {
          this.note('error', `Self-Balancing: no healthy broker to take ${p.key} from ${b}`);
          continue;
        }
        p.replicas.push(to);
        p.rep.set(to, this.newReplica({ ...p.meta, isr: [...p.meta.isr] }));
        const leader = p.meta.leader ? p.rep.get(p.meta.leader) : undefined;
        if (leader?.view?.leader === p.meta.leader) leader.followers.set(to, { leo: 0, lastCaughtUpT: -Infinity, lastFetchT: this.now, leaderLeoAtLastFetch: 0 });
        c.moving.set(p.key, { from: b, to });
        this.emit({ t: this.now, type: 'REPLICAS_CHANGE', tp: p.key, replicas: [...p.replicas], observers: [...p.observers] });
        this.commit(c, p, {}, `reassign ${b} → ${to}`);
      }
    }
    for (const [key, mv] of c.moving) {
      const p = this.parts.get(key)!;
      if (!p.meta.isr.includes(mv.to)) continue;
      p.replicas = p.replicas.filter((r) => r !== mv.from);
      p.rep.delete(mv.from);
      for (const r of p.rep.values()) r.followers.delete(mv.from);
      c.moving.delete(key);
      this.emit({ t: this.now, type: 'REPLICAS_CHANGE', tp: p.key, replicas: [...p.replicas], observers: [...p.observers] });
      this.commit(c, p, { isr: p.meta.isr.filter((r) => r !== mv.from) }, `reassignment done: ${mv.from} → ${mv.to}`);
      this.note('info', `Self-Balancing: ${p.key} now on [${p.replicas.join(', ')}]`);
    }
  }

  // ------------------------------------------------------------ correctness oracle

  /** Acked records absent from the committed leader's log = data loss. */
  private checkLoss(p: Partition, reason: string) {
    const leader = p.meta.leader;
    if (!leader || !p.acked.size) return;
    const log = p.rep.get(leader)!.log;
    const have = new Set(log.map((r) => r.id));
    let lost = 0;
    for (const id of p.acked) {
      if (!have.has(id) && !p.lostReported.has(id)) {
        p.lostReported.add(id);
        lost++;
      }
    }
    if (lost) {
      this.emit({ t: this.now, type: 'DATA_LOSS', tp: p.key, count: lost, reason });
      this.note('error', `${p.key}: ${lost} acknowledged records lost (${reason})`);
    }
  }
}

function endOffsetForEpoch(log: Rec[], epoch: number): number {
  if (epoch < 0) return 0;
  // first offset whose epoch > requested epoch (log epochs are monotonic)
  let lo = 0,
    hi = log.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (log[mid]!.epoch <= epoch) lo = mid + 1;
    else hi = mid;
  }
  // if leader never had that epoch, fall back to start of next lower epoch boundary
  return lo;
}

function rebuildSeqs(log: Rec[]) {
  const m = new Map<string, { seq: number; offset: number }>();
  for (const r of log) {
    const i = r.id.lastIndexOf(':');
    const prod = r.id.slice(0, i);
    const seq = Number(r.id.slice(i + 1));
    m.set(prod, { seq, offset: r.offset });
  }
  return m;
}
