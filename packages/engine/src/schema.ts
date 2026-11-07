/**
 * Contracts — single source of truth for the scenario manifest and the event log.
 *
 * Emitted to /contracts/*.schema.json (see scripts/emit-schema.ts) so the Python
 * bridge/harness consume the same shapes. Every producer of events (TS engine,
 * real-Kafka recorder, live bridge) speaks `SimEvent`; every consumer (renderer,
 * calibration diff) reduces `SimEvent`s with `reduceEvent`.
 */
import { z } from 'zod';

export const SCHEMA_VERSION = 1 as const;

const Id = z.string().min(1).regex(/^[A-Za-z0-9_.-]+$/, 'ids: [A-Za-z0-9_.-]');

// ---------------------------------------------------------------- topology

export const Zone = z.object({ id: Id, name: z.string().optional() });

/**
 * A region, optionally split into availability zones. Nodes, clients and k8s
 * workers are placed in a *location*: a zone id, or the region id itself when
 * the region has no zones (single-location region — the pre-zones model).
 */
export const Region = z.object({
  id: Id,
  name: z.string().optional(),
  zones: z.array(Zone).min(1).optional(),
});
export type RegionT = z.output<typeof Region>;

export interface Location {
  id: string;
  name: string;
  region: string;
  regionName: string;
  /** true when the location is an AZ inside a multi-zone region */
  isZone: boolean;
}

/** Flattened placement locations, in display order (regions, then their zones). */
export function locationsOf(m: { regions: RegionT[] }): Location[] {
  return m.regions.flatMap((r): Location[] =>
    r.zones?.length
      ? r.zones.map((z) => ({ id: z.id, name: z.name ?? z.id, region: r.id, regionName: r.name ?? r.id, isZone: true }))
      : [{ id: r.id, name: r.name ?? r.id, region: r.id, regionName: r.name ?? r.id, isZone: false }],
  );
}

/** Location ids covered by an id that may be a region or a location. */
export function expandLocation(m: { regions: RegionT[] }, id: string): string[] {
  const locs = locationsOf(m);
  const inRegion = locs.filter((l) => l.region === id).map((l) => l.id);
  return inRegion.length ? inRegion : locs.some((l) => l.id === id) ? [id] : [];
}

export const ClusterConfig = z.object({
  /** replica.lag.time.max.ms — follower not caught up for this long leaves ISR. */
  replicaLagTimeMaxMs: z.number().int().positive().default(30_000),
  /** min.insync.replicas (cluster default; topic may override). */
  minInsyncReplicas: z.number().int().positive().default(1),
  /** unclean.leader.election.enable */
  uncleanLeaderElection: z.boolean().default(false),
  /** broker.heartbeat.interval.ms */
  brokerHeartbeatIntervalMs: z.number().int().positive().default(2_000),
  /** broker.session.timeout.ms */
  brokerSessionTimeoutMs: z.number().int().positive().default(9_000),
  /** controller.quorum.fetch.timeout.ms — follower controllers start election after this. */
  quorumFetchTimeoutMs: z.number().int().positive().default(2_000),
  /** controller.quorum.election.timeout.ms (base; randomized up to 2x). */
  quorumElectionTimeoutMs: z.number().int().positive().default(1_000),
  /** replica.fetch.wait.max.ms — long-poll; sim uses it as follower fetch cadence. */
  replicaFetchWaitMaxMs: z.number().int().positive().default(500),
});

/** Kubernetes worker node. A manifest region is a k8s zone (topology.kubernetes.io/zone). */
export const K8sWorker = z.object({
  id: Id,
  zone: Id,
  /** false = cordoned / tainted NoSchedule for Kafka pods */
  schedulable: z.boolean().default(true),
  /** Kafka pods this worker can host (anti-affinity ⇒ usually 1) */
  capacity: z.number().int().positive().default(1),
});

/**
 * Optional platform: run the cluster on Kubernetes under an operator.
 * Pods = StatefulSet ordinals (one per controller/broker), PVCs bound to a zone.
 */
export const Platform = z.object({
  type: z.literal('kubernetes'),
  operator: z.enum(['cfk']).default('cfk'),
  workers: z.array(K8sWorker).min(1),
  /** zoneLocked: RWO block volume (EBS/PD) pinned to its zone. ephemeral: log lost when the pod moves. */
  storage: z.enum(['zoneLocked', 'ephemeral']).default('zoneLocked'),
  /** image start + JVM + log recovery before the broker is up */
  podStartupMs: z.number().int().nonnegative().default(20_000),
  /** node-monitor-grace-period: worker reported NotReady after this */
  nodeNotReadyAfterMs: z.number().int().nonnegative().default(40_000),
  /** SIGTERM → controlled shutdown → exit */
  terminationGraceMs: z.number().int().nonnegative().default(5_000),
  crashBackoffBaseMs: z.number().int().positive().default(10_000),
  crashBackoffMaxMs: z.number().int().positive().default(300_000),
  /** Confluent Self-Balancing Clusters: move replicas off a broker that stays failed. */
  selfBalancing: z
    .object({
      enabled: z.boolean().default(false),
      /** confluent.balancer.heal.broker.failure.threshold.ms */
      brokerFailureThresholdMs: z.number().int().positive().default(3_600_000),
    })
    .prefault({}),
});

export const Cluster = z.object({
  id: Id,
  name: z.string().optional(),
  /** Confluent-only behaviour is gated on this. */
  flavor: z.enum(['apache', 'confluent']).default('apache'),
  controllers: z.array(z.object({ id: Id, region: Id })).min(1),
  brokers: z.array(z.object({ id: Id, region: Id, rack: z.string().optional() })).min(1),
  config: ClusterConfig.prefault({}),
  /**
   * Where broker.rack comes from (rack-aware placement, RPP constraints):
   * zone = the broker's AZ/location, region = its region, none = no rack awareness.
   * An explicit broker `rack` overrides zone/region.
   */
  rack: z.enum(['zone', 'region', 'none']).default('zone'),
  platform: Platform.optional(),
});

export const Mirror = z.object({
  /** cluster_link: Confluent Cluster Linking (byte-for-byte, offsets preserved).
   *  mm2: Apache MirrorMaker 2 (consume+produce, offsets translated). */
  type: z.enum(['cluster_link', 'mm2']),
  sourceCluster: Id,
  sourceTopic: z.string().min(1),
  /** MM2 only: region where the Connect worker runs (defaults to first dest broker's region). */
  workerRegion: Id.optional(),
  /** Poll cadence of the mirror fetcher / MM2 consumer. */
  fetchIntervalMs: z.number().int().positive().default(500),
});

export const PlacementRule = z.object({ count: z.number().int().positive(), rack: z.string().min(1) });

export const Topic = z.object({
  cluster: Id,
  name: z.string().min(1),
  partitions: z.number().int().positive().default(1),
  /** Explicit assignment per partition: sync replicas (first = preferred leader). */
  replicas: z.array(z.array(Id).min(1)).optional(),
  /** Used when `replicas` omitted: rack/region-aware round robin. */
  replicationFactor: z.number().int().positive().optional(),
  /** Confluent MRC observers per partition (async, never in ISR unless promoted). */
  observers: z.array(z.array(Id)).optional(),
  /**
   * Confluent Replica Placement Policy (confluent.placement.constraints):
   * per-rack counts of sync replicas and observers. Replaces replicationFactor/replicas/observers.
   */
  placement: z
    .object({
      replicas: z.array(PlacementRule).min(1),
      observers: z.array(PlacementRule).default([]),
    })
    .optional(),
  /** Confluent MRC: promote observers when ISR < min.insync. */
  observerPromotionPolicy: z.enum(['none', 'under-min-isr']).default('none'),
  minInsyncReplicas: z.number().int().positive().optional(),
  mirror: Mirror.optional(),
});

export const Producer = z.object({
  id: Id,
  cluster: Id,
  region: Id,
  topic: z.string().min(1),
  acks: z.union([z.literal(0), z.literal(1), z.literal('all')]).default('all'),
  /** Records per second (deterministic spacing). */
  ratePerSec: z.number().positive().default(10),
  lingerMs: z.number().int().nonnegative().default(5),
  /** batch.size expressed in records (sim abstraction of bytes). */
  batchSize: z.number().int().positive().default(16),
  enableIdempotence: z.boolean().default(true),
  /** request.timeout.ms */
  requestTimeoutMs: z.number().int().positive().default(30_000),
  /** delivery.timeout.ms — total time a record may spend retrying. */
  deliveryTimeoutMs: z.number().int().positive().default(120_000),
  retryBackoffMs: z.number().int().nonnegative().default(100),
  startMs: z.number().int().nonnegative().default(0),
  stopMs: z.number().int().nonnegative().optional(),
});

export const Consumer = z.object({
  id: Id,
  cluster: Id,
  region: Id,
  group: z.string().min(1),
  topic: z.string().min(1),
  pollIntervalMs: z.number().int().positive().default(200),
  maxPollRecords: z.number().int().positive().default(500),
  startMs: z.number().int().nonnegative().default(0),
});

export const LatencyLink = z.object({ a: Id, b: Id, ms: z.number().nonnegative() });

// ---------------------------------------------------------------- actions

const At = z.number().int().nonnegative();

export const Action = z.discriminatedUnion('type', [
  /** Hard crash (SIGKILL). Log survives on disk, process state lost. */
  z.object({ type: z.literal('KILL_NODE'), at: At, node: Id }),
  /** Restart a killed node. */
  z.object({ type: z.literal('RESTART_NODE'), at: At, node: Id }),
  /** SIGSTOP — zombie: holds sockets, does no work, wakes up stale. */
  z.object({ type: z.literal('PAUSE_NODE'), at: At, node: Id }),
  z.object({ type: z.literal('RESUME_NODE'), at: At, node: Id }),
  /** Network partition: nodes/regions in different groups cannot talk. Unlisted nodes join group 0. */
  z.object({
    type: z.literal('PARTITION'),
    at: At,
    /** Each group is a list of region ids or node ids. */
    groups: z.array(z.array(Id).min(1)).min(2),
  }),
  z.object({ type: z.literal('HEAL'), at: At }),
  z.object({ type: z.literal('SET_LATENCY'), at: At, a: Id, b: Id, ms: z.number().nonnegative() }),
  /** Promote a mirror topic to writable (Cluster Linking failover / MM2 cutover). */
  z.object({ type: z.literal('FAILOVER'), at: At, cluster: Id, topic: z.string() }),
  z.object({ type: z.literal('STOP_PRODUCER'), at: At, producer: Id }),
  z.object({ type: z.literal('START_PRODUCER'), at: At, producer: Id }),
  // ---- Kubernetes (cluster.platform) ----
  /** Worker crashes: its pods' processes die; StatefulSet pods stay stuck until the node returns or is deleted. */
  z.object({ type: z.literal('K8S_NODE_FAIL'), at: At, worker: Id }),
  z.object({ type: z.literal('K8S_NODE_RECOVER'), at: At, worker: Id }),
  /** Node object deleted (or force-delete of its pods): StatefulSet can recreate the pods elsewhere. */
  z.object({ type: z.literal('K8S_NODE_DELETE'), at: At, worker: Id }),
  /** target = worker id or zone id */
  z.object({ type: z.literal('K8S_CORDON'), at: At, target: Id }),
  z.object({ type: z.literal('K8S_UNCORDON'), at: At, target: Id }),
  /** kubectl delete pod / drain: graceful (controlled shutdown), then recreated and rescheduled */
  z.object({ type: z.literal('POD_DELETE'), at: At, pod: Id }),
  /** process crash: kubelet restarts the container in place with exponential backoff */
  z.object({ type: z.literal('CONTAINER_CRASH'), at: At, pod: Id }),
  /** every worker in the zone (or in every zone of a region) fails and it is cut off */
  z.object({ type: z.literal('ZONE_OUTAGE'), at: At, zone: Id }),
  z.object({ type: z.literal('ZONE_RECOVER'), at: At, zone: Id }),
]);

// ---------------------------------------------------------------- manifest

export const Manifest = z.object({
  schemaVersion: z.literal(SCHEMA_VERSION),
  name: z.string().min(1),
  description: z.string().default(''),
  /** Scenario needs Confluent Platform features (shown as a badge). */
  requiresConfluent: z.boolean().default(false),
  seed: z.number().int().default(1),
  durationMs: z.number().int().positive().default(60_000),
  regions: z.array(Region).min(1),
  latency: z
    .object({
      /** Same location (same AZ, or same zoneless region). */
      intraRegionMs: z.number().nonnegative().default(1),
      /** Between availability zones of the same region. */
      interZoneMs: z.number().nonnegative().default(1.5),
      /** Default between regions not listed in `links`. */
      interRegionMs: z.number().nonnegative().default(30),
      /** +/- uniform jitter fraction applied per message. */
      jitter: z.number().min(0).max(1).default(0.1),
      links: z.array(LatencyLink).default([]),
    })
    .prefault({}),
  clusters: z.array(Cluster).min(1),
  topics: z.array(Topic).default([]),
  producers: z.array(Producer).default([]),
  consumers: z.array(Consumer).default([]),
  actions: z.array(Action).default([]),
});

export type ManifestInput = z.input<typeof Manifest>;
export type Manifest = z.output<typeof Manifest>;
export type ClusterT = z.output<typeof Cluster>;
export type TopicT = z.output<typeof Topic>;
export type ProducerT = z.output<typeof Producer>;
export type ConsumerT = z.output<typeof Consumer>;
export type ActionT = z.output<typeof Action>;

// ---------------------------------------------------------------- events

export const NodeKind = z.enum(['broker', 'controller', 'producer', 'consumer', 'mm2']);
export const NodeStatus = z.enum(['running', 'dead', 'paused']);

/** Topic-partition key: `<cluster>/<topic>/<partition>` */
const Tp = z.string();

export const SimEvent = z.discriminatedUnion('type', [
  z.object({ t: z.number(), type: z.literal('NODE_STATUS'), node: Id, status: NodeStatus, reason: z.string().optional() }),
  /** Controller's view: fenced brokers lead nothing and are dropped from ISRs. */
  z.object({ t: z.number(), type: z.literal('BROKER_FENCE'), cluster: Id, node: Id, fenced: z.boolean(), reason: z.string().optional() }),
  z.object({ t: z.number(), type: z.literal('CONTROLLER_ELECTED'), cluster: Id, node: Id, epoch: z.number().int() }),
  z.object({ t: z.number(), type: z.literal('CONTROLLER_LOST'), cluster: Id, node: Id }),
  z.object({
    t: z.number(),
    type: z.literal('LEADER_CHANGE'),
    tp: Tp,
    /** null = partition offline. */
    leader: Id.nullable(),
    epoch: z.number().int(),
    unclean: z.boolean().default(false),
  }),
  z.object({ t: z.number(), type: z.literal('ISR_CHANGE'), tp: Tp, isr: z.array(Id), reason: z.string().optional() }),
  /** Per-replica log end offset. */
  z.object({ t: z.number(), type: z.literal('LEO'), tp: Tp, node: Id, leo: z.number().int() }),
  /** High watermark as known by the leader. */
  z.object({ t: z.number(), type: z.literal('HW'), tp: Tp, hw: z.number().int() }),
  z.object({ t: z.number(), type: z.literal('TRUNCATE'), tp: Tp, node: Id, from: z.number().int(), to: z.number().int() }),
  z.object({ t: z.number(), type: z.literal('PRODUCE_ACK'), producer: Id, tp: Tp, count: z.number().int(), latencyMs: z.number() }),
  z.object({ t: z.number(), type: z.literal('PRODUCE_ERROR'), producer: Id, tp: Tp, count: z.number().int(), error: z.string(), retriable: z.boolean().default(false) }),
  z.object({ t: z.number(), type: z.literal('CONSUME'), consumer: Id, tp: Tp, count: z.number().int(), position: z.number().int(), lag: z.number().int() }),
  /** Acknowledged records that no longer exist on the current leader. */
  z.object({ t: z.number(), type: z.literal('DATA_LOSS'), tp: Tp, count: z.number().int(), reason: z.string() }),
  z.object({ t: z.number(), type: z.literal('MIRROR_LAG'), tp: Tp, sourceTp: Tp, lag: z.number().int() }),
  z.object({ t: z.number(), type: z.literal('MIRROR_PROMOTED'), tp: Tp, rpoRecords: z.number().int() }),
  z.object({ t: z.number(), type: z.literal('NETWORK'), partitioned: z.boolean(), groups: z.array(z.array(Id)).optional() }),
  z.object({ t: z.number(), type: z.literal('ACTION'), action: z.string(), detail: z.string() }),
  /** Human-readable explanation of something notable (shown in the event log). */
  z.object({ t: z.number(), type: z.literal('K8S_NODE'), node: Id, zone: Id, ready: z.boolean(), schedulable: z.boolean() }),
  z.object({
    t: z.number(),
    type: z.literal('POD'),
    pod: Id,
    phase: z.enum(['Pending', 'ContainerCreating', 'Running', 'Terminating', 'CrashLoopBackOff', 'Unknown']),
    worker: Id.nullable(),
    reason: z.string().optional(),
  }),
  /** Partition reassignment (e.g. Self-Balancing healing): new replica set. */
  z.object({ t: z.number(), type: z.literal('REPLICAS_CHANGE'), tp: Tp, replicas: z.array(Id), observers: z.array(Id) }),
  z.object({ t: z.number(), type: z.literal('NOTE'), level: z.enum(['info', 'warn', 'error']), node: Id.optional(), message: z.string() }),
  /** A message on the wire — renderer only; never required for state. */
  z.object({
    t: z.number(),
    type: z.literal('MSG'),
    from: Id,
    to: Id,
    kind: z.enum(['produce', 'produce_resp', 'fetch', 'fetch_resp', 'heartbeat', 'metadata', 'vote', 'link_fetch', 'consume']),
    arriveT: z.number(),
    dropped: z.boolean().default(false),
    records: z.number().int().default(0),
  }),
]);

export type SimEvent = z.output<typeof SimEvent>;
export type SimEventType = SimEvent['type'];
export type MsgKind = Extract<SimEvent, { type: 'MSG' }>['kind'];

export const EventLog = z.object({
  schemaVersion: z.literal(SCHEMA_VERSION),
  /** 'sim' | 'recording' | 'live' */
  source: z.enum(['sim', 'recording', 'live']),
  manifest: Manifest,
  events: z.array(SimEvent),
});
export type EventLog = z.output<typeof EventLog>;

export const tpKey = (cluster: string, topic: string, partition: number) => `${cluster}/${topic}/${partition}`;
export const parseTp = (tp: string) => {
  const parts = tp.split('/');
  const partition = Number(parts.pop());
  const cluster = parts.shift()!;
  return { cluster, topic: parts.join('/'), partition };
};

export function parseManifest(input: unknown): Manifest {
  const m = Manifest.parse(input);
  validateRefs(m);
  return m;
}

/** Cross-reference checks zod can't express. Throws with every problem listed. */
export function validateRefs(m: Manifest): void {
  const errs: string[] = [];
  const locs = locationsOf(m);
  // `regions` = valid placement locations (zones, or zoneless regions)
  const regions = new Set(locs.map((l) => l.id));
  const regionIds = new Set(m.regions.map((r) => r.id));
  const anyLoc = (id: string) => regions.has(id) || regionIds.has(id);
  {
    const seen = new Set<string>();
    for (const id of [...m.regions.map((r) => r.id), ...m.regions.flatMap((r) => (r.zones ?? []).map((z) => z.id))]) {
      if (seen.has(id)) errs.push(`duplicate region/zone id "${id}"`);
      seen.add(id);
    }
  }
  const where = (owner: string, loc: string) => {
    if (regions.has(loc)) return;
    const r = m.regions.find((x) => x.id === loc);
    errs.push(r?.zones ? `${owner}: region "${loc}" has zones; place it in one of ${r.zones.map((z) => z.id).join(', ')}` : `${owner}: unknown region/zone "${loc}"`);
  };
  for (const l of m.latency.links) for (const x of [l.a, l.b]) if (!anyLoc(x)) errs.push(`latency link: unknown region/zone "${x}"`);
  const nodes = new Set<string>();
  const dup = (id: string) => {
    if (nodes.has(id)) errs.push(`duplicate node id "${id}"`);
    nodes.add(id);
  };
  const clusters = new Map(m.clusters.map((c) => [c.id, c]));
  for (const c of m.clusters) {
    for (const n of [...c.controllers, ...c.brokers]) {
      dup(n.id);
      where(n.id, n.region);
    }
  }
  const topics = new Set<string>();
  for (const t of m.topics) {
    const c = clusters.get(t.cluster);
    if (!c) {
      errs.push(`topic ${t.name}: unknown cluster "${t.cluster}"`);
      continue;
    }
    topics.add(`${t.cluster}/${t.name}`);
    const brokerIds = new Set(c.brokers.map((b) => b.id));
    if (t.replicas && t.replicas.length !== t.partitions)
      errs.push(`topic ${t.name}: replicas has ${t.replicas.length} entries, partitions=${t.partitions}`);
    for (const list of [...(t.replicas ?? []), ...(t.observers ?? [])])
      for (const b of list) if (!brokerIds.has(b)) errs.push(`topic ${t.name}: "${b}" is not a broker of ${c.id}`);
    if (!t.replicas && (t.replicationFactor ?? 1) > c.brokers.length)
      errs.push(`topic ${t.name}: replicationFactor > broker count`);
    if (t.mirror) {
      const src = m.topics.find((s) => s.cluster === t.mirror!.sourceCluster && s.name === t.mirror!.sourceTopic);
      if (src && src.partitions !== t.partitions) errs.push(`topic ${t.name}: mirror must have ${src.partitions} partitions like its source`);
      if (t.mirror.sourceCluster === t.cluster) errs.push(`topic ${t.name}: mirror source must be another cluster`);
    }
    if (t.mirror?.type === 'cluster_link' && c.flavor !== 'confluent')
      errs.push(`topic ${t.name}: cluster_link requires a confluent-flavor destination cluster`);
    if ((t.observers?.length ?? 0) > 0 && c.flavor !== 'confluent')
      errs.push(`topic ${t.name}: observers require a confluent-flavor cluster`);
    if (t.placement) {
      const where = `topic ${t.name}: placement constraints`;
      if (c.flavor !== 'confluent') errs.push(`${where} (RPP) require a confluent-flavor cluster`);
      if (c.rack === 'none') errs.push(`${where} need rack awareness: set cluster ${c.id} rack to zone or region`);
      if (t.replicas || t.observers || t.replicationFactor) errs.push(`${where} replace replicationFactor/replicas/observers; set only one`);
      const racks = new Map<string, number>();
      for (const b of c.brokers) {
        const r = brokerRack(c, b, m);
        racks.set(r, (racks.get(r) ?? 0) + 1);
      }
      for (const [kind, rules] of [['replica', t.placement.replicas], ['observer', t.placement.observers]] as const) {
        const seen = new Set<string>();
        for (const r of rules) {
          if (seen.has(r.rack)) errs.push(`${where}: rack "${r.rack}" appears twice in ${kind} constraints; merge them into one count`);
          seen.add(r.rack);
        }
      }
      const need = new Map<string, number>();
      for (const rule of [...t.placement.replicas, ...t.placement.observers]) {
        if (!racks.has(rule.rack)) errs.push(`${where}: no broker has rack "${rule.rack}" (racks: ${[...racks.keys()].join(', ')})`);
        need.set(rule.rack, (need.get(rule.rack) ?? 0) + rule.count);
      }
      for (const [r, n] of need) if (racks.has(r) && n > racks.get(r)!) errs.push(`${where}: rack "${r}" needs ${n} brokers, has ${racks.get(r)}`);
      if (t.placement.observers.length && c.flavor !== 'confluent') errs.push(`${where}: observers require confluent`);
    }
  }
  for (const t of m.topics)
    if (t.mirror && !topics.has(`${t.mirror.sourceCluster}/${t.mirror.sourceTopic}`))
      errs.push(`topic ${t.name}: mirror source ${t.mirror.sourceCluster}/${t.mirror.sourceTopic} not declared`);
  for (const p of [...m.producers, ...m.consumers]) {
    dup(p.id);
    where(p.id, p.region);
    if (!topics.has(`${p.cluster}/${p.topic}`)) errs.push(`${p.id}: unknown topic ${p.cluster}/${p.topic}`);
  }
  const workers = new Map<string, string>(); // worker → zone
  const pods = new Set<string>();
  for (const c of m.clusters) {
    if (!c.platform) continue;
    if (c.flavor !== 'confluent') errs.push(`cluster ${c.id}: CFK runs Confluent Server — set flavor "confluent"`);
    for (const w of c.platform.workers) {
      if (workers.has(w.id) || nodes.has(w.id)) errs.push(`duplicate id "${w.id}" (worker)`);
      workers.set(w.id, w.zone);
      where(`worker ${w.id}`, w.zone);
    }
    for (const n of [...c.controllers, ...c.brokers]) pods.add(n.id);
    try {
      initialPlacement(c);
    } catch (e) {
      errs.push(`cluster ${c.id}: ${(e as Error).message}`);
    }
  }
  for (const a of m.actions) {
    if ('node' in a && !nodes.has(a.node)) errs.push(`action ${a.type}@${a.at}: unknown node "${a.node}"`);
    if ('worker' in a && !workers.has(a.worker)) errs.push(`action ${a.type}@${a.at}: unknown worker "${a.worker}"`);
    if ('pod' in a && !pods.has(a.pod)) errs.push(`action ${a.type}@${a.at}: "${a.pod}" is not a Kubernetes pod`);
    if ('target' in a && !workers.has(a.target) && !anyLoc(a.target)) errs.push(`action ${a.type}@${a.at}: unknown worker/zone/region "${a.target}"`);
    if ('zone' in a && !anyLoc(a.zone)) errs.push(`action ${a.type}@${a.at}: unknown zone/region "${a.zone}"`);
    if (a.type === 'PARTITION')
      for (const g of a.groups) for (const x of g) if (!anyLoc(x) && !nodes.has(x)) errs.push(`PARTITION: unknown "${x}"`);
    if (a.type === 'SET_LATENCY') for (const x of [a.a, a.b]) if (!anyLoc(x)) errs.push(`SET_LATENCY: unknown region/zone "${x}"`);
  }
  if (errs.length) throw new Error(`manifest "${m.name}" invalid:\n  - ${errs.join('\n  - ')}`);
}

type BrokerT = ClusterT['brokers'][number];
type RegionsLike = { regions: RegionT[] } | RegionT[];
const regionList = (r: RegionsLike) => (Array.isArray(r) ? r : r.regions);

/** broker.rack as Kafka sees it, derived from the cluster's rack mode. */
export function brokerRack(cluster: ClusterT, b: BrokerT, regions: RegionsLike): string {
  if (cluster.rack === 'none') return 'none';
  if (b.rack) return b.rack;
  if (cluster.rack === 'region') return regionList(regions).find((r) => r.id === b.region || r.zones?.some((z) => z.id === b.region))?.id ?? b.region;
  return b.region;
}

/** Distinct racks of a cluster, in broker order. */
export function rackIds(cluster: ClusterT, regions: RegionsLike): string[] {
  return [...new Set(cluster.brokers.map((b) => brokerRack(cluster, b, regions)))];
}

/**
 * Replica (and observer) assignment per partition:
 *  - placement constraints (RPP): `count` brokers from each named rack, rotated per partition
 *  - explicit `replicas` / `observers`
 *  - otherwise rack-interleaved round robin with `replicationFactor` (rack-aware placement)
 */
export function placementFor(cluster: ClusterT, topic: TopicT, regions: RegionsLike): { replicas: string[][]; observers: string[][] } {
  const byRack = new Map<string, string[]>();
  for (const b of cluster.brokers) {
    const r = brokerRack(cluster, b, regions);
    byRack.set(r, [...(byRack.get(r) ?? []), b.id]);
  }
  if (topic.placement) {
    const pl = topic.placement;
    const replicas: string[][] = [];
    const observers: string[][] = [];
    for (let p = 0; p < topic.partitions; p++) {
      const used = new Set<string>();
      const take = (rules: { count: number; rack: string }[]) =>
        rules.flatMap((rule) => {
          const pool = (byRack.get(rule.rack) ?? []).filter((b) => !used.has(b));
          const out: string[] = [];
          for (let k = 0; k < Math.min(rule.count, pool.length); k++) out.push(pool[(p + k) % pool.length]!);
          for (const b of out) used.add(b);
          return out;
        });
      replicas.push(take(pl.replicas));
      observers.push(take(pl.observers));
    }
    return { replicas, observers };
  }
  const lists = [...byRack.values()];
  const order: string[] = [];
  for (let i = 0; order.length < cluster.brokers.length; i++) for (const l of lists) if (l[i]) order.push(l[i]!);
  const rf = topic.replicationFactor ?? Math.min(3, order.length);
  return {
    replicas: Array.from({ length: topic.partitions }, (_, p) => topic.replicas?.[p] ?? Array.from({ length: rf }, (_, k) => order[(p + k) % order.length]!)),
    observers: Array.from({ length: topic.partitions }, (_, p) => topic.observers?.[p] ?? []),
  };
}

/** Sync-replica assignment only (see placementFor). */
export function assignReplicas(cluster: ClusterT, topic: TopicT, regions: RegionsLike = []): string[][] {
  return placementFor(cluster, topic, regions).replicas;
}

/**
 * Live bridge wire protocol (WebSocket, JSON text frames). The bridge sends a
 * `hello` once per connection, then `events` batches in time order. `t` is ms
 * since the bridge started observing (monotonic). The UI never sends commands
 * in v1 — fault injection against a real cluster stays in the harness.
 */
export const BridgeFrame = z.discriminatedUnion('kind', [
  z.object({ kind: z.literal('hello'), schemaVersion: z.literal(SCHEMA_VERSION), bridge: z.string(), manifest: Manifest }),
  z.object({ kind: z.literal('events'), events: z.array(SimEvent) }),
  z.object({ kind: z.literal('error'), message: z.string() }),
]);
export type BridgeFrame = z.output<typeof BridgeFrame>;

/**
 * Initial pod → worker placement for a Kubernetes cluster: each controller/broker
 * pod goes to a schedulable worker in its own zone with free capacity (in order).
 * Throws when a zone can't host its pods (reported as a manifest error).
 */
export function initialPlacement(cluster: ClusterT): Map<string, string> {
  const out = new Map<string, string>();
  const p = cluster.platform;
  if (!p) return out;
  const load = new Map<string, number>();
  for (const n of [...cluster.controllers, ...cluster.brokers]) {
    const w = p.workers.find((w) => w.zone === n.region && w.schedulable && (load.get(w.id) ?? 0) < w.capacity);
    if (!w) throw new Error(`pod ${n.id}: no schedulable worker with free capacity in zone ${n.region}`);
    load.set(w.id, (load.get(w.id) ?? 0) + 1);
    out.set(n.id, w.id);
  }
  return out;
}
