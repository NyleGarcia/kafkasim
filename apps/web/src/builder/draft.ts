/**
 * Scenario-builder draft operations. Pure: each op takes a draft and returns a
 * new one (structuredClone + mutate), cascading reference cleanup so the UI can
 * never leave dangling ids behind (topics → removed brokers, clients → removed
 * topics, actions → removed nodes, …). Validation is the engine's own parser.
 */
import { assignReplicas, Cluster, Consumer, expandLocation, locationsOf, Manifest, Platform, Producer, Topic, validateRefs, type ActionT, type ClusterT, type ConsumerT, type ProducerT, type TopicT } from '@kafkasim/engine';

export type Draft = Manifest;

const edit = (d: Draft, fn: (x: Draft) => void): Draft => {
  const x = structuredClone(d);
  fn(x);
  return x;
};

export function allIds(d: Draft): Set<string> {
  const s = new Set<string>(d.regions.map((r) => r.id));
  for (const r of d.regions) for (const z of r.zones ?? []) s.add(z.id);
  for (const c of d.clusters) {
    s.add(c.id);
    for (const n of [...c.controllers, ...c.brokers]) s.add(n.id);
    for (const w of c.platform?.workers ?? []) s.add(w.id);
  }
  for (const p of [...d.producers, ...d.consumers]) s.add(p.id);
  return s;
}

export function uniqueId(d: Draft, prefix: string, taken = allIds(d)): string {
  for (let i = 1; ; i++) if (!taken.has(`${prefix}${i}`)) return `${prefix}${i}`;
}

export const nodeIdsOf = (c: ClusterT) => [...c.controllers, ...c.brokers].map((n) => n.id);

// ------------------------------------------------------------------ blank

export function blankDraft(): Draft {
  return Manifest.parse({
    schemaVersion: 1,
    name: 'New scenario',
    description: '',
    durationMs: 60_000,
    regions: [{ id: 'dc1', name: 'DC 1' }],
    clusters: [
      {
        id: 'main',
        controllers: [1, 2, 3].map((i) => ({ id: `c${i}`, region: 'dc1' })),
        brokers: [1, 2, 3].map((i) => ({ id: `b${i}`, region: 'dc1' })),
        config: { minInsyncReplicas: 2 },
      },
    ],
    topics: [{ cluster: 'main', name: 'orders', partitions: 3, replicationFactor: 3 }],
    producers: [{ id: 'app', cluster: 'main', region: 'dc1', topic: 'orders', acks: 'all', ratePerSec: 20 }],
    consumers: [{ id: 'reader', cluster: 'main', region: 'dc1', group: 'g1', topic: 'orders' }],
    actions: [],
  });
}

// ------------------------------------------------------------------ validation

export interface Problem {
  path: string;
  message: string;
}

export function validate(d: Draft): Problem[] {
  const r = Manifest.safeParse(d);
  if (!r.success) return r.error.issues.map((i) => ({ path: i.path.join('.'), message: i.message }));
  try {
    validateRefs(r.data);
    return [];
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    return msg
      .split('\n')
      .slice(1)
      .map((l) => ({ path: '', message: l.replace(/^\s*-\s*/, '') }))
      .filter((p) => p.message);
  }
}

// ------------------------------------------------------------------ cleanup

/** Remove every reference to nodes that no longer exist. */
function scrub(x: Draft) {
  const nodes = new Set<string>();
  for (const c of x.clusters) for (const id of nodeIdsOf(c)) nodes.add(id);
  for (const p of [...x.producers, ...x.consumers]) nodes.add(p.id);
  // anything a region-or-zone reference may point at
  const regions = new Set([...x.regions.map((r) => r.id), ...locationsOf(x).map((l) => l.id)]);
  for (const t of x.topics) {
    const c = x.clusters.find((k) => k.id === t.cluster);
    if (!c) continue;
    const brokers = new Set(c.brokers.map((b) => b.id));
    if (t.replicas) {
      t.replicas = t.replicas.map((r) => r.filter((b) => brokers.has(b)));
      if (t.replicas.some((r) => !r.length)) delete t.replicas; // fall back to auto placement
    }
    if (t.observers) t.observers = t.observers.map((r) => r.filter((b) => brokers.has(b)));
    if (t.replicationFactor && t.replicationFactor > c.brokers.length) t.replicationFactor = c.brokers.length;
  }
  x.actions = x.actions.flatMap((a): ActionT[] => {
    if ('node' in a && !nodes.has(a.node)) return [];
    if (a.type === 'STOP_PRODUCER' || a.type === 'START_PRODUCER') return x.producers.some((p) => p.id === a.producer) ? [a] : [];
    if (a.type === 'PARTITION') {
      const groups = a.groups.map((g) => g.filter((id) => nodes.has(id) || regions.has(id))).filter((g) => g.length);
      return groups.length >= 2 ? [{ ...a, groups }] : [];
    }
    if (a.type === 'SET_LATENCY') return regions.has(a.a) && regions.has(a.b) ? [a] : [];
    if (a.type === 'FAILOVER') return x.topics.some((t) => t.cluster === a.cluster && t.name === a.topic && t.mirror) ? [a] : [];
    return [a];
  });
  x.latency.links = x.latency.links.filter((l) => regions.has(l.a) && regions.has(l.b));
}

// ------------------------------------------------------------------ general

export const setMeta = (d: Draft, patch: Partial<Pick<Draft, 'name' | 'description' | 'durationMs' | 'seed' | 'requiresConfluent'>>) => edit(d, (x) => Object.assign(x, patch));
export const setLatency = (d: Draft, patch: Partial<Draft['latency']>) => edit(d, (x) => Object.assign(x.latency, patch));

export function setLink(d: Draft, a: string, b: string, ms: number | null) {
  return edit(d, (x) => {
    x.latency.links = x.latency.links.filter((l) => !((l.a === a && l.b === b) || (l.a === b && l.b === a)));
    if (ms !== null) x.latency.links.push({ a, b, ms });
  });
}

// ------------------------------------------------------------------ regions

export function addRegion(d: Draft) {
  return edit(d, (x) => {
    const id = uniqueId(x, 'region');
    x.regions.push({ id, name: id });
  });
}

export const renameRegion = (d: Draft, id: string, name: string) => edit(d, (x) => (x.regions.find((r) => r.id === id)!.name = name));

/** Placement locations (zones, or zoneless regions) — what nodes/clients/workers reference. */
export const locations = (d: Draft) => locationsOf(d);
const firstLoc = (x: Draft) => locationsOf(x)[0]!.id;

/** Move every placement (nodes, clients, workers, MM2 worker) from one location to another. */
function relocate(x: Draft, from: Set<string>, to: string) {
  for (const c of x.clusters) {
    for (const n of [...c.controllers, ...c.brokers]) if (from.has(n.region)) n.region = to;
    for (const w of c.platform?.workers ?? []) if (from.has(w.zone)) w.zone = to;
  }
  for (const p of [...x.producers, ...x.consumers]) if (from.has(p.region)) p.region = to;
  for (const t of x.topics) if (t.mirror?.workerRegion && from.has(t.mirror.workerRegion)) t.mirror.workerRegion = to;
}

/** Returns an error string when deleting what's in these locations would leave a cluster with no controllers/brokers. */
export function emptiesCluster(d: Draft, gone: Set<string>, label: string): string | null {
  for (const c of d.clusters) {
    if (c.controllers.every((n) => gone.has(n.region))) return `Deleting would remove every controller of cluster ${c.id} (all in ${label}). Move them to another location instead.`;
    if (c.brokers.every((n) => gone.has(n.region))) return `Deleting would remove every broker of cluster ${c.id} (all in ${label}). Move them to another location instead.`;
  }
  return null;
}

/** What is placed in these locations (to show before a removal). */
export function placementsIn(d: Draft, locs: Set<string>) {
  const nodes = d.clusters.flatMap((c) => [...c.controllers, ...c.brokers].filter((n) => locs.has(n.region)).map((n) => n.id));
  const clients = [...d.producers, ...d.consumers].filter((p) => locs.has(p.region)).map((p) => p.id);
  const workers = d.clusters.flatMap((c) => (c.platform?.workers ?? []).filter((w) => locs.has(w.zone)).map((w) => w.id));
  return { nodes, clients, workers, total: nodes.length + clients.length + workers.length };
}

/** Locations an id covers (region → its AZs, or itself). */
export const locationsUnder = (d: Draft, id: string) => new Set(expandLocation(d, id));

function dropLocations(x: Draft, gone: Set<string>) {
  for (const c of x.clusters) {
    c.controllers = c.controllers.filter((n) => !gone.has(n.region));
    c.brokers = c.brokers.filter((n) => !gone.has(n.region));
    if (c.platform) c.platform.workers = c.platform.workers.filter((w) => !gone.has(w.zone));
  }
  const fallback = firstLoc(x);
  for (const p of [...x.producers, ...x.consumers]) if (gone.has(p.region)) p.region = fallback;
  for (const t of x.topics) if (t.mirror?.workerRegion && gone.has(t.mirror.workerRegion)) delete t.mirror.workerRegion;
  x.actions = x.actions.filter((a) => !(('worker' in a && !x.clusters.some((c) => c.platform?.workers.some((w) => w.id === a.worker))) || ('target' in a && gone.has(a.target)) || ('zone' in a && gone.has(a.zone))));
  scrub(x);
}

/** Returns an error string instead of a draft when the removal would leave a cluster empty. */
/**
 * Remove a region. `moveTo` = a location to move everything placed there into
 * (nodes, clients, k8s workers); without it those are deleted.
 */
export function removeRegion(d: Draft, id: string, moveTo?: string): Draft | string {
  if (d.regions.length === 1) return 'A scenario needs at least one region.';
  const gone = new Set(expandLocation(d, id));
  if (moveTo && gone.has(moveTo)) return 'Pick a location outside the region being removed.';
  if (!moveTo) {
    const err = emptiesCluster(d, gone, id);
    if (err) return err;
  }
  return edit(d, (x) => {
    if (moveTo) relocate(x, gone, moveTo);
    x.regions = x.regions.filter((r) => r.id !== id);
    dropLocations(x, gone);
  });
}

// ------------------------------------------------------------------ availability zones

const letters = 'abcdefghij';

/** Split a zoneless region into 3 AZs; everything placed in the region moves to the first AZ. */
export function splitRegion(d: Draft, regionId: string, count = 3) {
  return edit(d, (x) => {
    const r = x.regions.find((k) => k.id === regionId)!;
    if (r.zones?.length) return;
    const taken = allIds(x);
    r.zones = Array.from({ length: count }, (_, i) => {
      const want = `${regionId}-${letters[i]}`;
      const zid = taken.has(want) ? uniqueId(x, want, taken) : want;
      taken.add(zid);
      return { id: zid, name: `${r.name ?? r.id}${letters[i]}` };
    });
    relocate(x, new Set([regionId]), r.zones[0]!.id);
  });
}

export function addZone(d: Draft, regionId: string) {
  const r = d.regions.find((k) => k.id === regionId)!;
  if (!r.zones?.length) return splitRegion(d, regionId, 2);
  return edit(d, (x) => {
    const xr = x.regions.find((k) => k.id === regionId)!;
    const i = xr.zones!.length;
    const want = `${regionId}-${letters[i] ?? 'z'}`;
    const zid = allIds(x).has(want) ? uniqueId(x, want) : want;
    xr.zones!.push({ id: zid, name: `${xr.name ?? xr.id}${letters[i] ?? i}` });
  });
}

export const renameZone = (d: Draft, regionId: string, zoneId: string, name: string) =>
  edit(d, (x) => (x.regions.find((k) => k.id === regionId)!.zones!.find((z) => z.id === zoneId)!.name = name));

/** Remove an AZ. Removing the last AZ turns the region back into a single location. */
export function removeZone(d: Draft, regionId: string, zoneId: string, moveTo?: string): Draft | string {
  const r = d.regions.find((k) => k.id === regionId)!;
  if (r.zones!.length === 1)
    return edit(d, (x) => {
      const xr = x.regions.find((k) => k.id === regionId)!;
      delete xr.zones;
      relocate(x, new Set([zoneId]), regionId);
      scrub(x);
    });
  const gone = new Set([zoneId]);
  if (moveTo === zoneId) return 'Pick another location.';
  if (!moveTo) {
    const err = emptiesCluster(d, gone, zoneId);
    if (err) return err;
  }
  return edit(d, (x) => {
    if (moveTo) relocate(x, gone, moveTo);
    const xr = x.regions.find((k) => k.id === regionId)!;
    xr.zones = xr.zones!.filter((z) => z.id !== zoneId);
    dropLocations(x, gone);
  });
}

// ------------------------------------------------------------------ clusters

export function addCluster(d: Draft) {
  return edit(d, (x) => {
    const taken = allIds(x);
    const id = uniqueId(x, 'cluster', taken);
    taken.add(id);
    const region = firstLoc(x);
    const mk = (prefix: string) => {
      const nid = uniqueId(x, prefix, taken);
      taken.add(nid);
      return { id: nid, region };
    };
    x.clusters.push(
      Cluster.parse({
        id,
        controllers: [mk('c'), mk('c'), mk('c')],
        brokers: [mk('b'), mk('b'), mk('b')],
      }),
    );
  });
}

export function removeCluster(d: Draft, id: string): Draft | string {
  if (d.clusters.length === 1) return 'A scenario needs at least one cluster.';
  return edit(d, (x) => {
    x.clusters = x.clusters.filter((c) => c.id !== id);
    x.topics = x.topics.filter((t) => t.cluster !== id);
    for (const t of x.topics) if (t.mirror?.sourceCluster === id) delete t.mirror;
    x.producers = x.producers.filter((p) => p.cluster !== id);
    x.consumers = x.consumers.filter((p) => p.cluster !== id);
    scrub(x);
  });
}

export const patchCluster = (d: Draft, id: string, patch: Partial<Pick<ClusterT, 'name' | 'flavor'>>) =>
  edit(d, (x) => {
    const c = x.clusters.find((k) => k.id === id)!;
    Object.assign(c, patch);
    if (patch.flavor === 'apache') {
      // Confluent-only features can't stay on an apache cluster
      for (const t of x.topics.filter((t) => t.cluster === id)) {
        delete t.observers;
        t.observerPromotionPolicy = 'none';
        if (t.mirror?.type === 'cluster_link') t.mirror.type = 'mm2';
      }
    }
  });

export const patchClusterConfig = (d: Draft, id: string, patch: Partial<ClusterT['config']>) => edit(d, (x) => Object.assign(x.clusters.find((k) => k.id === id)!.config, patch));

/** Set how many controllers/brokers a cluster has in one region (adds/removes at the end). */
export function setNodeCount(d: Draft, clusterId: string, kind: 'controllers' | 'brokers', region: string, n: number): Draft | string {
  const c = d.clusters.find((k) => k.id === clusterId)!;
  const here = c[kind].filter((x) => x.region === region);
  const elsewhere = c[kind].length - here.length;
  if (n < 0 || elsewhere + n < 1) return `Cluster ${clusterId} needs at least one ${kind === 'brokers' ? 'broker' : 'controller'}.`;
  if (n > 9) return 'Up to 9 per region keeps the canvas readable.';
  return edit(d, (x) => {
    const xc = x.clusters.find((k) => k.id === clusterId)!;
    const list = xc[kind];
    const cur = list.filter((k) => k.region === region);
    if (n > cur.length) {
      const taken = allIds(x);
      for (let i = cur.length; i < n; i++) {
        const id = uniqueId(x, kind === 'brokers' ? 'b' : 'c', taken);
        taken.add(id);
        list.push({ id, region });
      }
    } else {
      const drop = new Set(cur.slice(n).map((k) => k.id));
      xc[kind] = list.filter((k) => !drop.has(k.id)) as typeof list;
    }
    scrub(x);
  });
}

export const setBrokerRack = (d: Draft, clusterId: string, broker: string, rack: string) =>
  edit(d, (x) => {
    const b = x.clusters.find((k) => k.id === clusterId)!.brokers.find((k) => k.id === broker)!;
    if (rack) b.rack = rack;
    else delete b.rack;
  });

// ------------------------------------------------------------------ topics

export function addTopic(d: Draft, clusterId = d.clusters[0]!.id) {
  return edit(d, (x) => {
    const c = x.clusters.find((k) => k.id === clusterId)!;
    const names = new Set(x.topics.filter((t) => t.cluster === clusterId).map((t) => t.name));
    let i = 1;
    while (names.has(`topic${i}`)) i++;
    x.topics.push(Topic.parse({ cluster: clusterId, name: `topic${i}`, partitions: 3, replicationFactor: Math.min(3, c.brokers.length) }));
  });
}

const topicIdx = (x: Draft, cluster: string, name: string) => x.topics.findIndex((t) => t.cluster === cluster && t.name === name);

export function removeTopic(d: Draft, cluster: string, name: string) {
  return edit(d, (x) => {
    x.topics.splice(topicIdx(x, cluster, name), 1);
    for (const t of x.topics) if (t.mirror?.sourceCluster === cluster && t.mirror.sourceTopic === name) delete t.mirror;
    x.producers = x.producers.filter((p) => !(p.cluster === cluster && p.topic === name));
    x.consumers = x.consumers.filter((p) => !(p.cluster === cluster && p.topic === name));
    scrub(x);
  });
}

/** Rename cascades to clients, mirrors and FAILOVER actions. */
export function renameTopic(d: Draft, cluster: string, name: string, to: string) {
  return edit(d, (x) => {
    x.topics[topicIdx(x, cluster, name)]!.name = to;
    for (const t of x.topics) if (t.mirror?.sourceCluster === cluster && t.mirror.sourceTopic === name) t.mirror.sourceTopic = to;
    for (const p of [...x.producers, ...x.consumers]) if (p.cluster === cluster && p.topic === name) p.topic = to;
    for (const a of x.actions) if (a.type === 'FAILOVER' && a.cluster === cluster && a.topic === name) a.topic = to;
  });
}

export function patchTopic(d: Draft, cluster: string, name: string, patch: Partial<TopicT>) {
  return edit(d, (x) => {
    const t = x.topics[topicIdx(x, cluster, name)]!;
    Object.assign(t, patch);
    for (const k of Object.keys(patch) as (keyof TopicT)[]) if (patch[k] === undefined) delete t[k];
    const c = x.clusters.find((k) => k.id === t.cluster)!;
    // keep explicit placement in step with the partition count
    if (patch.partitions !== undefined) {
      if (t.replicas) {
        const auto = assignReplicas(c, { ...t, replicas: undefined });
        t.replicas = Array.from({ length: t.partitions }, (_, i) => t.replicas![i] ?? auto[i]!);
      }
      if (t.observers) t.observers = Array.from({ length: t.partitions }, (_, i) => t.observers![i] ?? []);
      // mirrors must match their source's partition count
      for (const m of x.topics) if (m.mirror?.sourceCluster === t.cluster && m.mirror.sourceTopic === t.name) m.partitions = t.partitions;
    }
    if (patch.mirror) {
      const src = x.topics.find((s) => s.cluster === patch.mirror!.sourceCluster && s.name === patch.mirror!.sourceTopic);
      if (src) t.partitions = src.partitions;
    }
  });
}

/** Switch between automatic (RF) and explicit replica placement. */
export function setCustomPlacement(d: Draft, cluster: string, name: string, on: boolean) {
  return edit(d, (x) => {
    const t = x.topics[topicIdx(x, cluster, name)]!;
    const c = x.clusters.find((k) => k.id === cluster)!;
    if (on) {
      t.replicas = assignReplicas(c, t);
      delete t.replicationFactor;
    } else {
      t.replicationFactor = t.replicas?.[0]?.length ?? Math.min(3, c.brokers.length);
      delete t.replicas;
    }
  });
}

/** Toggle broker in a partition's replica (or observer) list. Order = click order; first = preferred leader. */
export function toggleReplica(d: Draft, cluster: string, name: string, partition: number, broker: string, role: 'replica' | 'observer') {
  return edit(d, (x) => {
    const t = x.topics[topicIdx(x, cluster, name)]!;
    if (!t.replicas) return;
    t.observers ??= Array.from({ length: t.partitions }, () => []);
    const reps = t.replicas[partition]!;
    const obs = t.observers[partition]!;
    const list = role === 'replica' ? reps : obs;
    const other = role === 'replica' ? obs : reps;
    const i = list.indexOf(broker);
    if (i >= 0) {
      if (role === 'replica' && list.length === 1) return; // a partition needs one replica
      list.splice(i, 1);
    } else {
      list.push(broker);
      const j = other.indexOf(broker);
      if (j >= 0) other.splice(j, 1);
    }
    if (t.observers.every((o) => !o.length)) delete t.observers;
  });
}

// ------------------------------------------------------------------ clients

export function addProducer(d: Draft) {
  return edit(d, (x) => {
    const t = x.topics[0];
    if (!t) return;
    const c = x.clusters.find((k) => k.id === t.cluster)!;
    x.producers.push(Producer.parse({ id: uniqueId(x, 'producer'), cluster: t.cluster, region: c.brokers[0]!.region, topic: t.name }));
  });
}

export function addConsumer(d: Draft) {
  return edit(d, (x) => {
    const t = x.topics[0];
    if (!t) return;
    const c = x.clusters.find((k) => k.id === t.cluster)!;
    x.consumers.push(Consumer.parse({ id: uniqueId(x, 'consumer'), cluster: t.cluster, region: c.brokers[0]!.region, group: 'g1', topic: t.name }));
  });
}

export const patchProducer = (d: Draft, id: string, patch: Partial<ProducerT>) =>
  edit(d, (x) => {
    const p = x.producers.find((k) => k.id === id)!;
    Object.assign(p, patch);
    if (patch.stopMs === undefined && 'stopMs' in patch) delete p.stopMs;
  });
export const patchConsumer = (d: Draft, id: string, patch: Partial<ConsumerT>) => edit(d, (x) => Object.assign(x.consumers.find((k) => k.id === id)!, patch));

export const removeClient = (d: Draft, id: string) =>
  edit(d, (x) => {
    x.producers = x.producers.filter((p) => p.id !== id);
    x.consumers = x.consumers.filter((p) => p.id !== id);
    scrub(x);
  });

// ------------------------------------------------------------------ actions

export function addAction(d: Draft) {
  return edit(d, (x) => {
    const b = x.clusters[0]!.brokers[0]!.id;
    x.actions.push({ type: 'KILL_NODE', at: Math.round(x.durationMs / 3 / 1000) * 1000, node: b });
    x.actions.sort((a, b) => a.at - b.at);
  });
}

export const setAction = (d: Draft, i: number, a: ActionT) =>
  edit(d, (x) => {
    x.actions[i] = a;
    x.actions.sort((p, q) => p.at - q.at);
  });

export const removeAction = (d: Draft, i: number) => edit(d, (x) => x.actions.splice(i, 1));

/** A sensible default action when the user switches an action's type. */
export function actionOfType(d: Draft, type: ActionT['type'], at: number): ActionT {
  const nodes = d.clusters.flatMap(nodeIdsOf);
  const node = nodes.find((n) => d.clusters.some((c) => c.brokers.some((b) => b.id === n))) ?? nodes[0]!;
  switch (type) {
    case 'KILL_NODE':
    case 'RESTART_NODE':
    case 'PAUSE_NODE':
    case 'RESUME_NODE':
      return { type, at, node };
    case 'PARTITION': {
      const [r0, r1] = d.regions;
      return { type, at, groups: r1 ? [[r0!.id], [r1.id]] : [[r0!.id], [node]] };
    }
    case 'HEAL':
      return { type, at };
    case 'SET_LATENCY': {
      const [r0, r1] = d.regions;
      return { type, at, a: r0!.id, b: (r1 ?? r0)!.id, ms: 100 };
    }
    case 'FAILOVER': {
      const m = d.topics.find((t) => t.mirror) ?? d.topics[0]!;
      return { type, at, cluster: m.cluster, topic: m.name };
    }
    case 'STOP_PRODUCER':
    case 'START_PRODUCER':
      return { type, at, producer: d.producers[0]?.id ?? 'producer1' };
    case 'K8S_NODE_FAIL':
    case 'K8S_NODE_RECOVER':
    case 'K8S_NODE_DELETE':
      return { type, at, worker: k8sWorkers(d)[0]?.id ?? 'w1' };
    case 'K8S_CORDON':
    case 'K8S_UNCORDON':
      return { type, at, target: k8sWorkers(d)[0]?.id ?? d.regions[0]!.id };
    case 'POD_DELETE':
    case 'CONTAINER_CRASH':
      return { type, at, pod: k8sPods(d).find((p) => p.startsWith('b')) ?? k8sPods(d)[0] ?? node };
    case 'ZONE_OUTAGE':
    case 'ZONE_RECOVER':
      return { type, at, zone: d.regions[0]!.id };
  }
}

export const k8sWorkers = (d: Draft) => d.clusters.flatMap((c) => c.platform?.workers ?? []);
export const k8sPods = (d: Draft) => d.clusters.filter((c) => c.platform).flatMap(nodeIdsOf);

// ------------------------------------------------------------------ Kubernetes platform

/** Turn Kubernetes (CFK) on/off for a cluster. CFK implies Confluent Server. */
export function setPlatform(d: Draft, clusterId: string, on: boolean) {
  return edit(d, (x) => {
    const c = x.clusters.find((k) => k.id === clusterId)!;
    if (!on) {
      delete c.platform;
      x.actions = x.actions.filter((a) => !('worker' in a || 'pod' in a || 'target' in a || a.type === 'ZONE_OUTAGE' || a.type === 'ZONE_RECOVER'));
      return;
    }
    c.flavor = 'confluent';
    const taken = allIds(x);
    const workers: { id: string; zone: string }[] = [];
    // one worker per Kafka pod in each zone, plus one spare so pods can move
    for (const r of locationsOf(x)) {
      const n = [...c.controllers, ...c.brokers].filter((k) => k.region === r.id).length;
      for (let i = 0; i < (n ? n + 1 : 0); i++) {
        const id = uniqueId(x, `w-${r.id}-`, taken);
        taken.add(id);
        workers.push({ id, zone: r.id });
      }
    }
    c.platform = Platform.parse({ type: 'kubernetes', workers });
  });
}

export const patchPlatform = (d: Draft, clusterId: string, patch: Partial<NonNullable<ClusterT['platform']>>) =>
  edit(d, (x) => Object.assign(x.clusters.find((k) => k.id === clusterId)!.platform!, patch));

export function setWorkerCount(d: Draft, clusterId: string, zone: string, n: number): Draft | string {
  if (n < 0 || n > 9) return 'Between 0 and 9 workers per zone.';
  return edit(d, (x) => {
    const pf = x.clusters.find((k) => k.id === clusterId)!.platform!;
    const here = pf.workers.filter((w) => w.zone === zone);
    if (n > here.length) {
      const taken = allIds(x);
      for (const w of pf.workers) taken.add(w.id);
      for (let i = here.length; i < n; i++) {
        const id = uniqueId(x, `w-${zone}-`, taken);
        taken.add(id);
        pf.workers.push({ id, zone, schedulable: true, capacity: 1 });
      }
    } else {
      const drop = new Set(here.slice(n).map((w) => w.id));
      pf.workers = pf.workers.filter((w) => !drop.has(w.id));
      x.actions = x.actions.filter((a) => !(('worker' in a && drop.has(a.worker)) || ('target' in a && drop.has(a.target))));
    }
  });
}

export const patchWorker = (d: Draft, clusterId: string, worker: string, patch: { schedulable?: boolean; capacity?: number }) =>
  edit(d, (x) => Object.assign(x.clusters.find((k) => k.id === clusterId)!.platform!.workers.find((w) => w.id === worker)!, patch));
