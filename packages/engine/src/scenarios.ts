/** Built-in scenarios. Each teaches one Kafka behaviour; tests assert the outcome. */
import type { ManifestInput } from './schema.ts';

const ctl3 = (region: string | string[], prefix = 'c') =>
  [0, 1, 2].map((i) => ({ id: `${prefix}${i + 1}`, region: Array.isArray(region) ? region[i]! : region }));
const brokers = (ids: string[], region: string) => ids.map((id) => ({ id, region }));


/** CFK cluster over 3 AZs: 1 controller + 2 brokers per AZ, `workersPerZone` workers (capacity 1) per AZ. */
const cfk = (
  opts: { id?: string; zones?: string[]; prefix?: string; workersPerZone?: number[]; rackAwareness?: boolean; selfBalancingMs?: number; storage?: 'zoneLocked' | 'ephemeral' } = {},
) => {
  const zones = opts.zones ?? ['az1', 'az2', 'az3'];
  const px = opts.prefix ?? '';
  const wpz = opts.workersPerZone ?? zones.map(() => 3);
  return {
    id: opts.id ?? 'cfk',
    flavor: 'confluent' as const,
    controllers: zones.map((z, i) => ({ id: `${px}c${i + 1}`, region: z })),
    brokers: zones.flatMap((z, i) => [{ id: `${px}b${2 * i + 1}`, region: z }, { id: `${px}b${2 * i + 2}`, region: z }]),
    config: { minInsyncReplicas: 2, replicaLagTimeMaxMs: 10_000 },
    platform: {
      type: 'kubernetes' as const,
      workers: zones.flatMap((z, i) => Array.from({ length: wpz[i]! }, (_, k) => ({ id: `w-${z}-${k + 1}`, zone: z }))),
      rackAwareness: opts.rackAwareness ?? true,
      storage: opts.storage ?? 'zoneLocked',
      selfBalancing: opts.selfBalancingMs ? { enabled: true, brokerFailureThresholdMs: opts.selfBalancingMs } : { enabled: false },
    },
  };
};
/** one AWS-style region with three availability zones */
const azs = [
  {
    id: 'us-east-1',
    name: 'us-east-1',
    zones: [
      { id: 'az1', name: 'us-east-1a' },
      { id: 'az2', name: 'us-east-1b' },
      { id: 'az3', name: 'us-east-1c' },
    ],
  },
];
const azLatency = { intraRegionMs: 0.4, interZoneMs: 1.2 };

export const scenarios: ManifestInput[] = [
  {
    schemaVersion: 1,
    name: 'Single cluster: broker failure',
    description:
      'RF=3, min.insync=2, acks=all. Broker b1 (leader of p0) is killed at 15s: producers time out until the controller fences b1 after broker.session.timeout.ms (9s) and elects a new leader from the ISR. b1 restarts at 35s, catches up and rejoins the ISR. No acknowledged data is lost.',
    durationMs: 50_000,
    regions: [{ id: 'dc1', name: 'DC 1' }],
    clusters: [
      {
        id: 'main',
        controllers: ctl3('dc1'),
        brokers: brokers(['b1', 'b2', 'b3'], 'dc1'),
        config: { minInsyncReplicas: 2 },
      },
    ],
    topics: [{ cluster: 'main', name: 'orders', partitions: 3, replicationFactor: 3 }],
    producers: [{ id: 'checkout', cluster: 'main', region: 'dc1', topic: 'orders', acks: 'all', ratePerSec: 40 }],
    consumers: [{ id: 'billing', cluster: 'main', region: 'dc1', group: 'billing', topic: 'orders' }],
    actions: [
      { type: 'KILL_NODE', at: 15_000, node: 'b1' },
      { type: 'RESTART_NODE', at: 35_000, node: 'b1' },
    ],
  },
  {
    schemaVersion: 1,
    name: 'acks=1 vs acks=all on leader crash',
    description:
      'Two topics share leader b1; its followers sit 25ms away. "clicks" uses acks=1: the leader acknowledges as soon as it has written the record, so whatever the followers had not fetched yet is lost when b1 crashes. "payments" uses acks=all: every acknowledged record is already on the ISR, so the new leader has it. Same crash, different durability.',
    durationMs: 30_000,
    regions: [{ id: 'az1' }, { id: 'az2' }],
    latency: { interRegionMs: 25 },
    clusters: [
      {
        id: 'main',
        controllers: ctl3('az2'),
        brokers: [
          { id: 'b1', region: 'az1' },
          { id: 'b2', region: 'az2' },
          { id: 'b3', region: 'az2' },
        ],
        config: { minInsyncReplicas: 2 },
      },
    ],
    topics: [
      { cluster: 'main', name: 'clicks', partitions: 1, replicas: [['b1', 'b2', 'b3']] },
      { cluster: 'main', name: 'payments', partitions: 1, replicas: [['b1', 'b2', 'b3']] },
    ],
    producers: [
      { id: 'web', cluster: 'main', region: 'az1', topic: 'clicks', acks: 1, ratePerSec: 400, lingerMs: 0, batchSize: 100 },
      { id: 'pay', cluster: 'main', region: 'az1', topic: 'payments', acks: 'all', ratePerSec: 400, lingerMs: 0, batchSize: 100 },
    ],
    actions: [{ type: 'KILL_NODE', at: 10_000, node: 'b1' }],
  },
  {
    schemaVersion: 1,
    name: 'min.insync.replicas: NOT_ENOUGH_REPLICAS',
    description:
      'RF=3, min.insync=2. After two brokers die the ISR shrinks to 1: acks=all producers get NOT_ENOUGH_REPLICAS (availability traded for durability) while the acks=1 producer keeps writing.',
    durationMs: 40_000,
    regions: [{ id: 'dc1' }],
    clusters: [{ id: 'main', controllers: ctl3('dc1'), brokers: brokers(['b1', 'b2', 'b3'], 'dc1'), config: { minInsyncReplicas: 2 } }],
    topics: [
      { cluster: 'main', name: 'ledger', partitions: 1, replicas: [['b1', 'b2', 'b3']] },
      { cluster: 'main', name: 'metrics', partitions: 1, replicas: [['b1', 'b2', 'b3']] },
    ],
    producers: [
      { id: 'bank', cluster: 'main', region: 'dc1', topic: 'ledger', acks: 'all', ratePerSec: 20 },
      { id: 'agent', cluster: 'main', region: 'dc1', topic: 'metrics', acks: 1, ratePerSec: 20 },
    ],
    actions: [
      { type: 'KILL_NODE', at: 8_000, node: 'b2' },
      { type: 'KILL_NODE', at: 10_000, node: 'b3' },
      { type: 'RESTART_NODE', at: 28_000, node: 'b2' },
    ],
  },
  {
    schemaVersion: 1,
    name: 'Zombie leader (SIGSTOP)',
    description:
      'Leader b1 is paused (GC pause / SIGSTOP) for 15s. The controller fences it and elects b2 with a new leader epoch. When b1 wakes up it still believes it leads. It first works through its socket buffer: it appends and acknowledges (acks=1) the produce request that was waiting there, then applies the new metadata, becomes a follower and truncates to the log of the new leader. That acknowledged batch is lost. Leader epochs keep the replicas consistent, but they cannot rescue an acks=1 write.',
    durationMs: 40_000,
    regions: [{ id: 'dc1' }],
    clusters: [{ id: 'main', controllers: ctl3('dc1'), brokers: brokers(['b1', 'b2', 'b3'], 'dc1') }],
    topics: [{ cluster: 'main', name: 'events', partitions: 1, replicas: [['b1', 'b2', 'b3']] }],
    producers: [{ id: 'svc', cluster: 'main', region: 'dc1', topic: 'events', acks: 1, ratePerSec: 50 }],
    consumers: [{ id: 'reader', cluster: 'main', region: 'dc1', group: 'g', topic: 'events' }],
    actions: [
      { type: 'PAUSE_NODE', at: 8_000, node: 'b1' },
      { type: 'RESUME_NODE', at: 23_000, node: 'b1' },
    ],
  },
  {
    schemaVersion: 1,
    name: 'Unclean leader election',
    description:
      'RF=2, min.insync=1, unclean.leader.election.enable=true. Follower b2 is cut off and drops out of the ISR. Then leader b1 dies: with no ISR member left the controller elects out-of-sync b2 and acknowledged records are lost. Set uncleanLeaderElection=false to see the partition stay offline instead.',
    durationMs: 45_000,
    regions: [{ id: 'a' }, { id: 'b' }],
    clusters: [
      {
        id: 'main',
        controllers: ctl3('a'),
        brokers: [
          { id: 'b1', region: 'a' },
          { id: 'b2', region: 'b' },
        ],
        config: { minInsyncReplicas: 1, uncleanLeaderElection: true, replicaLagTimeMaxMs: 10_000 },
      },
    ],
    topics: [{ cluster: 'main', name: 'audit', partitions: 1, replicas: [['b1', 'b2']] }],
    producers: [{ id: 'app', cluster: 'main', region: 'a', topic: 'audit', acks: 'all', ratePerSec: 20 }],
    actions: [
      { type: 'PARTITION', at: 5_000, groups: [['a'], ['b2']] },
      { type: 'KILL_NODE', at: 25_000, node: 'b1' },
      { type: 'HEAL', at: 26_000 },
    ],
  },
  {
    schemaVersion: 1,
    name: 'Stretch cluster: region partition',
    description:
      'One KRaft cluster across east/west plus a controller-only witness region (40ms WAN). acks=all produce latency includes the WAN round trip. At 15s west is cut off. East keeps quorum (2 of 3 controllers). acks=all writes stall until the west brokers are fenced and leave the ISR, then partition 0 resumes. Partition 1 was led by w1, so its in-flight request never gets a TCP reset and waits the full request.timeout.ms (30s) before it retries against the new leader e2. After the heal at 35s the west brokers catch up and rejoin.',
    durationMs: 55_000,
    regions: [
      { id: 'east', name: 'US-East' },
      { id: 'west', name: 'US-West' },
      { id: 'witness', name: 'Witness' },
    ],
    latency: { intraRegionMs: 1, interRegionMs: 40, links: [{ a: 'east', b: 'witness', ms: 15 }, { a: 'west', b: 'witness', ms: 30 }] },
    clusters: [
      {
        id: 'stretch',
        controllers: ctl3(['east', 'west', 'witness']),
        brokers: [
          { id: 'e1', region: 'east' },
          { id: 'e2', region: 'east' },
          { id: 'w1', region: 'west' },
          { id: 'w2', region: 'west' },
        ],
        config: { minInsyncReplicas: 2, replicaLagTimeMaxMs: 10_000 },
      },
    ],
    topics: [{ cluster: 'stretch', name: 'trades', partitions: 2, replicationFactor: 4 }],
    producers: [{ id: 'east-app', cluster: 'stretch', region: 'east', topic: 'trades', acks: 'all', ratePerSec: 30, lingerMs: 100 }],
    consumers: [{ id: 'west-risk', cluster: 'stretch', region: 'west', group: 'risk', topic: 'trades' }],
    actions: [
      { type: 'PARTITION', at: 15_000, groups: [['east', 'witness'], ['west']] },
      { type: 'HEAL', at: 35_000 },
    ],
  },
  {
    schemaVersion: 1,
    name: 'Cluster Linking DR (Confluent)',
    description:
      'Two independent clusters. DR brokers fetch byte-for-byte from the primary over the WAN (no Connect), so mirror offsets equal source offsets. All primary brokers die at 20s. At 25s the mirror is promoted: RPO is the acknowledged records the link had not copied yet.',
    requiresConfluent: true,
    durationMs: 40_000,
    regions: [
      { id: 'east', name: 'Primary (US-East)' },
      { id: 'west', name: 'DR (US-West)' },
    ],
    latency: { interRegionMs: 35 },
    clusters: [
      { id: 'primary', flavor: 'confluent', controllers: ctl3('east', 'pc'), brokers: brokers(['pb1', 'pb2', 'pb3'], 'east'), config: { minInsyncReplicas: 2 } },
      { id: 'dr', flavor: 'confluent', controllers: ctl3('west', 'dc'), brokers: brokers(['db1', 'db2', 'db3'], 'west'), config: { minInsyncReplicas: 2 } },
    ],
    topics: [
      { cluster: 'primary', name: 'orders', partitions: 2, replicationFactor: 3 },
      { cluster: 'dr', name: 'orders', partitions: 2, replicationFactor: 3, mirror: { type: 'cluster_link', sourceCluster: 'primary', sourceTopic: 'orders', fetchIntervalMs: 500 } },
    ],
    producers: [{ id: 'shop', cluster: 'primary', region: 'east', topic: 'orders', acks: 'all', ratePerSec: 50 }],
    actions: [
      { type: 'KILL_NODE', at: 20_000, node: 'pb1' },
      { type: 'KILL_NODE', at: 20_000, node: 'pb2' },
      { type: 'KILL_NODE', at: 20_000, node: 'pb3' },
      { type: 'FAILOVER', at: 25_000, cluster: 'dr', topic: 'orders' },
    ],
  },
  {
    schemaVersion: 1,
    name: 'MirrorMaker 2 DR (Apache)',
    description:
      'Apache Kafka DR: a MirrorMaker 2 Connect worker in the DR region consumes from the primary and produces to the DR cluster. Offsets on the DR topic are not the source offsets, so consumer offsets have to be translated through checkpoints. Compare its RPO with the Cluster Linking scenario.',
    durationMs: 40_000,
    regions: [
      { id: 'east', name: 'Primary (US-East)' },
      { id: 'west', name: 'DR (US-West)' },
    ],
    latency: { interRegionMs: 35 },
    clusters: [
      { id: 'primary', controllers: ctl3('east', 'pc'), brokers: brokers(['pb1', 'pb2', 'pb3'], 'east'), config: { minInsyncReplicas: 2 } },
      { id: 'dr', controllers: ctl3('west', 'dc'), brokers: brokers(['db1', 'db2', 'db3'], 'west'), config: { minInsyncReplicas: 2 } },
    ],
    topics: [
      { cluster: 'primary', name: 'orders', partitions: 2, replicationFactor: 3 },
      { cluster: 'dr', name: 'primary.orders', partitions: 2, replicationFactor: 3, mirror: { type: 'mm2', sourceCluster: 'primary', sourceTopic: 'orders', workerRegion: 'west', fetchIntervalMs: 500 } },
    ],
    producers: [{ id: 'shop', cluster: 'primary', region: 'east', topic: 'orders', acks: 'all', ratePerSec: 50 }],
    actions: [
      { type: 'KILL_NODE', at: 20_000, node: 'pb1' },
      { type: 'KILL_NODE', at: 20_000, node: 'pb2' },
      { type: 'KILL_NODE', at: 20_000, node: 'pb3' },
      { type: 'FAILOVER', at: 25_000, cluster: 'dr', topic: 'primary.orders' },
    ],
  },
  {
    schemaVersion: 1,
    name: 'MRC observers + promotion (Confluent)',
    description:
      'Confluent Multi-Region Cluster: sync replicas in east, async observers in west (outside the ISR, so they add no WAN latency to acks=all). When east loses brokers and the ISR drops below min.insync, observerPromotionPolicy=under-min-isr adds caught-up observers to the ISR so writes continue.',
    requiresConfluent: true,
    durationMs: 45_000,
    regions: [
      { id: 'east', name: 'US-East' },
      { id: 'west', name: 'US-West' },
      { id: 'witness', name: 'Witness' },
    ],
    latency: { interRegionMs: 40 },
    clusters: [
      {
        id: 'mrc',
        flavor: 'confluent',
        controllers: ctl3(['east', 'west', 'witness']),
        brokers: [
          { id: 'e1', region: 'east' },
          { id: 'e2', region: 'east' },
          { id: 'w1', region: 'west' },
          { id: 'w2', region: 'west' },
        ],
        config: { minInsyncReplicas: 2 },
      },
    ],
    topics: [
      {
        cluster: 'mrc',
        name: 'payments',
        partitions: 1,
        replicas: [['e1', 'e2']],
        observers: [['w1', 'w2']],
        observerPromotionPolicy: 'under-min-isr',
      },
    ],
    producers: [{ id: 'pos', cluster: 'mrc', region: 'east', topic: 'payments', acks: 'all', ratePerSec: 30 }],
    actions: [
      { type: 'KILL_NODE', at: 12_000, node: 'e2' },
      { type: 'RESTART_NODE', at: 32_000, node: 'e2' },
    ],
  },
  {
    schemaVersion: 1,
    name: 'CFK: AZ outage, zone-locked PVCs, Self-Balancing heal',
    description:
      'CFK on Kubernetes, 3 AZs, rack awareness on (broker.rack = zone), RF=3 so every partition has a replica in each AZ. us-east-1a fails at 10s. Its broker pods cannot move: their EBS-style PVCs are zone-locked, and the StatefulSet will not replace pods on an unreachable node anyway. Partitions stay online on the other two AZs, but under-replicated. After 30s (the default is 1h) Self-Balancing moves the dead brokers\' replicas onto healthy brokers, restoring RF=3, though now in only 2 AZs. The zone returns at 120s.',
    requiresConfluent: true,
    durationMs: 150_000,
    regions: azs,
    latency: azLatency,
    clusters: [cfk({ selfBalancingMs: 30_000 })],
    topics: [{ cluster: 'cfk', name: 'orders', partitions: 6, replicationFactor: 3 }],
    producers: [{ id: 'checkout', cluster: 'cfk', region: 'az2', topic: 'orders', acks: 'all', ratePerSec: 40 }],
    consumers: [{ id: 'fulfil', cluster: 'cfk', region: 'az3', group: 'fulfil', topic: 'orders' }],
    actions: [
      { type: 'ZONE_OUTAGE', at: 10_000, zone: 'az1' },
      { type: 'ZONE_RECOVER', at: 120_000, zone: 'az1' },
    ],
  },
  {
    schemaVersion: 1,
    name: 'CFK: broker container crash self-heals',
    description:
      'Broker b1 crashes three times. Kubernetes heals each crash with no human involved. The first time, kubelet restarts the container at once; the pod then needs ~20s (JVM start, log recovery) before the broker is back and catching up into the ISR. Repeated crashes put the pod in CrashLoopBackOff: 10s, then 20s waits before the next restart. The PVC stays attached, so the log survives and catch-up is incremental.',
    requiresConfluent: true,
    durationMs: 130_000,
    regions: azs,
    latency: azLatency,
    clusters: [cfk()],
    topics: [{ cluster: 'cfk', name: 'events', partitions: 3, replicationFactor: 3 }],
    producers: [{ id: 'svc', cluster: 'cfk', region: 'az1', topic: 'events', acks: 'all', ratePerSec: 40 }],
    actions: [
      { type: 'CONTAINER_CRASH', at: 10_000, pod: 'b1' },
      { type: 'CONTAINER_CRASH', at: 50_000, pod: 'b1' },
      { type: 'CONTAINER_CRASH', at: 85_000, pod: 'b1' },
    ],
  },
  {
    schemaVersion: 1,
    name: 'Kubernetes: rack awareness off + AZ outage',
    description:
      'The same 3-AZ cluster, RF=2, but without rack awareness: replica placement ignores zones, so some partitions have both replicas in us-east-1a. When that zone fails, those partitions go OFFLINE (no eligible leader), while partitions that happened to span zones survive. Turn rack awareness on in the builder to see every partition survive. The adaptive partitioner keeps keyless throughput up by routing around the dead partitions, but anything keyed to them would be stuck.',
    requiresConfluent: true,
    durationMs: 60_000,
    regions: azs,
    latency: azLatency,
    clusters: [cfk({ rackAwareness: false })],
    topics: [{ cluster: 'cfk', name: 'orders', partitions: 6, replicationFactor: 2, minInsyncReplicas: 1 }],
    producers: [{ id: 'checkout', cluster: 'cfk', region: 'az2', topic: 'orders', acks: 'all', ratePerSec: 40 }],
    actions: [{ type: 'ZONE_OUTAGE', at: 10_000, zone: 'az1' }],
  },
  {
    schemaVersion: 1,
    name: 'Kubernetes: drain into a full zone, broker stuck Pending',
    description:
      'us-east-1c has exactly three workers, one per Kafka pod, and no spare. At 10s the worker hosting b5 is cordoned and drained. Pod b5 gets a controlled shutdown: leadership moves first, so no unavailability. The StatefulSet recreates the pod, but its PVC is bound to us-east-1c and the only other worker there is full, so b5 sits Pending and its partitions stay under-replicated. Uncordoning at 60s lets b5 back onto its old node; it catches up and rejoins the ISR.',
    requiresConfluent: true,
    durationMs: 100_000,
    regions: azs,
    latency: azLatency,
    clusters: [cfk({ workersPerZone: [4, 4, 3] })],
    topics: [{ cluster: 'cfk', name: 'payments', partitions: 3, replicationFactor: 3 }],
    producers: [{ id: 'pos', cluster: 'cfk', region: 'az1', topic: 'payments', acks: 'all', ratePerSec: 30 }],
    actions: [
      { type: 'K8S_CORDON', at: 10_000, target: 'w-az3-2' },
      { type: 'POD_DELETE', at: 10_500, pod: 'b5' },
      { type: 'K8S_UNCORDON', at: 60_000, target: 'w-az3-2' },
    ],
  },
  {
    schemaVersion: 1,
    name: 'Multi-region CFK: 2 regions × 3 AZs, region outage + Cluster Linking failover',
    description:
      'Two CFK clusters, one per region, each spread over 3 availability zones: about 1ms between AZs, 65ms between regions. Inside each region RF=3 with rack awareness keeps a replica in every AZ, so a single-AZ failure (us-east-1a at 15s) costs nothing but redundancy. us-west-2 mirrors orders over a cluster link. At 40s all of us-east-1 goes dark. At 45s the mirror is promoted, and RPO is whatever the link had not copied in the last round trip.',
    requiresConfluent: true,
    durationMs: 70_000,
    regions: [
      { id: 'us-east-1', zones: [{ id: 'use1-az1', name: 'us-east-1a' }, { id: 'use1-az2', name: 'us-east-1b' }, { id: 'use1-az3', name: 'us-east-1c' }] },
      { id: 'us-west-2', zones: [{ id: 'usw2-az1', name: 'us-west-2a' }, { id: 'usw2-az2', name: 'us-west-2b' }, { id: 'usw2-az3', name: 'us-west-2c' }] },
    ],
    latency: { intraRegionMs: 0.4, interZoneMs: 1.2, interRegionMs: 65 },
    clusters: [cfk({ id: 'east', zones: ['use1-az1', 'use1-az2', 'use1-az3'], prefix: 'e' }), cfk({ id: 'west', zones: ['usw2-az1', 'usw2-az2', 'usw2-az3'], prefix: 'w' })],
    topics: [
      { cluster: 'east', name: 'orders', partitions: 3, replicationFactor: 3 },
      { cluster: 'west', name: 'orders', partitions: 3, replicationFactor: 3, mirror: { type: 'cluster_link', sourceCluster: 'east', sourceTopic: 'orders', fetchIntervalMs: 500 } },
    ],
    producers: [{ id: 'shop', cluster: 'east', region: 'use1-az2', topic: 'orders', acks: 'all', ratePerSec: 50 }],
    consumers: [{ id: 'analytics', cluster: 'east', region: 'use1-az3', group: 'analytics', topic: 'orders' }],
    actions: [
      { type: 'ZONE_OUTAGE', at: 15_000, zone: 'use1-az1' },
      { type: 'ZONE_OUTAGE', at: 40_000, zone: 'us-east-1' },
      { type: 'FAILOVER', at: 45_000, cluster: 'west', topic: 'orders' },
    ],
  },
];
