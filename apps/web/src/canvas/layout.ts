import { locationsOf, type Manifest } from '@kafkasim/engine';

export type NodeKind = 'broker' | 'controller' | 'producer' | 'consumer' | 'mm2' | 'worker';

export interface NodeBox {
  id: string;
  kind: NodeKind;
  cluster: string;
  region: string;
  x: number;
  y: number;
  r: number;
}

export interface ClusterBox {
  cluster: string;
  region: string;
  x: number;
  y: number;
  w: number;
  h: number;
  hue: number;
  flavor: 'apache' | 'confluent';
}

/** One column per placement location (an AZ, or a zoneless region). */
export interface RegionCol {
  id: string;
  name: string;
  region: string;
  x: number;
  w: number;
}

/** A region band spanning its zone columns (only drawn when the region has zones). */
export interface RegionBand {
  id: string;
  name: string;
  x: number;
  w: number;
  zoned: boolean;
}

export interface Layout {
  width: number;
  height: number;
  regions: RegionCol[];
  bands: RegionBand[];
  /** y where cluster blocks start (taller when zones need a second header row) */
  header: number;
  clusters: ClusterBox[];
  nodes: Map<string, NodeBox>;
}

const PAD = 16;
const HEADER = 34;
const ZONED_HEADER = 58;
const ZONE_GAP = 8;
const MIN_COL = 132;
const INSET = 8;
/** horizontal slot per node kind (labels under clients/workers need more room) */
const PITCH = { controller: 44, broker: 44, client: 72, worker: 60 } as const;
const RADIUS: Record<NodeKind, number> = { controller: 14, broker: 17, producer: 13, consumer: 13, mm2: 13, worker: 16 };
const ROW = 64;
const CLUSTER_HEADER = 22;
const CLUSTER_GAP = 14;

/**
 * Columns = regions. In each column, a block per cluster that has nodes there
 * (a stretch cluster gets one block per region, same hue). Rows inside a block:
 * controllers · brokers · clients. Pure function of manifest + size (retained mode:
 * recomputed only on resize / manifest change).
 */
export function computeLayout(m: Manifest, width: number): Layout {
  const locs = locationsOf(m);
  const zoned = m.regions.some((r) => r.zones?.length);
  const HEADER_H = zoned ? ZONED_HEADER : HEADER;
  // gaps: PAD between regions, a smaller gap between zones of the same region
  const gaps = locs.reduce((a, l, i) => a + (i === 0 ? 0 : locs[i - 1]!.region === l.region ? ZONE_GAP : PAD * 1.5), 0);
  // never squeeze a column below MIN_COL: the canvas grows and its container scrolls instead
  const colW = Math.max(MIN_COL, (width - PAD * 2 - gaps) / locs.length);
  const cols: RegionCol[] = [];
  let cx = PAD;
  locs.forEach((l, i) => {
    if (i > 0) cx += locs[i - 1]!.region === l.region ? ZONE_GAP : PAD * 1.5;
    cols.push({ id: l.id, name: l.name, region: l.region, x: cx, w: colW });
    cx += colW;
  });
  const bands: RegionBand[] = m.regions.map((r) => {
    const mine = cols.filter((c) => c.region === r.id);
    const x0 = mine[0]!.x;
    const x1 = mine.at(-1)!.x + mine.at(-1)!.w;
    return { id: r.id, name: r.name ?? r.id, x: x0, w: x1 - x0, zoned: !!r.zones?.length };
  });
  const nodes = new Map<string, NodeBox>();
  const clusters: ClusterBox[] = [];
  const hueOf = new Map(m.clusters.map((c, i) => [c.id, [199, 262, 152, 32, 330][i % 5]!]));

  const clients = new Map<string, { id: string; kind: NodeKind; cluster: string; region: string }[]>();
  const pushClient = (id: string, kind: NodeKind, cluster: string, region: string) => {
    const k = `${cluster}|${region}`;
    clients.set(k, [...(clients.get(k) ?? []), { id, kind, cluster, region }]);
  };
  for (const p of m.producers) pushClient(p.id, 'producer', p.cluster, p.region);
  for (const c of m.consumers) pushClient(c.id, 'consumer', c.cluster, c.region);
  for (const t of m.topics)
    if (t.mirror?.type === 'mm2') {
      const c = m.clusters.find((x) => x.id === t.cluster)!;
      pushClient(`mm2-${t.cluster}-${t.name}`, 'mm2', t.cluster, t.mirror.workerRegion ?? c.brokers[0]!.region);
    }

  let maxY = 0;
  for (const col of cols) {
    let y = HEADER_H + PAD;
    for (const c of m.clusters) {
      const ctl = c.controllers.filter((x) => x.region === col.id);
      const brk = c.brokers.filter((x) => x.region === col.id);
      const cli = clients.get(`${c.id}|${col.id}`) ?? [];
      const wrk = c.platform?.workers.filter((w) => w.zone === col.id) ?? [];
      // clients of a cluster in a region where it has no servers still need a block
      if (!ctl.length && !brk.length && !cli.length && !wrk.length) continue;
      // Fixed-pitch, left-aligned grid that wraps: appending a node never moves the existing ones.
      const perRow = (pitch: number) => Math.max(1, Math.floor((col.w - 2 * INSET) / pitch));
      const lines = (n: number, pitch: number) => (n ? Math.ceil(n / perRow(pitch)) : 0);
      const rowCount = lines(ctl.length, PITCH.controller) + lines(brk.length, PITCH.broker) + lines(cli.length, PITCH.client) + lines(wrk.length, PITCH.worker);
      const h = CLUSTER_HEADER + rowCount * ROW + 6;
      clusters.push({ cluster: c.id, region: col.id, x: col.x, y, w: col.w, h, hue: hueOf.get(c.id)!, flavor: c.flavor });
      let ry = y + CLUSTER_HEADER + ROW / 2;
      const place = (items: { id: string }[], kind: NodeKind | ((id: string) => NodeKind), pitch: number) => {
        if (!items.length) return;
        const per = perRow(pitch);
        items.forEach((it, i) => {
          const k = typeof kind === 'function' ? kind(it.id) : kind;
          const x = col.x + INSET + pitch / 2 + (i % per) * pitch;
          const yy = ry + Math.floor(i / per) * ROW;
          nodes.set(it.id, { id: it.id, kind: k, cluster: c.id, region: col.id, x, y: yy, r: RADIUS[k] });
        });
        ry += Math.ceil(items.length / per) * ROW;
      };
      place(ctl, 'controller', PITCH.controller);
      place(brk, 'broker', PITCH.broker);
      const kinds = new Map(cli.map((x) => [x.id, x.kind]));
      place(cli, (id) => kinds.get(id)!, PITCH.client);
      place(wrk, 'worker', PITCH.worker);
      y += h + CLUSTER_GAP;
    }
    maxY = Math.max(maxY, y);
  }
  return { width: Math.max(width, cx + PAD), height: Math.max(maxY + PAD, 260), regions: cols, bands, header: HEADER_H, clusters, nodes };
}

export function hitTest(layout: Layout, x: number, y: number): NodeBox | null {
  for (const n of layout.nodes.values()) if ((n.x - x) ** 2 + (n.y - y) ** 2 <= (n.r + 4) ** 2) return n;
  return null;
}

/** Click on a header: the region band (top row) → region id; the zone row → zone id. */
export function regionAt(layout: Layout, x: number, y: number): string | null {
  if (y > layout.header) return null;
  const zonedHeader = layout.header === ZONED_HEADER;
  if (zonedHeader && y < 28) {
    const b = layout.bands.find((r) => x >= r.x && x <= r.x + r.w);
    if (b) return b.id;
  }
  return layout.regions.find((r) => x >= r.x && x <= r.x + r.w)?.id ?? null;
}
