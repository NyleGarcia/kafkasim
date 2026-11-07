import type { Manifest } from '@kafkasim/engine';

export type NodeKind = 'broker' | 'controller' | 'producer' | 'consumer' | 'mm2';

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

export interface RegionCol {
  id: string;
  name: string;
  x: number;
  w: number;
}

export interface Layout {
  width: number;
  height: number;
  regions: RegionCol[];
  clusters: ClusterBox[];
  nodes: Map<string, NodeBox>;
}

const PAD = 16;
const HEADER = 34;
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
  const regions = m.regions;
  const colW = (width - PAD * (regions.length + 1)) / regions.length;
  const cols: RegionCol[] = regions.map((r, i) => ({ id: r.id, name: r.name ?? r.id, x: PAD + i * (colW + PAD), w: colW }));
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
    let y = HEADER + PAD;
    for (const c of m.clusters) {
      const ctl = c.controllers.filter((x) => x.region === col.id);
      const brk = c.brokers.filter((x) => x.region === col.id);
      const cli = clients.get(`${c.id}|${col.id}`) ?? [];
      // clients of a cluster in a region where it has no servers still need a block
      if (!ctl.length && !brk.length && !cli.length) continue;
      const rows = [ctl, brk, cli].filter((r) => r.length);
      const h = CLUSTER_HEADER + rows.length * ROW + 6;
      clusters.push({ cluster: c.id, region: col.id, x: col.x, y, w: col.w, h, hue: hueOf.get(c.id)!, flavor: c.flavor });
      let ry = y + CLUSTER_HEADER + ROW / 2;
      const place = (items: { id: string }[], kind: NodeKind | ((id: string) => NodeKind)) => {
        if (!items.length) return;
        const step = col.w / (items.length + 1);
        const r = Math.min(20, step / 2.6);
        items.forEach((it, i) => {
          const k = typeof kind === 'function' ? kind(it.id) : kind;
          nodes.set(it.id, { id: it.id, kind: k, cluster: c.id, region: col.id, x: col.x + step * (i + 1), y: ry, r: k === 'controller' ? r * 0.8 : k === 'broker' ? r : r * 0.75 });
        });
        ry += ROW;
      };
      place(ctl, 'controller');
      place(brk, 'broker');
      const kinds = new Map(cli.map((x) => [x.id, x.kind]));
      place(cli, (id) => kinds.get(id)!);
      y += h + CLUSTER_GAP;
    }
    maxY = Math.max(maxY, y);
  }
  return { width, height: Math.max(maxY + PAD, 260), regions: cols, clusters, nodes };
}

export function hitTest(layout: Layout, x: number, y: number): NodeBox | null {
  for (const n of layout.nodes.values()) if ((n.x - x) ** 2 + (n.y - y) ** 2 <= (n.r + 4) ** 2) return n;
  return null;
}

export function regionAt(layout: Layout, x: number, y: number): string | null {
  if (y > HEADER) return null;
  return layout.regions.find((r) => x >= r.x && x <= r.x + r.w)?.id ?? null;
}
