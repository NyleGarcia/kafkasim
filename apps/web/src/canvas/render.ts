import type { MsgKind, PartitionView, SimEvent, ViewState } from '@kafkasim/engine';

import type { Layout, NodeBox } from './layout';

type Msg = Extract<SimEvent, { type: 'MSG' }>;

export interface RenderOpts {
  t: number;
  /** sim-ms shown per message flight minimum, so 1ms LAN hops stay visible */
  visualWindowMs: number;
  hover: string | null;
  highlightTp: string | null;
  showControl: boolean;
  reducedMotion: boolean;
}

const C = {
  bg: '#020617',
  text: '#e2e8f0',
  muted: '#8b9ab3',
  border: '#1e2a44',
  nominal: '#22c55e',
  warning: '#eab308',
  critical: '#ef4444',
  info: '#38bdf8',
  observer: '#a78bfa',
};

const MSG_COLOR: Record<MsgKind, string> = {
  produce: '#38bdf8',
  produce_resp: '#7dd3fc',
  fetch: '#22c55e',
  fetch_resp: '#4ade80',
  heartbeat: '#64748b',
  metadata: '#eab308',
  vote: '#a78bfa',
  link_fetch: '#22d3ee',
  consume: '#2dd4bf',
};
const CONTROL: Set<MsgKind> = new Set(['heartbeat', 'vote', 'metadata']);

export function render(ctx: CanvasRenderingContext2D, L: Layout, v: ViewState, msgs: Msg[], o: RenderOpts) {
  ctx.clearRect(0, 0, L.width, L.height);
  drawRegions(ctx, L, v);
  for (const c of L.clusters) drawCluster(ctx, c, v);
  const hl = o.highlightTp ? v.partitions[o.highlightTp] : undefined;
  if (hl) drawReplicaLinks(ctx, L, hl);
  drawMessages(ctx, L, msgs, o);
  for (const n of L.nodes.values()) drawNode(ctx, n, v, o, hl);
}

function drawRegions(ctx: CanvasRenderingContext2D, L: Layout, v: ViewState) {
  for (const r of L.regions) {
    const cut = isCut(v, r.id);
    ctx.fillStyle = cut ? 'rgba(239,68,68,0.06)' : 'rgba(148,163,184,0.03)';
    roundRect(ctx, r.x - 6, 4, r.w + 12, L.height - 8, 10);
    ctx.fill();
    ctx.strokeStyle = cut ? 'rgba(239,68,68,0.5)' : 'rgba(148,163,184,0.12)';
    ctx.setLineDash(cut ? [6, 4] : []);
    ctx.lineWidth = 1;
    ctx.stroke();
    ctx.setLineDash([]);
    ctx.fillStyle = cut ? C.critical : C.muted;
    ctx.font = '600 11px "Inter Variable", sans-serif';
    ctx.textBaseline = 'middle';
    ctx.textAlign = 'left';
    ctx.fillText(r.name.toUpperCase(), r.x + 4, 20);
    if (cut) {
      ctx.textAlign = 'right';
      ctx.fillText('PARTITIONED', r.x + r.w - 4, 20);
    }
  }
}

/** A region is "cut" when the network is partitioned and it is not in group 0. */
function isCut(v: ViewState, region: string) {
  if (!v.partitioned || !v.groups) return false;
  return v.groups.slice(1).some((g) => g.includes(region));
}

function drawCluster(ctx: CanvasRenderingContext2D, c: Layout['clusters'][number], v: ViewState) {
  ctx.strokeStyle = `hsla(${c.hue}, 80%, 65%, 0.35)`;
  ctx.fillStyle = `hsla(${c.hue}, 60%, 40%, 0.06)`;
  roundRect(ctx, c.x, c.y, c.w, c.h, 8);
  ctx.fill();
  ctx.stroke();
  ctx.fillStyle = `hsla(${c.hue}, 85%, 72%, 0.9)`;
  ctx.font = '600 10px "JetBrains Mono Variable", monospace';
  ctx.textAlign = 'left';
  ctx.textBaseline = 'middle';
  ctx.fillText(c.cluster, c.x + 8, c.y + 11);
  const ctl = v.controllers[c.cluster];
  ctx.textAlign = 'right';
  ctx.fillStyle = ctl?.node ? C.muted : C.critical;
  const narrow = c.w < 200;
  ctx.fillText(ctl?.node ? (narrow ? `e${ctl.epoch}` : `kraft epoch ${ctl.epoch}`) : narrow ? 'NO CTRL' : 'NO ACTIVE CONTROLLER', c.x + c.w - 8, c.y + 11);
  if (c.flavor === 'confluent') {
    ctx.fillStyle = C.observer;
    ctx.textAlign = 'left';
    ctx.fillText('CP', c.x + 8 + ctx.measureText(c.cluster).width + 8, c.y + 11);
  }
}

function drawReplicaLinks(ctx: CanvasRenderingContext2D, L: Layout, p: PartitionView) {
  const leader = p.leader ? L.nodes.get(p.leader) : undefined;
  if (!leader) return;
  for (const r of [...p.replicas, ...p.observers]) {
    if (r === p.leader) continue;
    const n = L.nodes.get(r);
    if (!n) continue;
    const inIsr = p.isr.includes(r);
    ctx.strokeStyle = inIsr ? 'rgba(34,197,94,0.55)' : p.observers.includes(r) ? 'rgba(167,139,250,0.5)' : 'rgba(234,179,8,0.55)';
    ctx.setLineDash(inIsr ? [] : [4, 4]);
    ctx.lineWidth = 1.5;
    ctx.beginPath();
    ctx.moveTo(leader.x, leader.y);
    ctx.lineTo(n.x, n.y);
    ctx.stroke();
  }
  ctx.setLineDash([]);
}

function drawMessages(ctx: CanvasRenderingContext2D, L: Layout, msgs: Msg[], o: RenderOpts) {
  let drawn = 0;
  for (const m of msgs) {
    if (!o.showControl && CONTROL.has(m.kind)) continue;
    const a = L.nodes.get(m.from);
    const b = L.nodes.get(m.to);
    if (!a || !b) continue;
    const flight = Math.max(m.arriveT - m.t, o.visualWindowMs);
    const k = (o.t - m.t) / flight;
    if (k < 0 || k > 1) continue;
    if (m.dropped) {
      if (k > 0.35) continue;
      const x = a.x + (b.x - a.x) * k;
      const y = a.y + (b.y - a.y) * k;
      ctx.strokeStyle = C.critical;
      ctx.lineWidth = 1.5;
      ctx.beginPath();
      ctx.moveTo(x - 3, y - 3);
      ctx.lineTo(x + 3, y + 3);
      ctx.moveTo(x + 3, y - 3);
      ctx.lineTo(x - 3, y + 3);
      ctx.stroke();
      continue;
    }
    const e = o.reducedMotion ? 0.5 : easeInOut(k);
    // slight arc so request/response pairs don't overlap
    const mx = (a.x + b.x) / 2 + (b.y - a.y) * 0.08;
    const my = (a.y + b.y) / 2 - (b.x - a.x) * 0.08;
    const x = (1 - e) * (1 - e) * a.x + 2 * (1 - e) * e * mx + e * e * b.x;
    const y = (1 - e) * (1 - e) * a.y + 2 * (1 - e) * e * my + e * e * b.y;
    const size = m.records > 0 ? Math.min(5, 2 + Math.log2(1 + m.records) * 0.6) : 1.6;
    ctx.globalAlpha = m.records > 0 || CONTROL.has(m.kind) || m.kind.startsWith('produce') ? 0.95 : 0.35;
    ctx.fillStyle = MSG_COLOR[m.kind];
    ctx.beginPath();
    ctx.arc(x, y, size, 0, Math.PI * 2);
    ctx.fill();
    ctx.globalAlpha = 1;
    if (++drawn > 1500) break;
  }
}

function drawWorker(ctx: CanvasRenderingContext2D, n: NodeBox, v: ViewState, o: RenderOpts) {
  const w = v.workers[n.id];
  if (!w) return;
  const pods = Object.values(v.nodes).filter((x) => x.pod?.worker === n.id);
  const tone = !w.ready ? C.critical : !w.schedulable ? C.warning : C.muted;
  const wdt = n.r * 3.2;
  const h = n.r * 1.5;
  ctx.save();
  ctx.lineWidth = o.hover === n.id ? 2 : 1;
  ctx.strokeStyle = tone;
  ctx.fillStyle = !w.ready ? 'rgba(239,68,68,0.10)' : '#0b1222';
  ctx.setLineDash(w.schedulable ? [] : [4, 3]);
  roundRect(ctx, n.x - wdt / 2, n.y - h / 2, wdt, h, 4);
  ctx.fill();
  ctx.stroke();
  ctx.setLineDash([]);
  ctx.font = '9px "JetBrains Mono Variable", monospace';
  ctx.textAlign = 'center';
  ctx.textBaseline = 'middle';
  ctx.fillStyle = tone === C.muted ? C.text : tone;
  ctx.fillText(n.id.replace(/^w-/, ''), n.x, n.y - 4);
  ctx.fillStyle = C.muted;
  ctx.fillText(pods.length ? pods.map((p) => p.id).join(' ') : 'empty', n.x, n.y + 6);
  ctx.fillStyle = tone;
  const sub = !w.ready ? 'NotReady' : !w.schedulable ? 'cordoned' : '';
  if (sub) ctx.fillText(sub, n.x, n.y + h / 2 + 8);
  ctx.restore();
}

const POD_TONE: Record<string, string> = {
  Pending: C.warning,
  ContainerCreating: C.info,
  CrashLoopBackOff: C.critical,
  Terminating: C.warning,
  Unknown: C.critical,
};

function drawNode(ctx: CanvasRenderingContext2D, n: NodeBox, v: ViewState, o: RenderOpts, hl?: PartitionView) {
  if (n.kind === 'worker') return drawWorker(ctx, n, v, o);
  const nv = v.nodes[n.id];
  if (!nv) return;
  const status = nv.status;
  const base = status === 'dead' ? C.critical : status === 'paused' ? C.warning : nv.fenced ? C.warning : C.nominal;
  const hovered = o.hover === n.id;
  const isLeaderHl = hl && hl.leader === n.id;
  const inHl = hl && ([...hl.replicas, ...hl.observers].includes(n.id));

  ctx.save();
  if (hl && !inHl && (n.kind === 'broker')) ctx.globalAlpha = 0.35;
  ctx.lineWidth = hovered ? 2.5 : 1.5;
  ctx.strokeStyle = base;
  ctx.fillStyle = status === 'dead' ? 'rgba(239,68,68,0.12)' : '#0b1222';

  switch (n.kind) {
    case 'controller': {
      hexagon(ctx, n.x, n.y, n.r);
      ctx.fill();
      if (nv.activeController) {
        ctx.shadowColor = C.observer;
        ctx.shadowBlur = 12;
        ctx.strokeStyle = C.observer;
      }
      ctx.stroke();
      ctx.shadowBlur = 0;
      break;
    }
    case 'broker': {
      roundRect(ctx, n.x - n.r, n.y - n.r, n.r * 2, n.r * 2, 6);
      ctx.fill();
      if (nv.fenced && status !== 'dead') ctx.setLineDash([3, 3]);
      if (isLeaderHl) {
        ctx.shadowColor = C.info;
        ctx.shadowBlur = 14;
        ctx.strokeStyle = C.info;
      }
      ctx.stroke();
      ctx.setLineDash([]);
      ctx.shadowBlur = 0;
      break;
    }
    default: {
      ctx.beginPath();
      ctx.arc(n.x, n.y, n.r, 0, Math.PI * 2);
      ctx.fill();
      ctx.strokeStyle = status === 'running' ? (n.kind === 'producer' ? C.info : n.kind === 'mm2' ? C.observer : '#2dd4bf') : base;
      ctx.stroke();
    }
  }

  const isClient = n.kind === 'producer' || n.kind === 'consumer' || n.kind === 'mm2';
  ctx.fillStyle = status === 'dead' ? C.critical : C.text;
  ctx.font = `600 ${n.kind === 'broker' ? 11 : 9.5}px "JetBrains Mono Variable", monospace`;
  ctx.textAlign = 'center';
  ctx.textBaseline = 'middle';
  if (isClient) {
    // glyph inside, id underneath (ids are longer than the circle)
    ctx.fillStyle = ctx.strokeStyle as string;
    ctx.fillText(n.kind === 'producer' ? 'P' : n.kind === 'consumer' ? 'C' : 'M', n.x, n.y);
    ctx.fillStyle = status === 'dead' ? C.critical : C.text;
    ctx.fillText(n.id.length > 16 ? n.id.slice(0, 15) + '…' : n.id, n.x, n.y + n.r + 10);
  } else ctx.fillText(n.id.length > 8 ? n.id.slice(0, 7) + '…' : n.id, n.x, n.y);

  // sub-label
  ctx.font = '9px "JetBrains Mono Variable", monospace';
  ctx.fillStyle = C.muted;
  let sub = '';
  const phase = nv.pod && nv.pod.phase !== 'Running' ? nv.pod.phase : null;
  if (phase) sub = phase === 'ContainerCreating' ? 'STARTING' : phase === 'CrashLoopBackOff' ? 'CRASHLOOP' : phase.toUpperCase();
  else if (status !== 'running') sub = status === 'dead' ? 'DEAD' : 'PAUSED';
  else if (nv.fenced) sub = 'FENCED';
  else if (n.kind === 'broker') {
    const led = Object.values(v.partitions).filter((p) => p.leader === n.id).length;
    sub = led ? `leads ${led}` : '';
  } else if (n.kind === 'controller') sub = nv.activeController ? 'ACTIVE' : '';
  if (sub) {
    ctx.fillStyle = phase ? POD_TONE[phase]! : status === 'dead' ? C.critical : status === 'paused' || nv.fenced ? C.warning : nv.activeController ? C.observer : C.muted;
    ctx.fillText(sub, n.x, n.y + n.r + (isClient ? 21 : 10));
  }
  if (hl && inHl && n.kind === 'broker') {
    const leo = hl.leo[n.id] ?? 0;
    ctx.fillStyle = hl.isr.includes(n.id) ? C.nominal : hl.observers.includes(n.id) ? C.observer : C.warning;
    ctx.fillText(`LEO ${leo}`, n.x, n.y - n.r - 8);
  }
  ctx.restore();
}

function easeInOut(k: number) {
  return k < 0.5 ? 2 * k * k : 1 - (-2 * k + 2) ** 2 / 2;
}

function roundRect(ctx: CanvasRenderingContext2D, x: number, y: number, w: number, h: number, r: number) {
  ctx.beginPath();
  ctx.roundRect(x, y, w, h, r);
}

function hexagon(ctx: CanvasRenderingContext2D, x: number, y: number, r: number) {
  ctx.beginPath();
  for (let i = 0; i < 6; i++) {
    const a = (Math.PI / 3) * i + Math.PI / 6;
    const px = x + r * Math.cos(a);
    const py = y + r * Math.sin(a);
    if (i) ctx.lineTo(px, py);
    else ctx.moveTo(px, py);
  }
  ctx.closePath();
}

/** Messages possibly visible at t: sent within the visual window or still in flight. */
export function visibleMessages(msgs: Msg[], t: number, windowMs: number, maxFlightMs = 2_000): Msg[] {
  const start = t - Math.max(windowMs, maxFlightMs);
  let lo = 0,
    hi = msgs.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (msgs[mid]!.t < start) lo = mid + 1;
    else hi = mid;
  }
  const out: Msg[] = [];
  for (let i = lo; i < msgs.length && msgs[i]!.t <= t; i++) {
    const m = msgs[i]!;
    if (t - m.t <= Math.max(m.arriveT - m.t, windowMs)) out.push(m);
  }
  return out;
}
