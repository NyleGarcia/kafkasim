import { viewAt, type Manifest, type Timeline } from '@kafkasim/engine';
import { useEffect, useLayoutEffect, useMemo, useRef, useState, type RefObject } from 'react';

import { computeLayout, hitTest, regionAt, type Layout, type NodeBox } from '@/canvas/layout';
import { render, visibleMessages } from '@/canvas/render';

interface Props {
  manifest: Manifest;
  timeline: Timeline;
  tRef: RefObject<number>;
  speed: number;
  highlightTp: string | null;
  showControl: boolean;
  onNodeClick: (node: NodeBox, at: { x: number; y: number }) => void;
  onRegionClick: (region: string, at: { x: number; y: number }) => void;
}

/**
 * Retained-mode canvas: layout recomputed on resize/manifest only; each frame
 * reduces the view at tRef (keyframe + ≤500ms of events) and paints.
 */
export function TopologyCanvas({ manifest, timeline, tRef, speed, highlightTp, showControl, onNodeClick, onRegionClick }: Props) {
  const wrap = useRef<HTMLDivElement>(null);
  const canvas = useRef<HTMLCanvasElement>(null);
  const [width, setWidth] = useState(800);
  const [hover, setHover] = useState<string | null>(null);
  const layout: Layout = useMemo(() => computeLayout(manifest, width), [manifest, width]);
  const reducedMotion = useMemo(() => window.matchMedia?.('(prefers-reduced-motion: reduce)').matches ?? false, []);

  useEffect(() => {
    const el = wrap.current;
    if (!el) return;
    const ro = new ResizeObserver(([e]) => setWidth(Math.max(320, Math.floor(e!.contentRect.width))));
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  // live props for the frame loop without restarting it
  const live = useRef({ timeline, speed, highlightTp, showControl, hover });
  useLayoutEffect(() => {
    live.current = { timeline, speed, highlightTp, showControl, hover };
  }, [timeline, speed, highlightTp, showControl, hover]);

  useEffect(() => {
    const c = canvas.current;
    if (!c) return;
    const dpr = window.devicePixelRatio || 1;
    c.width = Math.round(layout.width * dpr);
    c.height = Math.round(layout.height * dpr);
    c.style.width = `${layout.width}px`;
    c.style.height = `${layout.height}px`;
    const ctx = c.getContext('2d')!;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    let raf = 0;
    let lastKey = '';
    let view = viewAt(live.current.timeline, tRef.current ?? 0);
    const frame = () => {
      const { timeline: tl, speed: sp, highlightTp: hl, showControl: sc, hover: hv } = live.current;
      const t = tRef.current ?? 0;
      const key = `${t}|${hl}|${sc}|${hv}|${tl.endT}|${tl.events.length}`;
      if (key !== lastKey) {
        lastKey = key;
        view = viewAt(tl, t);
        // ~250ms of wall clock per hop at current speed keeps LAN hops (1ms) visible
        const win = Math.max(1, 250 * sp);
        render(ctx, layout, view, visibleMessages(tl.msgs, t, win), { t, visualWindowMs: win, hover: hv, highlightTp: hl, showControl: sc, reducedMotion });
      }
      raf = requestAnimationFrame(frame);
    };
    raf = requestAnimationFrame(frame);
    return () => cancelAnimationFrame(raf);
  }, [layout, tRef, reducedMotion]);

  const local = (e: React.MouseEvent) => {
    const r = canvas.current!.getBoundingClientRect();
    return { x: e.clientX - r.left, y: e.clientY - r.top };
  };

  return (
    <div ref={wrap} className="relative w-full">
      <canvas
        ref={canvas}
        role="img"
        aria-label={`Topology of ${manifest.name}: ${manifest.regions.length} regions, ${manifest.clusters.length} clusters. Click a node to inject a fault.`}
        className="block"
        style={{ cursor: hover ? 'pointer' : 'default' }}
        onMouseMove={(e) => {
          const p = local(e);
          const n = hitTest(layout, p.x, p.y);
          setHover(n?.id ?? (regionAt(layout, p.x, p.y) ? `region:${regionAt(layout, p.x, p.y)}` : null));
        }}
        onMouseLeave={() => setHover(null)}
        onClick={(e) => {
          const p = local(e);
          const n = hitTest(layout, p.x, p.y);
          if (n) return onNodeClick(n, { x: e.clientX, y: e.clientY });
          const r = regionAt(layout, p.x, p.y);
          if (r) onRegionClick(r, { x: e.clientX, y: e.clientY });
        }}
      />
    </div>
  );
}
