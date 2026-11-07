import type { PartitionView, ViewState } from '@kafkasim/engine';

import { Badge } from '@/components/ui/badge';
import { cn } from '@/lib/utils';

interface Props {
  view: ViewState;
  highlight: string | null;
  onHighlight: (tp: string | null) => void;
  onFailover: (cluster: string, topic: string) => void;
}

export function PartitionTable({ view, highlight, onHighlight, onFailover }: Props) {
  const parts = Object.values(view.partitions);
  const topics = [...new Set(parts.map((p) => `${p.cluster}/${p.topic}`))];
  return (
    <section className="panel p-3" aria-labelledby="parts-h">
      <h2 id="parts-h" className="panel-title mb-2">Partitions</h2>
      <div className="space-y-3">
        {topics.map((key) => {
          const ps = parts.filter((p) => `${p.cluster}/${p.topic}` === key);
          const mirror = ps[0]!.mirrorOf;
          return (
            <div key={key}>
              <div className="mb-1 flex items-center gap-2">
                <span className="readout text-xs font-semibold">{key}</span>
                {mirror && (
                  <Badge variant="outline" className={cn('text-[10px]', ps[0]!.mirrorType === 'cluster_link' ? 'border-observer/50 text-observer' : 'border-info/50 text-info')}>
                    {ps[0]!.mirrorType === 'cluster_link' ? 'cluster link' : 'mm2'} ← {mirror.split('/').slice(0, 2).join('/')}
                  </Badge>
                )}
                {mirror && !ps[0]!.promoted && (
                  <button type="button" className="ml-auto rounded border border-critical/40 px-1.5 py-0.5 text-[10px] font-semibold text-critical hover:bg-critical/10" onClick={() => onFailover(ps[0]!.cluster, ps[0]!.topic)}>
                    FAILOVER
                  </button>
                )}
                {ps[0]!.promoted && <Badge className="ml-auto bg-nominal/15 text-[10px] text-nominal">promoted</Badge>}
              </div>
              <table className="w-full text-[11px]">
                <thead className="sr-only">
                  <tr>
                    <th>Partition</th>
                    <th>Leader / epoch</th>
                    <th>Replicas</th>
                    <th>High watermark</th>
                  </tr>
                </thead>
                <tbody>
                  {ps.map((p) => (
                    <Row key={p.tp} p={p} active={highlight === p.tp} onHighlight={onHighlight} />
                  ))}
                </tbody>
              </table>
            </div>
          );
        })}
      </div>
    </section>
  );
}

function Row({ p, active, onHighlight }: { p: PartitionView; active: boolean; onHighlight: (tp: string | null) => void }) {
  const leaderLeo = p.leader ? (p.leo[p.leader] ?? 0) : 0;
  const all = [...p.replicas, ...p.observers];
  const max = Math.max(1, ...Object.values(p.leo));
  return (
    <tr
      tabIndex={0}
      className={cn('cursor-pointer border-t border-border/60 align-top outline-none focus-visible:bg-muted', active && 'bg-muted')}
      onMouseEnter={() => onHighlight(p.tp)}
      onMouseLeave={() => onHighlight(null)}
      onFocus={() => onHighlight(p.tp)}
      onBlur={() => onHighlight(null)}
    >
      <td className="readout w-8 py-1 text-muted-foreground">p{p.partition}</td>
      <td className="readout w-20 py-1">
        {p.leader ? <span className="text-info">{p.leader}</span> : <span className="font-semibold text-critical">OFFLINE</span>}
        <span className="text-muted-foreground"> e{p.epoch}</span>
        {p.lost > 0 && <div className="text-critical">−{p.lost} lost</div>}
      </td>
      <td className="py-1">
        <div className="flex flex-col gap-0.5">
          {all.map((r) => {
            const inIsr = p.isr.includes(r);
            const obs = p.observers.includes(r);
            const leo = p.leo[r] ?? 0;
            return (
              <div key={r} className="flex items-center gap-1.5" title={`${r}: LEO ${leo}${inIsr ? ' · in ISR' : obs ? ' · observer' : ' · OUT of ISR'}`}>
                <span className={cn('readout w-10 shrink-0', inIsr ? 'text-nominal' : obs ? 'text-observer' : 'text-warning')}>
                  {r}
                  {r === p.leader ? '*' : ''}
                </span>
                <div className="relative h-1.5 flex-1 overflow-hidden rounded bg-muted">
                  <div className={cn('absolute inset-y-0 left-0', inIsr ? 'bg-nominal/70' : obs ? 'bg-observer/70' : 'bg-warning/70')} style={{ width: `${(leo / max) * 100}%` }} />
                  <div className="absolute inset-y-0 w-px bg-foreground/80" style={{ left: `${(p.hw / max) * 100}%` }} aria-hidden />
                </div>
                <span className="readout w-10 shrink-0 text-right text-muted-foreground">{leaderLeo - leo > 0 && inIsr === false ? `−${leaderLeo - leo}` : leo}</span>
              </div>
            );
          })}
        </div>
      </td>
      <td className="readout w-14 py-1 text-right" title="high watermark (consumers read up to here)">
        <span className="text-muted-foreground">HW </span>
        {p.hw}
        {p.mirrorOf && <div className="text-muted-foreground">lag {p.mirrorLag}</div>}
      </td>
    </tr>
  );
}
