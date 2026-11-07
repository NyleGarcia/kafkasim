import type { Manifest, ViewState } from '@kafkasim/engine';
import { ChevronDown } from 'lucide-react';

import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from '@/components/ui/dropdown-menu';
import { StatusDot, type StatusDotTone } from '@/components/ui/status-dot';
import { nodeFaults, type FaultOption } from '@/lib/faults';
import { cn } from '@/lib/utils';

/** Keyboard-accessible twin of the canvas: every node + its fault menu. */
export function NodesPanel({ manifest, view, onFault, disabled }: { manifest: Manifest; view: ViewState; onFault: (o: FaultOption) => void; disabled: boolean }) {
  const nodes = Object.values(view.nodes);
  return (
    <section className="panel p-3" aria-labelledby="nodes-h">
      <h2 id="nodes-h" className="panel-title mb-2">Nodes {disabled ? '· read-only' : '· inject faults'}</h2>
      <div className="space-y-2">
        {manifest.clusters.map((c) => (
          <div key={c.id}>
            <div className="readout mb-1 text-[10px] text-muted-foreground">{c.id}</div>
            <ul className="flex flex-wrap gap-1">
              {nodes
                .filter((n) => n.cluster === c.id)
                .map((n) => {
                  const tone: StatusDotTone = n.status === 'dead' ? 'error' : n.status === 'paused' || n.fenced ? 'warning' : n.activeController ? 'observer' : 'success';
                  const opts = nodeFaults(manifest, n);
                  return (
                    <li key={n.id}>
                      <DropdownMenu>
                        <DropdownMenuTrigger
                          disabled={disabled}
                          className={cn('readout inline-flex items-center gap-0.5 rounded border px-1.5 py-0.5 text-[11px] hover:bg-muted focus-visible:ring-2 focus-visible:ring-ring disabled:opacity-60', n.status === 'dead' && 'text-critical')}
                          aria-label={`${n.kind} ${n.id}: ${n.status}${n.fenced ? ', fenced' : ''}. Open fault menu.`}
                        >
                          <StatusDot tone={tone} label={n.status} animate={n.status !== 'running'} />
                          {n.id}
                          <ChevronDown className="size-3 opacity-60" />
                        </DropdownMenuTrigger>
                        <DropdownMenuContent className="w-56">
                          {opts.map((o) => (
                            <DropdownMenuItem key={o.label} onSelect={() => onFault(o)} className={cn(o.tone === 'critical' && 'text-critical', o.tone === 'warning' && 'text-warning')}>
                              {o.label}
                            </DropdownMenuItem>
                          ))}
                        </DropdownMenuContent>
                      </DropdownMenu>
                    </li>
                  );
                })}
            </ul>
          </div>
        ))}
      </div>
    </section>
  );
}
