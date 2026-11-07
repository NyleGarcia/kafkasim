import { notableEvents, type Timeline } from '@kafkasim/engine';
import { useMemo } from 'react';

import { LogLine, LogViewer } from '@/components/ui/log-viewer';
import { describe, fmtT } from '@/lib/describe';

export function EventLog({ timeline, t, onSeek }: { timeline: Timeline; t: number; onSeek: (t: number) => void }) {
  // bucket t to 100ms so the list doesn't re-filter every frame
  const bucket = Math.floor(t / 100);
  const lines = useMemo(() => {
    const out = [];
    for (const e of notableEvents(timeline, 0, bucket * 100 + 99, 400)) {
      const d = describe(e);
      if (d) out.push({ e, d });
    }
    return out.slice(-200);
  }, [timeline, bucket]);
  return (
    <section className="panel flex min-h-0 flex-col p-3" aria-labelledby="log-h">
      <h2 id="log-h" className="panel-title mb-2">Event log</h2>
      <LogViewer className="h-56 min-h-0 flex-1">
        {lines.length === 0 && <div className="px-1 text-muted-foreground">Nothing notable yet. Press play, or click a broker to inject a fault.</div>}
        {lines.map(({ e, d }, i) => (
          <LogLine key={`${e.t}-${i}`} level={d.level} time={fmtT(e.t)} source={d.source} onTimeClick={() => onSeek(e.t)}>
            {d.text}
          </LogLine>
        ))}
      </LogViewer>
    </section>
  );
}
