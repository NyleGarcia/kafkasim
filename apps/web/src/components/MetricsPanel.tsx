import { percentileOf, type ViewState } from '@/lib/metrics';
import { NumberTicker } from '@/components/ui/number-ticker';
import { cn } from '@/lib/utils';

export function MetricsPanel({ view, scrubbing }: { view: ViewState; scrubbing: boolean }) {
  const parts = Object.values(view.partitions);
  const urp = parts.filter((p) => p.leader && p.isr.length < p.replicas.length).length;
  const offline = parts.filter((p) => !p.leader).length;
  const lat = Object.values(view.clients).flatMap((c) => c.latencies);
  const retries = Object.values(view.clients).reduce((a, c) => a + c.retries, 0);
  const dur = scrubbing ? 0 : 300;
  const tiles: { label: string; value: number; tone?: 'critical' | 'warning' | 'nominal'; fmt?: (n: number) => string; hint: string }[] = [
    { label: 'Acked', value: view.totals.acked, hint: 'records acknowledged to producers' },
    { label: 'Consumed', value: view.totals.consumed, hint: 'records read by consumers (≤ high watermark)' },
    { label: 'Lost', value: view.totals.lost, tone: view.totals.lost ? 'critical' : 'nominal', hint: 'acknowledged records missing from the current leader' },
    { label: 'Failed', value: view.totals.produceErrors, tone: view.totals.produceErrors ? 'critical' : undefined, hint: 'records that exhausted delivery.timeout.ms or hit a fatal error' },
    { label: 'Retries', value: retries, tone: retries ? 'warning' : undefined, hint: 'retriable produce errors (NOT_LEADER, NOT_ENOUGH_REPLICAS, timeouts)' },
    { label: 'p99 ack ms', value: percentileOf(lat, 99), fmt: (n) => n.toFixed(1), hint: 'produce latency over the last 64 acks per producer' },
    { label: 'Under-repl.', value: urp, tone: urp ? 'warning' : undefined, hint: 'partitions with ISR smaller than the replica set' },
    { label: 'Offline', value: offline, tone: offline ? 'critical' : undefined, hint: 'partitions without a leader' },
  ];
  if (view.totals.rpo) tiles.push({ label: 'RPO recs', value: view.totals.rpo, tone: 'critical', hint: 'acked source records missing at mirror promotion' });
  return (
    <section className="panel p-3" aria-labelledby="metrics-h">
      <h2 id="metrics-h" className="panel-title mb-2">Telemetry</h2>
      <dl className="grid grid-cols-4 gap-2">
        {tiles.map((t) => (
          <div key={t.label} className="rounded-md bg-muted/60 px-2 py-1.5" title={t.hint}>
            <dt className="text-[10px] uppercase tracking-wider text-muted-foreground">{t.label}</dt>
            <dd className={cn('readout text-base font-semibold', t.tone === 'critical' && 'text-critical', t.tone === 'warning' && 'text-warning', t.tone === 'nominal' && 'text-nominal')}>
              <NumberTicker value={t.value} duration={dur} {...(t.fmt ? { format: t.fmt } : {})} />
            </dd>
          </div>
        ))}
      </dl>
    </section>
  );
}
