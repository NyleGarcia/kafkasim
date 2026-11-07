import { Plug, Upload } from 'lucide-react';
import { useState } from 'react';

import { Button } from '@/components/ui/button';
import { StatusDot } from '@/components/ui/status-dot';
import type { LiveStatus } from '@/hooks/useLiveBridge';

export function LivePanel({ status, error, bridge, onConnect, onDisconnect }: { status: LiveStatus; error: string | null; bridge: string | null; onConnect: (url: string) => void; onDisconnect: () => void }) {
  const [url, setUrl] = useState(() => new URLSearchParams(location.search).get('bridge') ?? 'ws://localhost:8765/events');
  const tone = status === 'open' ? 'success' : status === 'connecting' ? 'warning' : status === 'error' ? 'error' : 'neutral';
  return (
    <section className="panel p-3" aria-labelledby="live-h">
      <h2 id="live-h" className="panel-title mb-2 flex items-center gap-2">
        Live bridge <StatusDot tone={tone} label={status} showLabel animate={status === 'open' || status === 'connecting'} />
      </h2>
      <form
        className="flex gap-2"
        onSubmit={(e) => {
          e.preventDefault();
          if (status === 'open') onDisconnect();
          else onConnect(url);
        }}
      >
        <input value={url} onChange={(e) => setUrl(e.target.value)} aria-label="Bridge WebSocket URL" className="readout h-8 flex-1 rounded-md border bg-muted/40 px-2 text-xs outline-none focus-visible:ring-2 focus-visible:ring-ring" />
        <Button size="sm" type="submit" variant={status === 'open' ? 'secondary' : 'default'}>
          <Plug /> {status === 'open' ? 'Disconnect' : 'Connect'}
        </Button>
      </form>
      {bridge && <p className="mt-2 text-xs text-muted-foreground">Connected to {bridge}</p>}
      {error && (
        <p role="alert" className="mt-2 text-xs text-critical">
          {error}
        </p>
      )}
      <div className="mt-3 space-y-1 text-xs text-muted-foreground">
        <p>A browser cannot speak the Kafka protocol, so live mode needs the bridge running next to your cluster:</p>
        <pre className="readout overflow-x-auto rounded bg-muted/60 p-2 text-[11px] text-foreground">uvx --from git+https://github.com/NyleGarcia/kafkasim#subdirectory=bridge kafkasim-bridge --bootstrap localhost:9092</pre>
        <p>It polls the AdminClient and streams the same event contract the simulator emits. Live mode is read-only; to inject faults on real Kafka use the harness.</p>
      </div>
    </section>
  );
}

export function ReplayPanel({ onLoad, error, recordings, onPick }: { onLoad: (log: unknown) => void; error: string | null; recordings: { file: string; name: string }[]; onPick: (file: string) => void }) {
  return (
    <section className="panel p-3" aria-labelledby="replay-h">
      <h2 id="replay-h" className="panel-title mb-2">Replay a recording</h2>
      {recordings.length > 0 && (
        <ul className="mb-3 space-y-1">
          {recordings.map((r) => (
            <li key={r.file}>
              <button type="button" className="text-left text-sm text-info hover:underline" onClick={() => onPick(r.file)}>
                {r.name}
              </button>
            </li>
          ))}
        </ul>
      )}
      <label className="inline-flex cursor-pointer items-center gap-2 rounded-md border px-3 py-1.5 text-sm hover:bg-muted">
        <Upload className="size-4" /> Load event log (.json)
        <input
          type="file"
          accept="application/json,.json"
          className="sr-only"
          onChange={async (e) => {
            const f = e.target.files?.[0];
            if (!f) return;
            try {
              onLoad(JSON.parse(await f.text()));
            } catch (err) {
              onLoad({ __error: String(err) });
            }
          }}
        />
      </label>
      {error && (
        <p role="alert" className="mt-2 text-xs whitespace-pre-wrap text-critical">
          {error}
        </p>
      )}
      <p className="mt-3 text-xs text-muted-foreground">
        Event logs come from the real-Kafka harness (<code className="readout">harness/</code>) or from <em>Export log</em> in simulate mode. All three sources share one schema, so a replay renders exactly like a simulation.
      </p>
    </section>
  );
}
