import { initialView, parseManifest, type Manifest } from '@kafkasim/engine';
import { AlertTriangle, CheckCircle2, FileJson, Play, RotateCcw } from 'lucide-react';
import { useEffect, useMemo, useRef, useState } from 'react';

import { Button } from '@/components/ui/button';
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { computeLayout } from '@/canvas/layout';
import { render } from '@/canvas/render';

import * as D from './draft';
import { ClientsSection, ClustersSection, FaultsSection, GeneralSection, RegionsSection, TopicsSection } from './sections';

interface Props {
  open: boolean;
  initial: unknown;
  onOpenChange: (o: boolean) => void;
  onRun: (manifest: Manifest) => void;
}

/** Visual scenario editor: forms on the left, live topology + validation on the right. */
export function ScenarioBuilder({ open, initial, onOpenChange, onRun }: Props) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      {open && <Builder initial={initial} onClose={() => onOpenChange(false)} onRun={onRun} />}
    </Dialog>
  );
}

function toDraft(x: unknown): D.Draft {
  try {
    return parseManifest(x);
  } catch {
    return D.blankDraft();
  }
}

function Builder({ initial, onClose, onRun }: { initial: unknown; onClose: () => void; onRun: (m: Manifest) => void }) {
  const [d, setD] = useState<D.Draft>(() => toDraft(initial));
  const [notice, setNotice] = useState<string | null>(null);
  const [tab, setTab] = useState('topology');
  const problems = useMemo(() => D.validate(d), [d]);
  const set = (x: D.Draft | string) => {
    if (typeof x === 'string') setNotice(x);
    else {
      setNotice(null);
      setD(x);
    }
  };
  const props = { d, set };

  return (
    <DialogContent className="flex h-[92vh] w-[min(1400px,96vw)] max-w-none flex-col gap-0 p-0 sm:max-w-none">
      <DialogHeader className="flex-row items-center gap-3 border-b px-4 py-3 text-left">
        <div className="min-w-0 flex-1">
          <DialogTitle>Scenario builder</DialogTitle>
          <DialogDescription className="text-xs">Regions, clusters, topics, clients and timed faults. Every change is validated by the same parser the simulator uses.</DialogDescription>
        </div>
        <Button size="sm" variant="ghost" onClick={() => set(D.blankDraft())}>
          <RotateCcw /> Start blank
        </Button>
        <Button
          size="sm"
          disabled={problems.length > 0}
          onClick={() => {
            onRun(parseManifest(d));
            onClose();
          }}
        >
          <Play /> Run scenario
        </Button>
      </DialogHeader>
      <div className="grid min-h-0 flex-1 lg:grid-cols-[minmax(0,1fr)_440px]">
        <Tabs value={tab} onValueChange={setTab} className="flex min-h-0 flex-col">
          <TabsList className="mx-4 mt-3 w-fit flex-wrap">
            <TabsTrigger value="topology">Regions &amp; network</TabsTrigger>
            <TabsTrigger value="clusters">Clusters</TabsTrigger>
            <TabsTrigger value="topics">Topics ({d.topics.length})</TabsTrigger>
            <TabsTrigger value="clients">Clients ({d.producers.length + d.consumers.length})</TabsTrigger>
            <TabsTrigger value="faults">Faults ({d.actions.length})</TabsTrigger>
            <TabsTrigger value="json">
              <FileJson className="size-3.5" /> JSON
            </TabsTrigger>
          </TabsList>
          <div className="min-h-0 flex-1 overflow-y-auto px-4 py-4">
            <TabsContent value="topology" className="space-y-6">
              <GeneralSection {...props} />
              <RegionsSection {...props} />
            </TabsContent>
            <TabsContent value="clusters">
              <ClustersSection {...props} />
            </TabsContent>
            <TabsContent value="topics">
              <TopicsSection {...props} />
            </TabsContent>
            <TabsContent value="clients">
              <ClientsSection {...props} />
            </TabsContent>
            <TabsContent value="faults">
              <FaultsSection {...props} />
            </TabsContent>
            <TabsContent value="json">
              <JsonTab d={d} onApply={(x) => set(x)} />
            </TabsContent>
          </div>
        </Tabs>
        <aside className="flex min-h-0 flex-col gap-3 overflow-y-auto border-t bg-background/60 p-4 lg:border-t-0 lg:border-l">
          <div className="panel-title">Preview</div>
          <Preview d={problems.length ? null : d} />
          <div aria-live="polite" className="space-y-2">
            {notice && (
              <p role="alert" className="flex items-start gap-1.5 rounded-md border border-warning/40 bg-warning/10 p-2 text-xs text-warning">
                <AlertTriangle className="mt-0.5 size-3.5 shrink-0" />
                {notice}
              </p>
            )}
            {problems.length === 0 ? (
              <p className="flex items-center gap-1.5 text-xs text-nominal">
                <CheckCircle2 className="size-3.5" /> Valid: {d.clusters.reduce((a, c) => a + c.brokers.length, 0)} brokers, {d.topics.reduce((a, t) => a + t.partitions, 0)} partitions, {d.actions.length} faults
              </p>
            ) : (
              <div className="rounded-md border border-critical/40 bg-critical/10 p-2">
                <p className="mb-1 text-xs font-semibold text-critical">{problems.length} problem{problems.length > 1 ? 's' : ''}</p>
                <ul className="space-y-0.5 text-xs text-critical">
                  {problems.slice(0, 12).map((p, i) => (
                    <li key={i}>
                      {p.path && <code className="readout mr-1 opacity-80">{p.path}</code>}
                      {p.message}
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </div>
        </aside>
      </div>
    </DialogContent>
  );
}

/** Static topology preview: same layout + renderer as the simulation, no messages. */
function Preview({ d }: { d: D.Draft | null }) {
  const wrap = useRef<HTMLDivElement>(null);
  const canvas = useRef<HTMLCanvasElement>(null);
  const [width, setWidth] = useState(400);
  useEffect(() => {
    const el = wrap.current;
    if (!el) return;
    const ro = new ResizeObserver(([e]) => setWidth(Math.max(280, Math.floor(e!.contentRect.width))));
    ro.observe(el);
    return () => ro.disconnect();
  }, []);
  useEffect(() => {
    const c = canvas.current;
    if (!c || !d) return;
    const layout = computeLayout(d, width);
    const dpr = window.devicePixelRatio || 1;
    c.width = Math.round(layout.width * dpr);
    c.height = Math.round(layout.height * dpr);
    c.style.width = `${layout.width}px`;
    c.style.height = `${layout.height}px`;
    const ctx = c.getContext('2d')!;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    render(ctx, layout, initialView(d), [], { t: 0, visualWindowMs: 1, hover: null, highlightTp: null, showControl: false, reducedMotion: true });
  }, [d, width]);
  return (
    <div ref={wrap} className="panel overflow-hidden">
      {d ? <canvas ref={canvas} role="img" aria-label="Topology preview" className="block" /> : <p className="p-4 text-xs text-muted-foreground">Fix the problems below to see the preview.</p>}
    </div>
  );
}

function JsonTab({ d, onApply }: { d: D.Draft; onApply: (x: D.Draft | string) => void }) {
  const [text, setText] = useState(() => JSON.stringify(d, null, 2));
  const [error, setError] = useState<string | null>(null);
  return (
    <div className="space-y-2">
      <p className="text-xs text-muted-foreground">
        Advanced: the manifest as JSON (<code className="readout">contracts/manifest.schema.json</code>). Paste one or load a file, then apply.
      </p>
      <textarea
        value={text}
        onChange={(e) => setText(e.target.value)}
        spellCheck={false}
        aria-label="Manifest JSON"
        aria-invalid={!!error}
        className="readout h-[55vh] w-full resize-none rounded-md border bg-muted/40 p-3 text-xs outline-none focus-visible:ring-2 focus-visible:ring-ring"
      />
      {error && (
        <pre role="alert" className="max-h-32 overflow-auto rounded-md border border-critical/40 bg-critical/10 p-2 text-xs whitespace-pre-wrap text-critical">
          {error}
        </pre>
      )}
      <div className="flex items-center gap-2">
        <Button
          size="sm"
          onClick={() => {
            try {
              onApply(parseManifest(JSON.parse(text)));
              setError(null);
            } catch (e) {
              setError(e instanceof Error ? e.message : String(e));
            }
          }}
        >
          Apply JSON
        </Button>
        <Button size="sm" variant="ghost" onClick={() => setText(JSON.stringify(d, null, 2))}>
          Reset from form
        </Button>
        <label className="ml-auto cursor-pointer text-xs text-muted-foreground hover:text-foreground">
          <input type="file" accept="application/json,.json" className="sr-only" onChange={async (e) => e.target.files?.[0] && setText(await e.target.files[0].text())} />
          Load file…
        </label>
      </div>
    </div>
  );
}
