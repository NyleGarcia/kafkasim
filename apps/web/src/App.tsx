import { parseManifest, scenarios, viewAt, type ActionT, type Manifest, type Timeline } from '@kafkasim/engine';
import { Code2, Download, FilePlus2, SlidersHorizontal, Undo2 } from 'lucide-react';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import { EventLog } from '@/components/EventLog';
import { FaultMenu } from '@/components/FaultMenu';
import { MetricsPanel } from '@/components/MetricsPanel';
import { NodesPanel } from '@/components/NodesPanel';
import { PartitionTable } from '@/components/PartitionTable';
import { LivePanel, ReplayPanel } from '@/components/SourcePanels';
import { TimelineBar } from '@/components/TimelineBar';
import { TopologyCanvas } from '@/components/TopologyCanvas';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip';
import { ScenarioBuilder } from '@/builder/ScenarioBuilder';
import { blankDraft } from '@/builder/draft';
import { useLiveBridge } from '@/hooks/useLiveBridge';
import { usePlayback } from '@/hooks/usePlayback';
import { nodeFaults, regionFaults, workerFaults, zoneFaults, type FaultOption } from '@/lib/faults';
import { SimRunner } from '@/sim/runner';

type Mode = 'simulate' | 'replay' | 'live';

/** continuous mode stops extending here (memory: ~25 MB of events per simulated minute for big clusters) */
const MAX_RUN_MS = 30 * 60_000;

const slug = (s: string) => s.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
const SCENARIOS = scenarios.map((s) => ({ input: s, slug: slug(s.name), name: s.name }));

function initialScenario() {
  const want = new URLSearchParams(location.search).get('scenario');
  return Math.max(0, SCENARIOS.findIndex((s) => s.slug === want));
}

export default function App() {
  const [mode, setMode] = useState<Mode>('simulate');
  const [scenarioIdx, setScenarioIdx] = useState(initialScenario);
  const [custom, setCustom] = useState<unknown | null>(null);
  const [extra, setExtra] = useState<ActionT[]>([]);
  const [sim, setSim] = useState<{ timeline: Timeline; manifest: Manifest; wallMs: number } | null>(null);
  const [replay, setReplay] = useState<{ timeline: Timeline; manifest: Manifest } | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [replayError, setReplayError] = useState<string | null>(null);
  const [recordings, setRecordings] = useState<{ file: string; name: string }[]>([]);
  const [editOpen, setEditOpen] = useState<null | 'edit' | 'new'>(null);
  const [highlight, setHighlight] = useState<string | null>(null);
  const [showControl, setShowControl] = useState(true);
  const [menu, setMenu] = useState<{ at: { x: number; y: number }; title: string; options: FaultOption[] } | null>(null);
  const runner = useRef<SimRunner | null>(null);
  const live = useLiveBridge();

  const baseInput = custom ?? SCENARIOS[scenarioIdx]!.input;
  /** run length override (ms); null = the scenario's own durationMs */
  const [lengthMs, setLengthMs] = useState<number | null>(null);
  /** keep running past the end: extend the (deterministic) simulation as playback approaches it */
  const [continuous, setContinuous] = useState(true);
  const scenarioMs = useMemo(() => {
    try {
      return parseManifest(baseInput).durationMs;
    } catch {
      return 60_000;
    }
  }, [baseInput]);
  const input = useMemo(() => (lengthMs ? { ...(baseInput as object), durationMs: lengthMs } : baseInput), [baseInput, lengthMs]);
  const liveTimeline = live.timeline;
  const liveManifest = live.manifest;
  const active = useMemo(
    () => (mode === 'simulate' ? sim : mode === 'replay' ? replay : liveTimeline && liveManifest ? { timeline: liveTimeline, manifest: liveManifest } : null),
    [mode, sim, replay, liveTimeline, liveManifest],
  );
  const pb = usePlayback(active?.timeline.endT ?? 0, {
    follow: mode === 'live',
    hold: mode === 'simulate' && continuous,
    // extend the deterministic run as playback approaches its end (prefix stays identical)
    onNearEnd: (end) => end < MAX_RUN_MS && setLengthMs(Math.min(MAX_RUN_MS, Math.max(end + 60_000, Math.round(end * 1.5)))),
  });
  const { seek, setPlaying, tRef } = pb;

  useEffect(() => {
    runner.current = new SimRunner();
    return () => runner.current?.dispose();
  }, []);

  // run simulation whenever the manifest or injected actions change
  const inputKey = useMemo(() => JSON.stringify(input), [input]);
  const extraKey = useMemo(() => JSON.stringify(extra), [extra]);
  useEffect(() => {
    if (mode !== 'simulate' || !runner.current) return;
    let cancelled = false;
    runner.current
      .run({ kind: 'simulate', manifest: JSON.parse(inputKey), extraActions: JSON.parse(extraKey) })
      .then((r) => {
        if (cancelled) return;
        setSim({ timeline: r.timeline, manifest: r.manifest, wallMs: r.wallMs });
        setError(null);
      })
      .catch((e: Error) => {
        if (!cancelled && e.message !== 'superseded') setError(e.message);
      });
    return () => {
      cancelled = true;
    };
  }, [inputKey, extraKey, mode]);

  // bundled recordings (written by harness/record.py into public/replays)
  useEffect(() => {
    const ctl = new AbortController();
    fetch(`${import.meta.env.BASE_URL}replays/index.json`, { signal: ctl.signal })
      .then((r) => (r.ok ? r.json() : []))
      .then((xs: unknown) => setRecordings(Array.isArray(xs) ? (xs as { file: string; name: string }[]) : []))
      .catch(() => {});
    return () => ctl.abort();
  }, []);

  const loadReplay = useCallback(async (log: unknown) => {
    if (!runner.current) return;
    if (log && typeof log === 'object' && '__error' in log) return setReplayError(String((log as { __error: string }).__error));
    try {
      const r = await runner.current.run({ kind: 'replay', log });
      setReplay({ timeline: r.timeline, manifest: r.manifest });
      setReplayError(null);
      seek(0);
      setPlaying(true);
    } catch (e) {
      setReplayError(e instanceof Error ? e.message : String(e));
    }
  }, [seek, setPlaying]);

  const pickRecording = useCallback(
    (file: string) => {
      const ctl = new AbortController();
      fetch(`${import.meta.env.BASE_URL}replays/${file}`, { signal: ctl.signal })
        .then((r) => r.json())
        .then(loadReplay)
        .catch((e) => setReplayError(String(e)));
    },
    [loadReplay],
  );

  const view = useMemo(() => (active ? viewAt(active.timeline, pb.t) : null), [active, pb.t]);

  const inject = useCallback(
    (o: FaultOption) => {
      if (mode !== 'simulate') return;
      const at = Math.round(tRef.current ?? 0);
      setExtra((xs) => [...xs, { ...o.action, at } as ActionT]);
      setPlaying(true);
    },
    [mode, tRef, setPlaying],
  );

  const pickScenario = (v: string) => {
    setLengthMs(null);
    setScenarioIdx(Number(v));
    setCustom(null);
    setExtra([]);
    pb.seek(0);
    pb.setPlaying(true);
    const u = new URL(location.href);
    u.searchParams.set('scenario', SCENARIOS[Number(v)]!.slug);
    history.replaceState(null, '', u);
  };

  // space = play/pause (outside inputs)
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      const tag = (e.target as HTMLElement)?.tagName;
      if (e.code !== 'Space' || tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'BUTTON' || (e.target as HTMLElement)?.isContentEditable) return;
      e.preventDefault();
      setPlaying((p) => !p);
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [setPlaying]);

  // autoplay first simulation load
  const autoplayed = useRef(false);
  useEffect(() => {
    if (sim && !autoplayed.current) {
      autoplayed.current = true;
      setPlaying(true);
    }
  }, [sim, setPlaying]);

  const download = (name: string, data: unknown) => {
    const url = URL.createObjectURL(new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' }));
    const a = document.createElement('a');
    a.href = url;
    a.download = name;
    a.click();
    URL.revokeObjectURL(url);
  };

  const manifest = active?.manifest;
  let parsedInput: Manifest | null = null;
  try {
    parsedInput = parseManifest(input);
  } catch {
    parsedInput = null;
  }

  return (
    <div className="mx-auto flex min-h-full max-w-[1600px] flex-col gap-3 p-3 lg:p-4">
      <header className="flex flex-wrap items-center gap-3">
        <div className="flex items-baseline gap-2">
          <h1 className="readout text-lg font-bold tracking-tight">
            kafka<span className="text-primary">sim</span>
          </h1>
          <span className="hidden text-xs text-muted-foreground sm:inline">KRaft replication, failure &amp; DR, step by step</span>
        </div>
        <Tabs value={mode} onValueChange={(v) => setMode(v as Mode)}>
          <TabsList>
            <TabsTrigger value="simulate">Simulate</TabsTrigger>
            <TabsTrigger value="replay">Replay</TabsTrigger>
            <TabsTrigger value="live">Live</TabsTrigger>
          </TabsList>
        </Tabs>
        {mode === 'simulate' && (
          <>
            <Select value={custom ? 'custom' : String(scenarioIdx)} onValueChange={(v) => v !== 'custom' && pickScenario(v)}>
              <SelectTrigger className="w-80 max-w-full" aria-label="Scenario">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {SCENARIOS.map((s, i) => (
                  <SelectItem key={s.slug} value={String(i)}>
                    {s.name}
                  </SelectItem>
                ))}
                {custom !== null && <SelectItem value="custom">Custom manifest</SelectItem>}
              </SelectContent>
            </Select>
            <Button size="sm" variant="outline" onClick={() => setEditOpen('edit')}>
              <SlidersHorizontal /> Edit scenario
            </Button>
            <Button size="sm" variant="ghost" onClick={() => setEditOpen('new')}>
              <FilePlus2 /> New
            </Button>
            {extra.length > 0 && (
              <Button size="sm" variant="ghost" onClick={() => setExtra([])}>
                <Undo2 /> Undo {extra.length} injected
              </Button>
            )}
            {sim && (
              <Button size="sm" variant="ghost" onClick={() => download(`${slug(sim.manifest.name)}.events.json`, { schemaVersion: 1, source: 'sim', manifest: sim.manifest, events: sim.timeline.events })}>
                <Download /> Export log
              </Button>
            )}
          </>
        )}
        <div className="ml-auto flex items-center gap-2">
          {manifest?.requiresConfluent && (
            <Tooltip>
              <TooltipTrigger asChild>
                <Badge variant="outline" className="border-observer/50 text-observer">
                  Confluent Platform
                </Badge>
              </TooltipTrigger>
              <TooltipContent>This scenario uses Confluent-only features (Cluster Linking / MRC observers). Not in Apache Kafka.</TooltipContent>
            </Tooltip>
          )}
          <label className="flex items-center gap-1.5 text-xs text-muted-foreground">
            <input type="checkbox" checked={showControl} onChange={(e) => setShowControl(e.target.checked)} className="accent-primary" />
            control traffic
          </label>
          <a href="https://github.com/NyleGarcia/kafkasim" className="text-muted-foreground hover:text-foreground" aria-label="Source on GitHub">
            <Code2 className="size-4" />
          </a>
        </div>
      </header>

      {error && (
        <div role="alert" className="panel border-critical/50 p-3 text-sm whitespace-pre-wrap text-critical">
          {error}
        </div>
      )}

      {mode === 'replay' && !replay && <ReplayPanel onLoad={loadReplay} error={replayError} recordings={recordings} onPick={pickRecording} />}
      {mode === 'live' && <LivePanel status={live.status} error={live.error} bridge={live.bridge} onConnect={live.connect} onDisconnect={live.disconnect} />}

      {active && view && manifest ? (
        <div className="grid min-h-0 flex-1 gap-3 lg:grid-cols-[minmax(0,1fr)_400px]">
          <div className="flex min-w-0 flex-col gap-3">
            <div className="panel overflow-hidden">
              <div className="border-b px-3 py-2">
                <div className="flex items-center gap-2">
                  <h2 className="text-sm font-semibold">{manifest.name}</h2>
                  {mode === 'simulate' && sim && <span className="readout text-[10px] text-muted-foreground">{sim.timeline.events.length.toLocaleString()} events · {sim.wallMs.toFixed(0)}ms</span>}
                  {mode === 'replay' && (
                    <Button size="xs" variant="ghost" className="ml-auto" onClick={() => setReplay(null)}>
                      load another
                    </Button>
                  )}
                </div>
                {manifest.description && <p className="mt-1 max-w-[80ch] text-xs leading-relaxed text-muted-foreground">{manifest.description}</p>}
              </div>
              <TopologyCanvas
                manifest={manifest}
                timeline={active.timeline}
                tRef={pb.tRef}
                speed={pb.speed}
                highlightTp={highlight}
                showControl={showControl}
                onNodeClick={(n, at) => {
                  if (n.kind === 'worker') {
                    const w = view.workers[n.id];
                    if (w) setMenu({ at, title: `k8s worker ${n.id}${mode !== 'simulate' ? ' (read-only)' : ''}`, options: mode === 'simulate' ? workerFaults(w) : [] });
                    return;
                  }
                  const nv = view.nodes[n.id];
                  if (!nv) return;
                  setMenu({ at, title: `${n.kind} ${n.id}${mode !== 'simulate' ? ' (read-only)' : ''}`, options: mode === 'simulate' ? nodeFaults(manifest, nv) : [] });
                }}
                onRegionClick={(r, at) => setMenu({ at, title: `region ${r}`, options: mode === 'simulate' ? [...zoneFaults(manifest, r), ...regionFaults(manifest, r, view.partitioned)] : [] })}
              />
              <Legend />
            </div>
            <TimelineBar
              timeline={active.timeline}
              t={pb.t}
              playing={pb.playing}
              speed={pb.speed}
              live={mode === 'live'}
              onPlay={pb.setPlaying}
              onSeek={pb.seek}
              onSpeed={pb.setSpeed}
              injectedAt={mode === 'simulate' ? extra.map((a) => a.at) : []}
              {...(mode === 'simulate'
                ? {
                    length: { current: sim?.timeline.endT ?? 0, scenario: scenarioMs, max: MAX_RUN_MS, continuous },
                    onLength: (ms: number | null) => setLengthMs(ms),
                    onContinuous: setContinuous,
                  }
                : {})}
            />
            <EventLog timeline={active.timeline} t={pb.t} onSeek={pb.seek} />
          </div>
          <aside className="flex min-w-0 flex-col gap-3">
            <MetricsPanel view={view} scrubbing={!pb.playing} />
            <PartitionTable view={view} highlight={highlight} onHighlight={setHighlight} onFailover={(cluster, topic) => inject({ label: 'failover', hint: '', action: { type: 'FAILOVER', cluster, topic } })} />
            <NodesPanel manifest={manifest} view={view} onFault={inject} disabled={mode !== 'simulate'} />
          </aside>
        </div>
      ) : (
        mode === 'simulate' && !error && <div className="panel p-6 text-sm text-muted-foreground">Simulating…</div>
      )}

      {menu && <FaultMenu open at={menu.at} title={menu.title} options={menu.options} onPick={inject} onClose={() => setMenu(null)} />}
      <ScenarioBuilder
        open={editOpen !== null}
        onOpenChange={(o) => !o && setEditOpen(null)}
        initial={editOpen === 'new' ? blankDraft() : parsedInput ? baseInput : SCENARIOS[0]!.input}
        onRun={(m) => {
          setLengthMs(null);
          setCustom(m);
          setExtra([]);
          seek(0);
          setPlaying(true);
        }}
      />
      <footer className="pb-2 text-center text-[11px] text-muted-foreground">
        Deterministic model of Apache Kafka 4.x (KRaft) + labelled Confluent features. Model assumptions: <a className="underline hover:text-foreground" href="https://github.com/NyleGarcia/kafkasim/blob/main/docs/architecture/engine-model.md">engine-model.md</a>
      </footer>
    </div>
  );
}

function Legend() {
  const items = [
    ['bg-[#38bdf8]', 'produce'],
    ['bg-[#22c55e]', 'replica fetch'],
    ['bg-[#22d3ee]', 'cluster link'],
    ['bg-[#2dd4bf]', 'consume'],
    ['bg-[#eab308]', 'metadata'],
    ['bg-[#a78bfa]', 'KRaft vote'],
    ['bg-[#64748b]', 'heartbeat'],
  ] as const;
  return (
    <div className="flex flex-wrap items-center gap-x-3 gap-y-1 border-t px-3 py-1.5 text-[10px] text-muted-foreground">
      {/* k8s: wide boxes = worker nodes (dashed = cordoned, red = NotReady) */}
      {items.map(([c, l]) => (
        <span key={l} className="inline-flex items-center gap-1">
          <span className={`size-2 rounded-full ${c}`} aria-hidden />
          {l}
        </span>
      ))}
      <span className="ml-auto">hexagon = controller · square = broker · circle = client · wide box = k8s worker · dashed = fenced/cordoned · click to inject a fault</span>
    </div>
  );
}
