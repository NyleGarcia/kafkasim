/// <reference lib="webworker" />
/**
 * Simulation worker: parse → simulate → precompute timeline keyframes off the
 * main thread. Deterministic, so interactive fault injection = re-run with the
 * extra actions; the prefix before the injection point is identical.
 */
import { buildTimeline, EventLog, parseManifest, simulate, type ActionT } from '@kafkasim/engine';

export type WorkerRequest =
  | { id: number; kind: 'simulate'; manifest: unknown; extraActions: ActionT[] }
  | { id: number; kind: 'replay'; log: unknown };

export type WorkerResponse =
  | { id: number; ok: true; timeline: ReturnType<typeof buildTimeline>; manifest: ReturnType<typeof parseManifest>; wallMs: number; source: 'sim' | 'recording' }
  | { id: number; ok: false; error: string };

self.onmessage = (ev: MessageEvent<WorkerRequest>) => {
  const req = ev.data;
  try {
    const t0 = performance.now();
    if (req.kind === 'simulate') {
      const manifest = parseManifest(req.manifest);
      const r = simulate(manifest, { extraActions: req.extraActions });
      const timeline = buildTimeline(manifest, r.events, r.endT);
      post({ id: req.id, ok: true, timeline, manifest, wallMs: performance.now() - t0, source: 'sim' });
    } else {
      const log = EventLog.parse(req.log);
      const events = [...log.events].sort((a, b) => a.t - b.t);
      const endT = Math.max(log.manifest.durationMs, events.at(-1)?.t ?? 0);
      const timeline = buildTimeline(log.manifest, events, endT);
      post({ id: req.id, ok: true, timeline, manifest: log.manifest, wallMs: performance.now() - t0, source: 'recording' });
    }
  } catch (e) {
    post({ id: req.id, ok: false, error: e instanceof Error ? e.message : String(e) });
  }
};

function post(msg: WorkerResponse) {
  (self as unknown as DedicatedWorkerGlobalScope).postMessage(msg);
}
