import type { WorkerRequest, WorkerResponse } from './worker';

type Ok = Extract<WorkerResponse, { ok: true }>;
type Req = WorkerRequest extends infer R ? (R extends { id: number } ? Omit<R, 'id'> : never) : never;

/** Promise wrapper over the sim worker. Stale responses are dropped by id. */
export class SimRunner {
  private worker = new Worker(new URL('./worker.ts', import.meta.url), { type: 'module' });
  private seq = 0;
  private pending = new Map<number, { resolve: (r: Ok) => void; reject: (e: Error) => void }>();

  constructor() {
    this.worker.onmessage = (ev: MessageEvent<WorkerResponse>) => {
      const p = this.pending.get(ev.data.id);
      if (!p) return;
      this.pending.delete(ev.data.id);
      if (ev.data.ok) p.resolve(ev.data);
      else p.reject(new Error(ev.data.error));
    };
  }

  run(req: Req): Promise<Ok> {
    const id = ++this.seq;
    // only the latest request matters; reject older ones
    for (const [k, p] of this.pending) {
      p.reject(new Error('superseded'));
      this.pending.delete(k);
    }
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.worker.postMessage({ ...req, id } as WorkerRequest);
    });
  }

  dispose() {
    this.worker.terminate();
  }
}
