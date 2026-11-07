/** Seeded PRNG (mulberry32) — deterministic runs per manifest.seed. */
export function rng(seed: number) {
  let a = seed >>> 0 || 0x9e3779b9;
  const next = () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
  return {
    next,
    between: (lo: number, hi: number) => lo + (hi - lo) * next(),
    pick: <T>(xs: readonly T[]): T => xs[Math.floor(next() * xs.length)]!,
  };
}
export type Rng = ReturnType<typeof rng>;

/** Min-heap keyed by (t, seq): ties resolve in scheduling order → determinism. */
export class EventQueue<T> {
  private h: { t: number; s: number; v: T }[] = [];
  private seq = 0;
  get size() {
    return this.h.length;
  }
  push(t: number, v: T) {
    const h = this.h;
    h.push({ t, s: this.seq++, v });
    let i = h.length - 1;
    while (i > 0) {
      const p = (i - 1) >> 1;
      if (this.lt(h[p]!, h[i]!)) break;
      [h[p], h[i]] = [h[i]!, h[p]!];
      i = p;
    }
  }
  peekT(): number | undefined {
    return this.h[0]?.t;
  }
  pop(): { t: number; v: T } | undefined {
    const h = this.h;
    if (!h.length) return undefined;
    const top = h[0]!;
    const last = h.pop()!;
    if (h.length) {
      h[0] = last;
      let i = 0;
      for (;;) {
        const l = 2 * i + 1,
          r = l + 1;
        let m = i;
        if (l < h.length && this.lt(h[l]!, h[m]!)) m = l;
        if (r < h.length && this.lt(h[r]!, h[m]!)) m = r;
        if (m === i) break;
        [h[m], h[i]] = [h[i]!, h[m]!];
        i = m;
      }
    }
    return { t: top.t, v: top.v };
  }
  private lt(a: { t: number; s: number }, b: { t: number; s: number }) {
    return a.t < b.t || (a.t === b.t && a.s < b.s);
  }
}

export function percentile(sorted: number[], p: number): number {
  if (!sorted.length) return 0;
  const i = Math.min(sorted.length - 1, Math.max(0, Math.ceil((p / 100) * sorted.length) - 1));
  return sorted[i]!;
}
