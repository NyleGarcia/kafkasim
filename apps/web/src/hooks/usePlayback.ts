import { useCallback, useEffect, useLayoutEffect, useRef, useState } from 'react';

/**
 * Sim-time clock. `tRef` advances every animation frame (canvas reads it
 * directly); `t` state is throttled to ~12 Hz for React panels.
 */
/**
 * `hold`: at the end keep the clock armed instead of stopping, so playback resumes
 * by itself when the timeline grows (continuous mode extends the simulation).
 */
export function usePlayback(endT: number, opts: { follow?: boolean; hold?: boolean; onNearEnd?: (endT: number) => void } = {}) {
  const tRef = useRef(0);
  const [t, setT] = useState(0);
  const [playing, setPlaying] = useState(false);
  const [speed, setSpeed] = useState(1);
  const speedRef = useRef(speed);
  const endRef = useRef(endT);
  useLayoutEffect(() => {
    speedRef.current = speed;
    endRef.current = endT;
  }, [speed, endT]);
  const follow = !!opts.follow;
  const hold = !!opts.hold;
  const nearEnd = useRef(opts.onNearEnd);
  const firedFor = useRef(-1);
  useLayoutEffect(() => {
    nearEnd.current = opts.onNearEnd;
  });

  useEffect(() => {
    if (!playing) return;
    let raf = 0;
    let last = performance.now();
    let lastPublish = 0;
    const tick = (now: number) => {
      const dt = now - last;
      last = now;
      const end = endRef.current;
      const next = follow ? end : Math.min(end, tRef.current + dt * speedRef.current);
      tRef.current = next;
      // continuous mode: ask for more timeline ~3 s of wall clock before the end (once per end value)
      if (hold && nearEnd.current && firedFor.current !== end && next > end - 3_000 * Math.max(1, speedRef.current)) {
        firedFor.current = end;
        nearEnd.current(end);
      }
      if (now - lastPublish > 80 || next >= end) {
        lastPublish = now;
        setT(next);
      }
      if (!follow && !hold && next >= end) {
        setPlaying(false);
        return;
      }
      raf = requestAnimationFrame(tick);
    };
    raf = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(raf);
  }, [playing, follow, hold]);

  const seek = useCallback((to: number) => {
    const v = Math.max(0, Math.min(endRef.current, to));
    tRef.current = v;
    setT(v);
  }, []);

  return { tRef, t, playing, setPlaying, speed, setSpeed, seek };
}
