import { useCallback, useEffect, useLayoutEffect, useRef, useState } from 'react';

/**
 * Sim-time clock. `tRef` advances every animation frame (canvas reads it
 * directly); `t` state is throttled to ~12 Hz for React panels.
 */
export function usePlayback(endT: number, opts: { follow?: boolean } = {}) {
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
      if (now - lastPublish > 80 || next >= end) {
        lastPublish = now;
        setT(next);
      }
      if (!follow && next >= end) {
        setPlaying(false);
        return;
      }
      raf = requestAnimationFrame(tick);
    };
    raf = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(raf);
  }, [playing, follow]);

  const seek = useCallback((to: number) => {
    const v = Math.max(0, Math.min(endRef.current, to));
    tRef.current = v;
    setT(v);
  }, []);

  return { tRef, t, playing, setPlaying, speed, setSpeed, seek };
}
