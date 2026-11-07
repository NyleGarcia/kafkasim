import { BridgeFrame, buildTimeline, parseManifest, type Manifest, type SimEvent, type Timeline } from '@kafkasim/engine';
import { useCallback, useEffect, useRef, useState } from 'react';

export type LiveStatus = 'idle' | 'connecting' | 'open' | 'closed' | 'error';

/**
 * Live mode: connect to a local kafkasim-bridge over WebSocket. Frames are the
 * same SimEvent contract the engine emits, so the renderer is shared.
 * Timeline is rebuilt at most once a second (cheap: pure reduction).
 */
export function useLiveBridge() {
  const [status, setStatus] = useState<LiveStatus>('idle');
  const [error, setError] = useState<string | null>(null);
  const [manifest, setManifest] = useState<Manifest | null>(null);
  const [timeline, setTimeline] = useState<Timeline | null>(null);
  const [bridge, setBridge] = useState<string | null>(null);
  const ws = useRef<WebSocket | null>(null);
  const events = useRef<SimEvent[]>([]);
  const dirty = useRef(false);
  const manifestRef = useRef<Manifest | null>(null);

  const disconnect = useCallback(() => {
    ws.current?.close();
    ws.current = null;
  }, []);

  const connect = useCallback((url: string) => {
    ws.current?.close();
    events.current = [];
    setTimeline(null);
    setError(null);
    setStatus('connecting');
    let sock: WebSocket;
    try {
      sock = new WebSocket(url);
    } catch (e) {
      setStatus('error');
      setError(e instanceof Error ? e.message : String(e));
      return;
    }
    ws.current = sock;
    sock.onopen = () => setStatus('open');
    sock.onerror = () => {
      setStatus('error');
      setError(`Could not reach ${url}. Is kafkasim-bridge running? (Safari blocks ws://localhost from https pages; use Chrome/Firefox.)`);
    };
    sock.onclose = () => setStatus((s) => (s === 'error' ? s : 'closed'));
    sock.onmessage = (ev) => {
      let frame: BridgeFrame;
      try {
        frame = BridgeFrame.parse(JSON.parse(String(ev.data)));
      } catch (e) {
        setError(`bad frame: ${e instanceof Error ? e.message.slice(0, 200) : e}`);
        return;
      }
      if (frame.kind === 'hello') {
        const m = parseManifest(frame.manifest);
        manifestRef.current = m;
        setManifest(m);
        setBridge(frame.bridge);
        events.current = [];
      } else if (frame.kind === 'events') {
        events.current.push(...frame.events);
        dirty.current = true;
      } else setError(frame.message);
    };
  }, []);

  useEffect(() => {
    const id = window.setInterval(() => {
      const m = manifestRef.current;
      if (!dirty.current || !m) return;
      dirty.current = false;
      const evs = events.current;
      const endT = evs.at(-1)?.t ?? 0;
      setTimeline(buildTimeline(m, evs, endT, 1000));
    }, 1000);
    return () => {
      window.clearInterval(id);
      ws.current?.close();
    };
  }, []);

  return { status, error, manifest, timeline, bridge, connect, disconnect };
}
