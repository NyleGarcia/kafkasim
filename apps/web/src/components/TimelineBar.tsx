import type { SimEvent, Timeline } from '@kafkasim/engine';
import { Pause, Play, RotateCcw, SkipForward } from 'lucide-react';
import { useMemo } from 'react';

import { Button } from '@/components/ui/button';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Slider } from '@/components/ui/slider';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip';
import { describe, fmtT } from '@/lib/describe';
import { cn } from '@/lib/utils';

const SPEEDS = [0.05, 0.1, 0.25, 0.5, 1, 2, 4, 8];

interface Props {
  timeline: Timeline;
  t: number;
  playing: boolean;
  speed: number;
  live: boolean;
  onPlay: (p: boolean) => void;
  onSeek: (t: number) => void;
  onSpeed: (s: number) => void;
  injectedAt: number[];
  /** Simulate mode only: run length + continuous toggle */
  length?: { current: number; scenario: number; max: number; continuous: boolean };
  onLength?: (ms: number | null) => void;
  onContinuous?: (on: boolean) => void;
}

const LENGTHS = [2, 5, 10, 30].map((m) => m * 60_000);
const fmtLen = (ms: number) => (ms >= 60_000 ? `${+(ms / 60_000).toFixed(1)} min` : `${Math.round(ms / 1000)} s`);

export function TimelineBar({ timeline, t, playing, speed, live, onPlay, onSeek, onSpeed, injectedAt, length, onLength, onContinuous }: Props) {
  const end = Math.max(1, timeline.endT);
  const markers = useMemo(() => {
    const out: { t: number; tone: string; label: string; e: SimEvent }[] = [];
    for (const e of timeline.stateEvents) {
      if (e.type === 'ACTION') out.push({ t: e.t, tone: 'bg-info', label: describe(e)?.text ?? e.action, e });
      else if (e.type === 'DATA_LOSS') out.push({ t: e.t, tone: 'bg-critical', label: `${e.count} acked records lost`, e });
      else if (e.type === 'LEADER_CHANGE' && e.epoch > 0) out.push({ t: e.t, tone: e.leader ? 'bg-muted-foreground' : 'bg-critical', label: describe(e)?.text ?? '', e });
      else if (e.type === 'MIRROR_PROMOTED') out.push({ t: e.t, tone: 'bg-observer', label: `promoted, RPO ${e.rpoRecords}`, e });
    }
    // de-dupe markers closer than 0.4% of the track
    const dedup: typeof out = [];
    for (const m of out) if (!dedup.some((d) => d.tone === m.tone && Math.abs(d.t - m.t) < end * 0.004)) dedup.push(m);
    return dedup;
  }, [timeline, end]);

  return (
    <div className="panel flex flex-col gap-2 px-3 py-2">
      <div className="flex items-center gap-2">
        <Button size="icon-sm" variant="secondary" onClick={() => onPlay(!playing)} aria-label={playing ? 'Pause' : 'Play'} aria-keyshortcuts="Space">
          {playing ? <Pause /> : <Play />}
        </Button>
        {!live && (
          <Button size="icon-sm" variant="ghost" onClick={() => onSeek(0)} aria-label="Restart from 0">
            <RotateCcw />
          </Button>
        )}
        {live && (
          <Button size="sm" variant="ghost" onClick={() => onSeek(end)} aria-label="Jump to live">
            <SkipForward /> live
          </Button>
        )}
        <span className="readout min-w-28 text-sm">
          {fmtT(t)} <span className="text-muted-foreground">/ {fmtT(end)}</span>
        </span>
        <div className="ml-auto flex flex-wrap items-center gap-2">
          {length && onLength && onContinuous && (
            <>
              <label className="flex items-center gap-1.5 text-xs text-muted-foreground" title={`When playback reaches the end, the simulation is extended (deterministically) up to ${fmtLen(length.max)}`}>
                <input type="checkbox" checked={length.continuous} onChange={(e) => onContinuous(e.target.checked)} className="accent-primary" />
                keep running
              </label>
              <span className="text-xs text-muted-foreground">length</span>
              <Select
                value={LENGTHS.includes(length.current) ? String(length.current) : length.current === length.scenario ? 'scenario' : 'current'}
                onValueChange={(v) => onLength(v === 'scenario' ? null : v === 'current' ? length.current : Number(v))}
              >
                <SelectTrigger size="sm" className="readout w-32" aria-label="Run length">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="scenario">scenario ({fmtLen(length.scenario)})</SelectItem>
                  {!LENGTHS.includes(length.current) && length.current !== length.scenario && <SelectItem value="current">{fmtLen(length.current)} (extended)</SelectItem>}
                  {LENGTHS.map((ms) => (
                    <SelectItem key={ms} value={String(ms)}>
                      {fmtLen(ms)}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </>
          )}
          <span className="text-xs text-muted-foreground">speed</span>
          <Select value={String(speed)} onValueChange={(v) => onSpeed(Number(v))}>
            <SelectTrigger size="sm" className="readout w-20" aria-label="Playback speed">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {SPEEDS.map((s) => (
                <SelectItem key={s} value={String(s)} className="readout">
                  {s}×
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </div>
      <div className="relative pt-3">
        <div className="pointer-events-none absolute inset-x-0 top-0 h-3">
          {markers.map((m, i) => (
            <Tooltip key={i}>
              <TooltipTrigger asChild>
                <button
                  type="button"
                  className={cn('pointer-events-auto absolute top-0 h-3 w-1 -translate-x-1/2 rounded-sm opacity-80 hover:opacity-100', m.tone)}
                  style={{ left: `${(m.t / end) * 100}%` }}
                  onClick={() => onSeek(Math.max(0, m.t - 300))}
                  aria-label={`${fmtT(m.t)} ${m.label}`}
                />
              </TooltipTrigger>
              <TooltipContent side="top" className="readout text-xs">
                {fmtT(m.t)} · {m.label}
              </TooltipContent>
            </Tooltip>
          ))}
          {injectedAt.map((x, i) => (
            <span key={`inj${i}`} className="absolute -top-1 h-0 w-0 -translate-x-1/2 border-x-4 border-t-4 border-x-transparent border-t-primary" style={{ left: `${(x / end) * 100}%` }} aria-hidden />
          ))}
        </div>
        <Slider value={[t]} min={0} max={end} step={10} onValueChange={([v]) => onSeek(v!)} aria-label="Simulation time" />
      </div>
    </div>
  );
}
