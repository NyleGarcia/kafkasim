// Adapted from 21st.dev "Status Dot" — edwinvakayil (iconiq)
// Changes: theme class block removed; tones bound to MFD telemetry tokens; ripple CSS kept.
import { type CSSProperties, type HTMLAttributes, useLayoutEffect } from 'react';

import { cn } from '@/lib/utils';

export type StatusDotTone = 'neutral' | 'active' | 'success' | 'warning' | 'error' | 'observer';

const TONE_VAR: Record<StatusDotTone, string> = {
  neutral: 'var(--color-text-muted)',
  active: 'var(--telemetry-info)',
  success: 'var(--telemetry-nominal)',
  warning: 'var(--telemetry-warning)',
  error: 'var(--telemetry-critical)',
  observer: 'var(--telemetry-observer)',
};

const STYLE_ID = 'status-dot-styles';
const CSS = `
@keyframes status-dot-ripple {
  0% { box-shadow: 0 0 0 0 var(--sd-mid); }
  72% { box-shadow: 0 0 0 var(--sd-spread) transparent; }
  100% { box-shadow: 0 0 0 0 transparent; }
}
.status-dot-ripple {
  --sd-mid: color-mix(in srgb, var(--sd-color) 58%, transparent);
  animation: status-dot-ripple 2.25s ease-out infinite;
}
@media (prefers-reduced-motion: reduce) { .status-dot-ripple { animation: none; } }
`;

const SIZE = { sm: { dot: 6, box: 14, spread: '7px' }, md: { dot: 8, box: 20, spread: '9px' } } as const;

interface StatusDotProps extends HTMLAttributes<HTMLSpanElement> {
  tone: StatusDotTone;
  animate?: boolean;
  label: string;
  showLabel?: boolean;
  size?: keyof typeof SIZE;
}

export function StatusDot({ tone, animate = false, label, showLabel = false, size = 'sm', className, style, ...props }: StatusDotProps) {
  useLayoutEffect(() => {
    if (typeof document === 'undefined' || document.getElementById(STYLE_ID)) return;
    const el = document.createElement('style');
    el.id = STYLE_ID;
    el.textContent = CSS;
    document.head.appendChild(el);
  }, []);
  const s = SIZE[size];
  const color = { ['--sd-color' as string]: TONE_VAR[tone] } as CSSProperties;
  return (
    <span role="status" aria-label={showLabel ? undefined : label} className={cn('inline-flex items-center gap-1.5', className)} style={{ ...style, ...color }} {...props}>
      <span aria-hidden className="relative shrink-0" style={{ width: s.box, height: s.box }}>
        {animate &&
          [0, 1.125].map((d) => (
            <span
              key={d}
              className="status-dot-ripple absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 rounded-full"
              style={{ width: s.dot, height: s.dot, animationDelay: `${d}s`, ['--sd-spread' as string]: s.spread }}
            />
          ))}
        <span className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 rounded-full" style={{ width: s.dot, height: s.dot, background: 'var(--sd-color)' }} />
      </span>
      {showLabel && <span className="text-xs font-medium text-muted-foreground">{label}</span>}
    </span>
  );
}
