// Adapted from 21st.dev "Number Ticker" — danielpetho (fancycomponents.dev)
// Changes: tweens from the previous value whenever `value` changes (live telemetry),
// instead of a one-shot from→target; formatter support; instant when scrubbing.
import { animate, motion, useMotionValue, useTransform } from 'motion/react';
import { useEffect } from 'react';

import { cn } from '@/lib/utils';

interface NumberTickerProps {
  value: number;
  /** ms; 0 = jump (e.g. while scrubbing) */
  duration?: number;
  format?: (n: number) => string;
  className?: string;
}

const defaultFormat = (n: number) => Math.round(n).toLocaleString('en-US');

export function NumberTicker({ value, duration = 350, format = defaultFormat, className }: NumberTickerProps) {
  const mv = useMotionValue(value);
  const text = useTransform(mv, (v) => format(v));
  useEffect(() => {
    if (!duration) {
      mv.set(value);
      return;
    }
    const c = animate(mv, value, { duration: duration / 1000, ease: 'easeOut' });
    return () => c.stop();
  }, [value, duration, mv]);
  return <motion.span className={cn('tabular-nums', className)}>{text}</motion.span>;
}
