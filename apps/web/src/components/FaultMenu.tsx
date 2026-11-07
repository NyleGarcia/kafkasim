import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuLabel, DropdownMenuSeparator, DropdownMenuTrigger } from '@/components/ui/dropdown-menu';
import type { FaultOption } from '@/lib/faults';
import { cn } from '@/lib/utils';

interface Props {
  open: boolean;
  at: { x: number; y: number };
  title: string;
  options: FaultOption[];
  onPick: (o: FaultOption) => void;
  onClose: () => void;
}

/** Fault menu anchored at a canvas click position. */
export function FaultMenu({ open, at, title, options, onPick, onClose }: Props) {
  return (
    <DropdownMenu open={open} onOpenChange={(o) => !o && onClose()} modal={false}>
      <DropdownMenuTrigger asChild>
        <span aria-hidden className="pointer-events-none fixed size-px" style={{ left: at.x, top: at.y }} />
      </DropdownMenuTrigger>
      <DropdownMenuContent align="start" sideOffset={4} className="w-60">
        <DropdownMenuLabel className="readout text-xs">{title}</DropdownMenuLabel>
        <DropdownMenuSeparator />
        {options.length === 0 && <div className="px-2 py-1.5 text-xs text-muted-foreground">No faults available in this state.</div>}
        {options.map((o) => (
          <DropdownMenuItem key={o.label} onSelect={() => onPick(o)} className="flex flex-col items-start gap-0">
            <span className={cn('text-sm', o.tone === 'critical' && 'text-critical', o.tone === 'warning' && 'text-warning')}>{o.label}</span>
            <span className="text-[11px] text-muted-foreground">{o.hint}</span>
          </DropdownMenuItem>
        ))}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
