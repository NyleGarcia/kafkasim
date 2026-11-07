import { Minus, Plus } from 'lucide-react';
import { useId, useState, type ReactNode } from 'react';

import { Button } from '@/components/ui/button';
import { Checkbox } from '@/components/ui/checkbox';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip';
import { cn } from '@/lib/utils';

export function Field({ label, hint, htmlFor, children, className }: { label: string; hint?: string; htmlFor?: string; children: ReactNode; className?: string }) {
  return (
    <div className={cn('flex min-w-0 flex-col gap-1', className)}>
      <Label htmlFor={htmlFor} className="text-[11px] font-medium text-muted-foreground">
        {hint ? (
          <Tooltip>
            <TooltipTrigger asChild>
              <span className="cursor-help underline decoration-dotted underline-offset-2">{label}</span>
            </TooltipTrigger>
            <TooltipContent className="max-w-xs text-xs">{hint}</TooltipContent>
          </Tooltip>
        ) : (
          label
        )}
      </Label>
      {children}
    </div>
  );
}

/** Text that commits on blur / Enter (renames cascade, so not per keystroke). */
export function TextField({ label, hint, value, onCommit, pattern, className, placeholder }: { label: string; hint?: string; value: string; onCommit: (v: string) => void; pattern?: RegExp; className?: string; placeholder?: string }) {
  const id = useId();
  const [draft, setDraft] = useState<string | null>(null);
  const v = draft ?? value;
  const bad = pattern ? !pattern.test(v) : false;
  const commit = () => {
    if (draft !== null && !bad && draft !== value) onCommit(draft);
    setDraft(null);
  };
  return (
    <Field label={label} hint={hint} htmlFor={id} className={className}>
      <Input id={id} value={v} placeholder={placeholder} aria-invalid={bad} onChange={(e) => setDraft(e.target.value)} onBlur={commit} onKeyDown={(e) => e.key === 'Enter' && commit()} className="h-8 text-sm" />
    </Field>
  );
}

/** Number with optional unit scaling (e.g. ms stored, seconds shown). Commits on blur / Enter. */
export function NumberField({
  label,
  hint,
  value,
  onCommit,
  min,
  max,
  step = 1,
  scale = 1,
  unit,
  integer = true,
  className,
}: {
  label: string;
  hint?: string;
  value: number;
  onCommit: (v: number) => void;
  min?: number;
  max?: number;
  step?: number;
  scale?: number;
  unit?: string;
  integer?: boolean;
  className?: string;
}) {
  const id = useId();
  const [draft, setDraft] = useState<string | null>(null);
  const shown = draft ?? String(+(value / scale).toFixed(3));
  const parsed = Number(shown);
  const bad = shown.trim() === '' || !Number.isFinite(parsed) || (min !== undefined && parsed < min) || (max !== undefined && parsed > max);
  const commit = () => {
    if (draft !== null && !bad) {
      const raw = parsed * scale;
      onCommit(integer ? Math.round(raw) : raw);
    }
    setDraft(null);
  };
  return (
    <Field label={label} hint={hint} htmlFor={id} className={className}>
      <div className="flex items-center gap-1">
        <Input
          id={id}
          type="number"
          inputMode="decimal"
          value={shown}
          min={min}
          max={max}
          step={step}
          aria-invalid={bad}
          onChange={(e) => setDraft(e.target.value)}
          onBlur={commit}
          onKeyDown={(e) => e.key === 'Enter' && commit()}
          className="readout h-8 text-sm"
        />
        {unit && <span className="shrink-0 text-[11px] text-muted-foreground">{unit}</span>}
      </div>
    </Field>
  );
}

export function SelectField<T extends string>({ label, hint, value, options, onChange, className }: { label: string; hint?: string; value: T; options: { value: T; label: string; disabled?: boolean }[]; onChange: (v: T) => void; className?: string }) {
  const id = useId();
  return (
    <Field label={label} hint={hint} htmlFor={id} className={className}>
      <Select value={value} onValueChange={(v) => onChange(v as T)}>
        <SelectTrigger id={id} size="sm" className="w-full">
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          {options.map((o) => (
            <SelectItem key={o.value} value={o.value} disabled={o.disabled}>
              {o.label}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </Field>
  );
}

export function CheckField({ label, hint, checked, onChange, disabled }: { label: string; hint?: string; checked: boolean; onChange: (v: boolean) => void; disabled?: boolean }) {
  const id = useId();
  return (
    <div className="flex items-center gap-2 pt-4">
      <Checkbox id={id} checked={checked} onCheckedChange={(v) => onChange(v === true)} disabled={disabled} />
      <Label htmlFor={id} className="text-xs" title={hint}>
        {label}
      </Label>
    </div>
  );
}

/** − n + stepper with a typeable count (commits on blur / Enter). Fixed width so tables don't reflow. */
export function Stepper({ label, value, onChange, min = 0, max = 64 }: { label: string; value: number; onChange: (n: number) => void; min?: number; max?: number }) {
  const [draft, setDraft] = useState<string | null>(null);
  const commit = () => {
    if (draft === null) return;
    const n = Math.round(Number(draft));
    setDraft(null);
    if (Number.isFinite(n) && n !== value) onChange(Math.max(min, Math.min(max, n)));
  };
  return (
    <div className="flex w-[104px] shrink-0 items-center gap-1" role="group" aria-label={label}>
      <Button type="button" size="icon-xs" variant="outline" disabled={value <= min} onClick={() => onChange(value - 1)} aria-label={`fewer ${label}`}>
        <Minus />
      </Button>
      <input
        type="number"
        inputMode="numeric"
        min={min}
        max={max}
        aria-label={label}
        value={draft ?? value}
        onChange={(e) => setDraft(e.target.value)}
        onBlur={commit}
        onKeyDown={(e) => e.key === 'Enter' && commit()}
        className="readout h-6 w-10 [appearance:textfield] rounded border bg-transparent text-center text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring [&::-webkit-inner-spin-button]:appearance-none"
      />
      <Button type="button" size="icon-xs" variant="outline" disabled={value >= max} onClick={() => onChange(value + 1)} aria-label={`more ${label}`}>
        <Plus />
      </Button>
    </div>
  );
}

/** Multi-select chips; `order` shows the selection order (e.g. preferred leader = 1). */
export function Chips({ items, selected, onToggle, tone = 'nominal', order = false, label }: { items: string[]; selected: string[]; onToggle: (id: string) => void; tone?: 'nominal' | 'observer' | 'info' | 'warning'; order?: boolean; label: string }) {
  const tones = {
    nominal: 'border-nominal/60 bg-nominal/15 text-nominal',
    observer: 'border-observer/60 bg-observer/15 text-observer',
    info: 'border-info/60 bg-info/15 text-info',
    warning: 'border-warning/60 bg-warning/15 text-warning',
  };
  return (
    <div className="flex flex-wrap gap-1" role="group" aria-label={label}>
      {items.map((id) => {
        const i = selected.indexOf(id);
        const on = i >= 0;
        return (
          <button
            key={id}
            type="button"
            aria-pressed={on}
            onClick={() => onToggle(id)}
            className={cn('readout inline-flex h-6 items-center gap-1 rounded border px-1.5 text-[11px] transition-colors focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none', on ? tones[tone] : 'border-border text-muted-foreground hover:bg-muted')}
          >
            {order && on && <span className="text-[9px] opacity-80">{i + 1}</span>}
            {id}
          </button>
        );
      })}
    </div>
  );
}

export function Section({ title, description, actions, children }: { title: string; description?: string; actions?: ReactNode; children: ReactNode }) {
  return (
    <section className="space-y-3">
      <header className="flex items-start justify-between gap-3">
        <div>
          <h3 className="text-sm font-semibold">{title}</h3>
          {description && <p className="mt-0.5 max-w-[70ch] text-xs text-muted-foreground">{description}</p>}
        </div>
        {actions}
      </header>
      {children}
    </section>
  );
}

export function Card({ children, className }: { children: ReactNode; className?: string }) {
  return <div className={cn('space-y-3 rounded-lg border bg-muted/30 p-3', className)}>{children}</div>;
}
