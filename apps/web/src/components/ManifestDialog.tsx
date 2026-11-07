import { parseManifest } from '@kafkasim/engine';
import { useState } from 'react';

import { Button } from '@/components/ui/button';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog';

interface Props {
  open: boolean;
  initial: unknown;
  onOpenChange: (o: boolean) => void;
  onApply: (manifest: unknown) => void;
}

/** Edit / paste / upload a scenario manifest (contracts/manifest.schema.json). */
export function ManifestDialog({ open, initial, onOpenChange, onApply }: Props) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      {/* Radix unmounts closed content, so the editor re-initialises from `initial` on each open */}
      {open && <Editor initial={initial} onDone={() => onOpenChange(false)} onApply={onApply} />}
    </Dialog>
  );
}

function Editor({ initial, onDone, onApply }: { initial: unknown; onDone: () => void; onApply: (m: unknown) => void }) {
  const [text, setText] = useState(() => JSON.stringify(initial, null, 2));
  const [error, setError] = useState<string | null>(null);

  const apply = () => {
    try {
      const json = JSON.parse(text);
      parseManifest(json);
      onApply(json);
      onDone();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  return (
    <DialogContent className="max-w-3xl">
        <DialogHeader>
          <DialogTitle>Scenario manifest</DialogTitle>
          <DialogDescription>
            JSON matching <code className="readout">contracts/manifest.schema.json</code>. Topology, client configs and a timed list of actions. Validated before run.
          </DialogDescription>
        </DialogHeader>
        <textarea
          value={text}
          onChange={(e) => setText(e.target.value)}
          spellCheck={false}
          aria-label="Manifest JSON"
          aria-invalid={!!error}
          aria-describedby={error ? 'manifest-err' : undefined}
          className="readout h-[55vh] w-full resize-none rounded-md border bg-muted/40 p-3 text-xs outline-none focus-visible:ring-2 focus-visible:ring-ring"
        />
        {error && (
          <pre id="manifest-err" role="alert" className="max-h-32 overflow-auto rounded-md border border-critical/40 bg-critical/10 p-2 text-xs whitespace-pre-wrap text-critical">
            {error}
          </pre>
        )}
        <DialogFooter className="gap-2 sm:justify-between">
          <label className="cursor-pointer text-sm text-muted-foreground hover:text-foreground">
            <input
              type="file"
              accept="application/json,.json"
              className="sr-only"
              onChange={async (e) => {
                const f = e.target.files?.[0];
                if (f) setText(await f.text());
              }}
            />
            Load file…
          </label>
          <Button onClick={apply}>Validate &amp; run</Button>
        </DialogFooter>
    </DialogContent>
  );
}
