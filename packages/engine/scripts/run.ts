/** Headless sim: `tsx scripts/run.ts manifest.json [out.json]` → EventLog JSON (no MSG events). Used by harness/calibrate. */
import { readFileSync, writeFileSync } from 'node:fs';

import { parseManifest, simulate } from '../src/index.ts';

const [, , input, out] = process.argv;
if (!input) {
  console.error('usage: tsx scripts/run.ts manifest.json [out.json]');
  process.exit(2);
}
const manifest = parseManifest(JSON.parse(readFileSync(input, 'utf8')));
const r = simulate(manifest, { omitMessages: true });
const log = JSON.stringify({ schemaVersion: 1, source: 'sim', manifest, events: r.events });
if (out) writeFileSync(out, log);
else process.stdout.write(log);
