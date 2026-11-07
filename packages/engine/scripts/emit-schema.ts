/** Emit JSON Schema + built-in scenarios for non-TS consumers (bridge, harness). */
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { z } from 'zod';
import { BridgeFrame, EventLog, Manifest, SimEvent, parseManifest, scenarios } from '../src/index.ts';

const root = join(dirname(fileURLToPath(import.meta.url)), '../../../contracts');
const check = process.argv.includes('--check');
const files: [string, string][] = [
  ['manifest.schema.json', JSON.stringify(z.toJSONSchema(Manifest, { io: 'input' }), null, 2)],
  ['event.schema.json', JSON.stringify(z.toJSONSchema(SimEvent, { io: 'output' }), null, 2)],
  ['event-log.schema.json', JSON.stringify(z.toJSONSchema(EventLog, { io: 'output' }), null, 2)],
  ['bridge-frame.schema.json', JSON.stringify(z.toJSONSchema(BridgeFrame, { io: 'input' }), null, 2)],
];
for (const s of scenarios) {
  const slug = parseManifest(s).name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
  files.push([`scenarios/${slug}.json`, JSON.stringify(s, null, 2)]);
}
let stale = 0;
for (const [rel, body] of files) {
  const path = join(root, rel);
  if (check) {
    let cur = '';
    try {
      cur = readFileSync(path, 'utf8');
    } catch {}
    if (cur !== body + '\n') {
      console.error(`stale: contracts/${rel}`);
      stale++;
    }
    continue;
  }
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, body + '\n');
}
if (check && stale) process.exit(1);
console.log(check ? 'contracts up to date' : `wrote ${files.length} files to contracts/`);
