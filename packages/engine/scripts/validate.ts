/** Validate JSON files against the contracts: `tsx scripts/validate.ts <event-log|bridge-frames>.json ...` */
import { readFileSync } from 'node:fs';

import { BridgeFrame, EventLog } from '../src/index.ts';

let bad = 0;
for (const f of process.argv.slice(2)) {
  const doc = JSON.parse(readFileSync(f, 'utf8'));
  const r = Array.isArray(doc) ? BridgeFrame.array().safeParse(doc) : EventLog.safeParse(doc);
  if (r.success) console.log(`ok   ${f}`);
  else {
    bad++;
    console.error(`FAIL ${f}\n${r.error.message.slice(0, 2000)}`);
  }
}
process.exit(bad ? 1 : 0);
