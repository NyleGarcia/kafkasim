import { initialView, parseManifest, scenarios } from '@kafkasim/engine';
import { describe as group, expect, it } from 'vitest';

import { describe } from './describe';
import { nodeFaults, regionFaults } from './faults';

const m = parseManifest(scenarios.find((s) => s.name.startsWith('Stretch'))!);
const v = initialView(m);

group('fault menus', () => {
  it('running broker can be killed, paused or isolated; actions validate as manifest actions', () => {
    const opts = nodeFaults(m, v.nodes.e1!);
    expect(opts.map((o) => o.action.type)).toEqual(['KILL_NODE', 'PAUSE_NODE', 'PARTITION']);
    const withActions = { ...structuredClone(scenarios.find((s) => s.name.startsWith('Stretch'))!), actions: opts.map((o) => ({ ...o.action, at: 1000 })) };
    expect(() => parseManifest(withActions)).not.toThrow();
  });
  it('dead broker only restarts', () => {
    expect(nodeFaults(m, { ...v.nodes.e1!, status: 'dead' }).map((o) => o.action.type)).toEqual(['RESTART_NODE']);
  });
  it('region isolation lists every other region in group 0; heal only when partitioned', () => {
    const [iso] = regionFaults(m, 'west', false);
    expect(iso!.action).toEqual({ type: 'PARTITION', groups: [['east', 'witness'], ['west']] });
    expect(regionFaults(m, 'west', true).at(-1)!.action.type).toBe('HEAL');
  });
});

group('event descriptions', () => {
  it('flags data loss as error and hides retriable produce errors', () => {
    expect(describe({ t: 1, type: 'DATA_LOSS', tp: 'a/b/0', count: 3, reason: 'x' })!.level).toBe('error');
    expect(describe({ t: 1, type: 'PRODUCE_ERROR', producer: 'p', tp: 'a/b/0', count: 1, error: 'NOT_LEADER_OR_FOLLOWER', retriable: true })).toBeNull();
  });
});
