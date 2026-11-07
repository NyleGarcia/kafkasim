import { parseManifest, scenarios } from '@kafkasim/engine';
import { describe, expect, it } from 'vitest';

import * as D from './draft';

const ok = (d: D.Draft | string): D.Draft => {
  if (typeof d === 'string') throw new Error(d);
  return d;
};
const valid = (d: D.Draft) => expect(D.validate(d)).toEqual([]);

describe('scenario builder draft ops', () => {
  it('blank draft is valid and simulates', () => {
    valid(D.blankDraft());
  });

  it('every built-in scenario round-trips as a valid draft', () => {
    for (const s of scenarios) valid(parseManifest(s));
  });

  it('build a 2-region, 2-cluster DR setup without touching JSON', () => {
    let d = D.blankDraft();
    d = D.addRegion(d);
    const west = d.regions[1]!.id;
    d = D.addCluster(d);
    const dr = d.clusters[1]!.id;
    // move DR cluster entirely to west
    d = ok(D.setNodeCount(d, dr, 'brokers', west, 3));
    d = ok(D.setNodeCount(d, dr, 'controllers', west, 3));
    d = ok(D.setNodeCount(d, dr, 'brokers', 'dc1', 0));
    d = ok(D.setNodeCount(d, dr, 'controllers', 'dc1', 0));
    d = D.addTopic(d, dr);
    const t = d.topics.at(-1)!;
    d = D.patchTopic(d, dr, t.name, { mirror: { type: 'mm2', sourceCluster: 'main', sourceTopic: 'orders', fetchIntervalMs: 500 } });
    valid(d);
    expect(d.topics.at(-1)!.partitions).toBe(3); // forced to match source
    expect(d.clusters[1]!.brokers.every((b) => b.region === west)).toBe(true);
  });

  it('refuses to leave a cluster without brokers or remove the last region', () => {
    const d = D.blankDraft();
    expect(typeof D.setNodeCount(d, 'main', 'brokers', 'dc1', 0)).toBe('string');
    expect(typeof D.removeRegion(d, 'dc1')).toBe('string');
  });

  it('removing a broker scrubs custom placement, actions and partition groups', () => {
    let d = D.setCustomPlacement(D.blankDraft(), 'main', 'orders', true);
    d = D.setAction(D.addAction(d), 0, { type: 'KILL_NODE', at: 5000, node: 'b3' });
    d = ok(D.setNodeCount(d, 'main', 'brokers', 'dc1', 2));
    valid(d);
    expect(d.topics[0]!.replicas!.flat()).not.toContain('b3');
    expect(d.actions).toHaveLength(0);
  });

  it('custom placement: toggling keeps ≥1 replica and moves a broker between replica/observer', () => {
    let d = D.patchCluster(D.blankDraft(), 'main', { flavor: 'confluent' });
    d = D.setCustomPlacement(d, 'main', 'orders', true);
    d = D.toggleReplica(d, 'main', 'orders', 0, 'b3', 'observer');
    expect(d.topics[0]!.replicas![0]).not.toContain('b3');
    expect(d.topics[0]!.observers![0]).toEqual(['b3']);
    valid(d);
    for (const b of [...d.topics[0]!.replicas![0]!]) d = D.toggleReplica(d, 'main', 'orders', 0, b, 'replica');
    expect(d.topics[0]!.replicas![0]!.length).toBe(1);
  });

  it('switching a cluster to apache strips Confluent-only topic features', () => {
    let d = D.patchCluster(D.blankDraft(), 'main', { flavor: 'confluent' });
    d = D.setCustomPlacement(d, 'main', 'orders', true);
    d = D.toggleReplica(d, 'main', 'orders', 0, 'b3', 'observer');
    d = D.patchCluster(d, 'main', { flavor: 'apache' });
    expect(d.topics[0]!.observers).toBeUndefined();
    valid(d);
  });

  it('renaming a topic cascades to clients', () => {
    const d = D.renameTopic(D.blankDraft(), 'main', 'orders', 'payments');
    expect(d.producers[0]!.topic).toBe('payments');
    expect(d.consumers[0]!.topic).toBe('payments');
    valid(d);
  });

  it('every action type has a valid default', () => {
    let d = D.addRegion(D.blankDraft());
    for (const type of ['KILL_NODE', 'RESTART_NODE', 'PAUSE_NODE', 'RESUME_NODE', 'PARTITION', 'HEAL', 'SET_LATENCY', 'STOP_PRODUCER', 'START_PRODUCER'] as const) {
      d = D.setAction(D.addAction(d), d.actions.length, D.actionOfType(d, type, 1000));
    }
    valid(d);
  });
});
