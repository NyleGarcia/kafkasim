import type { ActionT, Manifest, NodeView } from '@kafkasim/engine';

/** ActionT minus `at`, distributed over the union (plain Omit collapses it). */
export type ActionDraft = ActionT extends infer A ? (A extends ActionT ? Omit<A, 'at'> : never) : never;

export interface FaultOption {
  label: string;
  hint: string;
  tone?: 'critical' | 'warning';
  action: ActionDraft;
}

/** Fault menu for a node, given its current state. */
export function nodeFaults(m: Manifest, n: NodeView): FaultOption[] {
  const out: FaultOption[] = [];
  if (n.status === 'running') {
    out.push({ label: 'Kill (SIGKILL)', hint: 'crash; log survives on disk', tone: 'critical', action: { type: 'KILL_NODE', node: n.id } });
    out.push({ label: 'Pause (SIGSTOP)', hint: 'zombie: holds sockets, does no work', tone: 'warning', action: { type: 'PAUSE_NODE', node: n.id } });
    const other = m.regions.find((r) => r.id !== n.region) ?? m.regions[0]!;
    out.push({ label: 'Isolate from network', hint: 'partition this node away from everyone', tone: 'warning', action: { type: 'PARTITION', groups: [[other.id], [n.id]] } });
  }
  if (n.status === 'dead') out.push({ label: 'Restart', hint: 'new incarnation; re-registers fenced, then catches up', action: { type: 'RESTART_NODE', node: n.id } });
  if (n.status === 'paused') out.push({ label: 'Resume (SIGCONT)', hint: 'wakes up with stale state + buffered requests', action: { type: 'RESUME_NODE', node: n.id } });
  if (n.kind === 'producer') {
    out.push({ label: 'Stop producing', hint: 'producer goes idle', action: { type: 'STOP_PRODUCER', producer: n.id } });
    out.push({ label: 'Start producing', hint: 'resume record generation', action: { type: 'START_PRODUCER', producer: n.id } });
  }
  return out;
}

export function regionFaults(m: Manifest, region: string, partitioned: boolean): FaultOption[] {
  const out: FaultOption[] = [];
  const others = m.regions.filter((r) => r.id !== region).map((r) => r.id);
  if (others.length) out.push({ label: `Isolate ${region}`, hint: 'cut this region off from all others', tone: 'warning', action: { type: 'PARTITION', groups: [others, [region]] } });
  if (partitioned) out.push({ label: 'Heal network', hint: 'remove all partitions', action: { type: 'HEAL' } });
  return out;
}
