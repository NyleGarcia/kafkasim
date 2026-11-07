import type { ActionT, Manifest, NodeView, WorkerView } from '@kafkasim/engine';

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
  if (n.pod) {
    // Kubernetes pod: crashes self-heal, deletes reschedule
    if (n.pod.phase === 'Running' && n.status === 'running') {
      out.push({ label: 'Crash container', hint: 'kubelet restarts it in place (back-off on repeats)', tone: 'critical', action: { type: 'CONTAINER_CRASH', pod: n.id } });
      out.push({ label: 'Delete pod (drain)', hint: 'controlled shutdown, then StatefulSet recreates and reschedules it', tone: 'warning', action: { type: 'POD_DELETE', pod: n.id } });
      out.push({ label: 'Pause (SIGSTOP)', hint: 'zombie: holds sockets, does no work', tone: 'warning', action: { type: 'PAUSE_NODE', node: n.id } });
    }
    if (n.status === 'paused') out.push({ label: 'Resume (SIGCONT)', hint: 'wakes up with stale state + buffered requests', action: { type: 'RESUME_NODE', node: n.id } });
    if (n.pod.phase === 'Unknown' || n.pod.phase === 'Pending') out.push({ label: 'Force-delete pod', hint: 'lets the StatefulSet recreate it (needs a worker in its PVC zone)', tone: 'warning', action: { type: 'POD_DELETE', pod: n.id } });
    return out;
  }
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

export function workerFaults(w: WorkerView): FaultOption[] {
  const out: FaultOption[] = [];
  if (w.ready) out.push({ label: 'Fail worker node', hint: 'pods die; StatefulSet pods stay stuck until the node returns or is deleted', tone: 'critical', action: { type: 'K8S_NODE_FAIL', worker: w.id } });
  else out.push({ label: 'Recover worker node', hint: 'kubelet restarts its pods in place', action: { type: 'K8S_NODE_RECOVER', worker: w.id } });
  out.push({ label: 'Delete node object', hint: 'StatefulSet may recreate its pods elsewhere in the same zone', tone: 'warning', action: { type: 'K8S_NODE_DELETE', worker: w.id } });
  out.push(
    w.schedulable
      ? { label: 'Cordon', hint: 'no new pods scheduled here', tone: 'warning', action: { type: 'K8S_CORDON', target: w.id } }
      : { label: 'Uncordon', hint: 'allow scheduling again', action: { type: 'K8S_UNCORDON', target: w.id } },
  );
  return out;
}

export function zoneFaults(m: Manifest, zone: string): FaultOption[] {
  if (!m.clusters.some((c) => c.platform?.workers.some((w) => w.zone === zone))) return [];
  return [
    { label: `AZ outage: ${zone}`, hint: 'every worker in the zone fails and the zone is cut off', tone: 'critical', action: { type: 'ZONE_OUTAGE', zone } },
    { label: `AZ recover: ${zone}`, hint: 'workers return, network heals', action: { type: 'ZONE_RECOVER', zone } },
    { label: `Cordon ${zone}`, hint: 'mark every worker in the zone unschedulable', tone: 'warning', action: { type: 'K8S_CORDON', target: zone } },
    { label: `Uncordon ${zone}`, hint: 'allow scheduling in the zone again', action: { type: 'K8S_UNCORDON', target: zone } },
  ];
}
