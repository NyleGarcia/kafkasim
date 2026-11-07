import { expandLocation, locationsOf, type ActionT, type Manifest, type NodeView, type WorkerView } from '@kafkasim/engine';

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
    if (n.pod.phase === 'ContainerCreating' || n.pod.phase === 'CrashLoopBackOff')
      out.push({ label: 'Delete pod', hint: 'StatefulSet recreates it from scratch (restart counter and back-off reset)', tone: 'warning', action: { type: 'POD_DELETE', pod: n.id } });
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

/** Isolate / heal for a region or an AZ (`id` may be either). */
export function regionFaults(m: Manifest, id: string, partitioned: boolean): FaultOption[] {
  const out: FaultOption[] = [];
  const inside = new Set(expandLocation(m, id));
  const others = locationsOf(m)
    .map((l) => l.id)
    .filter((l) => !inside.has(l));
  if (others.length) out.push({ label: `Isolate ${id}`, hint: 'network partition: cut it off from everything else (nodes keep running)', tone: 'warning', action: { type: 'PARTITION', groups: [others, [id]] } });
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

/** Power-level outage of an AZ or a whole region; cordon when it hosts Kubernetes workers. */
export function zoneFaults(m: Manifest, id: string): FaultOption[] {
  const locs = new Set(expandLocation(m, id));
  const isRegion = m.regions.some((r) => r.id === id) && m.regions.find((r) => r.id === id)!.zones?.length;
  const what = isRegion ? 'region' : 'AZ';
  const out: FaultOption[] = [
    { label: `${isRegion ? 'Region' : 'AZ'} outage: ${id}`, hint: `every node and k8s worker in the ${what} goes down and it is cut off`, tone: 'critical', action: { type: 'ZONE_OUTAGE', zone: id } },
    { label: `${isRegion ? 'Region' : 'AZ'} recover: ${id}`, hint: 'power and network return; nodes restart', action: { type: 'ZONE_RECOVER', zone: id } },
  ];
  if (m.clusters.some((c) => c.platform?.workers.some((w) => locs.has(w.zone)))) {
    out.push({ label: `Cordon ${id}`, hint: `mark every worker in the ${what} unschedulable`, tone: 'warning', action: { type: 'K8S_CORDON', target: id } });
    out.push({ label: `Uncordon ${id}`, hint: 'allow scheduling again', action: { type: 'K8S_UNCORDON', target: id } });
  }
  return out;
}
