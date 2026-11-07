import type { SimEvent } from '@kafkasim/engine';

import type { LogLevel } from '@/components/ui/log-viewer';

export interface Described {
  level: LogLevel;
  source: string;
  text: string;
}

/** Human text for notable events (the log panel + timeline markers). */
export function describe(e: SimEvent): Described | null {
  switch (e.type) {
    case 'NODE_STATUS':
      return { level: e.status === 'running' ? 'success' : e.status === 'dead' ? 'error' : 'warn', source: e.node, text: `${e.status.toUpperCase()}${e.reason ? ` (${e.reason})` : ''}` };
    case 'BROKER_FENCE':
      return { level: e.fenced ? 'warn' : 'success', source: e.cluster, text: `${e.node} ${e.fenced ? 'fenced' : 'unfenced'}${e.reason ? `: ${e.reason}` : ''}` };
    case 'CONTROLLER_ELECTED':
      return { level: 'info', source: e.cluster, text: `controller ${e.node} active (epoch ${e.epoch})` };
    case 'CONTROLLER_LOST':
      return { level: 'warn', source: e.cluster, text: `controller ${e.node} lost leadership` };
    case 'LEADER_CHANGE':
      if (e.epoch === 0) return null;
      return {
        level: e.leader === null || e.unclean ? 'error' : 'info',
        source: e.tp,
        text: e.leader === null ? `OFFLINE (epoch ${e.epoch})` : `leader → ${e.leader} (epoch ${e.epoch}${e.unclean ? ', UNCLEAN' : ''})`,
      };
    case 'ISR_CHANGE':
      if (e.reason === 'created') return null;
      return { level: 'debug', source: e.tp, text: `ISR [${e.isr.join(', ')}]${e.reason ? ` — ${e.reason}` : ''}` };
    case 'TRUNCATE':
      return { level: 'warn', source: e.tp, text: `${e.node} truncated ${e.from} → ${e.to}` };
    case 'DATA_LOSS':
      return { level: 'error', source: e.tp, text: `${e.count} ACKED RECORDS LOST — ${e.reason}` };
    case 'PRODUCE_ERROR':
      if (e.retriable) return null;
      return { level: 'error', source: e.producer, text: `${e.count} records failed: ${e.error}` };
    case 'MIRROR_PROMOTED':
      return { level: e.rpoRecords ? 'error' : 'success', source: e.tp, text: `mirror promoted, RPO ${e.rpoRecords} records` };
    case 'NETWORK':
      return { level: e.partitioned ? 'warn' : 'success', source: 'network', text: e.partitioned ? `partitioned: ${(e.groups ?? []).map((g) => `[${g.join(' ')}]`).join(' | ')}` : 'healed' };
    case 'ACTION':
      return { level: 'info', source: 'action', text: e.action.replace(/_/g, ' ').toLowerCase() + ' ' + actionTarget(e.detail) };
    case 'K8S_NODE':
      return { level: e.ready ? (e.schedulable ? 'success' : 'warn') : 'error', source: e.node, text: `worker ${e.ready ? 'Ready' : 'NotReady'}${e.schedulable ? '' : ', unschedulable'}` };
    case 'POD':
      if (e.t === 0) return null;
      return {
        level: e.phase === 'Running' ? 'success' : e.phase === 'Pending' || e.phase === 'Terminating' ? 'warn' : e.phase === 'ContainerCreating' ? 'info' : 'error',
        source: `pod/${e.pod}`,
        text: `${e.phase}${e.worker ? ` on ${e.worker}` : ''}${e.reason ? ` — ${e.reason}` : ''}`,
      };
    case 'REPLICAS_CHANGE':
      return { level: 'info', source: e.tp, text: `replicas → [${e.replicas.join(', ')}]` };
    case 'NOTE':
      // warn/error notes mostly duplicate structured events; platform notes don't
      if (e.level === 'info' || /Self-Balancing|NotReady|stuck/.test(e.message)) return { level: e.level, source: e.node ?? 'note', text: e.message };
      return null;
    default:
      return null;
  }
}

function actionTarget(detail: string) {
  try {
    const a = JSON.parse(detail) as Record<string, unknown>;
    return String(a.node ?? a.producer ?? a.topic ?? (a.groups ? JSON.stringify(a.groups) : '') ?? '');
  } catch {
    return '';
  }
}

export const fmtT = (ms: number) => {
  const s = ms / 1000;
  return `${Math.floor(s / 60)}:${(s % 60).toFixed(2).padStart(5, '0')}`;
};
