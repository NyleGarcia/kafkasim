import type { ActionT, ClusterT, TopicT } from '@kafkasim/engine';
import { Plus, Trash2 } from 'lucide-react';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Textarea } from '@/components/ui/textarea';
import { cn } from '@/lib/utils';

import * as D from './draft';
import { Card, CheckField, Chips, Field, NumberField, Section, SelectField, Stepper, TextField } from './fields';

export interface SectionProps {
  d: D.Draft;
  set: (d: D.Draft | string) => void;
}

const ID = /^[A-Za-z0-9_.-]+$/;

function RemoveButton({ label, onClick, disabled }: { label: string; onClick: () => void; disabled?: boolean }) {
  return (
    <Button type="button" size="icon-sm" variant="ghost" onClick={onClick} disabled={disabled} aria-label={label} title={label} className="text-muted-foreground hover:text-critical">
      <Trash2 />
    </Button>
  );
}

// ------------------------------------------------------------------ general

export function GeneralSection({ d, set }: SectionProps) {
  return (
    <Section title="Scenario" description="What the scenario is called, how long it runs, and the seed that makes it reproducible.">
      <div className="grid gap-3 sm:grid-cols-[1fr_120px_100px]">
        <TextField label="Name" value={d.name} onCommit={(name) => set(D.setMeta(d, { name }))} />
        <NumberField label="Duration" unit="s" value={d.durationMs} scale={1000} min={1} max={600} onCommit={(durationMs) => set(D.setMeta(d, { durationMs }))} />
        <NumberField label="Seed" hint="Same manifest + seed = identical run" value={d.seed} onCommit={(seed) => set(D.setMeta(d, { seed }))} />
      </div>
      <Field label="Description (shown above the canvas)">
        <Textarea defaultValue={d.description} onBlur={(e) => e.target.value !== d.description && set(D.setMeta(d, { description: e.target.value }))} rows={2} className="text-sm" />
      </Field>
    </Section>
  );
}

// ------------------------------------------------------------------ regions + network

export function RegionsSection({ d, set }: SectionProps) {
  const link = (a: string, b: string) => d.latency.links.find((l) => (l.a === a && l.b === b) || (l.a === b && l.b === a))?.ms;
  return (
    <div className="space-y-6">
      <Section
        title="Regions"
        description="Data centres or availability zones. Brokers, controllers and clients are placed in a region; latency is defined between regions."
        actions={
          <Button size="sm" variant="outline" onClick={() => set(D.addRegion(d))}>
            <Plus /> Region
          </Button>
        }
      >
        <div className="grid gap-2 lg:grid-cols-2">
          {d.regions.map((r) => (
            <Card key={r.id}>
              <div className="flex items-end gap-2">
                <TextField label={`Region · id ${r.id}`} value={r.name ?? r.id} onCommit={(n) => set(D.renameRegion(d, r.id, n))} className="flex-1" />
                <RemoveButton label={`Remove region ${r.id}`} onClick={() => set(D.removeRegion(d, r.id))} disabled={d.regions.length === 1} />
              </div>
              {r.zones?.length ? (
                <div className="space-y-1.5">
                  <div className="text-[11px] font-medium text-muted-foreground">Availability zones</div>
                  {r.zones.map((z) => (
                    <div key={z.id} className="flex items-end gap-2">
                      <TextField label={`id ${z.id}`} value={z.name ?? z.id} onCommit={(n) => set(D.renameZone(d, r.id, z.id, n))} className="flex-1" />
                      <RemoveButton label={`Remove zone ${z.id}`} onClick={() => set(D.removeZone(d, r.id, z.id))} />
                    </div>
                  ))}
                  <Button size="xs" variant="outline" onClick={() => set(D.addZone(d, r.id))}>
                    <Plus /> AZ
                  </Button>
                </div>
              ) : (
                <div className="flex items-center gap-2">
                  <p className="flex-1 text-[11px] text-muted-foreground">Single location. Split it to place nodes in separate availability zones, with cross-AZ latency inside the region.</p>
                  <Button size="xs" variant="outline" onClick={() => set(D.splitRegion(d, r.id))}>
                    Split into 3 AZs
                  </Button>
                </div>
              )}
            </Card>
          ))}
        </div>
      </Section>
      <Section title="Network" description="One-way latency per message, in three tiers: same AZ, cross-AZ (same region) and cross-region. Region pairs without an override use the default inter-region latency.">
        <div className="grid gap-3 sm:grid-cols-4">
          <NumberField label="Same AZ" hint="Same zone (or same zoneless region)" unit="ms" integer={false} min={0} step={0.1} value={d.latency.intraRegionMs} onCommit={(v) => set(D.setLatency(d, { intraRegionMs: v }))} />
          <NumberField label="Cross-AZ" hint="Between availability zones of one region" unit="ms" integer={false} min={0} step={0.1} value={d.latency.interZoneMs} onCommit={(v) => set(D.setLatency(d, { interZoneMs: v }))} />
          <NumberField label="Default inter-region" unit="ms" integer={false} min={0} value={d.latency.interRegionMs} onCommit={(v) => set(D.setLatency(d, { interRegionMs: v }))} />
          <NumberField label="Jitter" hint="± uniform fraction of each latency" unit="×" integer={false} min={0} max={1} step={0.05} value={d.latency.jitter} onCommit={(v) => set(D.setLatency(d, { jitter: v }))} />
        </div>
        {d.regions.length > 1 && (
          <div className="overflow-x-auto">
            <table className="text-xs">
              <caption className="sr-only">Latency overrides between regions, ms</caption>
              <thead>
                <tr>
                  <th />
                  {d.regions.map((r) => (
                    <th key={r.id} className="readout px-2 pb-1 text-left font-normal text-muted-foreground">
                      {r.id}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {d.regions.map((a, i) => (
                  <tr key={a.id}>
                    <th className="readout pr-2 text-left font-normal text-muted-foreground">{a.id}</th>
                    {d.regions.map((b, j) => (
                      <td key={b.id} className="p-0.5">
                        {j > i ? (
                          <input
                            type="number"
                            min={0}
                            aria-label={`latency ${a.id} to ${b.id} in ms`}
                            placeholder={String(d.latency.interRegionMs)}
                            defaultValue={link(a.id, b.id) ?? ''}
                            onBlur={(e) => set(D.setLink(d, a.id, b.id, e.target.value === '' ? null : Math.max(0, Number(e.target.value))))}
                            className="readout h-7 w-16 rounded border bg-transparent px-1.5 text-xs outline-none focus-visible:ring-2 focus-visible:ring-ring"
                          />
                        ) : (
                          <span className="block w-16 text-center text-muted-foreground/40">{i === j ? '·' : ''}</span>
                        )}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Section>
    </div>
  );
}

// ------------------------------------------------------------------ clusters

export function ClustersSection({ d, set }: SectionProps) {
  return (
    <Section
      title="Clusters"
      description="Each cluster has its own KRaft controller quorum. Spread controllers across regions for a stretch cluster, or add a second cluster for DR."
      actions={
        <Button size="sm" variant="outline" onClick={() => set(D.addCluster(d))}>
          <Plus /> Cluster
        </Button>
      }
    >
      {d.clusters.map((c) => {
        const cfg = c.config;
        const voters = c.controllers.length;
        return (
          <Card key={c.id}>
            <div className="flex items-end gap-2">
              <div className="readout text-sm font-semibold">{c.id}</div>
              <SelectField
                className="w-56"
                label="Flavor"
                value={c.flavor}
                onChange={(flavor) => set(D.patchCluster(d, c.id, { flavor }))}
                options={[
                  { value: 'apache', label: 'Apache Kafka' },
                  { value: 'confluent', label: 'Confluent Platform (links, observers)' },
                ]}
              />
              <div className="flex-1" />
              <RemoveButton label={`Remove cluster ${c.id}`} onClick={() => set(D.removeCluster(d, c.id))} disabled={d.clusters.length === 1} />
            </div>
            <table className="w-full text-xs">
              <thead>
                <tr className="text-left text-muted-foreground">
                  <th className="pb-1 font-normal">Region / AZ</th>
                  <th className="pb-1 font-normal">Controllers</th>
                  <th className="pb-1 font-normal">Brokers</th>
                  <th className="pb-1 font-normal">Nodes</th>
                </tr>
              </thead>
              <tbody>
                {D.locations(d).map((r) => {
                  const ctl = c.controllers.filter((x) => x.region === r.id);
                  const brk = c.brokers.filter((x) => x.region === r.id);
                  return (
                    <tr key={r.id} className="border-t border-border/60">
                      <td className="readout py-1.5">{r.isZone ? `${r.regionName} / ${r.name}` : r.name}</td>
                      <td className="py-1.5">
                        <Stepper label={`controllers in ${r.id}`} value={ctl.length} onChange={(n) => set(D.setNodeCount(d, c.id, 'controllers', r.id, n))} />
                      </td>
                      <td className="py-1.5">
                        <Stepper label={`brokers in ${r.id}`} value={brk.length} onChange={(n) => set(D.setNodeCount(d, c.id, 'brokers', r.id, n))} />
                      </td>
                      <td className="readout py-1.5 text-muted-foreground">{[...ctl, ...brk].map((x) => x.id).join(' ')}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
            {voters % 2 === 0 && <p className="text-xs text-warning">{voters} controllers: an even quorum tolerates no more failures than {voters - 1}. Prefer an odd count.</p>}
            <div className="grid gap-3 sm:grid-cols-3">
              <NumberField label="min.insync.replicas" min={1} value={cfg.minInsyncReplicas} onCommit={(v) => set(D.patchClusterConfig(d, c.id, { minInsyncReplicas: v }))} />
              <NumberField label="replica.lag.time.max" hint="A follower not caught up for this long leaves the ISR" unit="s" scale={1000} integer={false} min={0.1} value={cfg.replicaLagTimeMaxMs} onCommit={(v) => set(D.patchClusterConfig(d, c.id, { replicaLagTimeMaxMs: v }))} />
              <CheckField label="unclean.leader.election" hint="Allow an out-of-sync replica to become leader (data loss)" checked={cfg.uncleanLeaderElection} onChange={(v) => set(D.patchClusterConfig(d, c.id, { uncleanLeaderElection: v }))} />
              <NumberField label="broker.session.timeout" hint="Controller fences a broker after this long without a heartbeat" unit="s" scale={1000} integer={false} min={0.1} value={cfg.brokerSessionTimeoutMs} onCommit={(v) => set(D.patchClusterConfig(d, c.id, { brokerSessionTimeoutMs: v }))} />
              <NumberField label="broker.heartbeat.interval" unit="s" scale={1000} integer={false} min={0.05} value={cfg.brokerHeartbeatIntervalMs} onCommit={(v) => set(D.patchClusterConfig(d, c.id, { brokerHeartbeatIntervalMs: v }))} />
              <NumberField label="replica.fetch.wait.max" hint="Follower long-poll timeout" unit="ms" min={1} value={cfg.replicaFetchWaitMaxMs} onCommit={(v) => set(D.patchClusterConfig(d, c.id, { replicaFetchWaitMaxMs: v }))} />
            </div>
            <PlatformPanel d={d} set={set} c={c} />
          </Card>
        );
      })}
    </Section>
  );
}

function PlatformPanel({ d, set, c }: SectionProps & { c: ClusterT }) {
  const pf = c.platform;
  return (
    <div className="space-y-3 border-t pt-3">
      <SelectField
        className="w-72"
        label="Runs on"
        hint="Kubernetes (CFK): pods per broker/controller, zone-locked PVCs, kubelet restarts, scheduling constraints per zone"
        value={pf ? 'k8s' : 'vm'}
        onChange={(v) => set(D.setPlatform(d, c.id, v === 'k8s'))}
        options={[
          { value: 'vm', label: 'VMs / bare metal' },
          { value: 'k8s', label: 'Kubernetes: Confluent for Kubernetes' },
        ]}
      />
      {pf && (
        <>
          <p className="text-[11px] text-muted-foreground">Workers live in an AZ (topology.kubernetes.io/zone), or in the region itself if it has no AZs. Each Kafka pod needs a schedulable worker with free capacity in its zone (anti-affinity: 1 pod per worker by default).</p>
          <table className="w-full text-xs">
            <thead>
              <tr className="text-left text-muted-foreground">
                <th className="pb-1 font-normal">Zone</th>
                <th className="pb-1 font-normal">Kafka pods</th>
                <th className="pb-1 font-normal">Workers</th>
                <th className="pb-1 font-normal">Schedulable · capacity</th>
              </tr>
            </thead>
            <tbody>
              {D.locations(d).map((r) => {
                const ws = pf.workers.filter((w) => w.zone === r.id);
                const podsHere = [...c.controllers, ...c.brokers].filter((n) => n.region === r.id).length;
                const room = ws.filter((w) => w.schedulable).reduce((a, w) => a + w.capacity, 0);
                return (
                  <tr key={r.id} className="border-t border-border/60 align-top">
                    <td className="readout py-1.5">{r.isZone ? `${r.regionName} / ${r.name}` : r.name}</td>
                    <td className={cn('readout py-1.5', room < podsHere && 'text-critical')}>
                      {podsHere} / room {room}
                    </td>
                    <td className="py-1.5">
                      <Stepper label={`workers in ${r.id}`} value={ws.length} onChange={(n) => set(D.setWorkerCount(d, c.id, r.id, n))} />
                    </td>
                    <td className="py-1.5">
                      <div className="flex flex-wrap gap-1">
                        {ws.map((w) => (
                          <span key={w.id} className={cn('readout inline-flex items-center gap-1 rounded border px-1 py-0.5 text-[10px]', !w.schedulable && 'border-dashed text-warning')}>
                            <button type="button" aria-pressed={w.schedulable} title={w.schedulable ? 'schedulable: click to cordon' : 'cordoned: click to allow scheduling'} onClick={() => set(D.patchWorker(d, c.id, w.id, { schedulable: !w.schedulable }))} className="hover:underline">
                              {w.id}
                            </button>
                            <button type="button" title="capacity (Kafka pods); click to cycle 1-3" aria-label={`${w.id} capacity ${w.capacity}`} onClick={() => set(D.patchWorker(d, c.id, w.id, { capacity: (w.capacity % 3) + 1 }))} className="rounded bg-muted px-1 hover:bg-accent">
                              ×{w.capacity}
                            </button>
                          </span>
                        ))}
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
          <div className="grid gap-3 sm:grid-cols-3">
            <SelectField
              label="Storage"
              hint="zone-locked: EBS/PD-style RWO volume pinned to its zone. ephemeral: emptyDir/local, log lost when the pod moves"
              value={pf.storage}
              onChange={(storage) => set(D.patchPlatform(d, c.id, { storage }))}
              options={[
                { value: 'zoneLocked', label: 'PVC, zone-locked' },
                { value: 'ephemeral', label: 'ephemeral (lost on move)' },
              ]}
            />
            <CheckField label="Rack awareness (broker.rack = zone)" checked={pf.rackAwareness} onChange={(rackAwareness) => set(D.patchPlatform(d, c.id, { rackAwareness }))} />
            <NumberField label="Pod startup" hint="Image pull, JVM start, log recovery" unit="s" scale={1000} integer={false} min={0} value={pf.podStartupMs} onCommit={(v) => set(D.patchPlatform(d, c.id, { podStartupMs: v }))} />
            <NumberField label="Node NotReady after" hint="node-monitor-grace-period" unit="s" scale={1000} integer={false} min={0} value={pf.nodeNotReadyAfterMs} onCommit={(v) => set(D.patchPlatform(d, c.id, { nodeNotReadyAfterMs: v }))} />
            <NumberField label="CrashLoop back-off base" unit="s" scale={1000} integer={false} min={0.1} value={pf.crashBackoffBaseMs} onCommit={(v) => set(D.patchPlatform(d, c.id, { crashBackoffBaseMs: v }))} />
            <NumberField label="Termination grace" hint="SIGTERM → controlled shutdown → exit" unit="s" scale={1000} integer={false} min={0} value={pf.terminationGraceMs} onCommit={(v) => set(D.patchPlatform(d, c.id, { terminationGraceMs: v }))} />
            <CheckField label="Self-Balancing: heal failed brokers" hint="Move replicas off a broker that stays failed (Confluent)" checked={pf.selfBalancing.enabled} onChange={(enabled) => set(D.patchPlatform(d, c.id, { selfBalancing: { ...pf.selfBalancing, enabled } }))} />
            {pf.selfBalancing.enabled && (
              <NumberField
                label="…after broker failed for"
                hint="confluent.balancer.heal.broker.failure.threshold.ms (default 1h)"
                unit="s"
                scale={1000}
                integer={false}
                min={1}
                value={pf.selfBalancing.brokerFailureThresholdMs}
                onCommit={(v) => set(D.patchPlatform(d, c.id, { selfBalancing: { ...pf.selfBalancing, brokerFailureThresholdMs: v } }))}
              />
            )}
          </div>
        </>
      )}
    </div>
  );
}

// ------------------------------------------------------------------ topics

export function TopicsSection({ d, set }: SectionProps) {
  return (
    <Section
      title="Topics"
      description="One or many per cluster. Use automatic rack-aware placement with a replication factor, or pick replicas per partition (first = preferred leader)."
      actions={
        <div className="flex gap-1">
          {d.clusters.map((c) => (
            <Button key={c.id} size="sm" variant="outline" onClick={() => set(D.addTopic(d, c.id))}>
              <Plus /> Topic{d.clusters.length > 1 ? ` in ${c.id}` : ''}
            </Button>
          ))}
        </div>
      }
    >
      {d.topics.length === 0 && <p className="text-sm text-muted-foreground">No topics yet.</p>}
      {d.topics.map((t) => (
        <TopicCard key={`${t.cluster}/${t.name}`} d={d} set={set} t={t} />
      ))}
    </Section>
  );
}

function TopicCard({ d, set, t }: SectionProps & { t: TopicT }) {
  const c = d.clusters.find((k) => k.id === t.cluster)!;
  const brokers = c.brokers.map((b) => b.id);
  const confluent = c.flavor === 'confluent';
  const custom = !!t.replicas;
  const sources = d.topics.filter((s) => s.cluster !== t.cluster && !s.mirror);
  const patch = (p: Partial<TopicT>) => set(D.patchTopic(d, t.cluster, t.name, p));
  return (
    <Card>
      <div className="flex items-end gap-2">
        <Badge variant="outline" className="readout mb-1.5">
          {t.cluster}
        </Badge>
        <TextField label="Topic name" pattern={/^[A-Za-z0-9._-]+$/} value={t.name} onCommit={(n) => set(D.renameTopic(d, t.cluster, t.name, n))} className="flex-1" />
        <NumberField label="Partitions" min={1} max={12} value={t.partitions} onCommit={(partitions) => (t.mirror ? null : patch({ partitions }))} className="w-24" />
        {!custom && <NumberField label="Replication factor" min={1} max={brokers.length} value={t.replicationFactor ?? Math.min(3, brokers.length)} onCommit={(rf) => patch({ replicationFactor: rf })} className="w-32" />}
        <NumberField label="min.insync override" hint={`Empty = cluster default (${c.config.minInsyncReplicas})`} min={1} value={t.minInsyncReplicas ?? c.config.minInsyncReplicas} onCommit={(v) => patch({ minInsyncReplicas: v === c.config.minInsyncReplicas ? undefined : v })} className="w-32" />
        <RemoveButton label={`Remove topic ${t.name}`} onClick={() => set(D.removeTopic(d, t.cluster, t.name))} />
      </div>
      <div className="flex flex-wrap items-end gap-3">
        <CheckField label="Custom replica placement" checked={custom} onChange={(on) => set(D.setCustomPlacement(d, t.cluster, t.name, on))} />
        {confluent && custom && (
          <SelectField
            className="w-60"
            label="Observer promotion (MRC)"
            value={t.observerPromotionPolicy}
            onChange={(v) => patch({ observerPromotionPolicy: v })}
            options={[
              { value: 'none', label: 'never' },
              { value: 'under-min-isr', label: 'when ISR < min.insync' },
            ]}
          />
        )}
      </div>
      {custom && (
        <div className="space-y-1.5">
          {t.replicas!.map((r, p) => (
            <div key={p} className="flex flex-wrap items-center gap-2">
              <span className="readout w-6 text-xs text-muted-foreground">p{p}</span>
              <Chips label={`partition ${p} replicas`} items={brokers} selected={r} order onToggle={(b) => set(D.toggleReplica(d, t.cluster, t.name, p, b, 'replica'))} />
              {confluent && (
                <>
                  <span className="text-[10px] text-muted-foreground">observers</span>
                  <Chips label={`partition ${p} observers`} tone="observer" items={brokers.filter((b) => !r.includes(b))} selected={t.observers?.[p] ?? []} onToggle={(b) => set(D.toggleReplica(d, t.cluster, t.name, p, b, 'observer'))} />
                </>
              )}
            </div>
          ))}
          <p className="text-[11px] text-muted-foreground">Click order sets the order: 1 = preferred leader. Clean elections pick the first live in-sync replica in this order.</p>
        </div>
      )}
      <div className="flex flex-wrap items-end gap-3 border-t pt-3">
        <SelectField
          className="w-56"
          label="Mirror of another cluster"
          value={t.mirror ? t.mirror.type : 'none'}
          onChange={(v) => {
            if (v === 'none') return patch({ mirror: undefined });
            const src = t.mirror ?? (sources[0] ? { sourceCluster: sources[0].cluster, sourceTopic: sources[0].name, fetchIntervalMs: 500 } : null);
            if (!src) return set('Create a topic in another cluster first.');
            patch({ mirror: { ...src, type: v } });
          }}
          options={[
            { value: 'none', label: 'not mirrored' },
            { value: 'mm2', label: 'MirrorMaker 2 (Apache)' },
            { value: 'cluster_link', label: 'Cluster Linking (Confluent)', disabled: !confluent },
          ]}
        />
        {t.mirror && (
          <>
            <SelectField
              className="w-56"
              label="Source topic"
              value={`${t.mirror.sourceCluster}/${t.mirror.sourceTopic}`}
              onChange={(v) => {
                const [sc, ...rest] = v.split('/');
                patch({ mirror: { ...t.mirror!, sourceCluster: sc!, sourceTopic: rest.join('/') } });
              }}
              options={sources.map((s) => ({ value: `${s.cluster}/${s.name}`, label: `${s.cluster}/${s.name}` }))}
            />
            <NumberField label="Fetch interval" unit="ms" min={10} value={t.mirror.fetchIntervalMs} onCommit={(v) => patch({ mirror: { ...t.mirror!, fetchIntervalMs: v } })} className="w-32" />
            {t.mirror.type === 'mm2' && (
              <SelectField
                className="w-40"
                label="Connect worker region"
                value={t.mirror.workerRegion ?? c.brokers[0]!.region}
                onChange={(v) => patch({ mirror: { ...t.mirror!, workerRegion: v } })}
                options={D.locations(d).map((r) => ({ value: r.id, label: r.isZone ? `${r.regionName} / ${r.name}` : r.name }))}
              />
            )}
          </>
        )}
      </div>
    </Card>
  );
}

// ------------------------------------------------------------------ clients

export function ClientsSection({ d, set }: SectionProps) {
  const topicOpts = d.topics.map((t) => ({ value: `${t.cluster}/${t.name}`, label: `${t.cluster}/${t.name}` }));
  const regionOpts = D.locations(d).map((r) => ({ value: r.id, label: r.isZone ? `${r.regionName} / ${r.name}` : r.name }));
  const split = (v: string) => {
    const [cluster, ...rest] = v.split('/');
    return { cluster: cluster!, topic: rest.join('/') };
  };
  return (
    <div className="space-y-6">
      <Section
        title="Producers"
        description="Each producer writes keyless records at a fixed rate to one topic."
        actions={
          <Button size="sm" variant="outline" disabled={!d.topics.length} onClick={() => set(D.addProducer(d))}>
            <Plus /> Producer
          </Button>
        }
      >
        {d.producers.map((p) => (
          <Card key={p.id}>
            <div className="grid items-end gap-3 sm:grid-cols-[1fr_1.4fr_1fr_auto]">
              <TextField label="Id" pattern={ID} value={p.id} onCommit={() => set('Producer ids are fixed once created. Remove it and add a new one.')} />
              <SelectField label="Topic" value={`${p.cluster}/${p.topic}`} options={topicOpts} onChange={(v) => set(D.patchProducer(d, p.id, split(v)))} />
              <SelectField label="Region" value={p.region} options={regionOpts} onChange={(region) => set(D.patchProducer(d, p.id, { region }))} />
              <RemoveButton label={`Remove producer ${p.id}`} onClick={() => set(D.removeClient(d, p.id))} />
            </div>
            <div className="grid gap-3 sm:grid-cols-4">
              <SelectField
                label="acks"
                value={String(p.acks) as '0' | '1' | 'all'}
                onChange={(v) => set(D.patchProducer(d, p.id, { acks: v === 'all' ? 'all' : (Number(v) as 0 | 1) }))}
                options={[
                  { value: '0', label: '0: fire and forget' },
                  { value: '1', label: '1: leader only' },
                  { value: 'all', label: 'all: whole ISR' },
                ]}
              />
              <NumberField label="Rate" unit="rec/s" min={0.1} integer={false} value={p.ratePerSec} onCommit={(v) => set(D.patchProducer(d, p.id, { ratePerSec: v }))} />
              <NumberField label="linger.ms" min={0} value={p.lingerMs} onCommit={(v) => set(D.patchProducer(d, p.id, { lingerMs: v }))} />
              <NumberField label="batch size" hint="Simulated in records, not bytes" unit="recs" min={1} value={p.batchSize} onCommit={(v) => set(D.patchProducer(d, p.id, { batchSize: v }))} />
              <CheckField label="enable.idempotence" checked={p.enableIdempotence} onChange={(v) => set(D.patchProducer(d, p.id, { enableIdempotence: v }))} />
              <NumberField label="request.timeout" unit="s" scale={1000} integer={false} min={0.1} value={p.requestTimeoutMs} onCommit={(v) => set(D.patchProducer(d, p.id, { requestTimeoutMs: v }))} />
              <NumberField label="delivery.timeout" unit="s" scale={1000} integer={false} min={0.1} value={p.deliveryTimeoutMs} onCommit={(v) => set(D.patchProducer(d, p.id, { deliveryTimeoutMs: v }))} />
              <NumberField label="Start at" unit="s" scale={1000} integer={false} min={0} value={p.startMs} onCommit={(v) => set(D.patchProducer(d, p.id, { startMs: v }))} />
            </div>
          </Card>
        ))}
      </Section>
      <Section
        title="Consumers"
        description="Consumers in the same group split the topic's partitions. They read only up to the high watermark."
        actions={
          <Button size="sm" variant="outline" disabled={!d.topics.length} onClick={() => set(D.addConsumer(d))}>
            <Plus /> Consumer
          </Button>
        }
      >
        {d.consumers.map((c) => (
          <Card key={c.id}>
            <div className="grid items-end gap-3 sm:grid-cols-[1fr_1.4fr_1fr_1fr_100px_auto]">
              <TextField label="Id" pattern={ID} value={c.id} onCommit={() => set('Consumer ids are fixed once created.')} />
              <SelectField label="Topic" value={`${c.cluster}/${c.topic}`} options={topicOpts} onChange={(v) => set(D.patchConsumer(d, c.id, split(v)))} />
              <TextField label="Group" value={c.group} onCommit={(group) => set(D.patchConsumer(d, c.id, { group }))} />
              <SelectField label="Region" value={c.region} options={regionOpts} onChange={(region) => set(D.patchConsumer(d, c.id, { region }))} />
              <NumberField label="Poll" unit="ms" min={1} value={c.pollIntervalMs} onCommit={(v) => set(D.patchConsumer(d, c.id, { pollIntervalMs: v }))} />
              <RemoveButton label={`Remove consumer ${c.id}`} onClick={() => set(D.removeClient(d, c.id))} />
            </div>
          </Card>
        ))}
      </Section>
    </div>
  );
}

// ------------------------------------------------------------------ faults

const ACTION_TYPES: { value: ActionT['type']; label: string }[] = [
  { value: 'KILL_NODE', label: 'Kill node (SIGKILL)' },
  { value: 'RESTART_NODE', label: 'Restart node' },
  { value: 'PAUSE_NODE', label: 'Pause node (SIGSTOP)' },
  { value: 'RESUME_NODE', label: 'Resume node' },
  { value: 'PARTITION', label: 'Network partition' },
  { value: 'HEAL', label: 'Heal network' },
  { value: 'SET_LATENCY', label: 'Change latency' },
  { value: 'FAILOVER', label: 'Promote mirror (failover)' },
  { value: 'STOP_PRODUCER', label: 'Stop producer' },
  { value: 'START_PRODUCER', label: 'Start producer' },
  { value: 'CONTAINER_CRASH', label: 'k8s: crash container' },
  { value: 'POD_DELETE', label: 'k8s: delete pod (drain)' },
  { value: 'K8S_NODE_FAIL', label: 'k8s: fail worker node' },
  { value: 'K8S_NODE_RECOVER', label: 'k8s: recover worker node' },
  { value: 'K8S_NODE_DELETE', label: 'k8s: delete node object' },
  { value: 'K8S_CORDON', label: 'k8s: cordon worker/zone' },
  { value: 'K8S_UNCORDON', label: 'k8s: uncordon worker/zone' },
  { value: 'ZONE_OUTAGE', label: 'k8s: AZ outage' },
  { value: 'ZONE_RECOVER', label: 'k8s: AZ recover' },
];
const K8S_ACTION = /^(CONTAINER_CRASH|POD_DELETE|K8S_|ZONE_)/;

export function FaultsSection({ d, set }: SectionProps) {
  const nodes = d.clusters.flatMap((c) => D.nodeIdsOf(c));
  const nodeOpts = nodes.map((n) => ({ value: n, label: n }));
  // regions and their AZs, both valid fault targets (an AZ id is more specific than its region)
  const zoneIds = D.locations(d).filter((l) => l.isZone).map((l) => l.id);
  const regionIds = [...d.regions.map((r) => r.id), ...zoneIds];
  const mirrors = d.topics.filter((t) => t.mirror);
  const workers = D.k8sWorkers(d);
  const pods = D.k8sPods(d);
  const expandLoc = (id: string) => {
    const r = d.regions.find((x) => x.id === id);
    return r ? (r.zones?.map((z) => z.id) ?? [r.id]) : [id];
  };
  return (
    <Section
      title="Timed faults"
      description="Fired at fixed times in every run. You can also inject faults live by clicking nodes during playback."
      actions={
        <Button size="sm" variant="outline" onClick={() => set(D.addAction(d))}>
          <Plus /> Fault
        </Button>
      }
    >
      {d.actions.length === 0 && <p className="text-sm text-muted-foreground">No timed faults. The cluster runs healthy.</p>}
      {d.actions.map((a, i) => {
        const put = (x: ActionT) => set(D.setAction(d, i, x));
        return (
          <Card key={`${i}-${a.type}-${a.at}`} className="flex flex-wrap items-end gap-3 space-y-0">
            <NumberField label="At" unit="s" scale={1000} integer={false} min={0} max={d.durationMs / 1000} className="w-24" value={a.at} onCommit={(at) => put({ ...a, at })} />
            <SelectField className="w-52" label="Fault" value={a.type} options={ACTION_TYPES.map((o) => ({ ...o, disabled: (o.value === 'FAILOVER' && !mirrors.length) || (K8S_ACTION.test(o.value) && !workers.length) }))} onChange={(type) => put(D.actionOfType(d, type, a.at))} />
            {'worker' in a && <SelectField className="w-36" label="Worker" value={a.worker} options={workers.map((w) => ({ value: w.id, label: w.id }))} onChange={(worker) => put({ ...a, worker })} />}
            {'pod' in a && <SelectField className="w-32" label="Pod" value={a.pod} options={pods.map((p) => ({ value: p, label: p }))} onChange={(pod) => put({ ...a, pod })} />}
            {'target' in a && (
              <SelectField
                className="w-40"
                label="Worker or zone"
                value={a.target}
                options={[...regionIds.map((r) => ({ value: r, label: `zone ${r}` })), ...workers.map((w) => ({ value: w.id, label: w.id }))]}
                onChange={(target) => put({ ...a, target })}
              />
            )}
            {'zone' in a && <SelectField className="w-40" label="AZ or whole region" value={a.zone} options={regionIds.map((r) => ({ value: r, label: zoneIds.includes(r) ? `AZ ${r}` : `region ${r}` }))} onChange={(zone) => put({ ...a, zone })} />}
            {'node' in a && <SelectField className="w-32" label="Node" value={a.node} options={nodeOpts} onChange={(node) => put({ ...a, node })} />}
            {a.type === 'PARTITION' && (
              <Field label="Cut off (everything else stays together)" className="min-w-60 flex-1">
                <Chips
                  label="isolated regions and nodes"
                  tone="warning"
                  items={[...regionIds, ...nodes]}
                  selected={a.groups.slice(1).flat()}
                  onToggle={(id) => {
                    const cut = new Set(a.groups.slice(1).flat());
                    if (cut.has(id)) cut.delete(id);
                    else cut.add(id);
                    if (!cut.size) return set('Select at least one region, AZ or node to cut off.');
                    // group 0 = every location not cut off (explicit ids override their zone/region)
                    const cutLocs = new Set([...cut].flatMap((id) => expandLoc(id)));
                    const rest = D.locations(d)
                      .map((l) => l.id)
                      .filter((l) => !cutLocs.has(l));
                    put({ ...a, groups: [rest.length ? rest : [nodes.find((n) => !cut.has(n))!], [...cut]] });
                  }}
                />
              </Field>
            )}
            {a.type === 'SET_LATENCY' && (
              <>
                <SelectField className="w-28" label="From" value={a.a} options={regionIds.map((r) => ({ value: r, label: r }))} onChange={(v) => put({ ...a, a: v })} />
                <SelectField className="w-28" label="To" value={a.b} options={regionIds.map((r) => ({ value: r, label: r }))} onChange={(v) => put({ ...a, b: v })} />
                <NumberField className="w-28" label="Latency" unit="ms" min={0} integer={false} value={a.ms} onCommit={(ms) => put({ ...a, ms })} />
              </>
            )}
            {a.type === 'FAILOVER' && (
              <SelectField
                className="w-56"
                label="Mirror topic"
                value={`${a.cluster}/${a.topic}`}
                options={mirrors.map((t) => ({ value: `${t.cluster}/${t.name}`, label: `${t.cluster}/${t.name}` }))}
                onChange={(v) => {
                  const [cluster, ...rest] = v.split('/');
                  put({ ...a, cluster: cluster!, topic: rest.join('/') });
                }}
              />
            )}
            {(a.type === 'STOP_PRODUCER' || a.type === 'START_PRODUCER') && (
              <SelectField className="w-40" label="Producer" value={a.producer} options={d.producers.map((p) => ({ value: p.id, label: p.id }))} onChange={(producer) => put({ ...a, producer })} />
            )}
            <div className={cn('flex-1')} />
            <RemoveButton label={`Remove fault at ${a.at / 1000}s`} onClick={() => set(D.removeAction(d, i))} />
          </Card>
        );
      })}
    </Section>
  );
}
