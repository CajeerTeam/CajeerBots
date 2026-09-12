import { Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '@cajeerbots/db';
import { aggregateLatestSamples, collectObservabilitySnapshot, evaluateAlertRules, runDueProbes, runProbe } from '@cajeerbots/observability';
import type { Principal } from '@cajeerbots/security';

@Injectable()
export class ObservabilityService {
  constructor(private readonly prisma: PrismaService) {}

  async summary() {
    const since = new Date(Date.now() - 60 * 60 * 1000);
    const [samples, incidents, probes, alertRules] = await Promise.all([
      this.prisma.observabilityMetricSample.findMany({ where: { collectedAt: { gte: since } }, orderBy: { collectedAt: 'desc' }, take: 1000 }),
      this.prisma.observabilityIncident.findMany({ where: { status: 'open' }, orderBy: [{ severity: 'desc' }, { startedAt: 'desc' }], take: 20 }),
      this.prisma.observabilityProbe.findMany({ orderBy: [{ status: 'asc' }, { code: 'asc' }], take: 50 }),
      this.prisma.observabilityAlertRule.count({ where: { isEnabled: true } }),
    ]);
    const latest = aggregateLatestSamples(samples as never[]);
    const degraded = incidents.length > 0 || probes.some((probe) => probe.status === 'down');
    return { status: degraded ? 'degraded' : 'ok', latest, incidents, probes, activeAlertRules: alertRules, generatedAt: new Date().toISOString() };
  }

  async collect(input: { includeDiagnostics: boolean; evaluateAlerts: boolean; runProbes: boolean }, actor: Principal) {
    const snapshot = await collectObservabilitySnapshot(this.prisma, { includeDiagnostics: input.includeDiagnostics });
    const probes = input.runProbes ? await runDueProbes(this.prisma) : { checked: 0, results: [] };
    const alerts = input.evaluateAlerts ? await evaluateAlertRules(this.prisma) : { evaluated: 0, fired: 0, resolved: 0, events: [] };
    await this.audit(actor, 'observability.collect', 'snapshot', { inserted: snapshot.inserted, probes: probes.checked, alerts });
    return { snapshot, probes, alerts };
  }

  samples(query: { metric?: string; source?: string; severity?: string; communityId?: string; sinceMinutes: number; take: number }) {
    return this.prisma.observabilityMetricSample.findMany({
      where: {
        collectedAt: { gte: new Date(Date.now() - query.sinceMinutes * 60_000) },
        ...(query.metric ? { metric: query.metric } : {}),
        ...(query.source ? { source: query.source } : {}),
        ...(query.severity ? { severity: query.severity } : {}),
        ...(query.communityId ? { communityId: query.communityId } : {}),
      },
      orderBy: { collectedAt: 'desc' },
      take: query.take,
    });
  }

  probes(query: { status?: string; enabled?: boolean; take: number }) {
    return this.prisma.observabilityProbe.findMany({
      where: { ...(query.status ? { status: query.status } : {}), ...(query.enabled === undefined ? {} : { isEnabled: query.enabled }) },
      orderBy: [{ isEnabled: 'desc' }, { status: 'asc' }, { code: 'asc' }],
      take: query.take,
    });
  }

  async upsertProbe(input: { code: string; title: string; type: string; target?: string | null; intervalSeconds: number; timeoutMs: number; isEnabled: boolean; metadata?: Record<string, unknown> }, actor: Principal) {
    const nextCheckAt = input.isEnabled ? new Date() : null;
    const probe = await this.prisma.observabilityProbe.upsert({
      where: { code: input.code },
      create: { ...input, metadata: input.metadata as never, nextCheckAt },
      update: { title: input.title, type: input.type, target: input.target ?? null, intervalSeconds: input.intervalSeconds, timeoutMs: input.timeoutMs, isEnabled: input.isEnabled, metadata: input.metadata as never, nextCheckAt },
    });
    await this.audit(actor, 'observability.probe.upsert', probe.id, { code: probe.code, type: probe.type });
    return probe;
  }

  async runProbe(idOrCode: string, actor: Principal) {
    const probe = await this.prisma.observabilityProbe.findFirst({ where: { OR: [{ id: idOrCode }, { code: idOrCode }] } });
    if (!probe) throw new NotFoundException('Observability probe не найден');
    const result = await runProbe(this.prisma, probe);
    await this.audit(actor, 'observability.probe.run', probe.id, { code: probe.code, status: (result as any).status });
    return result;
  }

  alertRules(query: { metric?: string; enabled?: boolean; severity?: string; take: number }) {
    return this.prisma.observabilityAlertRule.findMany({
      where: { ...(query.metric ? { metric: query.metric } : {}), ...(query.enabled === undefined ? {} : { isEnabled: query.enabled }), ...(query.severity ? { severity: query.severity } : {}) },
      orderBy: [{ isEnabled: 'desc' }, { severity: 'desc' }, { code: 'asc' }],
      take: query.take,
      include: { _count: { select: { events: true } } },
    });
  }

  async upsertAlertRule(input: { code: string; title: string; metric: string; operator: string; threshold: number; severity: string; durationSeconds: number; cooldownSeconds: number; labels?: Record<string, unknown>; isEnabled: boolean }, actor: Principal) {
    const rule = await this.prisma.observabilityAlertRule.upsert({
      where: { code: input.code },
      create: { ...input, labels: input.labels as never },
      update: { title: input.title, metric: input.metric, operator: input.operator, threshold: input.threshold, severity: input.severity, durationSeconds: input.durationSeconds, cooldownSeconds: input.cooldownSeconds, labels: input.labels as never, isEnabled: input.isEnabled },
    });
    await this.audit(actor, 'observability.alert_rule.upsert', rule.id, { code: rule.code, metric: rule.metric, threshold: rule.threshold });
    return rule;
  }

  async evaluateAlerts(actor: Principal) {
    const result = await evaluateAlertRules(this.prisma);
    await this.audit(actor, 'observability.alert_rules.evaluate', 'alert_rules', result as never);
    return result;
  }

  incidents(query: { status?: string; severity?: string; metric?: string; take: number }) {
    return this.prisma.observabilityIncident.findMany({
      where: { ...(query.status ? { status: query.status } : {}), ...(query.severity ? { severity: query.severity } : {}), ...(query.metric ? { metric: query.metric } : {}) },
      orderBy: [{ status: 'asc' }, { startedAt: 'desc' }],
      take: query.take,
      include: { events: { orderBy: { createdAt: 'desc' }, take: 10 } },
    });
  }

  async resolveIncident(idOrCode: string, input: { reason?: string }, actor: Principal) {
    const incident = await this.prisma.observabilityIncident.findFirst({ where: { OR: [{ id: idOrCode }, { code: idOrCode }] } });
    if (!incident) throw new NotFoundException('Incident не найден');
    const updated = await this.prisma.observabilityIncident.update({ where: { id: incident.id }, data: { status: 'resolved', resolvedAt: new Date(), metadata: { ...(incident.metadata as Record<string, unknown> | null ?? {}), resolvedBy: actor.id, reason: input.reason } as never } });
    await this.prisma.observabilityAlertEvent.create({ data: { incidentId: incident.id, status: 'resolved', severity: incident.severity, metric: incident.metric ?? 'manual', value: incident.currentValue ?? 0, threshold: 0, message: input.reason ?? 'Incident resolved manually', resolvedAt: new Date(), metadata: { actorId: actor.id } as never } });
    await this.audit(actor, 'observability.incident.resolve', incident.id, { code: incident.code, reason: input.reason });
    return updated;
  }

  private audit(actor: Principal, action: string, resourceId: string, metadata: Record<string, unknown>) {
    return this.prisma.auditLog.create({ data: { actorId: actor.id, actorType: actor.type, action, resource: 'observability', resourceId, metadata: metadata as never } });
  }
}
