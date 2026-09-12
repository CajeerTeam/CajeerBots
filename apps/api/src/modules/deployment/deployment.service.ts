import { Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '@cajeerbots/db';
import { buildReleaseManifest, checkDockerComposeProduction, generateProductionEnv, parseEnvText, renderDefaultSystemdUnits, renderNginxConfig, validateProductionEnv } from '@cajeerbots/deployment';
import type { Principal } from '@cajeerbots/security';

@Injectable()
export class DeploymentService {
  constructor(private readonly prisma: PrismaService) {}

  targets(query: { kind?: string; environment?: string; status?: string; take: number }) {
    return this.prisma.deploymentTarget.findMany({
      where: {
        ...(query.kind ? { kind: query.kind } : {}),
        ...(query.environment ? { environment: query.environment } : {}),
        ...(query.status ? { status: query.status } : {}),
      },
      orderBy: [{ environment: 'asc' }, { code: 'asc' }],
      take: query.take,
    });
  }

  async upsertTarget(input: { code: string; title: string; kind: string; environment: string; baseUrl?: string | null; composeFiles: string[]; envFile?: string | null; metadata?: Record<string, unknown> }, actor: Principal) {
    const target = await this.prisma.deploymentTarget.upsert({
      where: { code: input.code },
      create: { code: input.code, title: input.title, kind: input.kind, environment: input.environment, baseUrl: input.baseUrl ?? null, composeFiles: input.composeFiles, envFile: input.envFile ?? null, metadata: input.metadata as never },
      update: { title: input.title, kind: input.kind, environment: input.environment, baseUrl: input.baseUrl ?? null, composeFiles: input.composeFiles, envFile: input.envFile ?? null, metadata: input.metadata as never },
    });
    await this.audit(actor, 'deployment.target.upsert', target.id, { code: target.code, environment: target.environment });
    return target;
  }

  async runChecks(targetId: string | undefined, input: { rootDir: string; composeFiles: string[] }, actor: Principal) {
    const target = targetId ? await this.prisma.deploymentTarget.findUnique({ where: { id: targetId } }) : null;
    if (targetId && !target) throw new NotFoundException('Deployment target не найден');
    const checks = await checkDockerComposeProduction(input.rootDir, input.composeFiles);
    const created = [];
    for (const check of checks) {
      created.push(await this.prisma.deploymentCheck.create({
        data: {
          targetId: target?.id ?? null,
          code: check.code,
          status: check.status,
          severity: check.severity,
          message: check.message,
          details: { ...(check.details ?? {}), rootDir: input.rootDir } as never,
        },
      }));
    }
    const status = checks.some((check) => check.status === 'fail') ? 'failed' : checks.some((check) => check.status === 'warn') ? 'warning' : 'ready';
    if (target) await this.prisma.deploymentTarget.update({ where: { id: target.id }, data: { status, lastCheckedAt: new Date() } });
    await this.audit(actor, 'deployment.check.run', target?.id ?? 'ad-hoc', { status, checks: checks.length });
    return { status, checks: created };
  }

  checks(query: { targetId?: string; status?: string; severity?: string; take: number }) {
    return this.prisma.deploymentCheck.findMany({
      where: {
        ...(query.targetId ? { targetId: query.targetId } : {}),
        ...(query.status ? { status: query.status } : {}),
        ...(query.severity ? { severity: query.severity } : {}),
      },
      orderBy: { checkedAt: 'desc' },
      take: query.take,
    });
  }

  async createRun(input: { targetId?: string | null; action: string; status: string; logs?: unknown[]; metadata?: Record<string, unknown> }, actor: Principal) {
    if (input.targetId) {
      const target = await this.prisma.deploymentTarget.findUnique({ where: { id: input.targetId } });
      if (!target) throw new NotFoundException('Deployment target не найден');
    }
    const run = await this.prisma.deploymentRun.create({ data: { targetId: input.targetId ?? null, action: input.action, status: input.status, logs: (input.logs ?? []) as never, metadata: input.metadata as never, actorId: actor.id } });
    await this.audit(actor, 'deployment.run.create', run.id, { action: run.action, status: run.status });
    return run;
  }

  async finishRun(id: string, input: { status: string; error?: string | null; logs?: unknown[]; metadata?: Record<string, unknown> }, actor: Principal) {
    const run = await this.prisma.deploymentRun.findUnique({ where: { id } });
    if (!run) throw new NotFoundException('Deployment run не найден');
    const finishedAt = new Date();
    const durationMs = finishedAt.getTime() - run.startedAt.getTime();
    const updated = await this.prisma.deploymentRun.update({ where: { id }, data: { status: input.status, error: input.error ?? null, logs: (input.logs ?? (run.logs as unknown[] | null) ?? []) as never, metadata: input.metadata as never, finishedAt, durationMs } });
    await this.audit(actor, 'deployment.run.finish', updated.id, { status: updated.status, durationMs });
    return updated;
  }

  renderEnv(input: Parameters<typeof generateProductionEnv>[0]) {
    return { filename: '.env.production', content: generateProductionEnv(input), generatedAt: new Date().toISOString() };
  }

  validateEnv(envText: string) {
    return validateProductionEnv(parseEnvText(envText));
  }

  renderNginx(input: Parameters<typeof renderNginxConfig>[0]) {
    return { filename: 'cajeerbots.nginx.conf', content: renderNginxConfig(input), generatedAt: new Date().toISOString() };
  }

  renderSystemd(input: { workdir: string; envFile: string }) {
    return { files: renderDefaultSystemdUnits(input.workdir, input.envFile), generatedAt: new Date().toISOString() };
  }

  async buildReleaseManifest(input: Parameters<typeof buildReleaseManifest>[0], actor: Principal) {
    const manifest = buildReleaseManifest(input);
    const artifact = await this.prisma.releaseArtifact.create({
      data: {
        version: input.version,
        channel: input.channel,
        artifactType: 'manifest',
        filename: `cajeerbots-${input.version}.manifest.json`,
        sha256: String(manifest.digest),
        sizeBytes: Buffer.byteLength(JSON.stringify(manifest)),
        manifest: manifest as never,
        status: 'created',
      },
    });
    await this.audit(actor, 'deployment.release_manifest.build', artifact.id, { version: artifact.version, channel: artifact.channel });
    return { artifact, manifest };
  }

  artifacts(query: { version?: string; channel?: string; status?: string; take?: number }) {
    return this.prisma.releaseArtifact.findMany({ where: { ...(query.version ? { version: query.version } : {}), ...(query.channel ? { channel: query.channel } : {}), ...(query.status ? { status: query.status } : {}) }, orderBy: { createdAt: 'desc' }, take: Math.min(query.take ?? 100, 500) });
  }

  private audit(actor: Principal, action: string, resourceId: string, metadata: Record<string, unknown>) {
    return this.prisma.auditLog.create({ data: { actorId: actor.id, actorType: actor.type, action, resource: 'deployment', resourceId, metadata: metadata as never } });
  }
}
