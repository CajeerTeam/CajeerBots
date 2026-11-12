import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '@cajeerbots/db';
import { checkEngineCompatibility, type ExtensionManifest } from '@cajeerbots/extension-sdk';
import { selectUpdateCandidate, updateSteps } from '@cajeerbots/registry';
import type { Principal } from '@cajeerbots/security';

const ENGINE_API_VERSION = '1.0.0-rc.3';

@Injectable()
export class UpdatesService {
  constructor(private readonly prisma: PrismaService) {}

  plans(query: { targetType?: string; status?: string; take: number }) {
    return this.prisma.updatePlan.findMany({
      where: { ...(query.targetType ? { targetType: query.targetType } : {}), ...(query.status ? { status: query.status } : {}) },
      orderBy: { createdAt: 'desc' },
      take: query.take,
      include: { source: true, version: { include: { package: true } }, history: { orderBy: { createdAt: 'desc' }, take: 3 } },
    });
  }

  history(take = 100) {
    return this.prisma.updateHistory.findMany({ orderBy: { createdAt: 'desc' }, take: Math.min(Math.max(take, 1), 500), include: { plan: true } });
  }

  async planCoreUpdate(input: { packageName: string; currentVersion: string; sourceId?: string; channel?: string; includePrerelease?: boolean }, actor: Principal) {
    const version = await this.selectCandidate(input.packageName, input.currentVersion, input.sourceId, input.channel, input.includePrerelease);
    const plan = await this.prisma.updatePlan.create({
      data: {
        targetType: 'core',
        packageName: input.packageName,
        currentVersion: input.currentVersion,
        targetVersion: version.version,
        sourceId: version.package.sourceId,
        versionId: version.id,
        steps: updateSteps('core', Boolean(version.signature)).map((step, order) => ({ order: order + 1, step, status: 'pending' })) as never,
        metadata: { engineApiVersion: ENGINE_API_VERSION, channel: input.channel ?? 'stable' } as never,
      },
    });
    await this.audit(actor, 'update.plan.core', plan.id, { packageName: input.packageName, currentVersion: input.currentVersion, targetVersion: version.version });
    return this.getPlan(plan.id);
  }

  async planExtensionUpdate(extensionId: string, input: { sourceId?: string; channel?: string; includePrerelease?: boolean }, actor: Principal) {
    const extension = await this.prisma.extension.findUnique({ where: { id: extensionId } });
    if (!extension) throw new NotFoundException('Расширение не найдено');
    const packageName = extension.registryPackageName ?? extension.name;
    const version = await this.selectCandidate(packageName, extension.version, input.sourceId ?? extension.registrySourceId ?? undefined, input.channel, input.includePrerelease);
    const plan = await this.prisma.updatePlan.create({
      data: {
        targetType: 'extension',
        targetRef: extension.id,
        packageName,
        currentVersion: extension.version,
        targetVersion: version.version,
        sourceId: version.package.sourceId,
        versionId: version.id,
        steps: updateSteps((version.package.kind === 'module' ? 'module' : 'plugin') as never, Boolean(version.signature)).map((step, order) => ({ order: order + 1, step, status: 'pending' })) as never,
        metadata: { extensionName: extension.name, engineApiVersion: ENGINE_API_VERSION, channel: input.channel ?? 'stable' } as never,
      },
    });
    await this.audit(actor, 'update.plan.extension', plan.id, { extensionId, packageName, from: extension.version, to: version.version });
    return this.getPlan(plan.id);
  }

  async applyPlan(id: string, input: { dryRun?: boolean; autoGrant?: boolean }, actor: Principal) {
    const plan = await this.prisma.updatePlan.findUnique({ where: { id }, include: { version: { include: { package: true } } } });
    if (!plan) throw new NotFoundException('Update plan не найден');
    if (!plan.version) throw new BadRequestException('Update plan не содержит registry version');
    if (plan.status === 'applied') return this.getPlan(id);
    if (input.dryRun) return { dryRun: true, plan, steps: plan.steps };

    try {
      let metadata: Record<string, unknown> = {};
      if (plan.targetType === 'extension') metadata = await this.applyExtensionPlan(plan, Boolean(input.autoGrant), actor);
      else if (plan.targetType === 'core') metadata = await this.applyCorePlan(plan);
      else throw new BadRequestException(`Неподдерживаемый targetType: ${plan.targetType}`);

      await this.prisma.$transaction(async (tx) => {
        await tx.updatePlan.update({ where: { id }, data: { status: 'applied', appliedAt: new Date(), error: null, metadata: { ...(asObject(plan.metadata)), ...metadata } as never } });
        await tx.updateHistory.create({ data: { planId: id, targetType: plan.targetType, targetRef: plan.targetRef, packageName: plan.packageName, fromVersion: plan.currentVersion, toVersion: plan.targetVersion, status: 'applied', steps: plan.steps as never, metadata: metadata as never, completedAt: new Date() } });
      });
      await this.audit(actor, 'update.plan.apply', id, { targetType: plan.targetType, packageName: plan.packageName, targetVersion: plan.targetVersion });
      return this.getPlan(id);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      await this.prisma.updatePlan.update({ where: { id }, data: { status: 'failed', error: message } });
      await this.prisma.updateHistory.create({ data: { planId: id, targetType: plan.targetType, targetRef: plan.targetRef, packageName: plan.packageName, fromVersion: plan.currentVersion, toVersion: plan.targetVersion, status: 'failed', steps: plan.steps as never, error: message, completedAt: new Date() } });
      throw error;
    }
  }

  async getPlan(id: string) {
    const plan = await this.prisma.updatePlan.findUnique({ where: { id }, include: { source: true, version: { include: { package: true } }, history: { orderBy: { createdAt: 'desc' } } } });
    if (!plan) throw new NotFoundException('Update plan не найден');
    return plan;
  }

  private async selectCandidate(packageName: string, currentVersion: string, sourceId?: string, channel = 'stable', includePrerelease = false) {
    const pkg = await this.prisma.registryPackage.findFirst({ where: { name: packageName, ...(sourceId ? { sourceId } : {}) }, include: { source: true, versions: true } });
    if (!pkg) throw new NotFoundException(`Registry package не найден: ${packageName}`);
    const result = selectUpdateCandidate({
      currentVersion,
      engineApiVersion: ENGINE_API_VERSION,
      channel: channel as never,
      includePrerelease,
      versions: pkg.versions.map((version) => ({
        version: version.version,
        engine: { range: version.engineRange },
        channel: version.channel as never,
        yanked: version.isYanked,
        releaseNotes: version.releaseNotes ?? undefined,
        artifact: { url: version.artifactUrl, sha256: version.artifactSha256, signature: version.signature ?? undefined, signatureAlgorithm: version.signatureAlgorithm as never },
        manifest: version.manifest as never,
        metadata: version.metadata as never,
      })),
    });
    if (!result.available || !result.targetVersion) throw new BadRequestException({ code: 'UPDATE_NOT_AVAILABLE', skipped: result.skipped });
    const version = pkg.versions.find((item) => item.version === result.targetVersion?.version);
    if (!version) throw new NotFoundException('Registry version не найдена после выбора candidate');
    return { ...version, package: pkg };
  }

  private async applyExtensionPlan(plan: { targetRef: string | null; targetVersion: string; currentVersion: string; packageName: string; sourceId: string | null; versionId: string | null; version: { manifest: unknown; signature: string | null; package: { sourceId: string; name: string } } | null }, autoGrant: boolean, actor: Principal): Promise<Record<string, unknown>> {
    if (!plan.targetRef) throw new BadRequestException('Extension update plan без targetRef');
    if (!plan.version?.manifest) throw new BadRequestException('Registry version не содержит extension manifest');
    const oldExtension = await this.prisma.extension.findUnique({ where: { id: plan.targetRef } });
    if (!oldExtension) throw new NotFoundException('Исходное расширение не найдено');
    const manifest = plan.version.manifest as ExtensionManifest;
    const compatibility = checkEngineCompatibility(manifest.engine.range, ENGINE_API_VERSION);
    if (!compatibility.compatible) throw new BadRequestException({ code: 'EXTENSION_INCOMPATIBLE', reasons: compatibility.reasons });

    const installed = await this.prisma.extension.upsert({
      where: { name_version: { name: manifest.name, version: manifest.version } },
      create: { name: manifest.name, version: manifest.version, engineRange: manifest.engine.range, manifest: manifest as never, signature: plan.version.signature ?? manifest.signature, status: 'installed', registrySourceId: plan.sourceId, registryPackageName: plan.packageName, registryVersionId: plan.versionId },
      update: { manifest: manifest as never, signature: plan.version.signature ?? manifest.signature, registrySourceId: plan.sourceId, registryPackageName: plan.packageName, registryVersionId: plan.versionId },
    });
    if (autoGrant) {
      for (const permission of manifest.permissions) {
        await this.prisma.extensionPermissionGrant.upsert({ where: { extensionId_permission: { extensionId: installed.id, permission } }, create: { extensionId: installed.id, permission, actorId: actor.id, reason: 'autoGrant при update plan apply' }, update: { revokedAt: null, actorId: actor.id } });
      }
    }
    await this.prisma.extension.update({ where: { id: oldExtension.id }, data: { status: 'disabled' } });
    return { oldExtensionId: oldExtension.id, newExtensionId: installed.id, autoGrant };
  }

  private async applyCorePlan(plan: { packageName: string; currentVersion: string; targetVersion: string; versionId: string | null }) {
    // Самообновление исходного кода должно выполняться CLI/CI на хосте. API фиксирует проверенный план и историю,
    // чтобы Docker/CI job мог безопасно скачать artifact, проверить checksum/signature и применить release.
    return { requiresHostApply: true, packageName: plan.packageName, from: plan.currentVersion, to: plan.targetVersion, versionId: plan.versionId };
  }

  private audit(actor: Principal, action: string, resourceId: string, metadata: Record<string, unknown>) {
    return this.prisma.auditLog.create({ data: { actorId: actor.id, actorType: actor.type, action, resource: 'update', resourceId, metadata: metadata as never } });
  }
}

function asObject(value: unknown): Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value) ? value as Record<string, unknown> : {};
}
