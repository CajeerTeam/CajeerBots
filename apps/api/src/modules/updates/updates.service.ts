import { BadRequestException, ForbiddenException, ConflictException, Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '@cajeerbots/db';
import { extensionManifestSchema } from '@cajeerbots/extension-sdk';
import { selectUpdateCandidate, updateSteps, fetchExtensionArtifact } from '@cajeerbots/registry';
import { hasScope, permissions, type Principal } from '@cajeerbots/security';

const ENGINE_API_VERSION = '1.0.0-rc.4';

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

  async applyPlan(id: string, input: { dryRun?: boolean; autoGrant?: boolean; discardPending?: boolean }, actor: Principal) {
    if (input.autoGrant && !hasScope(actor, permissions.extensionsPermissions)) throw new ForbiddenException('Permission grant scope required');
    const plan = await this.prisma.updatePlan.findUnique({ where: { id }, include: { version: { include: { package: { include: { source: true } } } } } });
    if (!plan) throw new NotFoundException('Update plan не найден');
    if (plan.status === 'applied') return this.getPlan(id);
    if (plan.status !== 'pending' && plan.status !== 'planned' && plan.status !== 'failed') throw new ConflictException('Plan is not applicable');
    if (plan.targetType !== 'extension') throw new BadRequestException('Core deployment is a host operation; use update stage-core. API cannot mark it applied.');
    if (!plan.targetRef || !plan.version || plan.targetVersion !== plan.version.version || plan.sourceId !== plan.version.package.sourceId) throw new BadRequestException('Invalid update plan');
    const version = plan.version;
    const artifact = await fetchExtensionArtifact(version, version.package.source, plan.packageName, ENGINE_API_VERSION).catch((error: Error) => { throw new BadRequestException(error.message); });
    const extensionId = plan.targetRef;
    const old = await this.prisma.extension.findUniqueOrThrow({ where: { id: extensionId }, include: { permissionGrants: { where: { revokedAt: null } } } });
    if (old.version !== plan.currentVersion || old.name !== artifact.manifest.name || old.registrySourceId !== plan.sourceId || !old.artifactSource || !old.artifactCodeSha256) throw new ConflictException('Plan is stale or source differs; reinstall legacy extension as a signed release first');
    const missing = artifact.manifest.permissions.filter((permission) => !old.permissionGrants.some((grant) => grant.permission === permission));
    if (input.dryRun) return { dryRun: true, plan, steps: plan.steps, missingPermissions: missing, requiresDisabled: old.status === 'enabled', pendingEvents: await this.prisma.extensionRuntimeEvent.count({ where: { extensionId, status: { in: ['processing', 'pending', 'failed'] } } }) };
    if (missing.length && !input.autoGrant) throw new BadRequestException({ code: 'EXTENSION_PERMISSIONS_NOT_GRANTED', missing });
    await this.prisma.$transaction(async (tx) => {
      // Serialize update, rollback and runtime claim for this extension.
      await tx.$queryRaw`SELECT id FROM extensions WHERE id = ${extensionId} FOR UPDATE`;
      const current = await tx.extension.findUniqueOrThrow({ where: { id: extensionId }, include: { permissionGrants: { where: { revokedAt: null } } } });
      if (current.version !== plan.currentVersion || current.registrySourceId !== plan.sourceId || current.status === 'enabled' || !current.artifactSource || !current.artifactCodeSha256) throw new ConflictException('Disable extension and refresh the plan before applying');
      if (await tx.extensionRuntimeEvent.count({ where: { extensionId, status: 'processing' } })) throw new ConflictException('Wait for running events before update');
      const pending = await tx.extensionRuntimeEvent.count({ where: { extensionId, status: { in: ['pending', 'failed'] } } });
      if (pending && !input.discardPending) throw new ConflictException('Drain pending events or explicitly pass discardPending');
      if (pending) await tx.extensionRuntimeEvent.updateMany({ where: { extensionId, status: { in: ['pending', 'failed'] } }, data: { status: 'dead', error: 'Discarded explicitly during version update', completedAt: new Date() } });
      const granted = new Set(current.permissionGrants.map((grant) => grant.permission));
      const required = [...new Set(artifact.manifest.permissions)];
      if (!input.autoGrant && required.some((permission) => !granted.has(permission))) throw new ConflictException('Permission grants changed');
      await tx.extensionRelease.upsert({ where: { extensionId_version: { extensionId, version: current.version } }, create: { extensionId, version: current.version, manifest: current.manifest as never, signature: current.signature, registryVersionId: current.registryVersionId, artifactSource: current.artifactSource, artifactCodeSha256: current.artifactCodeSha256 }, update: {} });
      await tx.extensionPermissionGrant.updateMany({ where: { extensionId, permission: { notIn: required }, revokedAt: null }, data: { revokedAt: new Date() } });
      if (input.autoGrant) for (const permission of required) await tx.extensionPermissionGrant.upsert({ where: { extensionId_permission: { extensionId, permission } }, create: { extensionId, permission, actorId: actor.id }, update: { revokedAt: null, actorId: actor.id } });
      await tx.extension.update({ where: { id: extensionId }, data: { version: artifact.manifest.version, engineRange: artifact.manifest.engine.range, manifest: artifact.manifest as never, artifactSource: artifact.artifactSource, artifactCodeSha256: artifact.artifactCodeSha256, signature: version.signature, registryVersionId: version.id, status: 'disabled' } });
      const steps = [{ step: 'verify_signed_artifact', status: 'completed' }, { step: 'snapshot_previous_release', status: 'completed' }, { step: 'replace_extension_code', status: 'completed' }];
      await tx.updatePlan.update({ where: { id }, data: { status: 'applied', appliedAt: new Date(), error: null, steps } });
      await tx.updateHistory.create({ data: { planId: id, targetType: 'extension', targetRef: extensionId, packageName: plan.packageName, fromVersion: plan.currentVersion, toVersion: plan.targetVersion, status: 'applied', steps, completedAt: new Date() } });
      await tx.auditLog.create({ data: { actorId: actor.id, actorType: actor.type, action: 'update.plan.apply', resource: 'extension', resourceId: extensionId, metadata: { planId: id, version: plan.targetVersion, discardedEvents: pending } } });
    }, { isolationLevel: 'Serializable' });
    return this.getPlan(id);
  }

  async rollback(id: string, actor: Principal, discardPending = false) {
    const plan = await this.prisma.updatePlan.findUnique({ where: { id } });
    if (!plan || plan.targetType !== 'extension' || !plan.targetRef || plan.status !== 'applied') throw new BadRequestException('Applied extension plan required');
    const extensionId = plan.targetRef;
    await this.prisma.$transaction(async (tx) => {
      await tx.$queryRaw`SELECT id FROM extensions WHERE id = ${extensionId} FOR UPDATE`;
      const current = await tx.extension.findUniqueOrThrow({ where: { id: extensionId } });
      if (current.version !== plan.targetVersion || current.registryVersionId !== plan.versionId || current.status === 'enabled') throw new ConflictException('Disable current extension; rollback must match installed version');
      if (await tx.extensionRuntimeEvent.count({ where: { extensionId, status: 'processing' } })) throw new ConflictException('Wait for running events before rollback');
      const pending = await tx.extensionRuntimeEvent.count({ where: { extensionId, status: { in: ['pending', 'failed'] } } });
      if (pending && !discardPending) throw new ConflictException('Drain pending events or explicitly pass discardPending');
      if (pending) await tx.extensionRuntimeEvent.updateMany({ where: { extensionId, status: { in: ['pending', 'failed'] } }, data: { status: 'dead', error: 'Discarded explicitly during rollback', completedAt: new Date() } });
      const freshPlan = await tx.updatePlan.findUniqueOrThrow({ where: { id } });
      if (freshPlan.status !== 'applied') throw new ConflictException('Plan already rolled back');
      const previous = await tx.extensionRelease.findUniqueOrThrow({ where: { extensionId_version: { extensionId, version: plan.currentVersion } } });
      const manifest = extensionManifestSchema.parse(previous.manifest);
      await tx.extension.update({ where: { id: extensionId }, data: { version: previous.version, engineRange: manifest.engine.range, manifest: previous.manifest as never, artifactSource: previous.artifactSource, artifactCodeSha256: previous.artifactCodeSha256, signature: previous.signature, registryVersionId: previous.registryVersionId, status: 'disabled' } });
      await tx.extensionPermissionGrant.updateMany({ where: { extensionId, permission: { notIn: manifest.permissions }, revokedAt: null }, data: { revokedAt: new Date() } });
      await tx.updatePlan.update({ where: { id }, data: { status: 'rolled_back' } });
      await tx.updateHistory.create({ data: { planId: id, targetType: 'extension', targetRef: extensionId, packageName: plan.packageName, fromVersion: plan.targetVersion, toVersion: previous.version, status: 'rolled_back', completedAt: new Date() } });
      await tx.auditLog.create({ data: { actorId: actor.id, actorType: actor.type, action: 'update.plan.rollback', resource: 'extension', resourceId: extensionId, metadata: { planId: id, version: previous.version, discardedEvents: pending } } });
    }, { isolationLevel: 'Serializable' });
    return this.getPlan(id);
  }

  async getPlan(id: string) {
    const plan = await this.prisma.updatePlan.findUnique({ where: { id }, include: { source: true, version: { include: { package: true } }, history: { orderBy: { createdAt: 'desc' } } } });
    if (!plan) throw new NotFoundException('Update plan не найден');
    return plan;
  }

  private async selectCandidate(packageName: string, currentVersion: string, sourceId?: string, channel = 'stable', includePrerelease = false) {
    const packages = await this.prisma.registryPackage.findMany({ where: { name: packageName, source: { isActive: true }, ...(sourceId ? { sourceId } : {}) }, take: 2, include: { source: true, versions: true } });
    if (packages.length > 1) throw new BadRequestException('Ambiguous registry package; specify sourceId');
    const pkg = packages[0];
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

  private audit(actor: Principal, action: string, resourceId: string, metadata: Record<string, unknown>) {
    return this.prisma.auditLog.create({ data: { actorId: actor.id, actorType: actor.type, action, resource: 'update', resourceId, metadata: metadata as never } });
  }
}

