import { Command } from 'commander';
import { PrismaService } from '@cajeerbots/db';
import { selectUpdateCandidate, updateSteps } from '@cajeerbots/registry';

const ENGINE_API_VERSION = '1.0.0-rc.1';

export function updateCommand(): Command {
  const command = new Command('update').description('Планы обновления ядра, модулей и плагинов через Registry');

  command.command('check').requiredOption('--package <name>', 'registry package name').requiredOption('--current-version <version>', 'текущая версия').option('--source <id>', 'registry source id').option('--channel <channel>', 'stable|lts|beta|alpha|security', 'stable').description('Создать update plan для package').action(async (options: { package: string; currentVersion: string; source?: string; channel: string }) => {
    await withPrisma(async (prisma) => {
      const pkg = await prisma.registryPackage.findFirst({ where: { name: options.package, ...(options.source ? { sourceId: options.source } : {}) }, include: { versions: true } });
      if (!pkg) throw new Error('Registry package не найден');
      const result = selectUpdateCandidate({ currentVersion: options.currentVersion, engineApiVersion: ENGINE_API_VERSION, channel: options.channel as never, versions: pkg.versions.map((version) => ({ version: version.version, engine: { range: version.engineRange }, channel: version.channel as never, yanked: version.isYanked, artifact: { url: version.artifactUrl, sha256: version.artifactSha256, signature: version.signature ?? undefined, signatureAlgorithm: version.signatureAlgorithm as never }, manifest: version.manifest as never })) });
      if (!result.available || !result.targetVersion) {
        console.log(JSON.stringify({ available: false, skipped: result.skipped }, null, 2));
        return;
      }
      const selected = pkg.versions.find((version) => version.version === result.targetVersion?.version);
      const plan = await prisma.updatePlan.create({ data: { targetType: pkg.kind === 'core' ? 'core' : 'extension', packageName: pkg.name, currentVersion: options.currentVersion, targetVersion: result.targetVersion.version, sourceId: pkg.sourceId, versionId: selected?.id, steps: updateSteps(pkg.kind as never, Boolean(selected?.signature)).map((step, order) => ({ order: order + 1, step, status: 'pending' })) as never, metadata: { channel: options.channel, engineApiVersion: ENGINE_API_VERSION } as never } });
      console.log(JSON.stringify(plan, null, 2));
    });
  });

  command.command('plans:list').option('--status <status>').option('--target-type <type>').description('Показать update plans').action(async (options: { status?: string; targetType?: string }) => {
    await withPrisma(async (prisma) => {
      const rows = await prisma.updatePlan.findMany({ where: { ...(options.status ? { status: options.status } : {}), ...(options.targetType ? { targetType: options.targetType } : {}) }, orderBy: { createdAt: 'desc' }, include: { source: true, version: { include: { package: true } } } });
      console.log(JSON.stringify(rows, null, 2));
    });
  });

  command.command('apply').argument('<planId>').option('--dry-run').description('Применить update plan или показать dry-run').action(async (planId: string, options: { dryRun?: boolean }) => {
    await withPrisma(async (prisma) => {
      const plan = await prisma.updatePlan.findUnique({ where: { id: planId }, include: { version: { include: { package: true } } } });
      if (!plan) throw new Error('Update plan не найден');
      if (options.dryRun) {
        console.log(JSON.stringify({ dryRun: true, plan }, null, 2));
        return;
      }
      await prisma.updatePlan.update({ where: { id: plan.id }, data: { status: 'applied', appliedAt: new Date() } });
      await prisma.updateHistory.create({ data: { planId: plan.id, targetType: plan.targetType, targetRef: plan.targetRef, packageName: plan.packageName, fromVersion: plan.currentVersion, toVersion: plan.targetVersion, status: 'applied', steps: plan.steps as never, completedAt: new Date() } });
      console.log(JSON.stringify({ applied: true, planId: plan.id, targetType: plan.targetType, packageName: plan.packageName, from: plan.currentVersion, to: plan.targetVersion }, null, 2));
    });
  });

  command.command('history:list').description('Показать update history').action(async () => {
    await withPrisma(async (prisma) => {
      console.log(JSON.stringify(await prisma.updateHistory.findMany({ orderBy: { createdAt: 'desc' }, take: 100 }), null, 2));
    });
  });

  return command;
}

async function withPrisma<T>(callback: (prisma: PrismaService) => Promise<T>): Promise<T> {
  const prisma = new PrismaService();
  try {
    return await callback(prisma);
  } finally {
    await prisma.$disconnect();
  }
}
