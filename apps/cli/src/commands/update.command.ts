import { Command } from 'commander';
import { writeFile } from 'node:fs/promises';
import { PrismaService } from '@cajeerbots/db';
import { downloadSignedArtifact, sha256Hex } from '@cajeerbots/registry';
import { apiClient, print } from '../api-client.js';
type Channel = 'stable' | 'lts' | 'beta' | 'alpha' | 'security';
export function updateCommand(): Command {
  const command = new Command('update').description('Проверенные обновления и rollback расширений');
  command.command('check').requiredOption('--package <name>').requiredOption('--current-version <version>').option('--source <id>').option('--channel <channel>', 'channel', 'stable').action(async (o: { package: string; currentVersion: string; source?: string; channel: Channel }) => print(await apiClient().planCoreUpdate({ packageName: o.package, currentVersion: o.currentVersion, sourceId: o.source, channel: o.channel })));
  command.command('plan-extension').argument('<id>').option('--source <id>').option('--channel <channel>', 'channel', 'stable').option('--include-prerelease').action(async (id: string, o: { source?: string; channel: Channel; includePrerelease?: boolean }) => print(await apiClient().planExtensionUpdate(id, { sourceId: o.source, channel: o.channel, includePrerelease: o.includePrerelease })));
  command.command('plans:list').option('--status <status>').option('--target-type <type>').action(async (o: { status?: string; targetType?: string }) => print(await apiClient().updatePlans(o)));
  command.command('apply').argument('<planId>').option('--dry-run').option('--auto-grant').option('--discard-pending').action(async (id: string, o: { dryRun?: boolean; autoGrant?: boolean; discardPending?: boolean }) => print(await apiClient().applyUpdatePlan(id, o)));
  command.command('rollback').argument('<planId>').option('--discard-pending').action(async (id: string, o: { discardPending?: boolean }) => print(await apiClient().rollbackUpdatePlan(id, o.discardPending))); 
  command.command('history:list').action(async () => print(await apiClient().updateHistory()));
  command.command('stage-core').argument('<planId>').requiredOption('--output <file>').description('Download verified core artifact to a new file; deploy via host release procedure').action(async (id: string, o: { output: string }) => {
    const prisma = new PrismaService();
    try {
      const plan = await prisma.updatePlan.findUnique({ where: { id }, include: { version: { include: { package: { include: { source: true } } } } } });
      if (!plan?.version || plan.targetType !== 'core' || plan.version.package.kind !== 'core' || plan.targetVersion !== plan.version.version || plan.sourceId !== plan.version.package.sourceId) throw new Error('Invalid core update plan');
      const bytes = await downloadSignedArtifact(plan.version, plan.version.package.source, 64 * 1024 * 1024);
      await writeFile(o.output, bytes, { flag: 'wx', mode: 0o600 });
      print({ staged: true, applied: false, path: o.output, sha256: sha256Hex(bytes), version: plan.targetVersion });
    } finally { await prisma.$disconnect(); }
  });
  return command;
}
