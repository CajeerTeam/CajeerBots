import { readFile } from 'node:fs/promises';
import { Command } from 'commander';
import { PrismaService } from '@cajeerbots/db';
import { persistSimulatedProviderUpdate, recordDebugEvent, runBotScenario } from '@cajeerbots/devtools';

export function devtoolsCommand(): Command {
  const command = new Command('devtools').description('DX-инструменты: debug sessions, bot scenarios и provider-update simulator');

  command.command('sessions:list')
    .option('--community <id>', 'community id')
    .option('--status <status>', 'active | ended')
    .option('--take <n>', 'limit', '50')
    .action(async (options: { community?: string; status?: string; take: string }) => {
      await withPrisma(async (prisma) => {
        const rows = await prisma.debugSession.findMany({ where: { ...(options.community ? { communityId: options.community } : {}), ...(options.status ? { status: options.status } : {}) }, orderBy: { createdAt: 'desc' }, take: Math.min(Number(options.take), 500), include: { _count: { select: { events: true } } } });
        console.log(JSON.stringify(rows, null, 2));
      });
    });

  command.command('sessions:start')
    .requiredOption('--title <title>', 'название debug-сессии')
    .option('--community <id>', 'community id')
    .option('--metadata <json>', 'metadata JSON', '{}')
    .action(async (options: { title: string; community?: string; metadata: string }) => {
      await withPrisma(async (prisma) => {
        const row = await prisma.debugSession.create({ data: { title: options.title, communityId: options.community ?? null, actorId: 'cli', actorType: 'system', metadata: JSON.parse(options.metadata) as never } });
        await recordDebugEvent(prisma, { sessionId: row.id, communityId: row.communityId, category: 'debug.session', message: `CLI debug session started: ${row.title}` });
        console.log(JSON.stringify(row, null, 2));
      });
    });

  command.command('sessions:end')
    .argument('<id>', 'debug session id')
    .action(async (id: string) => {
      await withPrisma(async (prisma) => {
        const row = await prisma.debugSession.update({ where: { id }, data: { status: 'ended', endedAt: new Date() } });
        await recordDebugEvent(prisma, { sessionId: id, communityId: row.communityId, category: 'debug.session', message: 'CLI debug session ended' });
        console.log(JSON.stringify(row, null, 2));
      });
    });

  command.command('events:list')
    .option('--session <id>', 'debug session id')
    .option('--community <id>', 'community id')
    .option('--level <level>', 'debug | info | warn | error')
    .option('--category <category>', 'category')
    .option('--take <n>', 'limit', '100')
    .action(async (options: { session?: string; community?: string; level?: string; category?: string; take: string }) => {
      await withPrisma(async (prisma) => {
        const rows = await prisma.debugEvent.findMany({ where: { ...(options.session ? { sessionId: options.session } : {}), ...(options.community ? { communityId: options.community } : {}), ...(options.level ? { level: options.level } : {}), ...(options.category ? { category: options.category } : {}) }, orderBy: { createdAt: 'desc' }, take: Math.min(Number(options.take), 1000) });
        console.log(JSON.stringify(rows, null, 2));
      });
    });

  command.command('events:record')
    .requiredOption('--category <category>', 'category')
    .requiredOption('--message <message>', 'message')
    .option('--session <id>', 'debug session id')
    .option('--community <id>', 'community id')
    .option('--level <level>', 'debug | info | warn | error', 'info')
    .option('--metadata <json>', 'metadata JSON', '{}')
    .action(async (options: { category: string; message: string; session?: string; community?: string; level: string; metadata: string }) => {
      await withPrisma(async (prisma) => {
        const row = await recordDebugEvent(prisma, { sessionId: options.session, communityId: options.community, level: options.level, category: options.category, message: options.message, metadata: JSON.parse(options.metadata) });
        console.log(JSON.stringify(row, null, 2));
      });
    });

  command.command('scenarios:list')
    .requiredOption('--community <id>', 'community id')
    .option('--provider <provider>', 'telegram | vk | discord')
    .option('--active <bool>', 'true | false')
    .option('--take <n>', 'limit', '50')
    .action(async (options: { community: string; provider?: string; active?: string; take: string }) => {
      await withPrisma(async (prisma) => {
        const rows = await prisma.botScenario.findMany({ where: { communityId: options.community, ...(options.provider ? { provider: options.provider as never } : {}), ...(options.active === undefined ? {} : { isActive: options.active === 'true' }) }, orderBy: [{ isActive: 'desc' }, { code: 'asc' }], take: Math.min(Number(options.take), 500), include: { _count: { select: { runs: true } } } });
        console.log(JSON.stringify(rows, null, 2));
      });
    });

  command.command('scenarios:set')
    .requiredOption('--community <id>', 'community id')
    .requiredOption('--code <code>', 'scenario code')
    .requiredOption('--title <title>', 'title')
    .requiredOption('--definition <path>', 'JSON файл definition')
    .option('--provider <provider>', 'telegram | vk | discord')
    .option('--account <id>', 'bot account id')
    .option('--disabled', 'создать выключенным')
    .action(async (options: { community: string; code: string; title: string; definition: string; provider?: string; account?: string; disabled?: boolean }) => {
      const definition = JSON.parse(await readFile(options.definition, 'utf8'));
      await withPrisma(async (prisma) => {
        const row = await prisma.botScenario.upsert({ where: { communityId_code: { communityId: options.community, code: options.code } }, create: { communityId: options.community, code: options.code, title: options.title, provider: options.provider as never, accountId: options.account ?? null, definition: definition as never, isActive: !options.disabled }, update: { title: options.title, provider: options.provider as never, accountId: options.account ?? null, definition: definition as never, isActive: !options.disabled } });
        console.log(JSON.stringify(row, null, 2));
      });
    });

  command.command('scenarios:run')
    .requiredOption('--community <id>', 'community id')
    .requiredOption('--scenario <idOrCode>', 'scenario id/code')
    .option('--provider <provider>', 'override provider')
    .option('--account <id>', 'override bot account id')
    .option('--dry-run', 'validate definition without DB writes for provider updates')
    .action(async (options: { community: string; scenario: string; provider?: string; account?: string; dryRun?: boolean }) => {
      await withPrisma(async (prisma) => {
        const scenario = await prisma.botScenario.findFirstOrThrow({ where: { communityId: options.community, OR: [{ id: options.scenario }, { code: options.scenario }] } });
        const run = await prisma.botScenarioRun.create({ data: { scenarioId: scenario.id, communityId: options.community, provider: (options.provider ?? scenario.provider) as never, accountId: options.account ?? scenario.accountId, triggeredBy: 'cli' } });
        const result = await runBotScenario({ prisma, communityId: options.community, scenarioId: scenario.id, scenarioCode: scenario.code, definition: scenario.definition, provider: (options.provider ?? scenario.provider) as never, accountId: options.account ?? scenario.accountId, triggeredBy: 'cli', dryRun: Boolean(options.dryRun) });
        const updated = await prisma.botScenarioRun.update({ where: { id: run.id }, data: { status: result.status, finishedAt: new Date(), durationMs: result.durationMs, stepsResult: result.steps as never, error: result.error ?? null } });
        console.log(JSON.stringify(updated, null, 2));
      });
    });

  command.command('runs:list')
    .requiredOption('--community <id>', 'community id')
    .option('--scenario <id>', 'scenario id')
    .option('--status <status>', 'succeeded | failed | running')
    .option('--take <n>', 'limit', '50')
    .action(async (options: { community: string; scenario?: string; status?: string; take: string }) => {
      await withPrisma(async (prisma) => {
        const rows = await prisma.botScenarioRun.findMany({ where: { communityId: options.community, ...(options.scenario ? { scenarioId: options.scenario } : {}), ...(options.status ? { status: options.status } : {}) }, orderBy: { createdAt: 'desc' }, take: Math.min(Number(options.take), 500), include: { scenario: true } });
        console.log(JSON.stringify(rows, null, 2));
      });
    });

  command.command('simulate:update')
    .requiredOption('--community <id>', 'community id')
    .requiredOption('--account <id>', 'bot account id')
    .requiredOption('--provider <provider>', 'telegram | vk | discord')
    .option('--type <type>', 'message | command | callback | form', 'message')
    .option('--text <text>', 'message text')
    .option('--command <command>', 'command name')
    .option('--callback <code>', 'callback code')
    .option('--user <id>', 'user external id', 'dev-user')
    .option('--chat <id>', 'chat external id')
    .option('--payload <json>', 'payload JSON', '{}')
    .action(async (options: { community: string; account: string; provider: 'telegram' | 'vk' | 'discord'; type: string; text?: string; command?: string; callback?: string; user: string; chat?: string; payload: string }) => {
      await withPrisma(async (prisma) => {
        const row = await persistSimulatedProviderUpdate(prisma, { communityId: options.community, accountId: options.account, provider: options.provider, type: options.type as never, text: options.text, command: options.command, callbackCode: options.callback, userExternalId: options.user, chatExternalId: options.chat ?? options.user, payload: JSON.parse(options.payload) });
        console.log(JSON.stringify(row, null, 2));
      });
    });

  return command;
}

async function withPrisma<T>(fn: (prisma: PrismaService) => Promise<T>): Promise<T> {
  const prisma = new PrismaService();
  await prisma.connect();
  try {
    return await fn(prisma);
  } finally {
    await prisma.close();
  }
}
