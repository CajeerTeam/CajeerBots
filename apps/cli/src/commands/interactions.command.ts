import { readFile } from 'node:fs/promises';
import { Command } from 'commander';
import { PrismaService } from '@cajeerbots/db';

export function interactionsCommand(): Command {
  const command = new Command('interactions').description('Команды, меню, callbacks и conversations без внешней панели');

  command.command('commands:list').requiredOption('--community <id>', 'community id').option('--provider <provider>', 'telegram | vk | discord').action(async (options: { community: string; provider?: 'telegram' | 'vk' | 'discord' }) => {
    await withPrisma(async (prisma) => {
      const rows = await prisma.interactionCommand.findMany({ where: { communityId: options.community, ...(options.provider ? { OR: [{ provider: options.provider }, { provider: null }] } : {}) }, orderBy: { name: 'asc' } });
      console.log(JSON.stringify(rows, null, 2));
    });
  });

  command.command('command:add')
    .requiredOption('--community <id>', 'community id')
    .requiredOption('--name <name>', 'command name')
    .requiredOption('--description <text>', 'description')
    .option('--provider <provider>', 'telegram | vk | discord')
    .option('--alias <alias...>', 'aliases')
    .option('--handler <handler>', 'handler key', 'template')
    .option('--template <key>', 'message template key')
    .option('--scope <scope...>', 'required scopes')
    .action(async (options: { community: string; name: string; description: string; provider?: 'telegram' | 'vk' | 'discord'; alias?: string[]; handler: string; template?: string; scope?: string[] }) => {
      await withPrisma(async (prisma) => {
        const row = await prisma.interactionCommand.create({ data: { communityId: options.community, name: options.name, description: options.description, provider: options.provider, aliases: options.alias ?? [], handler: options.handler, templateKey: options.template, scopes: options.scope ?? [] } });
        console.log(JSON.stringify(row, null, 2));
      });
    });

  command.command('menu:add')
    .requiredOption('--community <id>', 'community id')
    .requiredOption('--code <code>', 'menu code')
    .requiredOption('--title <title>', 'menu title')
    .requiredOption('--buttons <json-file>', 'JSON file with button rows')
    .option('--locale <locale>', 'locale', 'ru')
    .option('--template <key>', 'message template key')
    .action(async (options: { community: string; code: string; title: string; buttons: string; locale: string; template?: string }) => {
      const buttons = JSON.parse(await readFile(options.buttons, 'utf8'));
      await withPrisma(async (prisma) => {
        const row = await prisma.interactionMenu.create({ data: { communityId: options.community, code: options.code, title: options.title, locale: options.locale, templateKey: options.template, buttons } });
        console.log(JSON.stringify(row, null, 2));
      });
    });

  command.command('events:list').requiredOption('--community <id>', 'community id').option('--take <n>', 'limit', '50').action(async (options: { community: string; take: string }) => {
    await withPrisma(async (prisma) => {
      const rows = await prisma.interactionEvent.findMany({ where: { communityId: options.community }, orderBy: { createdAt: 'desc' }, take: Math.min(Number(options.take), 200) });
      console.log(JSON.stringify(rows, null, 2));
    });
  });

  command.command('conversation:add')
    .requiredOption('--community <id>', 'community id')
    .requiredOption('--code <code>', 'conversation code')
    .requiredOption('--title <title>', 'conversation title')
    .requiredOption('--steps <json-file>', 'JSON file with conversation steps')
    .option('--entry-command <name>', 'entry command name')
    .action(async (options: { community: string; code: string; title: string; steps: string; entryCommand?: string }) => {
      const steps = JSON.parse(await readFile(options.steps, 'utf8'));
      await withPrisma(async (prisma) => {
        const row = await prisma.conversationFlow.create({ data: { communityId: options.community, code: options.code, title: options.title, entryCommand: options.entryCommand, steps } });
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
