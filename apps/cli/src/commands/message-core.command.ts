import { readFile } from 'node:fs/promises';
import { Command } from 'commander';
import { PrismaService, upsertMessageButtonLayout } from '@cajeerbots/db';
import { renderStructuredMessage, validateBotForm, type BotFormField, type MessageProvider } from '@cajeerbots/message-core';

export function messageCoreCommand(): Command {
  const command = new Command('message-core').description('Message Core: локализация, кнопки, формы, preview/render');

  command.command('catalog:list')
    .requiredOption('--community <id>', 'community id')
    .option('--locale <locale>', 'locale')
    .option('--namespace <namespace>', 'namespace')
    .action(async (options: { community: string; locale?: string; namespace?: string }) => withPrisma(async (prisma) => {
      const rows = await prisma.localeCatalog.findMany({ where: { communityId: options.community, ...(options.locale ? { locale: options.locale } : {}), ...(options.namespace ? { namespace: options.namespace } : {}) }, orderBy: [{ namespace: 'asc' }, { locale: 'asc' }] });
      console.log(JSON.stringify(rows, null, 2));
    }));

  command.command('catalog:set')
    .requiredOption('--community <id>', 'community id')
    .option('--namespace <namespace>', 'namespace', 'default')
    .option('--locale <locale>', 'locale', 'ru')
    .option('--fallback-locale <locale>', 'fallback locale')
    .requiredOption('--file <path>', 'JSON-файл вида {"key":"text"}')
    .action(async (options: { community: string; namespace: string; locale: string; fallbackLocale?: string; file: string }) => withPrisma(async (prisma) => {
      const messages = JSON.parse(await readFile(options.file, 'utf8')) as Record<string, string>;
      const row = await prisma.localeCatalog.upsert({
        where: { communityId_namespace_locale: { communityId: options.community, namespace: options.namespace, locale: options.locale } },
        create: { communityId: options.community, namespace: options.namespace, locale: options.locale, fallbackLocale: options.fallbackLocale, messages: messages as never },
        update: { fallbackLocale: options.fallbackLocale, messages: messages as never, version: { increment: 1 } },
      });
      console.log(JSON.stringify(row, null, 2));
    }));

  command.command('buttons:list')
    .requiredOption('--community <id>', 'community id')
    .option('--code <code>', 'layout code')
    .action(async (options: { community: string; code?: string }) => withPrisma(async (prisma) => {
      const rows = await prisma.messageButtonLayout.findMany({ where: { communityId: options.community, ...(options.code ? { code: options.code } : {}) }, orderBy: [{ code: 'asc' }, { locale: 'asc' }] });
      console.log(JSON.stringify(rows, null, 2));
    }));

  command.command('buttons:set')
    .requiredOption('--community <id>', 'community id')
    .requiredOption('--code <code>', 'layout code')
    .option('--provider <provider>', 'telegram | vk | discord')
    .option('--locale <locale>', 'locale', 'ru')
    .requiredOption('--file <path>', 'JSON rows')
    .action(async (options: { community: string; code: string; provider?: MessageProvider; locale: string; file: string }) => withPrisma(async (prisma) => {
      const rows = JSON.parse(await readFile(options.file, 'utf8'));
      const row = await prisma.$transaction((tx) => upsertMessageButtonLayout(tx,
        { communityId: options.community, code: options.code, provider: options.provider ?? null, locale: options.locale, rows },
        { rows },
      ));
      console.log(JSON.stringify(row, null, 2));
    }));

  command.command('forms:list')
    .requiredOption('--community <id>', 'community id')
    .option('--active-only', 'только активные')
    .action(async (options: { community: string; activeOnly?: boolean }) => withPrisma(async (prisma) => {
      const rows = await prisma.messageFormDefinition.findMany({ where: { communityId: options.community, ...(options.activeOnly ? { isActive: true } : {}) }, orderBy: [{ code: 'asc' }, { locale: 'asc' }] });
      console.log(JSON.stringify(rows, null, 2));
    }));

  command.command('forms:set')
    .requiredOption('--community <id>', 'community id')
    .requiredOption('--code <code>', 'form code')
    .requiredOption('--title <title>', 'title')
    .option('--locale <locale>', 'locale', 'ru')
    .requiredOption('--fields <path>', 'JSON fields')
    .option('--success-template <key>', 'success template key')
    .action(async (options: { community: string; code: string; title: string; locale: string; fields: string; successTemplate?: string }) => withPrisma(async (prisma) => {
      const fields = JSON.parse(await readFile(options.fields, 'utf8')) as BotFormField[];
      const row = await prisma.messageFormDefinition.upsert({
        where: { communityId_code_locale: { communityId: options.community, code: options.code, locale: options.locale } },
        create: { communityId: options.community, code: options.code, title: options.title, locale: options.locale, fields: fields as never, successTemplateKey: options.successTemplate },
        update: { title: options.title, fields: fields as never, successTemplateKey: options.successTemplate, isActive: true },
      });
      console.log(JSON.stringify(row, null, 2));
    }));

  command.command('forms:submit')
    .requiredOption('--community <id>', 'community id')
    .requiredOption('--code <code>', 'form code')
    .option('--locale <locale>', 'locale', 'ru')
    .requiredOption('--payload <path>', 'JSON payload')
    .action(async (options: { community: string; code: string; locale: string; payload: string }) => withPrisma(async (prisma) => {
      const form = await prisma.messageFormDefinition.findUniqueOrThrow({ where: { communityId_code_locale: { communityId: options.community, code: options.code, locale: options.locale } } });
      const payload = JSON.parse(await readFile(options.payload, 'utf8')) as Record<string, unknown>;
      const validation = validateBotForm(form.fields as unknown as BotFormField[], payload);
      const row = await prisma.messageFormSubmission.create({ data: { communityId: options.community, formId: form.id, payload: payload as never, normalized: validation.normalized as never, errors: validation.errors as never, status: validation.ok ? 'accepted' : 'rejected' } });
      console.log(JSON.stringify({ submission: row, validation }, null, 2));
    }));

  command.command('render')
    .requiredOption('--community <id>', 'community id')
    .option('--template <key>', 'message template key')
    .option('--text <text>', 'inline text')
    .option('--locale <locale>', 'locale', 'ru')
    .option('--provider <provider>', 'telegram | vk | discord')
    .option('--variables <path>', 'JSON variables')
    .option('--buttons <path>', 'JSON buttons rows')
    .action(async (options: { community: string; template?: string; text?: string; locale: string; provider?: MessageProvider; variables?: string; buttons?: string }) => withPrisma(async (prisma) => {
      const variables = options.variables ? JSON.parse(await readFile(options.variables, 'utf8')) as Record<string, unknown> : {};
      const buttons = options.buttons ? JSON.parse(await readFile(options.buttons, 'utf8')) : undefined;
      const text = options.text ?? (options.template ? (await prisma.messageTemplate.findUniqueOrThrow({ where: { key_locale: { key: options.template, locale: options.locale } } })).body : undefined);
      if (!text) throw new Error('Нужно передать --text или --template');
      const rendered = renderStructuredMessage({ text, locale: options.locale, provider: options.provider, variables, buttons });
      console.log(JSON.stringify(rendered, null, 2));
    }));

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
