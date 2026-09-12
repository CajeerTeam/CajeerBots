import { readFile, writeFile } from 'node:fs/promises';
import { Command } from 'commander';
import { PrismaService } from '@cajeerbots/db';

export function importExportCommand(): Command {
  const command = new Command('community').description('Import/export данных community');

  command.command('export').requiredOption('--community <id>', 'community id').requiredOption('--output <file>', 'файл JSON').action(async (options: { community: string; output: string }) => {
    await withPrisma(async (prisma) => {
      const community = await prisma.community.findUniqueOrThrow({ where: { id: options.community } });
      const [settings, roles, users, botAccounts, messageTemplates, interactionCommands, interactionMenus, callbackActions, conversationFlows] = await Promise.all([
        prisma.setting.findMany({ where: { communityId: community.id }, orderBy: { key: 'asc' } }),
        prisma.role.findMany({ where: { communityId: community.id }, include: { permissions: { include: { permission: true } } }, orderBy: { priority: 'asc' } }),
        prisma.user.findMany({ where: { communityId: community.id }, include: { roles: { include: { role: true } } }, orderBy: { createdAt: 'asc' } }),
        prisma.botAccount.findMany({ where: { communityId: community.id }, orderBy: { createdAt: 'asc' } }),
        prisma.messageTemplate.findMany({ orderBy: [{ key: 'asc' }, { locale: 'asc' }] }),
        prisma.interactionCommand.findMany({ where: { communityId: community.id }, orderBy: { name: 'asc' } }),
        prisma.interactionMenu.findMany({ where: { communityId: community.id }, orderBy: [{ code: 'asc' }, { locale: 'asc' }] }),
        prisma.interactionCallbackAction.findMany({ where: { communityId: community.id }, orderBy: { code: 'asc' } }),
        prisma.conversationFlow.findMany({ where: { communityId: community.id }, orderBy: { code: 'asc' } }),
      ]);
      const payload = {
        kind: 'cajeerbots.community.export',
        version: '0.6.0',
        exportedAt: new Date().toISOString(),
        community: { slug: community.slug, title: community.title, language: community.language },
        settings: settings.map(({ key, value }) => ({ key, value })),
        roles: roles.map((role) => ({ code: role.code, title: role.title, priority: role.priority, permissions: role.permissions.map((entry) => entry.permission.scope) })),
        users: users.map((user) => ({ externalId: user.externalId, provider: user.provider, username: user.username, displayName: user.displayName, locale: user.locale, roles: user.roles.map((entry) => entry.role.code) })),
        botAccounts: botAccounts.map((account) => ({ provider: account.provider, mode: account.mode, externalId: account.externalId, username: account.username, tokenRef: account.tokenRef, isActive: account.isActive })),
        messageTemplates: messageTemplates.map((template) => ({ key: template.key, locale: template.locale, body: template.body, variables: template.variables, buttons: template.buttons })),
        interactionCommands: interactionCommands.map((command) => ({ name: command.name, description: command.description, provider: command.provider, type: command.type, aliases: command.aliases, scopes: command.scopes, handler: command.handler, templateKey: command.templateKey, conversationCode: command.conversationCode, cooldownSeconds: command.cooldownSeconds, isEnabled: command.isEnabled, metadata: command.metadata })),
        interactionMenus: interactionMenus.map((menu) => ({ code: menu.code, title: menu.title, locale: menu.locale, templateKey: menu.templateKey, buttons: menu.buttons, isActive: menu.isActive })),
        callbackActions: callbackActions.map((action) => ({ code: action.code, title: action.title, handler: action.handler, scopes: action.scopes, payload: action.payload, isActive: action.isActive })),
        conversationFlows: conversationFlows.map((flow) => ({ code: flow.code, title: flow.title, entryCommand: flow.entryCommand, steps: flow.steps, isActive: flow.isActive })),
      };
      await writeFile(options.output, JSON.stringify(payload, null, 2));
      console.log(JSON.stringify({ exported: true, output: options.output, settings: settings.length, roles: roles.length, users: users.length }, null, 2));
    });
  });

  command.command('import').requiredOption('--community <id>', 'community id').requiredOption('--input <file>', 'файл JSON').option('--apply', 'применить импорт, без флага только dry-run').action(async (options: { community: string; input: string; apply?: boolean }) => {
    const data = JSON.parse(await readFile(options.input, 'utf8')) as { settings?: Array<{ key: string; value: unknown }>; roles?: Array<{ code: string; title: string; priority: number; permissions?: string[] }>; users?: Array<{ externalId: string; provider: 'telegram' | 'vk' | 'discord'; username?: string; displayName?: string; locale?: string; roles?: string[] }>; botAccounts?: Array<{ provider: 'telegram' | 'vk' | 'discord'; mode: 'polling' | 'webhook'; externalId?: string; username?: string; tokenRef?: string; isActive?: boolean }>; messageTemplates?: Array<{ key: string; locale: string; body: string; variables?: unknown; buttons?: unknown }>; interactionCommands?: Array<{ name: string; description: string; provider?: 'telegram' | 'vk' | 'discord' | null; type?: string; aliases?: string[]; scopes?: string[]; handler?: string; templateKey?: string | null; conversationCode?: string | null; cooldownSeconds?: number; isEnabled?: boolean; metadata?: unknown }>; interactionMenus?: Array<{ code: string; title: string; locale?: string; templateKey?: string | null; buttons: unknown; isActive?: boolean }>; callbackActions?: Array<{ code: string; title: string; handler?: string; scopes?: string[]; payload?: unknown; isActive?: boolean }>; conversationFlows?: Array<{ code: string; title: string; entryCommand?: string | null; steps: unknown; isActive?: boolean }> };
    if (!options.apply) {
      console.log(JSON.stringify({ applied: false, mode: 'dry-run', settings: data.settings?.length ?? 0 }, null, 2));
      return;
    }
    await withPrisma(async (prisma) => {
      for (const item of data.settings ?? []) {
        await prisma.setting.upsert({ where: { communityId_key: { communityId: options.community, key: item.key } }, create: { communityId: options.community, key: item.key, value: item.value as never }, update: { value: item.value as never } });
      }
      for (const item of data.roles ?? []) {
        const role = await prisma.role.upsert({ where: { communityId_code: { communityId: options.community, code: item.code } }, create: { communityId: options.community, code: item.code, title: item.title, priority: item.priority }, update: { title: item.title, priority: item.priority } });
        for (const scope of item.permissions ?? []) {
          const permission = await prisma.permission.upsert({ where: { scope }, create: { scope }, update: {} });
          await prisma.rolePermission.upsert({ where: { roleId_permissionId: { roleId: role.id, permissionId: permission.id } }, create: { roleId: role.id, permissionId: permission.id }, update: {} });
        }
      }
      for (const item of data.users ?? []) {
        const user = await prisma.user.upsert({ where: { communityId_provider_externalId: { communityId: options.community, provider: item.provider, externalId: item.externalId } }, create: { communityId: options.community, provider: item.provider, externalId: item.externalId, username: item.username, displayName: item.displayName, locale: item.locale ?? 'ru' }, update: { username: item.username, displayName: item.displayName, locale: item.locale ?? 'ru' } });
        for (const roleCode of item.roles ?? []) {
          const role = await prisma.role.findUnique({ where: { communityId_code: { communityId: options.community, code: roleCode } } });
          if (role) await prisma.userRole.upsert({ where: { userId_roleId: { userId: user.id, roleId: role.id } }, create: { userId: user.id, roleId: role.id }, update: {} });
        }
      }
      for (const item of data.botAccounts ?? []) {
        const existing = await prisma.botAccount.findFirst({ where: { communityId: options.community, provider: item.provider, externalId: item.externalId ?? null } });
        if (existing) await prisma.botAccount.update({ where: { id: existing.id }, data: { mode: item.mode, username: item.username, tokenRef: item.tokenRef, isActive: item.isActive ?? true } });
        else await prisma.botAccount.create({ data: { communityId: options.community, provider: item.provider, mode: item.mode, externalId: item.externalId, username: item.username, tokenRef: item.tokenRef, isActive: item.isActive ?? true } });
      }
      for (const item of data.messageTemplates ?? []) {
        await prisma.messageTemplate.upsert({ where: { key_locale: { key: item.key, locale: item.locale } }, create: { key: item.key, locale: item.locale, body: item.body, variables: item.variables as never, buttons: item.buttons as never }, update: { body: item.body, variables: item.variables as never, buttons: item.buttons as never } });
      }

      for (const item of data.interactionCommands ?? []) {
        await prisma.interactionCommand.upsert({ where: { communityId_provider_name: { communityId: options.community, provider: item.provider ?? null, name: item.name } }, create: { communityId: options.community, name: item.name, description: item.description, provider: item.provider ?? null, type: item.type ?? 'command', aliases: item.aliases ?? [], scopes: item.scopes ?? [], handler: item.handler ?? 'template', templateKey: item.templateKey, conversationCode: item.conversationCode, cooldownSeconds: item.cooldownSeconds ?? 0, isEnabled: item.isEnabled ?? true, metadata: item.metadata as never }, update: { description: item.description, type: item.type ?? 'command', aliases: item.aliases ?? [], scopes: item.scopes ?? [], handler: item.handler ?? 'template', templateKey: item.templateKey, conversationCode: item.conversationCode, cooldownSeconds: item.cooldownSeconds ?? 0, isEnabled: item.isEnabled ?? true, metadata: item.metadata as never } });
      }
      for (const item of data.interactionMenus ?? []) {
        await prisma.interactionMenu.upsert({ where: { communityId_code_locale: { communityId: options.community, code: item.code, locale: item.locale ?? 'ru' } }, create: { communityId: options.community, code: item.code, title: item.title, locale: item.locale ?? 'ru', templateKey: item.templateKey, buttons: item.buttons as never, isActive: item.isActive ?? true }, update: { title: item.title, templateKey: item.templateKey, buttons: item.buttons as never, isActive: item.isActive ?? true } });
      }
      for (const item of data.callbackActions ?? []) {
        await prisma.interactionCallbackAction.upsert({ where: { communityId_code: { communityId: options.community, code: item.code } }, create: { communityId: options.community, code: item.code, title: item.title, handler: item.handler ?? 'menu.open', scopes: item.scopes ?? [], payload: item.payload as never, isActive: item.isActive ?? true }, update: { title: item.title, handler: item.handler ?? 'menu.open', scopes: item.scopes ?? [], payload: item.payload as never, isActive: item.isActive ?? true } });
      }
      for (const item of data.conversationFlows ?? []) {
        await prisma.conversationFlow.upsert({ where: { communityId_code: { communityId: options.community, code: item.code } }, create: { communityId: options.community, code: item.code, title: item.title, entryCommand: item.entryCommand, steps: item.steps as never, isActive: item.isActive ?? true }, update: { title: item.title, entryCommand: item.entryCommand, steps: item.steps as never, isActive: item.isActive ?? true } });
      }
      console.log(JSON.stringify({ applied: true, settings: data.settings?.length ?? 0, roles: data.roles?.length ?? 0, users: data.users?.length ?? 0, botAccounts: data.botAccounts?.length ?? 0, messageTemplates: data.messageTemplates?.length ?? 0, interactionCommands: data.interactionCommands?.length ?? 0 }, null, 2));
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
