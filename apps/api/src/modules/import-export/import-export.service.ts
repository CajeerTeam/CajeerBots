import { Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '@cajeerbots/db';
import type { Principal } from '@cajeerbots/security';

interface ImportPayload {
  mode: 'dry-run' | 'apply';
  data: {
    version: string;
    community?: { slug: string; title: string; language: string };
    settings: Array<{ key: string; value: unknown }>;
    roles: Array<{ code: string; title: string; priority: number; permissions: string[] }>;
    users: Array<{ externalId: string; provider: 'telegram' | 'vk' | 'discord'; username?: string; displayName?: string; locale: string; roles: string[] }>;
    botAccounts: Array<{ provider: 'telegram' | 'vk' | 'discord'; mode: 'polling' | 'webhook'; externalId?: string; username?: string; tokenRef?: string; isActive: boolean }>;
    messageTemplates: Array<{ key: string; locale: string; body: string; variables?: unknown; buttons?: unknown }>;
    interactionCommands?: Array<{ name: string; description: string; provider?: 'telegram' | 'vk' | 'discord' | null; type: string; aliases: string[]; scopes: string[]; handler: string; templateKey?: string | null; conversationCode?: string | null; cooldownSeconds: number; isEnabled: boolean; metadata?: unknown }>;
    interactionMenus?: Array<{ code: string; title: string; locale: string; templateKey?: string | null; buttons: unknown; isActive: boolean }>;
    callbackActions?: Array<{ code: string; title: string; handler: string; scopes: string[]; payload?: unknown; isActive: boolean }>;
    conversationFlows?: Array<{ code: string; title: string; entryCommand?: string | null; steps: unknown; isActive: boolean }>;
    localeCatalogs?: Array<{ namespace: string; locale: string; fallbackLocale?: string | null; messages: unknown }>;
    buttonLayouts?: Array<{ code: string; provider?: 'telegram' | 'vk' | 'discord' | null; locale: string; rows: unknown }>;
    messageForms?: Array<{ code: string; title: string; locale: string; fields: unknown; submitTemplateKey?: string | null; successTemplateKey?: string | null; isActive: boolean }>;
    eventTopics?: Array<{ name: string; description?: string | null; schema?: unknown; isActive: boolean; retentionDays: number }>;
    eventSubscriptions?: Array<{ name: string; topicPattern: string; targetType: string; targetRef: string; filters?: unknown; isActive: boolean; maxAttempts: number }>;
    botScenarios?: Array<{ code: string; title: string; provider?: 'telegram' | 'vk' | 'discord' | null; accountId?: string | null; definition: unknown; isActive: boolean }>;
  };
}

@Injectable()
export class ImportExportService {
  constructor(private readonly prisma: PrismaService) {}

  async exportCommunity(communityId: string, actor: Principal) {
    const community = await this.prisma.community.findUnique({ where: { id: communityId } });
    if (!community) throw new NotFoundException('Community не найдена');
    const [settings, roles, users, botAccounts, messageTemplates, interactionCommands, interactionMenus, callbackActions, conversationFlows, localeCatalogs, buttonLayouts, messageForms, eventTopics, eventSubscriptions, botScenarios] = await Promise.all([
      this.prisma.setting.findMany({ where: { communityId }, orderBy: { key: 'asc' } }),
      this.prisma.role.findMany({ where: { communityId }, include: { permissions: { include: { permission: true } } }, orderBy: { priority: 'asc' } }),
      this.prisma.user.findMany({ where: { communityId }, include: { roles: { include: { role: true } } }, orderBy: { createdAt: 'asc' } }),
      this.prisma.botAccount.findMany({ where: { communityId }, orderBy: { createdAt: 'asc' } }),
      this.prisma.messageTemplate.findMany({ orderBy: [{ key: 'asc' }, { locale: 'asc' }] }),
      this.prisma.interactionCommand.findMany({ where: { communityId }, orderBy: { name: 'asc' } }),
      this.prisma.interactionMenu.findMany({ where: { communityId }, orderBy: [{ code: 'asc' }, { locale: 'asc' }] }),
      this.prisma.interactionCallbackAction.findMany({ where: { communityId }, orderBy: { code: 'asc' } }),
      this.prisma.conversationFlow.findMany({ where: { communityId }, orderBy: { code: 'asc' } }),
      this.prisma.localeCatalog.findMany({ where: { communityId }, orderBy: [{ namespace: 'asc' }, { locale: 'asc' }] }),
      this.prisma.messageButtonLayout.findMany({ where: { communityId }, orderBy: [{ code: 'asc' }, { locale: 'asc' }] }),
      this.prisma.messageFormDefinition.findMany({ where: { communityId }, orderBy: [{ code: 'asc' }, { locale: 'asc' }] }),
      this.prisma.eventTopic.findMany({ orderBy: { name: 'asc' } }),
      this.prisma.eventSubscription.findMany({ orderBy: { name: 'asc' } }),
      this.prisma.botScenario.findMany({ where: { communityId }, orderBy: { code: 'asc' } }),
    ]);

    const payload = {
      kind: 'cajeerbots.community.export',
      version: '1.0.0-rc.3',
      exportedAt: new Date().toISOString(),
      community: { slug: community.slug, title: community.title, language: community.language },
      settings: settings.map(({ key, value }) => ({ key, value })),
      roles: roles.map((role) => ({ code: role.code, title: role.title, priority: role.priority, permissions: role.permissions.map((entry) => entry.permission.scope).sort() })),
      users: users.map((user) => ({ externalId: user.externalId, provider: user.provider, username: user.username, displayName: user.displayName, locale: user.locale, roles: user.roles.map((entry) => entry.role.code) })),
      botAccounts: botAccounts.map((account) => ({ provider: account.provider, mode: account.mode, externalId: account.externalId, username: account.username, tokenRef: account.tokenRef, isActive: account.isActive })),
      messageTemplates: messageTemplates.map((template) => ({ key: template.key, locale: template.locale, body: template.body, variables: template.variables, buttons: template.buttons })),
      interactionCommands: interactionCommands.map((command) => ({ name: command.name, description: command.description, provider: command.provider, type: command.type, aliases: command.aliases, scopes: command.scopes, handler: command.handler, templateKey: command.templateKey, conversationCode: command.conversationCode, cooldownSeconds: command.cooldownSeconds, isEnabled: command.isEnabled, metadata: command.metadata })),
      interactionMenus: interactionMenus.map((menu) => ({ code: menu.code, title: menu.title, locale: menu.locale, templateKey: menu.templateKey, buttons: menu.buttons, isActive: menu.isActive })),
      callbackActions: callbackActions.map((action) => ({ code: action.code, title: action.title, handler: action.handler, scopes: action.scopes, payload: action.payload, isActive: action.isActive })),
      conversationFlows: conversationFlows.map((flow) => ({ code: flow.code, title: flow.title, entryCommand: flow.entryCommand, steps: flow.steps, isActive: flow.isActive })),
      localeCatalogs: localeCatalogs.map((catalog) => ({ namespace: catalog.namespace, locale: catalog.locale, fallbackLocale: catalog.fallbackLocale, messages: catalog.messages })),
      buttonLayouts: buttonLayouts.map((layout) => ({ code: layout.code, provider: layout.provider, locale: layout.locale, rows: layout.rows })),
      messageForms: messageForms.map((form) => ({ code: form.code, title: form.title, locale: form.locale, fields: form.fields, submitTemplateKey: form.submitTemplateKey, successTemplateKey: form.successTemplateKey, isActive: form.isActive })),
      eventTopics: eventTopics.map((topic) => ({ name: topic.name, description: topic.description, schema: topic.schema, isActive: topic.isActive, retentionDays: topic.retentionDays })),
      eventSubscriptions: eventSubscriptions.map((subscription) => ({ name: subscription.name, topicPattern: subscription.topicPattern, targetType: subscription.targetType, targetRef: subscription.targetRef, filters: subscription.filters, isActive: subscription.isActive, maxAttempts: subscription.maxAttempts })),
      botScenarios: botScenarios.map((scenario) => ({ code: scenario.code, title: scenario.title, provider: scenario.provider, accountId: scenario.accountId, definition: scenario.definition, isActive: scenario.isActive })),
    };
    await this.audit(actor, 'import_export.export', 'community', communityId, { settings: settings.length, roles: roles.length, users: users.length, botAccounts: botAccounts.length });
    return payload;
  }

  async importCommunity(communityId: string, input: ImportPayload, actor: Principal) {
    const community = await this.prisma.community.findUnique({ where: { id: communityId } });
    if (!community) throw new NotFoundException('Community не найдена');
    const plan = {
      mode: input.mode,
      settings: input.data.settings.length,
      roles: input.data.roles.length,
      users: input.data.users.length,
      botAccounts: input.data.botAccounts.length,
      messageTemplates: input.data.messageTemplates.length,
      interactionCommands: input.data.interactionCommands?.length ?? 0,
      interactionMenus: input.data.interactionMenus?.length ?? 0,
      callbackActions: input.data.callbackActions?.length ?? 0,
      conversationFlows: input.data.conversationFlows?.length ?? 0,
      localeCatalogs: input.data.localeCatalogs?.length ?? 0,
      buttonLayouts: input.data.buttonLayouts?.length ?? 0,
      messageForms: input.data.messageForms?.length ?? 0,
      eventTopics: input.data.eventTopics?.length ?? 0,
      eventSubscriptions: input.data.eventSubscriptions?.length ?? 0,
      botScenarios: input.data.botScenarios?.length ?? 0,
    };
    if (input.mode === 'dry-run') return { applied: false, plan };

    await this.prisma.$transaction(async (tx) => {
      if (input.data.community) {
        await tx.community.update({ where: { id: communityId }, data: { title: input.data.community.title, language: input.data.community.language } });
      }

      for (const item of input.data.settings) {
        await tx.setting.upsert({ where: { communityId_key: { communityId, key: item.key } }, create: { communityId, key: item.key, value: item.value as never }, update: { value: item.value as never } });
      }

      for (const item of input.data.roles) {
        const role = await tx.role.upsert({ where: { communityId_code: { communityId, code: item.code } }, create: { communityId, code: item.code, title: item.title, priority: item.priority }, update: { title: item.title, priority: item.priority } });
        for (const scope of item.permissions) {
          const permission = await tx.permission.upsert({ where: { scope }, create: { scope }, update: {} });
          await tx.rolePermission.upsert({ where: { roleId_permissionId: { roleId: role.id, permissionId: permission.id } }, create: { roleId: role.id, permissionId: permission.id }, update: {} });
        }
      }

      for (const item of input.data.users) {
        const user = await tx.user.upsert({
          where: { communityId_provider_externalId: { communityId, provider: item.provider, externalId: item.externalId } },
          create: { communityId, provider: item.provider, externalId: item.externalId, username: item.username, displayName: item.displayName, locale: item.locale },
          update: { username: item.username, displayName: item.displayName, locale: item.locale },
        });
        for (const roleCode of item.roles) {
          const role = await tx.role.findUnique({ where: { communityId_code: { communityId, code: roleCode } } });
          if (role) await tx.userRole.upsert({ where: { userId_roleId: { userId: user.id, roleId: role.id } }, create: { userId: user.id, roleId: role.id }, update: {} });
        }
      }

      for (const item of input.data.botAccounts) {
        const existing = await tx.botAccount.findFirst({ where: { communityId, provider: item.provider, externalId: item.externalId ?? null } });
        if (existing) {
          await tx.botAccount.update({ where: { id: existing.id }, data: { mode: item.mode, username: item.username, tokenRef: item.tokenRef, isActive: item.isActive } });
        } else {
          await tx.botAccount.create({ data: { communityId, provider: item.provider, mode: item.mode, externalId: item.externalId, username: item.username, tokenRef: item.tokenRef, isActive: item.isActive } });
        }
      }

      for (const item of input.data.messageTemplates) {
        await tx.messageTemplate.upsert({ where: { key_locale: { key: item.key, locale: item.locale } }, create: { key: item.key, locale: item.locale, body: item.body, variables: item.variables as never, buttons: item.buttons as never }, update: { body: item.body, variables: item.variables as never, buttons: item.buttons as never } });
      }

      for (const item of input.data.interactionCommands ?? []) {
        await tx.interactionCommand.upsert({ where: { communityId_provider_name: { communityId, provider: item.provider ?? null, name: item.name } }, create: { communityId, name: item.name, description: item.description, provider: item.provider ?? null, type: item.type, aliases: item.aliases, scopes: item.scopes, handler: item.handler, templateKey: item.templateKey, conversationCode: item.conversationCode, cooldownSeconds: item.cooldownSeconds, isEnabled: item.isEnabled, metadata: item.metadata as never }, update: { description: item.description, type: item.type, aliases: item.aliases, scopes: item.scopes, handler: item.handler, templateKey: item.templateKey, conversationCode: item.conversationCode, cooldownSeconds: item.cooldownSeconds, isEnabled: item.isEnabled, metadata: item.metadata as never } });
      }

      for (const item of input.data.interactionMenus ?? []) {
        await tx.interactionMenu.upsert({ where: { communityId_code_locale: { communityId, code: item.code, locale: item.locale } }, create: { communityId, code: item.code, title: item.title, locale: item.locale, templateKey: item.templateKey, buttons: item.buttons as never, isActive: item.isActive }, update: { title: item.title, templateKey: item.templateKey, buttons: item.buttons as never, isActive: item.isActive } });
      }

      for (const item of input.data.callbackActions ?? []) {
        await tx.interactionCallbackAction.upsert({ where: { communityId_code: { communityId, code: item.code } }, create: { communityId, code: item.code, title: item.title, handler: item.handler, scopes: item.scopes, payload: item.payload as never, isActive: item.isActive }, update: { title: item.title, handler: item.handler, scopes: item.scopes, payload: item.payload as never, isActive: item.isActive } });
      }

      for (const item of input.data.conversationFlows ?? []) {
        await tx.conversationFlow.upsert({ where: { communityId_code: { communityId, code: item.code } }, create: { communityId, code: item.code, title: item.title, entryCommand: item.entryCommand, steps: item.steps as never, isActive: item.isActive }, update: { title: item.title, entryCommand: item.entryCommand, steps: item.steps as never, isActive: item.isActive } });
      }

      for (const item of input.data.localeCatalogs ?? []) {
        await tx.localeCatalog.upsert({ where: { communityId_namespace_locale: { communityId, namespace: item.namespace, locale: item.locale } }, create: { communityId, namespace: item.namespace, locale: item.locale, fallbackLocale: item.fallbackLocale, messages: item.messages as never }, update: { fallbackLocale: item.fallbackLocale, messages: item.messages as never, version: { increment: 1 } } });
      }

      for (const item of input.data.buttonLayouts ?? []) {
        await tx.messageButtonLayout.upsert({ where: { communityId_code_provider_locale: { communityId, code: item.code, provider: item.provider ?? null, locale: item.locale } }, create: { communityId, code: item.code, provider: item.provider ?? null, locale: item.locale, rows: item.rows as never }, update: { rows: item.rows as never } });
      }

      for (const item of input.data.messageForms ?? []) {
        await tx.messageFormDefinition.upsert({ where: { communityId_code_locale: { communityId, code: item.code, locale: item.locale } }, create: { communityId, code: item.code, title: item.title, locale: item.locale, fields: item.fields as never, submitTemplateKey: item.submitTemplateKey, successTemplateKey: item.successTemplateKey, isActive: item.isActive }, update: { title: item.title, fields: item.fields as never, submitTemplateKey: item.submitTemplateKey, successTemplateKey: item.successTemplateKey, isActive: item.isActive } });
      }


      for (const item of input.data.eventTopics ?? []) {
        await tx.eventTopic.upsert({ where: { name: item.name }, create: { name: item.name, description: item.description, schema: item.schema as never, isActive: item.isActive, retentionDays: item.retentionDays }, update: { description: item.description, schema: item.schema as never, isActive: item.isActive, retentionDays: item.retentionDays } });
      }

      for (const item of input.data.eventSubscriptions ?? []) {
        await tx.eventSubscription.upsert({ where: { name: item.name }, create: { name: item.name, topicPattern: item.topicPattern, targetType: item.targetType, targetRef: item.targetRef, filters: item.filters as never, isActive: item.isActive, maxAttempts: item.maxAttempts }, update: { topicPattern: item.topicPattern, targetType: item.targetType, targetRef: item.targetRef, filters: item.filters as never, isActive: item.isActive, maxAttempts: item.maxAttempts, disabledAt: item.isActive ? null : new Date() } });
      }

      for (const item of input.data.botScenarios ?? []) {
        await tx.botScenario.upsert({ where: { communityId_code: { communityId, code: item.code } }, create: { communityId, code: item.code, title: item.title, provider: item.provider ?? null, accountId: item.accountId, definition: item.definition as never, isActive: item.isActive }, update: { title: item.title, provider: item.provider ?? null, accountId: item.accountId, definition: item.definition as never, isActive: item.isActive } });
      }

    });

    await this.audit(actor, 'import_export.import', 'community', communityId, plan);
    return { applied: true, plan };
  }

  private audit(actor: Principal, action: string, resource: string, resourceId: string, metadata: Record<string, unknown>) {
    return this.prisma.auditLog.create({ data: { actorId: actor.id, actorType: actor.type, action, resource, resourceId, metadata: metadata as never } });
  }
}
