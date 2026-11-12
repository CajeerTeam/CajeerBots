import { validBotTokenRef } from '@cajeerbots/security';
import { Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '@cajeerbots/db';
import { restoreCommunitySnapshot } from '@cajeerbots/backup';
import type { Principal } from '@cajeerbots/security';

import { importCommunitySchema } from './import-export.schemas.js';
import type { z } from 'zod';

type ImportPayload = z.infer<typeof importCommunitySchema>;

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
      version: '1.0.0-rc.6',
      exportedAt: new Date().toISOString(),
      community: { slug: community.slug, title: community.title, language: community.language },
      settings: settings.map(({ key, value }) => ({ key, value })),
      roles: roles.map((role) => ({ code: role.code, title: role.title, priority: role.priority, permissions: role.permissions.map((entry) => entry.permission.scope).sort() })),
      users: users.map((user) => ({ externalId: user.externalId, provider: user.provider, username: user.username, displayName: user.displayName, locale: user.locale, roles: user.roles.map((entry) => entry.role.code) })),
      botAccounts: botAccounts.map((account) => ({ provider: account.provider, mode: account.mode, externalId: account.externalId, username: account.username, tokenRef: account.tokenRef && validBotTokenRef(account.provider, account.tokenRef, communityId) ? account.tokenRef : null, isActive: account.isActive })),
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
    const result = await restoreCommunitySnapshot(this.prisma, communityId, input.data, input.mode);
    if (result.applied) await this.audit(actor, 'import_export.import', 'community', communityId, result.plan);
    return result;
  }

  private audit(actor: Principal, action: string, resource: string, resourceId: string, metadata: Record<string, unknown>) {
    return this.prisma.auditLog.create({ data: { actorId: actor.id, actorType: actor.type, action, resource, resourceId, metadata: metadata as never } });
  }
}
