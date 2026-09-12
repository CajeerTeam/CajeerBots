export interface CommunityBackupPayload {
  kind: 'cajeerbots.community.export';
  version: string;
  exportedAt: string;
  community: { slug: string; title: string; language: string };
  settings: unknown[];
  roles: unknown[];
  users: unknown[];
  botAccounts: unknown[];
  messageTemplates: unknown[];
  interactionCommands: unknown[];
  interactionMenus: unknown[];
  callbackActions: unknown[];
  conversationFlows: unknown[];
  localeCatalogs: unknown[];
  buttonLayouts: unknown[];
  messageForms: unknown[];
  eventTopics: unknown[];
  eventSubscriptions: unknown[];
  botScenarios: unknown[];
}

export async function exportCommunitySnapshot(prisma: any, communityId: string): Promise<CommunityBackupPayload> {
  const community = await prisma.community.findUnique({ where: { id: communityId } });
  if (!community) throw new Error('Community не найдена');
  const [settings, roles, users, botAccounts, messageTemplates, interactionCommands, interactionMenus, callbackActions, conversationFlows, localeCatalogs, buttonLayouts, messageForms, eventTopics, eventSubscriptions, botScenarios] = await Promise.all([
    prisma.setting.findMany({ where: { communityId }, orderBy: { key: 'asc' } }),
    prisma.role.findMany({ where: { communityId }, include: { permissions: { include: { permission: true } } }, orderBy: { priority: 'asc' } }),
    prisma.user.findMany({ where: { communityId }, include: { roles: { include: { role: true } } }, orderBy: { createdAt: 'asc' } }),
    prisma.botAccount.findMany({ where: { communityId }, orderBy: { createdAt: 'asc' } }),
    prisma.messageTemplate.findMany({ orderBy: [{ key: 'asc' }, { locale: 'asc' }] }),
    prisma.interactionCommand.findMany({ where: { communityId }, orderBy: { name: 'asc' } }),
    prisma.interactionMenu.findMany({ where: { communityId }, orderBy: [{ code: 'asc' }, { locale: 'asc' }] }),
    prisma.interactionCallbackAction.findMany({ where: { communityId }, orderBy: { code: 'asc' } }),
    prisma.conversationFlow.findMany({ where: { communityId }, orderBy: { code: 'asc' } }),
    prisma.localeCatalog.findMany({ where: { communityId }, orderBy: [{ namespace: 'asc' }, { locale: 'asc' }] }),
    prisma.messageButtonLayout.findMany({ where: { communityId }, orderBy: [{ code: 'asc' }, { locale: 'asc' }] }),
    prisma.messageFormDefinition.findMany({ where: { communityId }, orderBy: [{ code: 'asc' }, { locale: 'asc' }] }),
    prisma.eventTopic.findMany({ orderBy: { name: 'asc' } }),
    prisma.eventSubscription.findMany({ orderBy: { name: 'asc' } }),
    prisma.botScenario.findMany({ where: { communityId }, orderBy: { code: 'asc' } }),
  ]);

  return {
    kind: 'cajeerbots.community.export',
    version: '1.0.0-rc.1',
    exportedAt: new Date().toISOString(),
    community: { slug: community.slug, title: community.title, language: community.language },
    settings: settings.map(({ key, value }: any) => ({ key, value })),
    roles: roles.map((role: any) => ({ code: role.code, title: role.title, priority: role.priority, permissions: role.permissions.map((entry: any) => entry.permission.scope).sort() })),
    users: users.map((user: any) => ({ externalId: user.externalId, provider: user.provider, username: user.username, displayName: user.displayName, locale: user.locale, roles: user.roles.map((entry: any) => entry.role.code) })),
    botAccounts: botAccounts.map((account: any) => ({ provider: account.provider, mode: account.mode, externalId: account.externalId, username: account.username, tokenRef: account.tokenRef, isActive: account.isActive })),
    messageTemplates: messageTemplates.map((template: any) => ({ key: template.key, locale: template.locale, body: template.body, variables: template.variables, buttons: template.buttons })),
    interactionCommands: interactionCommands.map((command: any) => ({ name: command.name, description: command.description, provider: command.provider, type: command.type, aliases: command.aliases, scopes: command.scopes, handler: command.handler, templateKey: command.templateKey, conversationCode: command.conversationCode, cooldownSeconds: command.cooldownSeconds, isEnabled: command.isEnabled, metadata: command.metadata })),
    interactionMenus: interactionMenus.map((menu: any) => ({ code: menu.code, title: menu.title, locale: menu.locale, templateKey: menu.templateKey, buttons: menu.buttons, isActive: menu.isActive })),
    callbackActions: callbackActions.map((action: any) => ({ code: action.code, title: action.title, handler: action.handler, scopes: action.scopes, payload: action.payload, isActive: action.isActive })),
    conversationFlows: conversationFlows.map((flow: any) => ({ code: flow.code, title: flow.title, entryCommand: flow.entryCommand, steps: flow.steps, isActive: flow.isActive })),
    localeCatalogs: localeCatalogs.map((catalog: any) => ({ namespace: catalog.namespace, locale: catalog.locale, fallbackLocale: catalog.fallbackLocale, messages: catalog.messages })),
    buttonLayouts: buttonLayouts.map((layout: any) => ({ code: layout.code, provider: layout.provider, locale: layout.locale, rows: layout.rows })),
    messageForms: messageForms.map((form: any) => ({ code: form.code, title: form.title, locale: form.locale, fields: form.fields, submitTemplateKey: form.submitTemplateKey, successTemplateKey: form.successTemplateKey, isActive: form.isActive })),
    eventTopics: eventTopics.map((topic: any) => ({ name: topic.name, description: topic.description, schema: topic.schema, isActive: topic.isActive, retentionDays: topic.retentionDays })),
    eventSubscriptions: eventSubscriptions.map((subscription: any) => ({ name: subscription.name, topicPattern: subscription.topicPattern, targetType: subscription.targetType, targetRef: subscription.targetRef, filters: subscription.filters, isActive: subscription.isActive, maxAttempts: subscription.maxAttempts })),
    botScenarios: botScenarios.map((scenario: any) => ({ code: scenario.code, title: scenario.title, provider: scenario.provider, accountId: scenario.accountId, definition: scenario.definition, isActive: scenario.isActive })),
  };
}

export function includedResourcesFromCommunityPayload(payload: CommunityBackupPayload): string[] {
  return Object.entries(payload)
    .filter(([, value]) => Array.isArray(value))
    .filter(([, value]) => (value as unknown[]).length > 0)
    .map(([key]) => key)
    .sort();
}
