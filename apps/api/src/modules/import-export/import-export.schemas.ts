import { z } from 'zod';

const providerSchema = z.enum(['telegram', 'vk', 'discord']);

export const importCommunitySchema = z.object({
  mode: z.enum(['dry-run', 'apply']).default('dry-run'),
  data: z.object({
    version: z.string(),
    exportedAt: z.string().optional(),
    community: z.object({ slug: z.string(), title: z.string(), language: z.string() }).optional(),
    settings: z.array(z.object({ key: z.string(), value: z.unknown() })).default([]),
    roles: z.array(z.object({ code: z.string(), title: z.string(), priority: z.number().int(), permissions: z.array(z.string()).default([]) })).default([]),
    users: z.array(z.object({ externalId: z.string(), provider: providerSchema, username: z.string().nullable().optional(), displayName: z.string().nullable().optional(), locale: z.string().default('ru'), roles: z.array(z.string()).default([]) })).default([]),
    botAccounts: z.array(z.object({ provider: providerSchema, mode: z.enum(['polling', 'webhook']), externalId: z.string().nullable().optional(), username: z.string().nullable().optional(), tokenRef: z.string().nullable().optional(), isActive: z.boolean().default(true) })).default([]),
    messageTemplates: z.array(z.object({ key: z.string(), locale: z.string(), body: z.string(), variables: z.unknown().optional(), buttons: z.unknown().optional() })).default([]),
    interactionCommands: z.array(z.object({ name: z.string(), description: z.string(), provider: providerSchema.nullable().optional(), type: z.string().default('command'), aliases: z.array(z.string()).default([]), scopes: z.array(z.string()).default([]), handler: z.string().default('template'), templateKey: z.string().nullable().optional(), conversationCode: z.string().nullable().optional(), cooldownSeconds: z.number().int().default(0), isEnabled: z.boolean().default(true), metadata: z.unknown().optional() })).default([]),
    interactionMenus: z.array(z.object({ code: z.string(), title: z.string(), locale: z.string().default('ru'), templateKey: z.string().nullable().optional(), buttons: z.unknown(), isActive: z.boolean().default(true) })).default([]),
    callbackActions: z.array(z.object({ code: z.string(), title: z.string(), handler: z.string().default('menu.open'), scopes: z.array(z.string()).default([]), payload: z.unknown().optional(), isActive: z.boolean().default(true) })).default([]),
    conversationFlows: z.array(z.object({ code: z.string(), title: z.string(), entryCommand: z.string().nullable().optional(), steps: z.unknown(), isActive: z.boolean().default(true) })).default([]),
    localeCatalogs: z.array(z.object({ namespace: z.string().default('default'), locale: z.string().default('ru'), fallbackLocale: z.string().nullable().optional(), messages: z.unknown() })).default([]),
    buttonLayouts: z.array(z.object({ code: z.string(), provider: providerSchema.nullable().optional(), locale: z.string().default('ru'), rows: z.unknown() })).default([]),
    messageForms: z.array(z.object({ code: z.string(), title: z.string(), locale: z.string().default('ru'), fields: z.unknown(), submitTemplateKey: z.string().nullable().optional(), successTemplateKey: z.string().nullable().optional(), isActive: z.boolean().default(true) })).default([]),
    eventTopics: z.array(z.object({ name: z.string(), description: z.string().nullable().optional(), schema: z.unknown().optional(), isActive: z.boolean().default(true), retentionDays: z.number().int().default(30) })).default([]),
    eventSubscriptions: z.array(z.object({ name: z.string(), topicPattern: z.string(), targetType: z.string(), targetRef: z.string(), filters: z.unknown().optional(), isActive: z.boolean().default(true), maxAttempts: z.number().int().default(8) })).default([]),
    botScenarios: z.array(z.object({ code: z.string(), title: z.string(), provider: providerSchema.nullable().optional(), accountId: z.string().nullable().optional(), definition: z.unknown(), isActive: z.boolean().default(true) })).default([]),
  }),
});
