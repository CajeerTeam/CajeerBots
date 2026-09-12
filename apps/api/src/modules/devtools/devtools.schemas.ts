import { z } from 'zod';
import { botScenarioDefinitionSchema } from '@cajeerbots/devtools';

const providerSchema = z.enum(['telegram', 'vk', 'discord']);

export const createDebugSessionSchema = z.object({
  communityId: z.string().optional(),
  title: z.string().min(1).max(160),
  metadata: z.record(z.unknown()).optional(),
});

export const listDebugSessionsQuerySchema = z.object({
  communityId: z.string().optional(),
  status: z.string().optional(),
  take: z.coerce.number().int().min(1).max(500).default(50),
});

export const recordDebugEventSchema = z.object({
  sessionId: z.string().optional(),
  communityId: z.string().optional(),
  level: z.enum(['debug', 'info', 'warn', 'error']).default('info'),
  category: z.string().min(1).max(120),
  message: z.string().min(1).max(2000),
  metadata: z.record(z.unknown()).optional(),
});

export const listDebugEventsQuerySchema = z.object({
  sessionId: z.string().optional(),
  communityId: z.string().optional(),
  level: z.string().optional(),
  category: z.string().optional(),
  take: z.coerce.number().int().min(1).max(1000).default(100),
});

export const upsertBotScenarioSchema = z.object({
  code: z.string().min(1).max(120).regex(/^[a-z0-9][a-z0-9._-]*$/i),
  title: z.string().min(1).max(200),
  provider: providerSchema.optional().nullable(),
  accountId: z.string().optional().nullable(),
  definition: botScenarioDefinitionSchema,
  isActive: z.boolean().default(true),
});

export const listBotScenariosQuerySchema = z.object({
  provider: providerSchema.optional(),
  active: z.coerce.boolean().optional(),
  q: z.string().optional(),
  take: z.coerce.number().int().min(1).max(500).default(50),
});

export const runScenarioSchema = z.object({
  accountId: z.string().optional(),
  provider: providerSchema.optional(),
  dryRun: z.boolean().optional().default(false),
});

export const listScenarioRunsQuerySchema = z.object({
  scenarioId: z.string().optional(),
  status: z.string().optional(),
  take: z.coerce.number().int().min(1).max(500).default(50),
});

export const simulateProviderUpdateSchema = z.object({
  accountId: z.string(),
  provider: providerSchema,
  type: z.enum(['message', 'command', 'callback', 'form']).default('message'),
  text: z.string().optional(),
  command: z.string().optional(),
  callbackCode: z.string().optional(),
  userExternalId: z.string().default('dev-user'),
  chatExternalId: z.string().optional(),
  payload: z.record(z.unknown()).optional(),
});
