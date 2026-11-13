import { z } from 'zod';
import { jsonRecordSchema, jsonValueSchema } from '../shared/json.schemas.js';

const providerSchema = z.enum(['telegram', 'vk', 'discord']);

export const createInteractionCommandSchema = z.object({
  name: z.string().min(1).max(80).regex(/^[a-z0-9_\-:.]+$/i),
  description: z.string().min(1).max(500),
  provider: providerSchema.optional().nullable(),
  type: z.enum(['command', 'slash_command', 'message']).default('command'),
  aliases: z.array(z.string().min(1).max(80)).default([]),
  scopes: z.array(z.string().min(1).max(120)).default([]),
  handler: z.string().min(1).max(120).default('template'),
  templateKey: z.string().min(1).max(160).optional().nullable(),
  conversationCode: z.string().min(1).max(120).optional().nullable(),
  cooldownSeconds: z.number().int().min(0).max(86400).default(0),
  isEnabled: z.boolean().default(true),
  metadata: jsonRecordSchema.optional(),
});

export const updateInteractionCommandSchema = createInteractionCommandSchema.partial();

export const createInteractionMenuSchema = z.object({
  code: z.string().min(1).max(120).regex(/^[a-z0-9_\-:.]+$/i),
  title: z.string().min(1).max(160),
  locale: z.string().min(2).max(12).default('ru'),
  templateKey: z.string().min(1).max(160).optional().nullable(),
  buttons: z.array(z.array(z.object({
    id: z.string().min(1).max(120),
    label: z.string().min(1).max(80),
    action: z.string().min(1).max(120),
    payload: jsonRecordSchema.optional(),
  }))).default([]),
  isActive: z.boolean().default(true),
});

export const updateInteractionMenuSchema = createInteractionMenuSchema.partial();

export const createCallbackActionSchema = z.object({
  code: z.string().min(1).max(64).regex(/^[a-z0-9_\-:.]+$/i).refine((code) => !code.startsWith('rc4:'), 'Reserved callback prefix'),
  title: z.string().min(1).max(160),
  handler: z.string().min(1).max(120).default('menu.open'),
  scopes: z.array(z.string().min(1).max(120)).default([]),
  payload: jsonRecordSchema.optional(),
  isActive: z.boolean().default(true),
});

export const updateCallbackActionSchema = createCallbackActionSchema.partial();

export const conversationStepSchema = z.object({
  id: z.string().min(1).max(120),
  messageKey: z.string().min(1).max(160).optional(),
  text: z.string().min(1).max(4000).optional(),
  input: z.enum(['none', 'text', 'number', 'boolean', 'choice']).default('none'),
  variable: z.string().min(1).max(120).optional(),
  choices: z.array(z.object({ label: z.string().min(1).max(80), value: z.string().min(1).max(200) })).optional(),
  next: z.string().min(1).max(120).optional(),
  complete: z.boolean().optional(),
});

export const createConversationFlowSchema = z.object({
  code: z.string().min(1).max(120).regex(/^[a-z0-9_\-:.]+$/i),
  title: z.string().min(1).max(160),
  entryCommand: z.string().min(1).max(80).optional().nullable(),
  steps: z.array(conversationStepSchema).min(1).max(50).superRefine((steps, ctx) => {
    const ids = new Set(steps.map((step) => step.id));
    if (ids.size !== steps.length) ctx.addIssue({ code: 'custom', message: 'Duplicate step IDs' });
    for (const step of steps) {
      if (step.next && !ids.has(step.next)) ctx.addIssue({ code: 'custom', message: `Unknown next step: ${step.next}` });
      if (step.input === 'choice' && (!step.choices?.length || new Set(step.choices.map((c) => c.value)).size !== step.choices.length)) ctx.addIssue({ code: 'custom', message: `Invalid choices: ${step.id}` });
      if (step.variable && ['__proto__', 'constructor', 'prototype'].includes(step.variable)) ctx.addIssue({ code: 'custom', message: 'Unsafe variable' });
    }
  }),
  isActive: z.boolean().default(true),
});

export const updateConversationFlowSchema = createConversationFlowSchema.partial();

export const recordInteractionEventSchema = z.object({
  accountId: z.string().min(1).optional().nullable(),
  provider: providerSchema,
  type: z.string().min(1).max(80),
  name: z.string().min(1).max(120).optional().nullable(),
  userExternalId: z.string().min(1).max(240),
  status: z.string().min(1).max(80).default('handled'),
  payload: jsonValueSchema.optional(),
  response: jsonValueSchema.optional(),
  error: z.string().max(4000).optional().nullable(),
});
