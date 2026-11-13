import { z } from 'zod';

export const providerSchema = z.enum(['telegram', 'vk', 'discord']);

const buttonSchema = z.object({
  id: z.string().min(1).max(80),
  label: z.string().min(1).max(80),
  action: z.string().min(1).max(160),
  url: z.string().url().optional(),
  payload: z.record(z.string(), z.unknown()).optional(),
});

export const buttonRowsSchema = z.array(z.array(buttonSchema).min(1).max(5)).min(1).max(12);

export const upsertCatalogSchema = z.object({
  namespace: z.string().min(1).max(80).default('default'),
  locale: z.string().min(2).max(12).default('ru'),
  fallbackLocale: z.string().min(2).max(12).optional().nullable(),
  messages: z.record(z.string(), z.string().max(8000)),
});

export const upsertButtonLayoutSchema = z.object({
  code: z.string().min(1).max(120),
  provider: providerSchema.optional().nullable(),
  locale: z.string().min(2).max(12).default('ru'),
  rows: buttonRowsSchema,
});

export const formFieldSchema = z.object({
  name: z.string().min(1).max(80).regex(/^[a-zA-Z0-9_.-]+$/),
  label: z.string().min(1).max(160),
  type: z.enum(['text', 'number', 'boolean', 'select', 'multiselect']),
  required: z.boolean().optional(),
  min: z.number().optional(),
  max: z.number().optional(),
  pattern: z.string().max(400).optional(),
  options: z.array(z.object({ label: z.string().min(1).max(160), value: z.string().min(1).max(160) })).optional(),
});

export const upsertFormSchema = z.object({
  code: z.string().min(1).max(120),
  title: z.string().min(1).max(240),
  locale: z.string().min(2).max(12).default('ru'),
  fields: z.array(formFieldSchema).min(1).max(40).superRefine((fields, ctx) => {
    if (new Set(fields.map((field) => field.name)).size !== fields.length) ctx.addIssue({ code: 'custom', message: 'Duplicate form fields' });
    for (const field of fields) {
      if (['__proto__', 'constructor', 'prototype'].includes(field.name)) ctx.addIssue({ code: 'custom', message: 'Unsafe form field name' });
      if (['select', 'multiselect'].includes(field.type) && (!field.options?.length || new Set(field.options.map((o) => o.value)).size !== field.options.length)) ctx.addIssue({ code: 'custom', message: `Invalid options: ${field.name}` });
      if (field.pattern) { try { new RegExp(field.pattern); } catch { ctx.addIssue({ code: 'custom', message: `Invalid pattern: ${field.name}` }); } }
    }
  }),
  submitTemplateKey: z.string().min(1).max(160).optional().nullable(),
  successTemplateKey: z.string().min(1).max(160).optional().nullable(),
  isActive: z.boolean().default(true),
});

export const submitFormSchema = z.object({
  provider: providerSchema.optional().nullable(),
  userExternalId: z.string().max(160).optional().nullable(),
  payload: z.record(z.string(), z.unknown()),
});

export const renderMessageSchema = z.object({
  locale: z.string().min(2).max(12).default('ru'),
  provider: providerSchema.optional(),
  templateKey: z.string().min(1).max(160).optional(),
  catalogKey: z.string().min(1).max(160).optional(),
  namespace: z.string().min(1).max(80).default('default'),
  text: z.string().min(1).max(8000).optional(),
  variables: z.record(z.string(), z.union([z.string(), z.number(), z.boolean(), z.null(), z.record(z.string(), z.unknown()), z.array(z.unknown())])).default({}),
  buttonLayoutCode: z.string().min(1).max(120).optional(),
  buttons: z.unknown().optional(),
  parseMode: z.enum(['plain', 'markdown', 'html']).default('plain'),
  missingVariables: z.enum(['empty', 'keep', 'throw']).default('empty'),
}).refine((value) => Boolean(value.templateKey || value.catalogKey || value.text), {
  message: 'Нужно передать templateKey, catalogKey или text',
});
