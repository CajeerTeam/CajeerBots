import { z } from 'zod';

export const upsertMessageTemplateSchema = z.object({
  key: z.string().min(1).max(160),
  locale: z.string().min(2).max(12).default('ru'),
  body: z.string().min(1).max(8000),
  variables: z.record(z.string(), z.unknown()).optional(),
  buttons: z.unknown().optional(),
});

export const renderMessageTemplateSchema = z.object({
  locale: z.string().min(2).max(12).default('ru'),
  variables: z.record(z.string(), z.union([z.string(), z.number(), z.boolean(), z.null()])).default({}),
});
