import { z } from 'zod';

export const planCoreUpdateSchema = z.object({
  packageName: z.string().min(1).default('cajeerbots/core'),
  currentVersion: z.string().min(1),
  sourceId: z.string().optional(),
  channel: z.enum(['stable', 'lts', 'beta', 'alpha', 'security']).default('stable'),
  includePrerelease: z.boolean().default(false),
});

export const planExtensionUpdateSchema = z.object({
  sourceId: z.string().optional(),
  channel: z.enum(['stable', 'lts', 'beta', 'alpha', 'security']).default('stable'),
  includePrerelease: z.boolean().default(false),
});

export const listUpdatePlansQuerySchema = z.object({
  targetType: z.string().optional(),
  status: z.string().optional(),
  take: z.coerce.number().int().min(1).max(500).default(100),
});

export const applyUpdatePlanSchema = z.object({
  dryRun: z.boolean().default(false),
  autoGrant: z.boolean().default(false),
  discardPending: z.boolean().default(false),
});

export const rollbackUpdatePlanSchema = z.object({ discardPending: z.boolean().default(false) });
