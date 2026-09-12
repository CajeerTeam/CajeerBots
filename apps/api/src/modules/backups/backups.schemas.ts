import { z } from 'zod';

export const createBackupSchema = z.object({
  title: z.string().min(1).max(160).optional(),
  key: z.string().min(3).max(500).optional(),
  metadata: z.record(z.unknown()).optional(),
});

export const restoreBackupSchema = z.object({
  targetCommunityId: z.string().optional(),
  apply: z.boolean().default(false),
  strategy: z.enum(['merge']).default('merge'),
});

export const deleteStorageObjectSchema = z.object({
  hard: z.boolean().default(false),
});
