import { z } from 'zod';

export const upsertScheduledJobSchema = z.object({
  code: z.string().min(2).max(120).regex(/^[a-z0-9_.:-]+$/),
  title: z.string().min(2).max(200),
  type: z.enum(['built_in', 'event_publish', 'http']).default('built_in'),
  handler: z.string().min(2).max(160).optional(),
  schedule: z.string().min(1).max(120).default('manual'),
  timezone: z.string().min(2).max(80).default('UTC'),
  payload: z.record(z.string(), z.unknown()).optional(),
  isEnabled: z.boolean().default(true),
  maxAttempts: z.number().int().min(1).max(25).default(3),
  timeoutSeconds: z.number().int().min(5).max(3600).default(300),
});

export const listJobsQuerySchema = z.object({
  enabled: z.coerce.boolean().optional(),
  type: z.string().optional(),
  q: z.string().optional(),
  take: z.coerce.number().int().min(1).max(500).default(100),
});

export const listRunsQuerySchema = z.object({
  jobId: z.string().optional(),
  status: z.string().optional(),
  take: z.coerce.number().int().min(1).max(500).default(100),
});

export const runNowSchema = z.object({
  resetLock: z.boolean().default(false),
  payloadPatch: z.record(z.string(), z.unknown()).optional(),
});

export const maintenanceSnapshotQuerySchema = z.object({
  kind: z.string().optional(),
  status: z.string().optional(),
  take: z.coerce.number().int().min(1).max(500).default(100),
});
