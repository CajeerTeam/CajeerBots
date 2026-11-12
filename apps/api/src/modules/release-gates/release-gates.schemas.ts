import { z } from 'zod';

export const listFreezesQuerySchema = z.object({
  version: z.string().optional(),
  channel: z.string().optional(),
  status: z.string().optional(),
  take: z.coerce.number().int().min(1).max(500).default(100),
});

export const upsertFreezeSchema = z.object({
  version: z.string().min(1).default('1.0.0-rc.5'),
  channel: z.enum(['alpha', 'beta', 'rc', 'stable', 'lts', 'security']).default('rc'),
  status: z.enum(['open', 'locked', 'released', 'cancelled']).default('locked'),
  reason: z.string().max(2000).optional().nullable(),
  scope: z.record(z.unknown()).default({ features: 'frozen', allowedChanges: ['bugfix', 'test', 'docs', 'security'] }),
});

export const listQualityGatesQuerySchema = z.object({
  category: z.string().optional(),
  enabled: z.coerce.boolean().optional(),
  take: z.coerce.number().int().min(1).max(500).default(100),
});

export const upsertQualityGateSchema = z.object({
  code: z.string().min(1).max(160),
  title: z.string().min(1).max(300),
  category: z.string().min(1).max(80),
  severity: z.enum(['info', 'warning', 'error', 'blocker']).default('error'),
  isEnabled: z.boolean().default(true),
  config: z.record(z.unknown()).default({}),
});

export const runGatesSchema = z.object({
  version: z.string().min(1).default('1.0.0-rc.5'),
  channel: z.enum(['alpha', 'beta', 'rc', 'stable', 'lts', 'security']).default('rc'),
  rootDir: z.string().min(1).default(process.cwd()),
  trigger: z.string().min(1).max(80).default('api'),
  persistBlockers: z.boolean().default(true),
  useDatabaseGates: z.boolean().default(true),
});

export const listRunsQuerySchema = z.object({
  version: z.string().optional(),
  channel: z.string().optional(),
  status: z.string().optional(),
  take: z.coerce.number().int().min(1).max(500).default(100),
});

export const listBlockersQuerySchema = z.object({
  status: z.string().optional(),
  severity: z.string().optional(),
  runId: z.string().optional(),
  take: z.coerce.number().int().min(1).max(1000).default(200),
});

export const resolveBlockerSchema = z.object({
  status: z.enum(['open', 'accepted', 'fixed', 'false_positive']).default('fixed'),
  reason: z.string().max(2000).optional().nullable(),
});

export const createReportSchema = z.object({
  version: z.string().min(1).default('1.0.0-rc.5'),
  channel: z.enum(['alpha', 'beta', 'rc', 'stable', 'lts', 'security']).default('rc'),
  gateRunId: z.string().optional(),
  rootDir: z.string().min(1).default(process.cwd()),
  createRunIfMissing: z.boolean().default(true),
});

export const listReportsQuerySchema = z.object({
  version: z.string().optional(),
  channel: z.string().optional(),
  status: z.string().optional(),
  take: z.coerce.number().int().min(1).max(500).default(100),
});
