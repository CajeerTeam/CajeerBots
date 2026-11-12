import { z } from 'zod';

const channelSchema = z.enum(['rc', 'stable', 'lts', 'security']).default('rc');

export const listQuerySchema = z.object({
  version: z.string().optional(),
  channel: z.string().optional(),
  status: z.string().optional(),
  take: z.coerce.number().int().min(1).max(500).default(100),
});

export const createApiFreezeSchema = z.object({
  version: z.string().min(1).default('1.0.0-rc.5'),
  channel: channelSchema,
  sourcePath: z.string().min(1).default('contracts/openapi/cajeerbots.openapi.yaml'),
  rootDir: z.string().min(1).default(process.cwd()),
  status: z.enum(['frozen', 'superseded', 'cancelled']).default('frozen'),
  notes: z.string().max(4000).optional().nullable(),
});

export const verifyApiFreezeSchema = z.object({
  freezeId: z.string().optional(),
  version: z.string().min(1).default('1.0.0-rc.5'),
  channel: channelSchema,
  sourcePath: z.string().min(1).optional(),
  rootDir: z.string().min(1).default(process.cwd()),
  allowAdditive: z.boolean().default(false),
});

export const upsertCompatibilityTargetSchema = z.object({
  code: z.string().min(1).max(160),
  kind: z.enum(['runtime', 'database', 'cache', 'queue', 'provider', 'deployment', 'tooling']),
  name: z.string().min(1).max(240),
  versionRange: z.string().min(1).max(120),
  required: z.boolean().default(true),
  status: z.enum(['supported', 'experimental', 'deprecated', 'blocked']).default('supported'),
  notes: z.string().max(4000).optional().nullable(),
  checks: z.array(z.record(z.unknown())).default([]),
});

export const buildMatrixSchema = z.object({
  version: z.string().min(1).default('1.0.0-rc.5'),
  channel: channelSchema,
  persist: z.boolean().default(true),
});

export const createUpgradePlanSchema = z.object({
  fromVersion: z.string().min(1).default('0.18.0-beta.3'),
  toVersion: z.string().min(1).default('1.0.0-rc.5'),
  channel: channelSchema,
  hasBackup: z.boolean().default(false),
  status: z.enum(['draft', 'ready', 'blocked']).optional(),
});

export const runUpgradePlanSchema = z.object({
  mode: z.enum(['dry-run', 'apply']).default('dry-run'),
  rootDir: z.string().min(1).default(process.cwd()),
  notes: z.string().max(4000).optional().nullable(),
});

export const runReadinessSchema = z.object({
  version: z.string().min(1).default('1.0.0-rc.5'),
  channel: channelSchema,
  rootDir: z.string().min(1).default(process.cwd()),
  persist: z.boolean().default(true),
});
