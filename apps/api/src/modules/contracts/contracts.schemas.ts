import { z } from 'zod';

export const listSnapshotsQuerySchema = z.object({
  version: z.string().optional(),
  baselineOnly: z.coerce.boolean().optional(),
  take: z.coerce.number().int().min(1).max(500).default(50),
});

export const createSnapshotSchema = z.object({
  sourcePath: z.string().min(1).default('contracts/openapi/cajeerbots.openapi.yaml'),
  version: z.string().optional(),
  makeBaseline: z.boolean().default(false),
});

export const runContractCheckSchema = z.object({
  snapshotId: z.string().optional(),
  baselineId: z.string().optional(),
  sourcePath: z.string().min(1).default('contracts/openapi/cajeerbots.openapi.yaml'),
  sdkSourcePath: z.string().min(1).default('packages/sdk/src/client.ts'),
  failOnBreaking: z.boolean().default(true),
  failOnUnsecured: z.boolean().default(true),
  minSdkCoveragePercent: z.number().min(0).max(100).default(50),
});

export const listChecksQuerySchema = z.object({
  status: z.string().optional(),
  checkType: z.string().optional(),
  snapshotId: z.string().optional(),
  take: z.coerce.number().int().min(1).max(500).default(100),
});

export const generateSdkSchema = z.object({
  snapshotId: z.string().optional(),
  sourcePath: z.string().min(1).default('contracts/openapi/cajeerbots.openapi.yaml'),
  sdkSourcePath: z.string().min(1).default('packages/sdk/src/client.ts'),
  outputMode: z.enum(['database', 'file']).default('database'),
  outputDir: z.string().min(1).default('packages/sdk/src/generated'),
});

export const listSdkRunsQuerySchema = z.object({
  status: z.string().optional(),
  snapshotId: z.string().optional(),
  take: z.coerce.number().int().min(1).max(500).default(100),
});

export const listDeprecationsQuerySchema = z.object({
  status: z.string().optional(),
  take: z.coerce.number().int().min(1).max(500).default(100),
});

export const upsertDeprecationSchema = z.object({
  operationId: z.string().min(1).max(200),
  method: z.string().min(1).max(20).transform((value) => value.toUpperCase()),
  path: z.string().min(1).max(300),
  status: z.enum(['planned', 'deprecated', 'removed', 'cancelled']).default('planned'),
  reason: z.string().max(2000).optional().nullable(),
  replacement: z.string().max(200).optional().nullable(),
  deprecatedSince: z.string().max(50).optional().nullable(),
  removeAfter: z.string().max(50).optional().nullable(),
  metadata: z.record(z.unknown()).optional(),
});
