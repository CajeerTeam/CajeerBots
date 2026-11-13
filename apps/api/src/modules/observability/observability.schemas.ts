import { z } from 'zod';

export const listSamplesQuerySchema = z.object({
  metric: z.string().optional(),
  source: z.string().optional(),
  severity: z.string().optional(),
  communityId: z.string().optional(),
  sinceMinutes: z.coerce.number().int().min(1).max(60 * 24 * 30).default(60),
  take: z.coerce.number().int().min(1).max(2000).default(200),
});

export const collectSnapshotSchema = z.object({
  includeDiagnostics: z.boolean().optional().default(true),
  evaluateAlerts: z.boolean().optional().default(true),
  runProbes: z.boolean().optional().default(true),
});

export const upsertProbeSchema = z.object({
  code: z.string().min(1).max(120).regex(/^[a-z0-9][a-z0-9._-]*$/i),
  title: z.string().min(1).max(200),
  type: z.enum(['database', 'redis_tcp', 'tcp', 'storage']).default('database'),
  target: z.string().optional().nullable(),
  intervalSeconds: z.number().int().min(10).max(86400).default(60),
  timeoutMs: z.number().int().min(100).max(30000).default(2000),
  isEnabled: z.boolean().default(true),
  metadata: z.record(z.string(), z.unknown()).optional(),
});

export const listProbesQuerySchema = z.object({
  status: z.string().optional(),
  enabled: z.coerce.boolean().optional(),
  take: z.coerce.number().int().min(1).max(500).default(100),
});

export const upsertAlertRuleSchema = z.object({
  code: z.string().min(1).max(120).regex(/^[a-z0-9][a-z0-9._-]*$/i),
  title: z.string().min(1).max(200),
  metric: z.string().min(1).max(200),
  operator: z.enum(['gt', 'gte', 'lt', 'lte', 'eq', 'neq']).default('gte'),
  threshold: z.number(),
  severity: z.enum(['info', 'warning', 'critical']).default('warning'),
  durationSeconds: z.number().int().min(0).max(86400).default(0),
  cooldownSeconds: z.number().int().min(0).max(86400).default(300),
  labels: z.record(z.string(), z.unknown()).optional(),
  isEnabled: z.boolean().default(true),
});

export const listAlertRulesQuerySchema = z.object({
  metric: z.string().optional(),
  enabled: z.coerce.boolean().optional(),
  severity: z.string().optional(),
  take: z.coerce.number().int().min(1).max(500).default(100),
});

export const listIncidentsQuerySchema = z.object({
  status: z.string().optional(),
  severity: z.string().optional(),
  metric: z.string().optional(),
  take: z.coerce.number().int().min(1).max(500).default(100),
});

export const resolveIncidentSchema = z.object({
  reason: z.string().max(2000).optional(),
});
