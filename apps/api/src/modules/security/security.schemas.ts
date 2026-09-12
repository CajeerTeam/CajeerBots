import { z } from 'zod';

export const listSecurityPoliciesQuerySchema = z.object({
  category: z.string().optional(),
  enabled: z.coerce.boolean().optional(),
  take: z.coerce.number().int().min(1).max(500).default(100),
});

export const upsertSecurityPolicySchema = z.object({
  code: z.string().min(1).max(180),
  title: z.string().min(1).max(300),
  category: z.string().min(1).max(80),
  severity: z.enum(['info', 'warning', 'error', 'critical']).default('error'),
  isEnabled: z.boolean().default(true),
  config: z.record(z.unknown()).default({}),
});

export const runSecurityAuditSchema = z.object({
  version: z.string().min(1).default('1.0.0-rc.1'),
  channel: z.string().min(1).default('beta'),
  trigger: z.string().min(1).max(80).default('api'),
  persist: z.boolean().default(true),
});

export const listSecurityAuditRunsQuerySchema = z.object({
  version: z.string().optional(),
  channel: z.string().optional(),
  status: z.string().optional(),
  take: z.coerce.number().int().min(1).max(500).default(100),
});

export const listSecurityFindingsQuerySchema = z.object({
  runId: z.string().optional(),
  status: z.string().optional(),
  severity: z.string().optional(),
  category: z.string().optional(),
  subjectType: z.string().optional(),
  subjectId: z.string().optional(),
  take: z.coerce.number().int().min(1).max(1000).default(200),
});

export const resolveSecurityFindingSchema = z.object({
  status: z.enum(['open', 'accepted', 'fixed', 'false_positive']).default('fixed'),
  reason: z.string().max(2000).optional().nullable(),
});

export const listRiskSnapshotsQuerySchema = z.object({
  version: z.string().optional(),
  channel: z.string().optional(),
  status: z.string().optional(),
  take: z.coerce.number().int().min(1).max(500).default(100),
});
