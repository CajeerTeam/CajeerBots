import { z } from 'zod';

const take = z.coerce.number().int().min(1).max(500).default(100);

export const listSuitesQuerySchema = z.object({ category: z.string().optional(), active: z.coerce.boolean().optional(), take });
export const upsertSuiteSchema = z.object({ code: z.string().min(2).max(120), title: z.string().min(2).max(200), category: z.string().default('mvp'), description: z.string().optional().nullable(), isActive: z.boolean().default(true), metadata: z.record(z.unknown()).default({}) });
export const listCasesQuerySchema = z.object({ suiteCode: z.string().optional(), target: z.string().optional(), kind: z.string().optional(), provider: z.string().optional(), active: z.coerce.boolean().optional(), take });
export const upsertCaseSchema = z.object({ suiteCode: z.string().min(2).max(120), code: z.string().min(2).max(120), title: z.string().min(2).max(200), target: z.string().default('api'), kind: z.string().default('multi_step'), provider: z.string().optional().nullable(), priority: z.coerce.number().int().default(100), isActive: z.boolean().default(true), timeoutMs: z.coerce.number().int().min(1000).max(600000).default(30000), definition: z.record(z.unknown()).default({ steps: [] }), expected: z.record(z.unknown()).default({}) });
export const runSuiteSchema = z.object({ version: z.string().default('1.0.0-rc.3'), channel: z.string().default('beta'), trigger: z.string().default('manual'), baseUrl: z.string().optional(), apiToken: z.string().optional(), dryRun: z.boolean().default(false), rootDir: z.string().optional() });
export const runCaseSchema = runSuiteSchema.extend({ suiteCode: z.string().optional() });
export const queueRunSchema = z.object({ suiteCode: z.string().optional(), caseCode: z.string().optional(), version: z.string().default('1.0.0-rc.3'), channel: z.string().default('beta'), trigger: z.string().default('queued'), rootDir: z.string().optional(), environment: z.record(z.unknown()).default({}) });
export const listRunsQuerySchema = z.object({ suiteCode: z.string().optional(), caseCode: z.string().optional(), status: z.string().optional(), version: z.string().optional(), channel: z.string().optional(), take });
export const listStepRunsQuerySchema = z.object({ runId: z.string().optional(), caseId: z.string().optional(), status: z.string().optional(), kind: z.string().optional(), take });
export const coverageSchema = z.object({ version: z.string().default('1.0.0-rc.3'), channel: z.string().default('beta'), persist: z.boolean().default(true) });
export const listCoverageQuerySchema = z.object({ version: z.string().optional(), channel: z.string().optional(), status: z.string().optional(), take });
