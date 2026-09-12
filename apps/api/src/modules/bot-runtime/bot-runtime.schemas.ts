import { z } from 'zod';

export const listRuntimeQuerySchema = z.object({
  accountId: z.string().optional(),
  status: z.string().optional(),
  take: z.coerce.number().int().min(1).max(500).default(50),
});

export const retryOutboundSchema = z.object({
  resetAttempts: z.boolean().optional().default(false),
});
