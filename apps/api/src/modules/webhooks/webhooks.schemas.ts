import { z } from 'zod';

export const incomingEventSchema = z.object({
  id: z.string().min(1),
  type: z.string().min(1),
  occurredAt: z.string().datetime().optional(),
  payload: z.unknown(),
  metadata: z.record(z.unknown()).optional(),
});
