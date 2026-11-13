import { z } from 'zod';
import { jsonRecordSchema, jsonValueSchema } from '../shared/json.schemas.js';

export const incomingEventSchema = z.object({
  id: z.string().min(1),
  type: z.string().min(1),
  occurredAt: z.string().datetime().optional(),
  payload: jsonValueSchema,
  metadata: jsonRecordSchema.optional(),
});
