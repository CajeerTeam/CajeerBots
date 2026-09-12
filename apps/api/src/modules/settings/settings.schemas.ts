import { z } from 'zod';

export const upsertSettingSchema = z.object({
  value: z.unknown(),
  reason: z.string().max(400).optional(),
});
