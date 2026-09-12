import { z } from 'zod';

export const createWebhookEndpointSchema = z.object({
  title: z.string().min(2).max(120),
  url: z.string().url(),
  events: z.array(z.string().min(1)).min(1),
  secret: z.string().min(16).optional(),
  isActive: z.boolean().default(true),
});
