import { z } from 'zod';

const jsonRecord = z.record(z.string(), z.unknown());

export const upsertEventTopicSchema = z.object({
  description: z.string().max(500).optional(),
  schema: jsonRecord.optional(),
  isActive: z.boolean().optional(),
  retentionDays: z.coerce.number().int().min(1).max(3650).optional(),
});

export const publishEventSchema = z.object({
  id: z.string().min(1).max(200).optional(),
  topic: z.string().min(1).max(180).optional(),
  type: z.string().min(1).max(180),
  source: z.string().min(1).max(80).default('api'),
  communityId: z.string().min(1).optional(),
  aggregateType: z.string().min(1).max(120).optional(),
  aggregateId: z.string().min(1).max(200).optional(),
  payload: z.unknown(),
  metadata: jsonRecord.optional(),
  headers: jsonRecord.optional(),
  fanoutToOutbox: z.boolean().default(false),
});

export const createEventSubscriptionSchema = z.object({
  name: z.string().min(2).max(120),
  topicPattern: z.string().min(1).max(180),
  targetType: z.enum(['outbox', 'webhook_endpoint', 'extension_hook', 'log']),
  targetRef: z.string().min(1).max(300),
  filters: jsonRecord.optional(),
  isActive: z.boolean().optional(),
  maxAttempts: z.coerce.number().int().min(1).max(50).optional(),
});

export const updateEventSubscriptionSchema = createEventSubscriptionSchema.partial().omit({ name: true });

export const listEventsQuerySchema = z.object({
  topic: z.string().optional(),
  type: z.string().optional(),
  communityId: z.string().optional(),
  aggregateType: z.string().optional(),
  aggregateId: z.string().optional(),
  source: z.string().optional(),
  take: z.coerce.number().int().min(1).max(500).default(50),
});

export const retryDeliverySchema = z.object({
  resetAttempts: z.boolean().default(false),
});
