import { z } from 'zod';

export const createCommunitySchema = z.object({
  slug: z.string().min(2).max(64).regex(/^[a-z0-9][a-z0-9-]*[a-z0-9]$/),
  title: z.string().min(2).max(120),
  language: z.string().min(2).max(12).default('ru'),
});

export const updateCommunitySchema = createCommunitySchema.partial().extend({
  language: createCommunitySchema.shape.language.unwrap().optional(),
}).refine((value) => Object.keys(value).length > 0, 'Нужно передать хотя бы одно поле');
