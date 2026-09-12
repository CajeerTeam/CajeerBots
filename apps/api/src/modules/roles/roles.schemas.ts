import { z } from 'zod';

export const createRoleSchema = z.object({
  code: z.string().min(2).max(80).regex(/^[a-z0-9][a-z0-9:_-]*$/),
  title: z.string().min(2).max(120),
  priority: z.number().int().min(0).max(10000).default(100),
  scopes: z.array(z.string().min(1)).default([]),
});

export const setRolePermissionsSchema = z.object({
  scopes: z.array(z.string().min(1)).default([]),
});
