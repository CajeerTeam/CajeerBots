import { z } from 'zod';

export const botProviderSchema = z.enum(['telegram', 'vk', 'discord']);

export const createUserSchema = z.object({
  provider: botProviderSchema,
  externalId: z.string().min(1).max(120),
  username: z.string().max(120).optional(),
  displayName: z.string().max(160).optional(),
  locale: z.string().min(2).max(12).default('ru'),
});

export const updateUserSchema = z.object({
  username: z.string().max(120).optional(),
  displayName: z.string().max(160).optional(),
  locale: z.string().min(2).max(12).optional(),
}).refine((value) => Object.keys(value).length > 0, 'Нужно передать хотя бы одно поле');

export const assignRoleSchema = z.object({
  roleId: z.string().min(1).optional(),
  roleCode: z.string().min(1).max(80).optional(),
}).refine((value) => Boolean(value.roleId || value.roleCode), 'Нужно передать roleId или roleCode');
