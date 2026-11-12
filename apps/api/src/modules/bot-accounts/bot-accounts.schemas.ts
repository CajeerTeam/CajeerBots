import { z } from 'zod';

const provider = z.enum(['telegram', 'vk', 'discord']);
const mode = z.enum(['polling', 'webhook']);

export const createBotAccountSchema = z.object({
  provider,
  mode: mode.default('polling'),
  externalId: z.string().max(120).optional(),
  username: z.string().max(120).optional(),
  tokenRef: z.string().max(400).regex(/^(env:[A-Z][A-Z0-9_]*|secret:[A-Za-z0-9_.:-]+)$/).optional(),
  isActive: z.boolean().default(true),
});

export const updateBotAccountSchema = z.object({
  mode: mode.optional(),
  externalId: z.string().max(120).optional(),
  username: z.string().max(120).optional(),
  tokenRef: z.string().max(400).regex(/^(env:[A-Z][A-Z0-9_]*|secret:[A-Za-z0-9_.:-]+)$/).optional(),
  isActive: z.boolean().optional(),
}).refine((value) => Object.keys(value).length > 0, 'Нужно передать хотя бы одно поле');
