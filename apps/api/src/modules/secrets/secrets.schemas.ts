import { z } from 'zod';

export const secretKeySchema = z.string().min(2).max(160).regex(/^[a-zA-Z0-9._:-]+$/, 'secret key может содержать буквы, цифры, точку, двоеточие, underscore и dash');

export const createSecretSchema = z.object({
  key: secretKeySchema,
  value: z.string().min(1),
  description: z.string().max(500).optional(),
  reason: z.string().max(500).optional(),
});

export const updateSecretSchema = z.object({
  value: z.string().min(1),
  description: z.string().max(500).optional(),
  reason: z.string().max(500).optional(),
});

export const rotateSecretSchema = updateSecretSchema;
