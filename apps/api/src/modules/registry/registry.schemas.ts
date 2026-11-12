import { z } from 'zod';

export const createRegistrySourceSchema = z.object({
  name: z.string().min(1).max(120),
  url: z.string().min(1),
  type: z.string().min(1).max(80).default('gitflic_registry'),
  authSecretRef: z.string().max(200).optional(),
  publicKey: z.string().optional(),
  isActive: z.boolean().default(true),
});

export const updateRegistrySourceSchema = createRegistrySourceSchema.partial();

export const listRegistryPackagesQuerySchema = z.object({
  sourceId: z.string().optional(),
  kind: z.string().optional(),
  q: z.string().optional(),
  take: z.coerce.number().int().min(1).max(500).default(100),
});

export const installRegistryPackageSchema = z.object({
  sourceId: z.string().optional(),
  version: z.string().optional(),
  channel: z.enum(['stable', 'lts', 'beta', 'alpha', 'security']).optional(),
  autoGrant: z.boolean().default(false),
});
