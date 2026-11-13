import { z } from 'zod';

const extensionCommandSchema = z.object({
  name: z.string().min(1).max(80).regex(/^[a-z0-9_\-:.]+$/i),
  description: z.string().min(1).max(500),
  templateKey: z.string().min(1).max(160).optional(),
  scopes: z.array(z.string().min(1).max(120)).default([]),
});

export const extensionManifestSchema = z.object({
  name: z.string().min(1),
  version: z.string().min(1),
  displayName: z.string().min(1),
  description: z.string().optional(),
  engine: z.object({
    apiVersion: z.string().min(1),
    range: z.string().min(1),
  }),
  permissions: z.array(z.string()).default([]),
  hooks: z.array(z.string()).default([]),
  entry: z.string().default('./dist/index.js'),
  commands: z.array(extensionCommandSchema).default([]),
  configSchema: z.record(z.string(), z.unknown()).optional(),
  capabilities: z.array(z.string()).default([]),
  signature: z.string().optional(),
});

export type ExtensionManifest = z.infer<typeof extensionManifestSchema>;
