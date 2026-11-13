import { z } from 'zod';
import { jsonRecordSchema, jsonValueSchema } from '../shared/json.schemas.js';
import { extensionManifestSchema } from '@cajeerbots/extension-sdk';

const permissionSchema = z.string().min(1).max(160);

export const installExtensionSchema = z.object({
  manifest: extensionManifestSchema,
  signature: z.string().optional(),
  autoGrant: z.boolean().default(false),
});

export const grantExtensionPermissionsSchema = z.object({
  permissions: z.array(permissionSchema).min(1),
  reason: z.string().max(500).optional(),
});

export const revokeExtensionPermissionsSchema = grantExtensionPermissionsSchema;

export const updateExtensionConfigSchema = z.object({
  value: jsonRecordSchema.default({}),
  reason: z.string().max(500).optional(),
});

export const enqueueExtensionEventSchema = z.object({
  hook: z.string().min(1).max(160),
  payload: jsonValueSchema.default({}),
});

export const setExtensionStorageSchema = z.object({
  value: jsonValueSchema,
});
