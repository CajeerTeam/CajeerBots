import { z } from 'zod';
import { extensionManifestSchema } from '@cajeerbots/extension-sdk';
import { compareSemver } from './semver.js';

export const registryKindSchema = z.enum(['core', 'module', 'plugin', 'theme', 'sdk']);
export const registryChannelSchema = z.enum(['stable', 'lts', 'beta', 'alpha', 'security']);

export const registryArtifactSchema = z.object({
  url: z.string().min(1),
  sha256: z.string().regex(/^[a-f0-9]{64}$/i),
  size: z.number().int().positive().optional(),
  signature: z.string().optional(),
  signatureAlgorithm: z.enum(['ed25519', 'sha256-hmac', 'none']).default('none'),
});

export const registryVersionSchema = z.object({
  version: z.string().min(1),
  engine: z.object({ range: z.string().min(1) }).default({ range: '*' }),
  channel: registryChannelSchema.default('stable'),
  publishedAt: z.string().datetime().optional(),
  yanked: z.boolean().default(false),
  releaseNotes: z.string().optional(),
  artifact: registryArtifactSchema,
  manifest: extensionManifestSchema.optional(),
  metadata: z.record(z.string(), z.unknown()).optional(),
});

export const registryPackageSchema = z.object({
  name: z.string().min(1).max(160),
  displayName: z.string().max(200).optional(),
  description: z.string().max(2000).optional(),
  kind: registryKindSchema.default('plugin'),
  publisher: z.string().max(160).optional(),
  homepage: z.string().url().optional(),
  repository: z.string().url().optional(),
  license: z.string().max(80).optional(),
  official: z.boolean().default(false),
  versions: z.array(registryVersionSchema).min(1),
  metadata: z.record(z.string(), z.unknown()).optional(),
});

export const registryManifestSchema = z.object({
  schemaVersion: z.literal('1.0'),
  name: z.string().min(1),
  generatedAt: z.string().datetime().optional(),
  packages: z.array(registryPackageSchema).default([]),
  signature: z.string().optional(),
});

export type RegistryManifest = z.infer<typeof registryManifestSchema>;
export type RegistryPackage = z.infer<typeof registryPackageSchema>;
export type RegistryPackageVersion = z.infer<typeof registryVersionSchema>;
export type RegistryChannel = z.infer<typeof registryChannelSchema>;
export type RegistryKind = z.infer<typeof registryKindSchema>;

export function parseRegistryManifest(value: unknown): RegistryManifest {
  return registryManifestSchema.parse(value);
}

export function latestVersion(versions: readonly RegistryPackageVersion[], channel?: RegistryChannel): RegistryPackageVersion | null {
  const candidates = versions.filter((version) => !version.yanked && (!channel || version.channel === channel));
  return candidates.sort((a, b) => compareVersionsDesc(a.version, b.version))[0] ?? null;
}

function compareVersionsDesc(a: string, b: string): number {
  return compareSemver(b, a);
}
