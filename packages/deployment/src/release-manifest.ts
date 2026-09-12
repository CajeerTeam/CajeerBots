import { createHash } from 'node:crypto';
import { readFile, stat } from 'node:fs/promises';
import type { ReleaseFileInput, ReleaseManifestInput } from './types.js';

export function buildReleaseManifest(input: ReleaseManifestInput): Record<string, unknown> {
  const sortedFiles = [...input.files].sort((a, b) => a.path.localeCompare(b.path));
  const digest = createHash('sha256')
    .update(JSON.stringify({ product: input.product, version: input.version, channel: input.channel, files: sortedFiles }))
    .digest('hex');
  return {
    schema: 'cajeerbots.release.manifest.v1',
    product: input.product,
    version: input.version,
    channel: input.channel,
    commit: input.commit ?? null,
    createdBy: input.createdBy ?? 'cajeerbots',
    createdAt: new Date().toISOString(),
    files: sortedFiles,
    digest,
  };
}

export async function fileToReleaseInput(path: string, publicPath = path): Promise<ReleaseFileInput> {
  const [buffer, info] = await Promise.all([readFile(path), stat(path)]);
  return { path: publicPath, sizeBytes: info.size, sha256: createHash('sha256').update(buffer).digest('hex') };
}

export function verifyReleaseManifest(manifest: Record<string, unknown>): { valid: boolean; errors: string[] } {
  const errors: string[] = [];
  if (manifest.schema !== 'cajeerbots.release.manifest.v1') errors.push('Неверная schema release manifest');
  if (typeof manifest.product !== 'string' || !manifest.product) errors.push('product обязателен');
  if (typeof manifest.version !== 'string' || !/^\d+\.\d+\.\d+/.test(manifest.version)) errors.push('version должен быть SemVer');
  if (!Array.isArray(manifest.files)) errors.push('files должен быть массивом');
  return { valid: errors.length === 0, errors };
}
