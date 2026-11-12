import { createHash } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { lstat } from 'node:fs/promises';
import type { ReleaseFileInput, ReleaseManifestInput } from './types.js';

const versionPattern = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$/;
const channels = ['alpha', 'beta', 'rc', 'stable', 'lts', 'security'];
function digestOf(product: unknown, version: unknown, channel: unknown, files: unknown): string {
  return createHash('sha256').update(JSON.stringify({ product, version, channel, files })).digest('hex');
}
function validFilePath(path: string): boolean {
  return path.length <= 512 && path.split('/').every(part => /^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(part));
}
export function buildReleaseManifest(input: ReleaseManifestInput): Record<string, unknown> {
  const sortedFiles = [...input.files].sort((a, b) => a.path.localeCompare(b.path));
  const manifest = {
    schema: 'cajeerbots.release.manifest.v1', product: input.product, version: input.version, channel: input.channel,
    commit: input.commit ?? null, createdBy: input.createdBy ?? 'cajeerbots', createdAt: new Date().toISOString(),
    files: sortedFiles, digest: digestOf(input.product, input.version, input.channel, sortedFiles),
  };
  const check = verifyReleaseManifest(manifest);
  if (!check.valid) throw new Error(check.errors.join('; '));
  return manifest;
}
export async function fileToReleaseInput(path: string, publicPath = path): Promise<ReleaseFileInput> {
  const info = await lstat(path);
  if (!info.isFile()) throw new Error('Release artifact must be a regular file');
  const hash = createHash('sha256'); let sizeBytes = 0;
  for await (const chunk of createReadStream(path)) { hash.update(chunk); sizeBytes += chunk.length; }
  if (sizeBytes !== info.size) throw new Error('Release artifact changed during read');
  return { path: publicPath, sizeBytes, sha256: hash.digest('hex') };
}
export function verifyReleaseManifest(manifest: Record<string, unknown>): { valid: boolean; errors: string[] } {
  const errors: string[] = [];
  if (manifest.schema !== 'cajeerbots.release.manifest.v1') errors.push('Неверная schema release manifest');
  if (typeof manifest.product !== 'string' || !manifest.product) errors.push('product обязателен');
  if (typeof manifest.version !== 'string' || !versionPattern.test(manifest.version)) errors.push('version должен быть SemVer');
  if (typeof manifest.channel !== 'string' || !channels.includes(manifest.channel)) errors.push('Invalid channel');
  if (!Array.isArray(manifest.files) || !manifest.files.length || manifest.files.length > 1000) errors.push('Invalid file list');
  else {
    const seen = new Set<string>();
    for (const file of manifest.files) {
      if (!file || typeof file.path !== 'string' || !validFilePath(file.path) || seen.has(file.path.toLowerCase()) || !Number.isSafeInteger(file.sizeBytes) || file.sizeBytes < 0 || typeof file.sha256 !== 'string' || !/^[a-f0-9]{64}$/i.test(file.sha256)) { errors.push('Invalid or duplicate artifact'); continue; }
      seen.add(file.path.toLowerCase());
    }
    if (manifest.digest !== digestOf(manifest.product, manifest.version, manifest.channel, manifest.files)) errors.push('Manifest digest mismatch');
  }
  return { valid: errors.length === 0, errors };
}
