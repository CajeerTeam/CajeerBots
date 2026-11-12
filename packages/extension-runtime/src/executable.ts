import { createHash } from 'node:crypto';
import { realpath } from 'node:fs/promises';
import { isAbsolute, resolve, relative, sep } from 'node:path';
import { pathToFileURL } from 'node:url';
import type { ExtensionManifest } from '@cajeerbots/extension-sdk';
interface ExecutableExtension { artifactSource?: string | null; artifactCodeSha256?: string | null; registryVersionId?: string | null }

export function resolveExtensionEntry(root: string, manifest: ExtensionManifest): string {
  const entry = manifest.entry || './dist/index.js';
  if (!/^@?[a-zA-Z0-9][a-zA-Z0-9._-]*(?:\/[a-zA-Z0-9][a-zA-Z0-9._-]*)*$/.test(manifest.name) || isAbsolute(entry) || entry.includes('\\') || entry.includes(':')) throw new Error('Unsafe extension path');
  const base = resolve(root, manifest.name);
  const target = resolve(base, entry);
  if (!target.startsWith(base + sep)) throw new Error('Unsafe extension entry');
  return target;
}

export async function resolveExecutable(extension: ExecutableExtension, manifest: ExtensionManifest, root = process.env.EXTENSIONS_ROOT ?? './extensions'): Promise<string> {
  if (extension.artifactSource && extension.artifactCodeSha256) {
    if (createHash('sha256').update(extension.artifactSource).digest('hex') !== extension.artifactCodeSha256) throw new Error('Stored extension code integrity failure');
    return `data:text/javascript;base64,${Buffer.from(extension.artifactSource).toString('base64')}`;
  }
  if (extension.registryVersionId || process.env.EXTENSIONS_ALLOW_LOCAL !== 'true') throw new Error('Signed extension artifact required (local code requires EXTENSIONS_ALLOW_LOCAL=true)');
  const base = await realpath(root);
  const target = await realpath(resolveExtensionEntry(base, manifest));
  const rel = relative(base, target);
  if (rel.startsWith('..' + sep) || rel === '..' || isAbsolute(rel)) throw new Error('Extension symlink escapes root');
  return pathToFileURL(target).href;
}
