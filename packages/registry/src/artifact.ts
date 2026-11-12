import { createHash } from 'node:crypto';
import { isDeepStrictEqual } from 'node:util';
import { spawnSync } from 'node:child_process';
import { extensionManifestSchema, checkEngineCompatibility } from '@cajeerbots/extension-sdk';
import { safeHttpRequest } from '@cajeerbots/security';
import { verifyArtifactIntegrity } from './integrity.js';

export const MAX_ARTIFACT_BYTES = 2 * 1024 * 1024;
export interface SignedVersion { version: string; manifest: unknown; artifactUrl: string; artifactSha256: string; artifactSize?: number | null; signature?: string | null; signatureAlgorithm?: string | null; isYanked: boolean }
export interface TrustedSource { isActive: boolean; publicKey?: string | null }
export async function downloadSignedArtifact(version: SignedVersion, source: TrustedSource, maxBytes = MAX_ARTIFACT_BYTES): Promise<Buffer> {
  if (!source.isActive || version.isYanked || !source.publicKey || !version.signature || version.signatureAlgorithm !== 'ed25519') throw new Error('Active trusted Ed25519 registry release required');
  if (new URL(version.artifactUrl).protocol !== 'https:') throw new Error('Artifact requires HTTPS');
  if (version.artifactSize != null && version.artifactSize > maxBytes) throw new Error('Artifact too large');
  const response = await safeHttpRequest(version.artifactUrl, { method: 'GET', maxResponseBytes: maxBytes });
  if (!response.ok) throw new Error(`Artifact download failed: ${response.status}`);
  const bytes = response.bytes();
  if (version.artifactSize != null && version.artifactSize !== bytes.length) throw new Error('Artifact size mismatch');
  const verified = verifyArtifactIntegrity({ bytes, sha256: version.artifactSha256, signature: version.signature, signatureAlgorithm: version.signatureAlgorithm, publicKey: source.publicKey });
  if (!verified.ok) throw new Error(verified.reasons.join('; '));
  return bytes;
}
export function parseExtensionArtifact(bytes: Buffer, expected: { name: string; version: string; manifest: unknown }, engine: string) {
  const raw = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes)) as { format?: unknown; manifest?: unknown; source?: unknown };
  if (raw.format !== 'cajeer-extension-v1' || typeof raw.source !== 'string' || !raw.source.trim()) throw new Error('Invalid extension bundle');
  const manifest = extensionManifestSchema.parse(raw.manifest);
  if (manifest.name !== expected.name || manifest.version !== expected.version || !isDeepStrictEqual(manifest, extensionManifestSchema.parse(expected.manifest))) throw new Error('Signed manifest does not match registry release');
  if (!checkEngineCompatibility(manifest.engine.range, engine).compatible) throw new Error('Incompatible extension');
  const checked = spawnSync(process.execPath, ['--input-type=module', '--check'], { input: raw.source, timeout: 5000, maxBuffer: 65536 });
  if (checked.status !== 0) throw new Error('Extension ESM syntax check failed');
  return { manifest, artifactSource: raw.source, artifactCodeSha256: createHash('sha256').update(raw.source).digest('hex') };
}
export async function fetchExtensionArtifact(version: SignedVersion, source: TrustedSource, name: string, engine: string) {
  return parseExtensionArtifact(await downloadSignedArtifact(version, source), { name, version: version.version, manifest: version.manifest }, engine);
}
