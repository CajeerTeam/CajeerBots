import { createHash, createPrivateKey, createPublicKey, sign, verify } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { lstat, readFile, realpath } from 'node:fs/promises';
import { join } from 'node:path';

const channels = new Set(['alpha', 'beta', 'rc', 'stable', 'lts', 'security']);
export const validVersion = value => typeof value === 'string' && /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$/.test(value);
const validName = value => typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$/.test(value);
function check(condition, message) { if (!condition) throw new Error(message); }
export async function artifactFingerprint(dir, name) {
  check(validName(name), 'Invalid artifact filename');
  const path = join(dir, name);
  const stat = await lstat(path);
  check(stat.isFile() && !stat.isSymbolicLink(), 'Artifact must be a regular file');
  const hash = createHash('sha256'); let sizeBytes = 0;
  for await (const chunk of createReadStream(path)) { hash.update(chunk); sizeBytes += chunk.length; }
  check(sizeBytes === stat.size && sizeBytes > 0, 'Artifact empty or changed during read');
  return { path: name, sizeBytes, sha256: hash.digest('hex') };
}
export function manifestDigest(manifest) {
  return createHash('sha256').update(JSON.stringify({ product: manifest.product, version: manifest.version, channel: manifest.channel, files: manifest.files })).digest('hex');
}
export function validateManifest(manifest) {
  check(manifest?.schema === 'cajeerbots.release.manifest.v1' && manifest.product === 'cajeerbots', 'Invalid release schema/product');
  check(validVersion(manifest.version) && channels.has(manifest.channel), 'Invalid release version/channel');
  check(typeof manifest.commit === 'string' && /^[a-f0-9]{40}$/.test(manifest.commit), 'Invalid release commit');
  check(Array.isArray(manifest.files) && manifest.files.length > 0 && manifest.files.length <= 32, 'Invalid artifact list');
  const seen = new Set();
  for (const file of manifest.files) {
    check(file && validName(file.path) && !seen.has(file.path.toLowerCase()) && Number.isSafeInteger(file.sizeBytes) && file.sizeBytes > 0 && /^[a-f0-9]{64}$/.test(file.sha256), 'Invalid/duplicate artifact');
    seen.add(file.path.toLowerCase());
  }
  check(manifest.digest === manifestDigest(manifest), 'Manifest digest mismatch');
  return manifest;
}
export function signManifest(bytes, privatePem) {
  const key = createPrivateKey(privatePem); check(key.asymmetricKeyType === 'ed25519', 'Ed25519 signing key required');
  return sign(null, bytes, key).toString('base64') + '\n';
}
export async function verifyReleaseFiles(dir, manifestName, signatureName, trustedPublicPem, expectedVersion) {
  check(validName(manifestName) && validName(signatureName), 'Invalid manifest filename');
  dir = await realpath(dir);
  const manifestPath = join(dir, manifestName), signaturePath = join(dir, signatureName);
  for (const [path, limit] of [[manifestPath, 1024 * 1024], [signaturePath, 256]]) {
    const stat = await lstat(path); check(stat.isFile() && stat.size <= limit, 'Invalid manifest/signature file');
  }
  const bytes = await readFile(manifestPath), signature = (await readFile(signaturePath, 'utf8')).trim();
  const key = createPublicKey(trustedPublicPem);
  check(key.asymmetricKeyType === 'ed25519' && /^[A-Za-z0-9+/]{86}==$/.test(signature) && verify(null, bytes, key, Buffer.from(signature, 'base64')), 'Release signature invalid');
  const manifest = validateManifest(JSON.parse(bytes.toString('utf8')));
  check(!expectedVersion || manifest.version === expectedVersion, 'Unexpected release version');
  for (const file of manifest.files) {
    check(file.path !== manifestName && file.path !== signatureName, 'Manifest references itself');
    const actual = await artifactFingerprint(dir, file.path);
    check(actual.sizeBytes === file.sizeBytes && actual.sha256 === file.sha256, `Artifact mismatch: ${file.path}`);
  }
  return manifest;
}
