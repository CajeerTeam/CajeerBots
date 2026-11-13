// Fail closed before expensive release work. Never print key material.
import { createPrivateKey, createPublicKey, sign, verify } from 'node:crypto';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

export function validateSigningKeys(privatePem, publicPem) {
  if (!privatePem?.trim()) throw new Error('Missing RELEASE_ED25519_PRIVATE_KEY secret in environment release');
  if (!publicPem?.trim()) throw new Error('Missing RELEASE_ED25519_PUBLIC_KEY variable in environment release');
  let privateKey, publicKey;
  try {
    if (!privatePem.trim().startsWith('-----BEGIN PRIVATE KEY-----')) throw new Error();
    privateKey = createPrivateKey(privatePem.trim());
  } catch {
    throw new Error('RELEASE_ED25519_PRIVATE_KEY must be an unencrypted PKCS#8 PEM private key (BEGIN PRIVATE KEY)');
  }
  try {
    if (!publicPem.trim().startsWith('-----BEGIN PUBLIC KEY-----')) throw new Error();
    publicKey = createPublicKey(publicPem.trim());
  } catch {
    throw new Error('RELEASE_ED25519_PUBLIC_KEY must be an SPKI PEM public key (BEGIN PUBLIC KEY)');
  }
  if (privateKey.asymmetricKeyType !== 'ed25519' || publicKey.asymmetricKeyType !== 'ed25519') {
    throw new Error('Release keys must both be Ed25519');
  }
  const challenge = Buffer.from('cajeerbots.release.signing-preflight.v1');
  if (!verify(null, challenge, publicKey, sign(null, challenge, privateKey))) {
    throw new Error('Release Ed25519 private and public keys do not match');
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  try {
    validateSigningKeys(process.env.RELEASE_PRIVATE_KEY, process.env.RELEASE_PUBLIC_KEY);
    console.log('Release Ed25519 signing key pair validated');
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
