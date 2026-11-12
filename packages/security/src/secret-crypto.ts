import { createCipheriv, createDecipheriv, createHash, randomBytes, timingSafeEqual } from 'node:crypto';

export interface EncryptedSecretPayload {
  algorithm: 'aes-256-gcm';
  ciphertext: string;
  iv: string;
  authTag: string;
}

export function encryptSecret(plainText: string, keyMaterial: string): EncryptedSecretPayload {
  assertSecretKeyMaterial(keyMaterial);
  const iv = randomBytes(12);
  const key = deriveKey(keyMaterial);
  const cipher = createCipheriv('aes-256-gcm', key, iv);
  const ciphertext = Buffer.concat([cipher.update(plainText, 'utf8'), cipher.final()]);
  const authTag = cipher.getAuthTag();
  return {
    algorithm: 'aes-256-gcm',
    ciphertext: ciphertext.toString('base64url'),
    iv: iv.toString('base64url'),
    authTag: authTag.toString('base64url'),
  };
}

export function decryptSecret(payload: EncryptedSecretPayload, keyMaterial: string): string {
  assertSecretKeyMaterial(keyMaterial);
  if (payload.algorithm !== 'aes-256-gcm') throw new Error(`Неподдерживаемый алгоритм секрета: ${payload.algorithm}`);
  const key = deriveKey(keyMaterial);
  const iv = Buffer.from(payload.iv, 'base64url');
  const tag = Buffer.from(payload.authTag, 'base64url');
  if (iv.length !== 12 || tag.length !== 16) throw new Error('Invalid AES-GCM IV or authentication tag');
  const decipher = createDecipheriv('aes-256-gcm', key, iv, { authTagLength: 16 });
  decipher.setAuthTag(tag);
  return Buffer.concat([decipher.update(Buffer.from(payload.ciphertext, 'base64url')), decipher.final()]).toString('utf8');
}

export function maskSecret(value: string): string {
  if (!value) return '';
  if (value.length <= 8) return '••••';
  return `${value.slice(0, 4)}••••${value.slice(-4)}`;
}

export function assertSecretKeyMaterial(value: string | undefined): asserts value is string {
  if (!value || value.length < 16) throw new Error('SECRETS_ENCRYPTION_KEY должен быть задан и быть не короче 16 символов');
}

export function safeCompareSecret(left: string, right: string): boolean {
  const a = Buffer.from(left);
  const b = Buffer.from(right);
  return a.length === b.length && timingSafeEqual(a, b);
}

function deriveKey(keyMaterial: string): Buffer {
  return createHash('sha256').update(keyMaterial).digest();
}
