import { createHash, createHmac, randomBytes, timingSafeEqual } from 'node:crypto';

export interface SignedPayloadOptions {
  timestamp?: number;
  toleranceSeconds?: number;
}

export function sha256(input: string): string {
  return createHash('sha256').update(input).digest('hex');
}

export function randomSecret(bytes = 32): string {
  return randomBytes(bytes).toString('base64url');
}

export function signPayload(payload: string, secret: string, options: SignedPayloadOptions = {}): string {
  const timestamp = options.timestamp ?? Math.floor(Date.now() / 1000);
  const digest = createHmac('sha256', secret).update(`${timestamp}.${payload}`).digest('hex');
  return `t=${timestamp},v1=${digest}`;
}

export function verifyPayloadSignature(payload: string, secret: string, signature: string, options: SignedPayloadOptions = {}): boolean {
  const parsed = parseSignature(signature);
  if (!parsed) return false;

  const toleranceSeconds = options.toleranceSeconds ?? 300;
  const now = Math.floor(Date.now() / 1000);
  if (Math.abs(now - parsed.timestamp) > toleranceSeconds) return false;

  const expected = signPayload(payload, secret, { timestamp: parsed.timestamp }).split('v1=')[1];
  if (!expected) return false;

  const expectedBuffer = Buffer.from(expected, 'hex');
  const actualBuffer = Buffer.from(parsed.digest, 'hex');
  return expectedBuffer.length === actualBuffer.length && timingSafeEqual(expectedBuffer, actualBuffer);
}

function parseSignature(signature: string): { timestamp: number; digest: string } | null {
  const parts = new Map(signature.split(',').map((item) => {
    const [key, value] = item.trim().split('=');
    return [key, value] as const;
  }));
  const timestamp = Number(parts.get('t'));
  const digest = parts.get('v1');
  if (!Number.isSafeInteger(timestamp) || !digest || !/^[a-f0-9]{64}$/i.test(digest)) return null;
  return { timestamp, digest };
}
