import { createHmac, randomBytes, timingSafeEqual } from 'node:crypto';

const BASE32_ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';

export interface TotpOptions {
  period?: number;
  digits?: number;
  window?: number;
  timestamp?: number;
}

export function createTotpSecret(bytes = 20): string {
  return base32Encode(randomBytes(bytes));
}

export function createOtpAuthUri(input: { issuer: string; accountName: string; secret: string; digits?: number; period?: number }): string {
  const issuer = encodeURIComponent(input.issuer);
  const account = encodeURIComponent(input.accountName);
  const digits = input.digits ?? 6;
  const period = input.period ?? 30;
  return `otpauth://totp/${issuer}:${account}?secret=${input.secret}&issuer=${issuer}&algorithm=SHA1&digits=${digits}&period=${period}`;
}

export function generateTotp(secret: string, options: TotpOptions = {}): string {
  const period = options.period ?? 30;
  const digits = options.digits ?? 6;
  const timestamp = options.timestamp ?? Date.now();
  const counter = Math.floor(timestamp / 1000 / period);
  return hotp(secret, counter, digits);
}

export function verifyTotp(token: string, secret: string, options: TotpOptions = {}): boolean {
  const normalized = token.replace(/\s+/g, '');
  const window = options.window ?? 1;
  const period = options.period ?? 30;
  const timestamp = options.timestamp ?? Date.now();
  for (let offset = -window; offset <= window; offset += 1) {
    const candidate = generateTotp(secret, { ...options, timestamp: timestamp + offset * period * 1000 });
    if (safeEqual(candidate, normalized)) return true;
  }
  return false;
}

function hotp(secret: string, counter: number, digits: number): string {
  const key = base32Decode(secret);
  const counterBuffer = Buffer.alloc(8);
  counterBuffer.writeBigUInt64BE(BigInt(counter));
  const hmac = createHmac('sha1', key).update(counterBuffer).digest();
  const offset = hmac[hmac.length - 1]! & 0x0f;
  const binary = ((hmac[offset]! & 0x7f) << 24) | ((hmac[offset + 1]! & 0xff) << 16) | ((hmac[offset + 2]! & 0xff) << 8) | (hmac[offset + 3]! & 0xff);
  const otp = binary % 10 ** digits;
  return otp.toString().padStart(digits, '0');
}

function base32Encode(buffer: Buffer): string {
  let bits = 0;
  let value = 0;
  let output = '';
  for (const byte of buffer) {
    value = (value << 8) | byte;
    bits += 8;
    while (bits >= 5) {
      output += BASE32_ALPHABET[(value >>> (bits - 5)) & 31];
      bits -= 5;
    }
  }
  if (bits > 0) output += BASE32_ALPHABET[(value << (5 - bits)) & 31];
  return output;
}

function base32Decode(value: string): Buffer {
  const normalized = value.replace(/=+$/g, '').replace(/\s+/g, '').toUpperCase();
  let bits = 0;
  let current = 0;
  const bytes: number[] = [];
  for (const char of normalized) {
    const index = BASE32_ALPHABET.indexOf(char);
    if (index < 0) throw new Error('Некорректный TOTP secret');
    current = (current << 5) | index;
    bits += 5;
    if (bits >= 8) {
      bytes.push((current >>> (bits - 8)) & 0xff);
      bits -= 8;
    }
  }
  return Buffer.from(bytes);
}

function safeEqual(left: string, right: string): boolean {
  const leftBuffer = Buffer.from(left);
  const rightBuffer = Buffer.from(right);
  return leftBuffer.length === rightBuffer.length && timingSafeEqual(leftBuffer, rightBuffer);
}
