import { pbkdf2Sync, randomBytes, timingSafeEqual } from 'node:crypto';

const CURRENT_ALGORITHM = 'pbkdf2-sha256';
const ITERATIONS = 210_000;
const KEY_LENGTH = 32;

export interface PasswordHashOptions {
  pepper?: string;
  iterations?: number;
}

export function hashPassword(password: string, options: PasswordHashOptions = {}): string {
  if (password.length < 10) throw new Error('Пароль должен быть не короче 10 символов');
  const salt = randomBytes(16).toString('base64url');
  const iterations = options.iterations ?? ITERATIONS;
  const digest = derive(password, salt, iterations, options.pepper);
  return `${CURRENT_ALGORITHM}$${iterations}$${salt}$${digest}`;
}

export function verifyPassword(password: string, encoded: string, options: PasswordHashOptions = {}): boolean {
  const [algorithm, rawIterations, salt, digest] = encoded.split('$');
  if (algorithm !== CURRENT_ALGORITHM || !rawIterations || !salt || !digest) return false;
  const iterations = Number(rawIterations);
  if (!Number.isInteger(iterations) || iterations < 100_000) return false;
  const expected = derive(password, salt, iterations, options.pepper);
  return safeEqual(expected, digest);
}

export function hashRecoveryCode(code: string, pepper = ''): string {
  return derive(code.replace(/\s+/g, '').toUpperCase(), 'cajeerbots-recovery-code', 160_000, pepper);
}

export function createRecoveryCodes(count = 10): string[] {
  return Array.from({ length: count }, () => {
    const left = randomBytes(5).toString('base64url').replace(/[^A-Z0-9]/gi, '').slice(0, 8).toUpperCase();
    const right = randomBytes(5).toString('base64url').replace(/[^A-Z0-9]/gi, '').slice(0, 8).toUpperCase();
    return `${left}-${right}`;
  });
}

function derive(password: string, salt: string, iterations: number, pepper = ''): string {
  return pbkdf2Sync(`${password}${pepper}`, salt, iterations, KEY_LENGTH, 'sha256').toString('base64url');
}

function safeEqual(left: string, right: string): boolean {
  const leftBuffer = Buffer.from(left);
  const rightBuffer = Buffer.from(right);
  return leftBuffer.length === rightBuffer.length && timingSafeEqual(leftBuffer, rightBuffer);
}
