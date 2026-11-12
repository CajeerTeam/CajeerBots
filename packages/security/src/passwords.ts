import { pbkdf2, pbkdf2Sync, randomBytes, timingSafeEqual } from 'node:crypto';

const CURRENT_ALGORITHM = 'pbkdf2-sha256';
const ITERATIONS = 210_000;
const KEY_LENGTH = 32;
const MAX_ITERATIONS = 1_000_000;
function validIterations(n: number): boolean { return Number.isSafeInteger(n) && n >= 100_000 && n <= MAX_ITERATIONS; }
// Bound CPU work and keep request handlers off the event loop.
function deriveAsync(password: string, salt: string, iterations: number, pepper = ''): Promise<string> {
  return new Promise((resolve, reject) => pbkdf2(`${password}${pepper}`, salt, iterations, KEY_LENGTH, 'sha256', (error, key) => error ? reject(error) : resolve(key.toString('base64url'))));
}
export async function hashPasswordAsync(password: string, options: PasswordHashOptions = {}): Promise<string> {
  if (password.length < 10 || password.length > 512) throw new Error('Invalid password length');
  const iterations = options.iterations ?? ITERATIONS;
  if (!validIterations(iterations)) throw new Error('Invalid PBKDF2 iterations');
  const salt = randomBytes(16).toString('base64url');
  return `${CURRENT_ALGORITHM}$${iterations}$${salt}$${await deriveAsync(password, salt, iterations, options.pepper)}`;
}
export async function verifyPasswordAsync(password: string, encoded: string, options: PasswordHashOptions = {}): Promise<boolean> {
  const parts = encoded.split('$');
  const [algorithm, raw, salt, digest] = parts;
  const iterations = Number(raw);
  if (password.length > 512 || parts.length !== 4 || algorithm !== CURRENT_ALGORITHM || !validIterations(iterations) || !salt || !digest || !/^[A-Za-z0-9_-]{22}$/.test(salt) || !/^[A-Za-z0-9_-]{43}$/.test(digest)) return false;
  return safeEqual(await deriveAsync(password, salt, iterations, options.pepper), digest);
}

export interface PasswordHashOptions {
  pepper?: string;
  iterations?: number;
}

export function hashPassword(password: string, options: PasswordHashOptions = {}): string {
  if (password.length < 10 || password.length > 512) throw new Error('Пароль должен быть не короче 10 символов');
  const salt = randomBytes(16).toString('base64url');
  const iterations = options.iterations ?? ITERATIONS;
  if (!validIterations(iterations)) throw new Error('Invalid PBKDF2 iterations');
  const digest = derive(password, salt, iterations, options.pepper);
  return `${CURRENT_ALGORITHM}$${iterations}$${salt}$${digest}`;
}

export function verifyPassword(password: string, encoded: string, options: PasswordHashOptions = {}): boolean {
  const [algorithm, rawIterations, salt, digest] = encoded.split('$');
  if (algorithm !== CURRENT_ALGORITHM || !rawIterations || !salt || !digest) return false;
  const iterations = Number(rawIterations);
  if (!validIterations(iterations) || password.length > 512 || encoded.split('$').length !== 4 || !/^[A-Za-z0-9_-]{22}$/.test(salt) || !/^[A-Za-z0-9_-]{43}$/.test(digest)) return false;
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
