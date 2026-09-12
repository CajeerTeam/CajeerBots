import { createHmac, randomBytes, timingSafeEqual } from 'node:crypto';

export interface JwtPrincipalPayload {
  sub: string;
  typ: 'user' | 'api_key' | 'system' | 'extension';
  scopes: string[];
  iat: number;
  exp: number;
}

export interface IssuedJwt {
  token: string;
  expiresAt: string;
}

export interface ApiTokenMaterial {
  token: string;
  hash: string;
  prefix: string;
}

export function createApiToken(pepper: string, prefix = 'cjb'): ApiTokenMaterial {
  const token = `${prefix}_${randomBytes(32).toString('base64url')}`;
  return { token, hash: hashApiToken(token, pepper), prefix };
}

export function hashApiToken(token: string, pepper: string): string {
  return createHmac('sha256', pepper).update(token).digest('hex');
}

export function signJwt(
  input: Omit<JwtPrincipalPayload, 'iat' | 'exp'>,
  secret: string,
  ttlSeconds = 3600,
): IssuedJwt {
  const now = Math.floor(Date.now() / 1000);
  const payload: JwtPrincipalPayload = { ...input, iat: now, exp: now + ttlSeconds };
  const encodedHeader = base64urlJson({ alg: 'HS256', typ: 'JWT' });
  const encodedPayload = base64urlJson(payload);
  const signature = hmac(`${encodedHeader}.${encodedPayload}`, secret);
  return { token: `${encodedHeader}.${encodedPayload}.${signature}`, expiresAt: new Date(payload.exp * 1000).toISOString() };
}

export function verifyJwt(token: string, secret: string): JwtPrincipalPayload | null {
  const [header, payload, signature] = token.split('.');
  if (!header || !payload || !signature) return null;
  const expected = hmac(`${header}.${payload}`, secret);
  if (!safeEqual(signature, expected)) return null;
  const parsedHeader = parseJson<Record<string, unknown>>(header);
  if (parsedHeader?.['alg'] !== 'HS256') return null;
  const parsed = parseJson<JwtPrincipalPayload>(payload);
  if (!parsed || typeof parsed.sub !== 'string' || !Array.isArray(parsed.scopes)) return null;
  if (parsed.exp < Math.floor(Date.now() / 1000)) return null;
  return parsed;
}

function hmac(value: string, secret: string): string {
  return createHmac('sha256', secret).update(value).digest('base64url');
}

function base64urlJson(value: unknown): string {
  return Buffer.from(JSON.stringify(value)).toString('base64url');
}

function parseJson<T>(value: string): T | null {
  try {
    return JSON.parse(Buffer.from(value, 'base64url').toString('utf8')) as T;
  } catch {
    return null;
  }
}

function safeEqual(left: string, right: string): boolean {
  const leftBuffer = Buffer.from(left);
  const rightBuffer = Buffer.from(right);
  return leftBuffer.length === rightBuffer.length && timingSafeEqual(leftBuffer, rightBuffer);
}
