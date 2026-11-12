import { createHash, randomBytes } from 'node:crypto';
import type { ProductionEnvOptions } from './types.js';

const requiredProductionKeys = [
  'NODE_ENV',
  'DATABASE_URL',
  'REDIS_URL',
  'JWT_SECRET',
  'CAJEER_ADMIN_TOKEN',
  'API_TOKEN_PEPPER',
  'PASSWORD_PEPPER',
  'REFRESH_TOKEN_PEPPER',
  'RECOVERY_CODE_PEPPER',
  'WEBHOOK_SIGNING_SECRET',
  'SECRETS_ENCRYPTION_KEY',
  'APP_PUBLIC_URL',
] as const;

export function generateSecret(bytes = 32): string {
  return randomBytes(bytes).toString('base64url');
}

export function generateProductionEnv(options: Partial<ProductionEnvOptions> = {}): string {
  const appUrl = options.appUrl ?? 'https://example.cajeer.ru';
  const botWebhookPublicUrl = options.botWebhookPublicUrl ?? appUrl;
  const postgresPassword = generateSecret(32);
  const values: Record<string, string> = {
    POSTGRES_DB: 'cajeerbots',
    POSTGRES_USER: 'cajeerbots',
    POSTGRES_PASSWORD: postgresPassword,
    NODE_ENV: 'production',
    APP_NAME: 'CajeerBots',
    APP_HOST: '0.0.0.0',
    APP_PORT: '3000',
    APP_PUBLIC_URL: appUrl,
    DATABASE_URL: options.databaseUrl ?? `postgresql://cajeerbots:${postgresPassword}@postgres:5432/cajeerbots?schema=public`,
    REDIS_URL: options.redisUrl ?? 'redis://redis:6379/0',
    JWT_SECRET: options.jwtSecret ?? generateSecret(48),
    CAJEER_ADMIN_TOKEN: options.adminToken ?? generateSecret(32),
    SECRETS_ENCRYPTION_KEY: options.encryptionKey ?? createHash('sha256').update(generateSecret(64)).digest('base64'),
    API_TOKEN_PEPPER: generateSecret(32),
    WEBHOOK_SIGNING_SECRET: generateSecret(32),
    PASSWORD_PEPPER: generateSecret(32),
    REFRESH_TOKEN_PEPPER: generateSecret(32),
    RECOVERY_CODE_PEPPER: generateSecret(32),
    TOTP_ISSUER: 'CajeerBots',
    BOT_WEBHOOK_HOST: '0.0.0.0',
    BOT_WEBHOOK_PORT: '3101',
    BOT_MODE: 'polling',
    QUEUE_DRIVER: 'redis',
    WORKER_HEALTH_PORT: '3102',
    BOT_WEBHOOK_PUBLIC_URL: botWebhookPublicUrl,
    BOT_RUNTIME_HEARTBEAT_MS: '15000',
    BOT_RUNTIME_STALE_MS: '60000',
    EXTENSIONS_ROOT: '/var/lib/cajeerbots/extensions',
    STORAGE_DRIVER: options.storageDriver ?? 'local',
    STORAGE_LOCAL_ROOT: '/var/lib/cajeerbots/storage',
    S3_ENDPOINT: options.s3Endpoint ?? '',
    S3_BUCKET: options.s3Bucket ?? '',
    API_RATE_LIMIT_MAX: '300',
    API_RATE_LIMIT_WINDOW: '1 minute',
    OTEL_ENABLED: 'false',
    PROMETHEUS_ENABLED: 'true',
  };

  return Object.entries(values)
    .map(([key, value]) => `${key}=${quoteEnvValue(value)}`)
    .join('\n') + '\n';
}

export function parseEnvText(text: string): Record<string, string> {
  const env: Record<string, string> = {};
  for (const rawLine of text.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith('#')) continue;
    const index = line.indexOf('=');
    if (index <= 0) continue;
    const key = line.slice(0, index).trim();
    let value = line.slice(index + 1).trim();
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }
    env[key] = value;
  }
  return env;
}

export function validateProductionEnv(env: Record<string, string>): { valid: boolean; errors: string[]; warnings: string[] } {
  const errors: string[] = [];
  const warnings: string[] = [];
  for (const key of requiredProductionKeys) {
    if (!env[key]) errors.push(`Не задана переменная ${key}`);
  }
  for (const key of ['JWT_SECRET', 'CAJEER_ADMIN_TOKEN', 'SECRETS_ENCRYPTION_KEY', 'PASSWORD_PEPPER', 'REFRESH_TOKEN_PEPPER', 'RECOVERY_CODE_PEPPER', 'API_TOKEN_PEPPER', 'WEBHOOK_SIGNING_SECRET']) {
    const value = env[key] ?? '';
    if (value && value.length < 32) errors.push(`${key} слишком короткая для production`);
    if (/^(change-me|development|secret|example|replace)/i.test(value)) errors.push(`${key} содержит демонстрационное значение`);
  }
  if (env.NODE_ENV !== 'production') warnings.push('NODE_ENV не равен production');
  if (env.APP_PUBLIC_URL && !/^https:\/\//.test(env.APP_PUBLIC_URL)) warnings.push('APP_PUBLIC_URL для production должен использовать HTTPS');
  if (env.STORAGE_DRIVER === 's3' && (!env.S3_ENDPOINT || !env.S3_BUCKET)) errors.push('Для STORAGE_DRIVER=s3 нужны S3_ENDPOINT и S3_BUCKET');
  return { valid: errors.length === 0, errors, warnings };
}

function quoteEnvValue(value: string): string {
  if (!value) return '';
  if (/^[A-Za-z0-9_./:@?=&+-]+$/.test(value)) return value;
  return JSON.stringify(value);
}
