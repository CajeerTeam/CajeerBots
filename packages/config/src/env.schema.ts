import { z } from 'zod';

const secret = z.string().min(16);
const optionalUrl = z.string().url().optional();

export const envSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  APP_NAME: z.string().default('CajeerBots'),
  APP_LANGUAGE: z.string().default('ru'),
  APP_PORT: z.coerce.number().int().positive().default(3000),
  APP_HOST: z.string().default('0.0.0.0'),
  APP_PUBLIC_URL: z.string().url().default('http://localhost:3000'),

  DATABASE_URL: z.string().min(1),
  REDIS_URL: z.string().min(1),

  JWT_SECRET: secret,
  JWT_ACCESS_TTL_SECONDS: z.coerce.number().int().positive().default(3600),
  JWT_REFRESH_TTL_SECONDS: z.coerce.number().int().positive().default(60 * 60 * 24 * 30),
  API_TOKEN_PEPPER: secret,
  PASSWORD_PEPPER: secret.optional(),
  REFRESH_TOKEN_PEPPER: secret.optional(),
  RECOVERY_CODE_PEPPER: secret.optional(),
  TOTP_ISSUER: z.string().default('CajeerBots'),
  API_RATE_LIMIT_MAX: z.coerce.number().int().positive().default(300),
  API_RATE_LIMIT_WINDOW: z.string().default('1 minute'),
  SECRETS_ENCRYPTION_KEY: secret.optional(),
  CAJEER_ADMIN_TOKEN: secret.optional(),
  WEBHOOK_SIGNING_SECRET: secret.optional(),

  BOT_MODE: z.enum(['polling', 'webhook']).default('polling'),
  BOT_WEBHOOK_HOST: z.string().default('0.0.0.0'),
  BOT_WEBHOOK_PORT: z.coerce.number().int().positive().default(3101),
  BOT_WEBHOOK_PUBLIC_URL: z.string().url().optional(),
  BOT_RUNTIME_HEARTBEAT_MS: z.coerce.number().int().positive().default(15000),
  BOT_RUNTIME_STALE_MS: z.coerce.number().int().positive().default(60000),
  VK_CALLBACK_CONFIRMATION: z.string().optional(),
  CAJEER_DEFAULT_COMMUNITY_ID: z.string().optional(),
  TELEGRAM_BOT_TOKEN: z.string().optional(),
  VK_BOT_TOKEN: z.string().optional(),
  DISCORD_BOT_TOKEN: z.string().optional(),

  QUEUE_DRIVER: z.enum(['redis', 'postgres', 'rabbitmq']).default('redis'),
  STORAGE_DRIVER: z.enum(['local', 's3']).default('local'),
  STORAGE_LOCAL_ROOT: z.string().default('./storage'),

  S3_ENDPOINT: optionalUrl,
  S3_REGION: z.string().default('ru-1'),
  S3_BUCKET: z.string().optional(),
  S3_ACCESS_KEY_ID: z.string().optional(),
  S3_SECRET_ACCESS_KEY: z.string().optional(),

  PROMETHEUS_ENABLED: z.coerce.boolean().default(true),
  OTEL_ENABLED: z.coerce.boolean().default(false),
  OTEL_SERVICE_NAME: z.string().default('cajeerbots'),

  IMPORT_EXPORT_MAX_BYTES: z.coerce.number().int().positive().default(5_000_000),
  BACKUP_RETENTION_DAYS: z.coerce.number().int().positive().default(30),
  BACKUP_OBJECT_PREFIX: z.string().default('backups'),
  BACKUP_WORKER_INTERVAL_MS: z.coerce.number().int().positive().default(60_000),
  BACKUP_DELETED_OBJECT_RETENTION_DAYS: z.coerce.number().int().nonnegative().default(7),
  EXTENSIONS_ROOT: z.string().default('./extensions'),
  EXTENSION_RUNTIME_ENABLED: z.coerce.boolean().default(true),

  EVENT_API_WORKER_INTERVAL_MS: z.coerce.number().int().positive().default(5000),
  EVENT_API_WORKER_BATCH_SIZE: z.coerce.number().int().positive().default(25),
  EVENT_API_DEFAULT_RETENTION_DAYS: z.coerce.number().int().positive().default(30),
});

export type CajeerEnv = z.infer<typeof envSchema>;
