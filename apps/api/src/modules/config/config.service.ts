import { Injectable } from '@nestjs/common';
import { envSchema, loadConfig } from '@cajeerbots/config';

const secretKeys = ['JWT_SECRET', 'API_TOKEN_PEPPER', 'SECRETS_ENCRYPTION_KEY', 'CAJEER_ADMIN_TOKEN', 'WEBHOOK_SIGNING_SECRET', 'TELEGRAM_BOT_TOKEN', 'VK_BOT_TOKEN', 'DISCORD_BOT_TOKEN', 'S3_SECRET_ACCESS_KEY'];

@Injectable()
export class RuntimeConfigService {
  validate() {
    const result = envSchema.safeParse(process.env);
    if (!result.success) {
      return {
        ok: false,
        issues: result.error.issues.map((issue) => ({ path: issue.path.join('.'), message: issue.message })),
      };
    }
    const productionIssues: string[] = [];
    if (result.data.NODE_ENV === 'production') {
      if (!result.data.SECRETS_ENCRYPTION_KEY) productionIssues.push('SECRETS_ENCRYPTION_KEY обязателен в production');
      for (const key of ['JWT_SECRET', 'API_TOKEN_PEPPER', 'SECRETS_ENCRYPTION_KEY'] as const) {
        if (['change-me-development-only', 'development-secret', 'secret'].includes(String(result.data[key] ?? ''))) {
          productionIssues.push(`${key} не должен быть дефолтным в production`);
        }
      }
    }
    return { ok: productionIssues.length === 0, issues: productionIssues.map((message) => ({ path: 'production', message })) };
  }

  summary() {
    const config = loadConfig(process.env);
    return {
      app: {
        name: config.APP_NAME,
        language: config.APP_LANGUAGE,
        host: config.APP_HOST,
        port: config.APP_PORT,
        publicUrl: config.APP_PUBLIC_URL,
        nodeEnv: config.NODE_ENV,
      },
      runtime: {
        botMode: config.BOT_MODE,
        queueDriver: config.QUEUE_DRIVER,
        storageDriver: config.STORAGE_DRIVER,
        prometheusEnabled: config.PROMETHEUS_ENABLED,
        otelEnabled: config.OTEL_ENABLED,
        extensionsRoot: config.EXTENSIONS_ROOT,
        extensionRuntimeEnabled: config.EXTENSION_RUNTIME_ENABLED,
      },
      configured: {
        database: Boolean(config.DATABASE_URL),
        redis: Boolean(config.REDIS_URL),
        telegramEnvToken: Boolean(config.TELEGRAM_BOT_TOKEN),
        vkEnvToken: Boolean(config.VK_BOT_TOKEN),
        discordEnvToken: Boolean(config.DISCORD_BOT_TOKEN),
        secretsEncryptionKey: Boolean(config.SECRETS_ENCRYPTION_KEY),
        webhookSigningSecret: Boolean(config.WEBHOOK_SIGNING_SECRET),
        s3: config.STORAGE_DRIVER === 's3' ? Boolean(config.S3_ENDPOINT && config.S3_BUCKET && config.S3_ACCESS_KEY_ID && config.S3_SECRET_ACCESS_KEY) : false,
        extensionsRoot: Boolean(config.EXTENSIONS_ROOT),
      },
    };
  }

  redactedEnv() {
    return Object.fromEntries(
      Object.keys(envSchema.shape).map((key) => {
        const value = process.env[key];
        return [key, secretKeys.includes(key) && value ? redact(value) : value ?? null];
      }),
    );
  }
}

function redact(value: string): string {
  if (value.length <= 8) return '***';
  return `${value.slice(0, 4)}***${value.slice(-4)}`;
}
