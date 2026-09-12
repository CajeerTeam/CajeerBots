import { envSchema, type CajeerEnv } from './env.schema.js';

export class ConfigError extends Error {
  constructor(public readonly issues: string[]) {
    super(`Некорректная конфигурация CajeerBots: ${issues.join('; ')}`);
    this.name = 'ConfigError';
  }
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): CajeerEnv {
  const result = envSchema.safeParse(env);
  if (!result.success) {
    throw new ConfigError(result.error.issues.map((issue) => `${issue.path.join('.')}: ${issue.message}`));
  }
  return result.data;
}

export function loadConfigOrThrow(env: NodeJS.ProcessEnv = process.env): CajeerEnv {
  const config = loadConfig(env);
  if (config.NODE_ENV === 'production') {
    const weak = ['change-me-development-only', 'development-secret', 'secret'];
    if (weak.includes(config.JWT_SECRET) || weak.includes(config.API_TOKEN_PEPPER) || weak.includes(config.SECRETS_ENCRYPTION_KEY ?? '') || weak.includes(config.PASSWORD_PEPPER ?? '') || weak.includes(config.REFRESH_TOKEN_PEPPER ?? '')) {
      throw new ConfigError(['production нельзя запускать с дефолтными секретами']);
    }
    if (!config.SECRETS_ENCRYPTION_KEY) {
      throw new ConfigError(['SECRETS_ENCRYPTION_KEY обязателен в production']);
    }
    if (!config.PASSWORD_PEPPER || !config.REFRESH_TOKEN_PEPPER || !config.RECOVERY_CODE_PEPPER) {
      throw new ConfigError(['PASSWORD_PEPPER, REFRESH_TOKEN_PEPPER и RECOVERY_CODE_PEPPER обязательны в production']);
    }
  }
  return config;
}
