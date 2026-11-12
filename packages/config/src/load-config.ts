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
    const required = ['JWT_SECRET', 'API_TOKEN_PEPPER', 'SECRETS_ENCRYPTION_KEY', 'PASSWORD_PEPPER', 'REFRESH_TOKEN_PEPPER', 'RECOVERY_CODE_PEPPER', 'CAJEER_ADMIN_TOKEN', 'WEBHOOK_SIGNING_SECRET'] as const;
    const issues = required.filter((key) => {
      const value = config[key];
      return !value || value.length < 32 || /^(change-me|development|secret|example|replace)/i.test(value);
    }).map((key) => `${key}: требуется отдельный сильный секрет (не менее 32 символов)`);
    if (issues.length) throw new ConfigError(issues);
  }
  return config;
}
