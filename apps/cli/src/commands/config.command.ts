import { Command } from 'commander';
import { ConfigError, loadConfigOrThrow } from '@cajeerbots/config';

export function configCommand(): Command {
  const command = new Command('config').description('Проверка runtime-конфигурации CajeerBots');

  command.command('validate').description('Проверить .env/process env').action(() => {
    try {
      const config = loadConfigOrThrow(process.env);
      console.log(JSON.stringify({ ok: true, nodeEnv: config.NODE_ENV, appPublicUrl: config.APP_PUBLIC_URL, botMode: config.BOT_MODE, queueDriver: config.QUEUE_DRIVER, storageDriver: config.STORAGE_DRIVER }, null, 2));
    } catch (error) {
      if (error instanceof ConfigError) {
        console.error(JSON.stringify({ ok: false, issues: error.issues }, null, 2));
        process.exitCode = 1;
        return;
      }
      throw error;
    }
  });

  command.command('summary').description('Показать краткое состояние конфигурации без секретов').action(() => {
    const config = loadConfigOrThrow(process.env);
    console.log(JSON.stringify({
      app: { name: config.APP_NAME, language: config.APP_LANGUAGE, publicUrl: config.APP_PUBLIC_URL, nodeEnv: config.NODE_ENV },
      runtime: { botMode: config.BOT_MODE, queueDriver: config.QUEUE_DRIVER, storageDriver: config.STORAGE_DRIVER, prometheus: config.PROMETHEUS_ENABLED, otel: config.OTEL_ENABLED },
      configured: { telegram: Boolean(config.TELEGRAM_BOT_TOKEN), vk: Boolean(config.VK_BOT_TOKEN), discord: Boolean(config.DISCORD_BOT_TOKEN), encryptedSecrets: Boolean(config.SECRETS_ENCRYPTION_KEY) },
    }, null, 2));
  });

  return command;
}
