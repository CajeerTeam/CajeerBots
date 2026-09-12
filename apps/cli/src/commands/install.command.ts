import { existsSync } from 'node:fs';
import { mkdir, writeFile } from 'node:fs/promises';
import { Command } from 'commander';
import { randomSecret } from '@cajeerbots/security';

export function installCommand(): Command {
  return new Command('install')
    .description('Создать рабочий .env и локальные директории CajeerBots')
    .option('--force', 'перезаписать .env')
    .action(async (options: { force?: boolean }) => {
      if (existsSync('.env') && !options.force) {
        console.log('.env уже существует. Используйте --force для перезаписи.');
        return;
      }
      await mkdir('storage', { recursive: true });
      await writeFile('.env', envTemplate());
      console.log('Создан .env и директория storage/. Дальше: pnpm prisma:migrate && pnpm dev');
    });
}

function envTemplate(): string {
  return `NODE_ENV=development
APP_NAME=CajeerBots
APP_LANGUAGE=ru
APP_HOST=0.0.0.0
APP_PORT=3000
APP_PUBLIC_URL=http://localhost:3000
DATABASE_URL=postgresql://cajeerbots:cajeerbots@localhost:5432/cajeerbots?schema=public
REDIS_URL=redis://localhost:6379
JWT_SECRET=${randomSecret()}
JWT_ACCESS_TTL_SECONDS=3600
API_TOKEN_PEPPER=${randomSecret()}
CAJEER_ADMIN_TOKEN=${randomSecret()}
WEBHOOK_SIGNING_SECRET=${randomSecret()}
BOT_MODE=polling
QUEUE_DRIVER=redis
STORAGE_DRIVER=local
STORAGE_LOCAL_ROOT=./storage
PROMETHEUS_ENABLED=true
OTEL_ENABLED=false
`;
}
