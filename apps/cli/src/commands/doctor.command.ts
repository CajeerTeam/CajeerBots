import { access, mkdir, rm, writeFile } from 'node:fs/promises';
import { createConnection } from 'node:net';
import { dirname, join } from 'node:path';
import { Command } from 'commander';
import { PrismaService } from '@cajeerbots/db';

export function doctorCommand(): Command {
  return new Command('doctor')
    .description('Проверить систему: БД, Redis, очереди, storage, настройки')
    .option('--with-db-query', 'проверить SELECT 1 через Prisma')
    .action(async (options: { withDbQuery?: boolean }) => {
      console.log('CajeerBots Doctor 0.6.0');
      await print('node', async () => process.version);
      await print('database tcp', async () => checkTcp(process.env.DATABASE_URL, 5432));
      if (options.withDbQuery) await print('database query', checkPrisma);
      await print('redis', async () => checkTcp(process.env.REDIS_URL, 6379));
      await print('queue', async () => (process.env.QUEUE_DRIVER ?? 'redis'));
      await print('storage', async () => checkStorage(process.env.STORAGE_LOCAL_ROOT ?? './storage'));
      await print('jwt secret', async () => process.env.JWT_SECRET ? 'configured' : 'not configured');
      await print('api token pepper', async () => process.env.API_TOKEN_PEPPER ? 'configured' : 'not configured');
      await print('password pepper', async () => (process.env.PASSWORD_PEPPER ?? process.env.API_TOKEN_PEPPER) ? 'configured' : 'not configured');
      await print('refresh token pepper', async () => (process.env.REFRESH_TOKEN_PEPPER ?? process.env.API_TOKEN_PEPPER) ? 'configured' : 'not configured');
      await print('recovery code pepper', async () => (process.env.RECOVERY_CODE_PEPPER ?? process.env.API_TOKEN_PEPPER) ? 'configured' : 'not configured');
      await print('totp issuer', async () => process.env.TOTP_ISSUER ?? 'CajeerBots');
      await print('admin token', async () => process.env.CAJEER_ADMIN_TOKEN ? 'configured' : 'not configured');
    });
}

async function print(name: string, fn: () => Promise<string> | string): Promise<void> {
  try {
    console.log(`- ${name}: ok (${await fn()})`);
  } catch (error) {
    console.log(`- ${name}: down (${(error as Error).message})`);
  }
}

async function checkPrisma(): Promise<string> {
  const prisma = new PrismaService();
  await prisma.connect();
  try {
    await prisma.health();
    return 'SELECT 1';
  } finally {
    await prisma.close();
  }
}

async function checkStorage(root: string): Promise<string> {
  const probe = join(root, '.cajeer-doctor');
  await mkdir(dirname(probe), { recursive: true });
  await writeFile(probe, 'ok');
  await access(probe);
  await rm(probe, { force: true });
  return root;
}

async function checkTcp(rawUrl: string | undefined, defaultPort: number): Promise<string> {
  if (!rawUrl) throw new Error('URL не задан');
  const url = new URL(rawUrl);
  const port = Number(url.port || defaultPort);
  await new Promise<void>((resolve, reject) => {
    const socket = createConnection({ host: url.hostname, port, timeout: 1500 });
    socket.once('connect', () => { socket.destroy(); resolve(); });
    socket.once('timeout', () => { socket.destroy(); reject(new Error('timeout')); });
    socket.once('error', reject);
  });
  return `${url.hostname}:${port}`;
}
