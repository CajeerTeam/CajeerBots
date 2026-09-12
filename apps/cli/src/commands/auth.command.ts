import { Command } from 'commander';
import { PrismaService } from '@cajeerbots/db';
import { createApiToken, hashPassword, signJwt } from '@cajeerbots/security';

export function authCommand(): Command {
  const command = new Command('auth').description('JWT/API key/admin security управление для CajeerBots');

  command
    .command('token')
    .description('Выпустить локальный bootstrap JWT')
    .option('--scope <scope...>', 'scopes токена', ['*'])
    .option('--ttl <seconds>', 'TTL в секундах', '3600')
    .action((options: { scope: string[]; ttl: string }) => {
      const secret = process.env.JWT_SECRET;
      if (!secret) throw new Error('JWT_SECRET не задан');
      const issued = signJwt({ sub: 'cli-bootstrap', typ: 'system', scopes: options.scope }, secret, Number(options.ttl));
      console.log(JSON.stringify(issued, null, 2));
    });

  command
    .command('admin:create')
    .description('Создать локальный admin account с password hash')
    .requiredOption('--email <email>', 'email администратора')
    .requiredOption('--password <password>', 'пароль администратора, минимум 10 символов')
    .option('--display-name <name>', 'отображаемое имя')
    .option('--scope <scope...>', 'scopes аккаунта', ['*'])
    .option('--admin-token <token>', 'bootstrap token, если CAJEER_ADMIN_TOKEN задан')
    .action(async (options: { email: string; password: string; displayName?: string; scope: string[]; adminToken?: string }) => {
      const expected = process.env.CAJEER_ADMIN_TOKEN;
      if (expected && options.adminToken !== expected) throw new Error('Некорректный CAJEER_ADMIN_TOKEN');
      const prisma = new PrismaService();
      await prisma.connect();
      try {
        const created = await prisma.adminAccount.create({
          data: {
            email: options.email.toLowerCase(),
            displayName: options.displayName,
            passwordHash: hashPassword(options.password, { pepper: process.env.PASSWORD_PEPPER ?? process.env.API_TOKEN_PEPPER }),
            scopes: Array.from(new Set(options.scope)),
          },
          select: { id: true, email: true, displayName: true, scopes: true, isActive: true, twoFactorEnabled: true, createdAt: true },
        });
        await prisma.auditLog.create({ data: { actorType: 'system', actorId: 'cli', action: 'auth.admin.create', resource: 'admin_account', resourceId: created.id, metadata: { email: created.email, scopes: created.scopes } as never } });
        console.log(JSON.stringify(created, null, 2));
      } finally {
        await prisma.close();
      }
    });

  command
    .command('admin:list')
    .description('Показать admin accounts')
    .action(async () => {
      const prisma = new PrismaService();
      await prisma.connect();
      try {
        const rows = await prisma.adminAccount.findMany({ orderBy: { createdAt: 'desc' }, select: { id: true, email: true, displayName: true, scopes: true, isActive: true, twoFactorEnabled: true, lastLoginAt: true, createdAt: true } });
        console.log(JSON.stringify(rows, null, 2));
      } finally {
        await prisma.close();
      }
    });

  command
    .command('sessions:list')
    .description('Показать admin sessions')
    .option('--account-id <id>', 'фильтр по admin account')
    .action(async (options: { accountId?: string }) => {
      const prisma = new PrismaService();
      await prisma.connect();
      try {
        const rows = await prisma.adminSession.findMany({
          where: options.accountId ? { accountId: options.accountId } : {},
          orderBy: { createdAt: 'desc' },
          select: { id: true, accountId: true, ip: true, userAgent: true, expiresAt: true, lastUsedAt: true, revokedAt: true, createdAt: true },
        });
        console.log(JSON.stringify(rows, null, 2));
      } finally {
        await prisma.close();
      }
    });

  command
    .command('sessions:revoke')
    .description('Отозвать admin session')
    .requiredOption('--id <id>', 'ID session')
    .action(async (options: { id: string }) => {
      const prisma = new PrismaService();
      await prisma.connect();
      try {
        const updated = await prisma.adminSession.update({ where: { id: options.id }, data: { revokedAt: new Date() } });
        await prisma.auditLog.create({ data: { actorType: 'system', actorId: 'cli', action: 'auth.session.revoke', resource: 'admin_session', resourceId: updated.id } });
        console.log(JSON.stringify({ revoked: true, id: updated.id }, null, 2));
      } finally {
        await prisma.close();
      }
    });

  command
    .command('api-key:create')
    .description('Создать API key в базе данных')
    .requiredOption('--title <title>', 'название ключа')
    .option('--scope <scope...>', 'scopes ключа', ['*'])
    .option('--expires-at <iso>', 'дата истечения ISO 8601')
    .action(async (options: { title: string; scope: string[]; expiresAt?: string }) => {
      const pepper = process.env.API_TOKEN_PEPPER;
      if (!pepper) throw new Error('API_TOKEN_PEPPER не задан');
      const prisma = new PrismaService();
      await prisma.connect();
      try {
        const material = createApiToken(pepper);
        const created = await prisma.apiKey.create({
          data: { title: options.title, keyHash: material.hash, scopes: Array.from(new Set(options.scope)), ...(options.expiresAt ? { expiresAt: new Date(options.expiresAt) } : {}) },
        });
        console.log(JSON.stringify({ id: created.id, title: created.title, token: material.token, scopes: created.scopes, expiresAt: created.expiresAt }, null, 2));
      } finally {
        await prisma.close();
      }
    });

  return command;
}
