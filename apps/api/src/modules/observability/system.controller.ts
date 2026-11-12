import { Controller, Get, UseGuards } from '@nestjs/common';
import { createMetricsRegistry, createDiagnosticReport } from '@cajeerbots/observability';
import { permissions } from '@cajeerbots/security';
import { RequireScopes } from '../shared/scopes.decorator.js';
import { ScopedAuthGuard } from '../shared/scoped-auth.guard.js';

@Controller('system')
export class SystemController {
  @Get('report')
  @UseGuards(ScopedAuthGuard)
  @RequireScopes(permissions.systemRead)
  getSystemReport() {
    return {
      app: process.env.APP_NAME ?? 'CajeerBots',
      version: '1.0.0-rc.6',
      runtime: process.version,
      language: process.env.APP_LANGUAGE ?? 'ru',
      database: 'PostgreSQL 17+ primary, PostgreSQL 18+ recommended',
      cache: 'Redis',
      queue: process.env.QUEUE_DRIVER ?? 'redis',
      storage: process.env.STORAGE_DRIVER ?? 'local',
      botMode: process.env.BOT_MODE ?? 'polling',
      productCapabilities: ['auth', 'rbac', 'users', 'roles', 'bot_accounts', 'message_templates', 'webhook_deliveries'],
    };
  }

  @Get('diagnostics')
  @UseGuards(ScopedAuthGuard)
  @RequireScopes(permissions.systemRead)
  getDiagnostics() {
    return createDiagnosticReport({
      databaseUrl: process.env.DATABASE_URL,
      redisUrl: process.env.REDIS_URL,
      queueDriver: process.env.QUEUE_DRIVER,
      storageRoot: process.env.STORAGE_LOCAL_ROOT ?? './storage',
    });
  }

  @Get('metrics')
  async getMetrics(): Promise<string> {
    const registry = createMetricsRegistry();
    return registry.metrics();
  }
}
