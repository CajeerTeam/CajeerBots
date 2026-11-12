import { Controller, Get, ServiceUnavailableException } from '@nestjs/common';
import { createDiagnosticReport } from '@cajeerbots/observability';
import { PrismaService } from '@cajeerbots/db';

@Controller('health')
export class HealthController {
  constructor(private readonly prisma: PrismaService) {}

  @Get()
  async getHealth() {
    const report = await createDiagnosticReport({
      databaseUrl: process.env.DATABASE_URL,
      redisUrl: process.env.REDIS_URL,
      queueDriver: process.env.QUEUE_DRIVER,
      storageRoot: process.env.STORAGE_LOCAL_ROOT ?? './storage',
      databaseProbe: async () => { await this.prisma.health(); },
    });
    const publicReport = { status: report.status, checks: Object.fromEntries(Object.entries(report.checks).map(([name, check]) => [name, { status: check.status, latencyMs: check.latencyMs }])) };
    if (report.status !== 'ok') throw new ServiceUnavailableException(publicReport);
    return publicReport;
  }
}
