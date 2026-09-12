import { Controller, Get } from '@nestjs/common';
import { createDiagnosticReport } from '@cajeerbots/observability';
import { PrismaService } from '@cajeerbots/db';

@Controller('health')
export class HealthController {
  constructor(private readonly prisma: PrismaService) {}

  @Get()
  async getHealth() {
    return createDiagnosticReport({
      databaseUrl: process.env.DATABASE_URL,
      redisUrl: process.env.REDIS_URL,
      queueDriver: process.env.QUEUE_DRIVER,
      storageRoot: process.env.STORAGE_LOCAL_ROOT ?? './storage',
      databaseProbe: async () => { await this.prisma.health(); },
    });
  }
}
