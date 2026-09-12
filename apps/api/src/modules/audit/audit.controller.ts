import { Controller, Get, Query, UseGuards } from '@nestjs/common';
import { PrismaService } from '@cajeerbots/db';
import { permissions } from '@cajeerbots/security';
import { RequireScopes } from '../shared/scopes.decorator.js';
import { ScopedAuthGuard } from '../shared/scoped-auth.guard.js';

@Controller('audit')
@UseGuards(ScopedAuthGuard)
@RequireScopes(permissions.auditRead)
export class AuditController {
  constructor(private readonly prisma: PrismaService) {}

  @Get()
  list(@Query('take') take?: string, @Query('action') action?: string) {
    const limit = Math.min(Number(take ?? 50), 200);
    return this.prisma.auditLog.findMany({
      where: action ? { action } : {},
      orderBy: { createdAt: 'desc' },
      take: Number.isFinite(limit) ? limit : 50,
    });
  }
}
