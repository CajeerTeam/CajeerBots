import { Body, Controller, Get, Param, Post, Query, UseGuards } from '@nestjs/common';
import { permissions, type Principal } from '@cajeerbots/security';
import { CurrentPrincipal } from '../shared/auth-context.js';
import { RequireScopes } from '../shared/scopes.decorator.js';
import { ScopedAuthGuard } from '../shared/scoped-auth.guard.js';
import { parseBody } from '../shared/zod.js';
import { BackupsService } from './backups.service.js';
import { createBackupSchema, restoreBackupSchema } from './backups.schemas.js';

@Controller('backups')
@UseGuards(ScopedAuthGuard)
export class BackupsController {
  constructor(private readonly service: BackupsService) {}

  @Get()
  @RequireScopes(permissions.backupsRead)
  backups(@Query('communityId') communityId?: string, @Query('status') status?: string, @Query('take') take?: string) {
    return this.service.backups({ communityId, status, take: take ? Number(take) : undefined });
  }

  @Get('restore-runs')
  @RequireScopes(permissions.backupsRead)
  restoreRuns(@Query('artifactId') artifactId?: string, @Query('communityId') communityId?: string, @Query('status') status?: string, @Query('take') take?: string) {
    return this.service.restoreRuns({ artifactId, communityId, status, take: take ? Number(take) : undefined });
  }

  @Get(':id')
  @RequireScopes(permissions.backupsRead)
  backup(@Param('id') id: string) {
    return this.service.backup(id);
  }

  @Post(':id/verify')
  @RequireScopes(permissions.backupsRead)
  verify(@Param('id') id: string) {
    return this.service.verifyBackup(id);
  }

  @Post(':id/restore')
  @RequireScopes(permissions.backupsRestore)
  restore(@Param('id') id: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.service.restoreBackup(id, parseBody(restoreBackupSchema, body), principal);
  }
}

@Controller('communities/:communityId/backups')
@UseGuards(ScopedAuthGuard)
export class CommunityBackupsController {
  constructor(private readonly service: BackupsService) {}

  @Post()
  @RequireScopes(permissions.backupsWrite)
  create(@Param('communityId') communityId: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.service.createCommunityBackup(communityId, parseBody(createBackupSchema, body ?? {}), principal);
  }
}
