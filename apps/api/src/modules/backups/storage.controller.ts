import { Body, Controller, Delete, Get, Param, Query, UseGuards } from '@nestjs/common';
import { permissions, type Principal } from '@cajeerbots/security';
import { CurrentPrincipal } from '../shared/auth-context.js';
import { RequireScopes } from '../shared/scopes.decorator.js';
import { ScopedAuthGuard } from '../shared/scoped-auth.guard.js';
import { parseBody } from '../shared/zod.js';
import { BackupsService } from './backups.service.js';
import { deleteStorageObjectSchema } from './backups.schemas.js';

@Controller('storage')
@UseGuards(ScopedAuthGuard)
export class StorageController {
  constructor(private readonly service: BackupsService) {}

  @Get('objects')
  @RequireScopes(permissions.storageRead)
  objects(@Query('status') status?: string, @Query('refType') refType?: string, @Query('refId') refId?: string, @Query('take') take?: string) {
    return this.service.storageObjects({ status, refType, refId, take: take ? Number(take) : undefined });
  }

  @Get('objects/:id')
  @RequireScopes(permissions.storageRead)
  object(@Param('id') id: string) {
    return this.service.storageObject(id);
  }

  @Delete('objects/:id')
  @RequireScopes(permissions.storageWrite)
  deleteObject(@Param('id') id: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    const input = parseBody(deleteStorageObjectSchema, body ?? {});
    return this.service.deleteStorageObject(id, input.hard, principal);
  }
}
