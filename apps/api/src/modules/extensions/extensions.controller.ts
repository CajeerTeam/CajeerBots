import { Body, Controller, Delete, Get, Param, Post, Put, Query, UseGuards } from '@nestjs/common';
import { permissions, type Principal } from '@cajeerbots/security';
import { CurrentPrincipal } from '../shared/auth-context.js';
import { RequireScopes } from '../shared/scopes.decorator.js';
import { ScopedAuthGuard } from '../shared/scoped-auth.guard.js';
import { parseBody } from '../shared/zod.js';
import { ExtensionsService } from './extensions.service.js';
import {
  enqueueExtensionEventSchema,
  grantExtensionPermissionsSchema,
  installExtensionSchema,
  revokeExtensionPermissionsSchema,
  setExtensionStorageSchema,
  updateExtensionConfigSchema,
} from './extensions.schemas.js';

@Controller('extensions')
@UseGuards(ScopedAuthGuard)
export class ExtensionsController {
  constructor(private readonly extensions: ExtensionsService) {}

  @Get()
  @RequireScopes(permissions.extensionsRead)
  listExtensions(@Query('status') status?: 'installed' | 'enabled' | 'disabled' | 'failed') {
    return this.extensions.list(status);
  }

  @Get(':id')
  @RequireScopes(permissions.extensionsRead)
  get(@Param('id') id: string) {
    return this.extensions.get(id);
  }

  @Post('install')
  @RequireScopes(permissions.extensionsInstall)
  install(@Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    const parsed = parseBody(installExtensionSchema, body);
    return this.extensions.install(parsed.manifest, principal, parsed.signature, parsed.autoGrant);
  }

  @Post(':id/enable')
  @RequireScopes(permissions.extensionsWrite)
  enable(@Param('id') id: string, @CurrentPrincipal() principal: Principal) {
    return this.extensions.enable(id, principal);
  }

  @Post(':id/disable')
  @RequireScopes(permissions.extensionsWrite)
  disable(@Param('id') id: string, @CurrentPrincipal() principal: Principal) {
    return this.extensions.setStatus(id, 'disabled', principal);
  }

  @Get(':id/permissions')
  @RequireScopes(permissions.extensionsRead)
  permissions(@Param('id') id: string) {
    return this.extensions.permissions(id);
  }

  @Post(':id/permissions/grant')
  @RequireScopes(permissions.extensionsPermissions)
  grantPermissions(@Param('id') id: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.extensions.grantPermissions(id, parseBody(grantExtensionPermissionsSchema, body), principal);
  }

  @Post(':id/permissions/revoke')
  @RequireScopes(permissions.extensionsPermissions)
  revokePermissions(@Param('id') id: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.extensions.revokePermissions(id, parseBody(revokeExtensionPermissionsSchema, body), principal);
  }

  @Get(':id/config/history')
  @RequireScopes(permissions.extensionsRead)
  configHistory(@Param('id') id: string) {
    return this.extensions.configHistory(id);
  }

  @Put(':id/config')
  @RequireScopes(permissions.extensionsConfig)
  setConfig(@Param('id') id: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.extensions.setConfig(id, parseBody(updateExtensionConfigSchema, body), principal);
  }

  @Get(':id/events')
  @RequireScopes(permissions.extensionsRuntime)
  events(@Param('id') id: string, @Query('take') take?: string, @Query('status') status?: string) {
    return this.extensions.events(id, Number(take ?? 50), status);
  }

  @Post(':id/events')
  @RequireScopes(permissions.extensionsRuntime)
  enqueueEvent(@Param('id') id: string, @Body() body: unknown) {
    return this.extensions.enqueueEvent(id, parseBody(enqueueExtensionEventSchema, body));
  }

  @Post('events/:eventId/retry')
  @RequireScopes(permissions.extensionsRuntime)
  retryEvent(@Param('eventId') eventId: string) {
    return this.extensions.retryEvent(eventId);
  }

  @Get(':id/storage')
  @RequireScopes(permissions.extensionsStorage)
  storage(@Param('id') id: string, @Query('namespace') namespace?: string) {
    return this.extensions.storage(id, namespace ?? 'default');
  }

  @Put(':id/storage/:namespace/:key')
  @RequireScopes(permissions.extensionsStorage)
  setStorage(@Param('id') id: string, @Param('namespace') namespace: string, @Param('key') key: string, @Body() body: unknown) {
    return this.extensions.setStorage(id, namespace, key, parseBody(setExtensionStorageSchema, body));
  }

  @Delete(':id/storage/:namespace/:key')
  @RequireScopes(permissions.extensionsStorage)
  deleteStorage(@Param('id') id: string, @Param('namespace') namespace: string, @Param('key') key: string) {
    return this.extensions.deleteStorage(id, namespace, key);
  }

  @Get(':id/logs')
  @RequireScopes(permissions.extensionsRuntime)
  logs(@Param('id') id: string, @Query('take') take?: string) {
    return this.extensions.logs(id, Number(take ?? 100));
  }
}
