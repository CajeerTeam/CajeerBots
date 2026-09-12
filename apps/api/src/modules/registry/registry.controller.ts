import { Body, Controller, Get, Param, Post, Put, Query, UseGuards } from '@nestjs/common';
import { permissions, type Principal } from '@cajeerbots/security';
import { CurrentPrincipal } from '../shared/auth-context.js';
import { RequireScopes } from '../shared/scopes.decorator.js';
import { ScopedAuthGuard } from '../shared/scoped-auth.guard.js';
import { parseBody } from '../shared/zod.js';
import { createRegistrySourceSchema, installRegistryPackageSchema, listRegistryPackagesQuerySchema, updateRegistrySourceSchema } from './registry.schemas.js';
import { RegistryService } from './registry.service.js';

@Controller('registry')
@UseGuards(ScopedAuthGuard)
export class RegistryController {
  constructor(private readonly registry: RegistryService) {}

  @Get('sources')
  @RequireScopes(permissions.registryRead)
  sources() {
    return this.registry.sources();
  }

  @Post('sources')
  @RequireScopes(permissions.registryWrite)
  createSource(@Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.registry.createSource(parseBody(createRegistrySourceSchema, body), principal);
  }

  @Put('sources/:id')
  @RequireScopes(permissions.registryWrite)
  updateSource(@Param('id') id: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.registry.updateSource(id, parseBody(updateRegistrySourceSchema, body), principal);
  }

  @Post('sources/:id/sync')
  @RequireScopes(permissions.registrySync)
  syncSource(@Param('id') id: string, @CurrentPrincipal() principal: Principal) {
    return this.registry.syncSource(id, principal);
  }

  @Get('packages')
  @RequireScopes(permissions.registryRead)
  packages(@Query() query: Record<string, unknown>) {
    return this.registry.packages(listRegistryPackagesQuerySchema.parse(query));
  }

  @Get('packages/:name')
  @RequireScopes(permissions.registryRead)
  packageByName(@Param('name') name: string, @Query('sourceId') sourceId?: string) {
    return this.registry.packageByName(name, sourceId);
  }

  @Post('packages/:name/install')
  @RequireScopes(permissions.extensionsInstall)
  installPackage(@Param('name') name: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.registry.installPackage(name, parseBody(installRegistryPackageSchema, body ?? {}), principal);
  }
}
