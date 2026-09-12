import { Body, Controller, Get, Param, Put, Post, UseGuards } from '@nestjs/common';
import { permissions, type Principal } from '@cajeerbots/security';
import { CurrentPrincipal } from '../shared/auth-context.js';
import { RequireScopes } from '../shared/scopes.decorator.js';
import { ScopedAuthGuard } from '../shared/scoped-auth.guard.js';
import { parseBody } from '../shared/zod.js';
import { RolesService } from './roles.service.js';
import { createRoleSchema, setRolePermissionsSchema } from './roles.schemas.js';

@Controller('communities/:communityId/roles')
@UseGuards(ScopedAuthGuard)
export class RolesController {
  constructor(private readonly roles: RolesService) {}

  @Get('permissions')
  @RequireScopes(permissions.rolesRead)
  listPermissions() {
    return this.roles.listPermissions();
  }

  @Get()
  @RequireScopes(permissions.rolesRead)
  list(@Param('communityId') communityId: string) {
    return this.roles.list(communityId);
  }

  @Get(':id')
  @RequireScopes(permissions.rolesRead)
  get(@Param('communityId') communityId: string, @Param('id') id: string) {
    return this.roles.get(communityId, id);
  }

  @Post()
  @RequireScopes(permissions.rolesWrite)
  create(@Param('communityId') communityId: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.roles.create(communityId, parseBody(createRoleSchema, body), principal);
  }

  @Put(':id/permissions')
  @RequireScopes(permissions.rolesWrite)
  setPermissions(@Param('communityId') communityId: string, @Param('id') id: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    const parsed = parseBody(setRolePermissionsSchema, body);
    return this.roles.setPermissions(communityId, id, parsed.scopes, principal);
  }
}
