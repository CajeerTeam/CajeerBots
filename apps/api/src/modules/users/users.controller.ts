import { Body, Controller, Delete, Get, Param, Patch, Post, UseGuards } from '@nestjs/common';
import { permissions, type Principal } from '@cajeerbots/security';
import { parseBody } from '../shared/zod.js';
import { CurrentPrincipal } from '../shared/auth-context.js';
import { RequireScopes } from '../shared/scopes.decorator.js';
import { ScopedAuthGuard } from '../shared/scoped-auth.guard.js';
import { UsersService } from './users.service.js';
import { assignRoleSchema, createUserSchema, updateUserSchema } from './users.schemas.js';

@Controller('communities/:communityId/users')
@UseGuards(ScopedAuthGuard)
export class UsersController {
  constructor(private readonly users: UsersService) {}

  @Get()
  @RequireScopes(permissions.usersRead)
  list(@Param('communityId') communityId: string) {
    return this.users.list(communityId);
  }

  @Get(':id')
  @RequireScopes(permissions.usersRead)
  get(@Param('communityId') communityId: string, @Param('id') id: string) {
    return this.users.get(communityId, id);
  }

  @Post()
  @RequireScopes(permissions.usersWrite)
  create(@Param('communityId') communityId: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.users.create(communityId, parseBody(createUserSchema, body), principal);
  }

  @Patch(':id')
  @RequireScopes(permissions.usersWrite)
  update(@Param('communityId') communityId: string, @Param('id') id: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.users.update(communityId, id, parseBody(updateUserSchema, body), principal);
  }

  @Post(':id/roles')
  @RequireScopes(permissions.usersWrite)
  assignRole(@Param('communityId') communityId: string, @Param('id') id: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.users.assignRole(communityId, id, parseBody(assignRoleSchema, body), principal);
  }

  @Delete(':id/roles/:roleId')
  @RequireScopes(permissions.usersWrite)
  removeRole(@Param('communityId') communityId: string, @Param('id') id: string, @Param('roleId') roleId: string, @CurrentPrincipal() principal: Principal) {
    return this.users.removeRole(communityId, id, roleId, principal);
  }
}
