import { Body, Controller, Get, Param, Patch, Post, UseGuards } from '@nestjs/common';
import { permissions, type Principal } from '@cajeerbots/security';
import { CurrentPrincipal } from '../shared/auth-context.js';
import { RequireScopes } from '../shared/scopes.decorator.js';
import { ScopedAuthGuard } from '../shared/scoped-auth.guard.js';
import { parseBody } from '../shared/zod.js';
import { CommunitiesService } from './communities.service.js';
import { createCommunitySchema, updateCommunitySchema } from './communities.schemas.js';

@Controller('communities')
@UseGuards(ScopedAuthGuard)
export class CommunitiesController {
  constructor(private readonly communities: CommunitiesService) {}

  @Get()
  @RequireScopes(permissions.communitiesRead)
  list() {
    return this.communities.list();
  }

  @Get(':id')
  @RequireScopes(permissions.communitiesRead)
  get(@Param('id') id: string) {
    return this.communities.get(id);
  }

  @Post()
  @RequireScopes(permissions.communitiesWrite)
  create(@Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.communities.create(parseBody(createCommunitySchema, body), principal);
  }

  @Patch(':id')
  @RequireScopes(permissions.communitiesWrite)
  update(@Param('id') id: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.communities.update(id, parseBody(updateCommunitySchema, body), principal);
  }
}
