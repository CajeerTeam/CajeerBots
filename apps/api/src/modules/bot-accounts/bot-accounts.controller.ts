import { Body, Controller, Get, Param, Patch, Post, UseGuards } from '@nestjs/common';
import { permissions, type Principal } from '@cajeerbots/security';
import { CurrentPrincipal } from '../shared/auth-context.js';
import { RequireScopes } from '../shared/scopes.decorator.js';
import { ScopedAuthGuard } from '../shared/scoped-auth.guard.js';
import { parseBody } from '../shared/zod.js';
import { BotAccountsService } from './bot-accounts.service.js';
import { createBotAccountSchema, updateBotAccountSchema } from './bot-accounts.schemas.js';

@Controller('communities/:communityId/bot-accounts')
@UseGuards(ScopedAuthGuard)
export class BotAccountsController {
  constructor(private readonly accounts: BotAccountsService) {}

  @Get()
  @RequireScopes(permissions.botAccountsRead)
  list(@Param('communityId') communityId: string) {
    return this.accounts.list(communityId);
  }

  @Get(':id')
  @RequireScopes(permissions.botAccountsRead)
  get(@Param('communityId') communityId: string, @Param('id') id: string) {
    return this.accounts.get(communityId, id);
  }

  @Post()
  @RequireScopes(permissions.botAccountsWrite)
  create(@Param('communityId') communityId: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.accounts.create(communityId, parseBody(createBotAccountSchema, body), principal);
  }

  @Patch(':id')
  @RequireScopes(permissions.botAccountsWrite)
  update(@Param('communityId') communityId: string, @Param('id') id: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.accounts.update(communityId, id, parseBody(updateBotAccountSchema, body), principal);
  }

  @Post(':id/enable')
  @RequireScopes(permissions.botAccountsWrite)
  enable(@Param('communityId') communityId: string, @Param('id') id: string, @CurrentPrincipal() principal: Principal) {
    return this.accounts.setActive(communityId, id, true, principal);
  }

  @Post(':id/disable')
  @RequireScopes(permissions.botAccountsWrite)
  disable(@Param('communityId') communityId: string, @Param('id') id: string, @CurrentPrincipal() principal: Principal) {
    return this.accounts.setActive(communityId, id, false, principal);
  }
}
