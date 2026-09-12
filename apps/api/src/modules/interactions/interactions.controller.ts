import { Body, Controller, Get, Param, Patch, Post, Query, UseGuards } from '@nestjs/common';
import { permissions, type Principal } from '@cajeerbots/security';
import { CurrentPrincipal } from '../shared/auth-context.js';
import { RequireScopes } from '../shared/scopes.decorator.js';
import { ScopedAuthGuard } from '../shared/scoped-auth.guard.js';
import { parseBody } from '../shared/zod.js';
import { InteractionsService } from './interactions.service.js';
import {
  createCallbackActionSchema,
  createConversationFlowSchema,
  createInteractionCommandSchema,
  createInteractionMenuSchema,
  recordInteractionEventSchema,
  updateCallbackActionSchema,
  updateConversationFlowSchema,
  updateInteractionCommandSchema,
  updateInteractionMenuSchema,
} from './interactions.schemas.js';

@Controller('communities/:communityId/interactions')
@UseGuards(ScopedAuthGuard)
export class InteractionsController {
  constructor(private readonly interactions: InteractionsService) {}

  @Get('commands')
  @RequireScopes(permissions.interactionsRead)
  commands(@Param('communityId') communityId: string, @Query('provider') provider?: 'telegram' | 'vk' | 'discord') {
    return this.interactions.commands(communityId, provider);
  }

  @Get('commands/:id')
  @RequireScopes(permissions.interactionsRead)
  getCommand(@Param('communityId') communityId: string, @Param('id') id: string) {
    return this.interactions.getCommand(communityId, id);
  }

  @Post('commands')
  @RequireScopes(permissions.interactionsWrite)
  createCommand(@Param('communityId') communityId: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.interactions.createCommand(communityId, parseBody(createInteractionCommandSchema, body), principal);
  }

  @Patch('commands/:id')
  @RequireScopes(permissions.interactionsWrite)
  updateCommand(@Param('communityId') communityId: string, @Param('id') id: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.interactions.updateCommand(communityId, id, parseBody(updateInteractionCommandSchema, body), principal);
  }

  @Get('menus')
  @RequireScopes(permissions.interactionsRead)
  menus(@Param('communityId') communityId: string, @Query('locale') locale?: string) {
    return this.interactions.menus(communityId, locale);
  }

  @Get('menus/:id')
  @RequireScopes(permissions.interactionsRead)
  getMenu(@Param('communityId') communityId: string, @Param('id') id: string) {
    return this.interactions.getMenu(communityId, id);
  }

  @Post('menus')
  @RequireScopes(permissions.interactionsWrite)
  createMenu(@Param('communityId') communityId: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.interactions.createMenu(communityId, parseBody(createInteractionMenuSchema, body), principal);
  }

  @Patch('menus/:id')
  @RequireScopes(permissions.interactionsWrite)
  updateMenu(@Param('communityId') communityId: string, @Param('id') id: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.interactions.updateMenu(communityId, id, parseBody(updateInteractionMenuSchema, body), principal);
  }

  @Get('callbacks')
  @RequireScopes(permissions.interactionsRead)
  callbacks(@Param('communityId') communityId: string) {
    return this.interactions.callbacks(communityId);
  }

  @Get('callbacks/:id')
  @RequireScopes(permissions.interactionsRead)
  getCallback(@Param('communityId') communityId: string, @Param('id') id: string) {
    return this.interactions.getCallback(communityId, id);
  }

  @Post('callbacks')
  @RequireScopes(permissions.interactionsWrite)
  createCallback(@Param('communityId') communityId: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.interactions.createCallback(communityId, parseBody(createCallbackActionSchema, body), principal);
  }

  @Patch('callbacks/:id')
  @RequireScopes(permissions.interactionsWrite)
  updateCallback(@Param('communityId') communityId: string, @Param('id') id: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.interactions.updateCallback(communityId, id, parseBody(updateCallbackActionSchema, body), principal);
  }

  @Get('conversations')
  @RequireScopes(permissions.interactionsRead)
  conversations(@Param('communityId') communityId: string) {
    return this.interactions.conversations(communityId);
  }

  @Get('conversations/:id')
  @RequireScopes(permissions.interactionsRead)
  getConversation(@Param('communityId') communityId: string, @Param('id') id: string) {
    return this.interactions.getConversation(communityId, id);
  }

  @Post('conversations')
  @RequireScopes(permissions.interactionsWrite)
  createConversation(@Param('communityId') communityId: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.interactions.createConversation(communityId, parseBody(createConversationFlowSchema, body), principal);
  }

  @Patch('conversations/:id')
  @RequireScopes(permissions.interactionsWrite)
  updateConversation(@Param('communityId') communityId: string, @Param('id') id: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.interactions.updateConversation(communityId, id, parseBody(updateConversationFlowSchema, body), principal);
  }

  @Get('events')
  @RequireScopes(permissions.interactionsRead)
  events(@Param('communityId') communityId: string, @Query('take') take?: string) {
    return this.interactions.events(communityId, Number(take ?? 50));
  }

  @Post('events')
  @RequireScopes(permissions.interactionsWrite)
  recordEvent(@Param('communityId') communityId: string, @Body() body: unknown) {
    return this.interactions.recordEvent(communityId, parseBody(recordInteractionEventSchema, body));
  }
}
