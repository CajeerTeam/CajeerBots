import { Body, Controller, Get, Param, Post, Query, UseGuards } from '@nestjs/common';
import { permissions, type Principal } from '@cajeerbots/security';
import { CurrentPrincipal } from '../shared/auth-context.js';
import { RequireScopes } from '../shared/scopes.decorator.js';
import { ScopedAuthGuard } from '../shared/scoped-auth.guard.js';
import { parseBody } from '../shared/zod.js';
import { BotRuntimeService } from './bot-runtime.service.js';
import { listRuntimeQuerySchema, retryOutboundSchema } from './bot-runtime.schemas.js';

@Controller('communities/:communityId/bot-runtime')
@UseGuards(ScopedAuthGuard)
export class BotRuntimeController {
  constructor(private readonly runtime: BotRuntimeService) {}

  @Get('status')
  @RequireScopes(permissions.botRuntimeRead)
  status(@Param('communityId') communityId: string, @Query('accountId') accountId?: string) {
    return this.runtime.status(communityId, accountId);
  }

  @Get('inbound-updates')
  @RequireScopes(permissions.botRuntimeRead)
  inbound(@Param('communityId') communityId: string, @Query() query: unknown) {
    return this.runtime.listInbound(communityId, parseBody(listRuntimeQuerySchema, query));
  }

  @Get('outbound-messages')
  @RequireScopes(permissions.botRuntimeRead)
  outbound(@Param('communityId') communityId: string, @Query() query: unknown) {
    return this.runtime.listOutbound(communityId, parseBody(listRuntimeQuerySchema, query));
  }

  @Post('outbound-messages/:id/retry')
  @RequireScopes(permissions.botRuntimeWrite)
  retryOutbound(@Param('communityId') communityId: string, @Param('id') id: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    const data = parseBody(retryOutboundSchema, body ?? {});
    return this.runtime.retryOutbound(communityId, id, principal, data.resetAttempts);
  }

  @Get('accounts/:accountId/webhook-url')
  @RequireScopes(permissions.botRuntimeRead)
  webhookUrl(@Param('communityId') communityId: string, @Param('accountId') accountId: string) {
    return this.runtime.webhookUrl(communityId, accountId);
  }
}
