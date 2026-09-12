import { Body, Controller, Get, Param, Post, UseGuards } from '@nestjs/common';
import { permissions, type Principal } from '@cajeerbots/security';
import { CurrentPrincipal } from '../shared/auth-context.js';
import { RequireScopes } from '../shared/scopes.decorator.js';
import { ScopedAuthGuard } from '../shared/scoped-auth.guard.js';
import { parseBody } from '../shared/zod.js';
import { WebhookEndpointsService } from './webhook-endpoints.service.js';
import { createWebhookEndpointSchema } from './webhook-endpoints.schemas.js';

@Controller('webhook-endpoints')
@UseGuards(ScopedAuthGuard)
export class WebhookEndpointsController {
  constructor(private readonly endpoints: WebhookEndpointsService) {}

  @Get()
  @RequireScopes(permissions.webhooksRead)
  list() {
    return this.endpoints.list();
  }

  @Post()
  @RequireScopes(permissions.webhooksWrite)
  create(@Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.endpoints.create(parseBody(createWebhookEndpointSchema, body), principal);
  }

  @Post(':id/enable')
  @RequireScopes(permissions.webhooksWrite)
  enable(@Param('id') id: string, @CurrentPrincipal() principal: Principal) {
    return this.endpoints.setActive(id, true, principal);
  }

  @Post(':id/disable')
  @RequireScopes(permissions.webhooksWrite)
  disable(@Param('id') id: string, @CurrentPrincipal() principal: Principal) {
    return this.endpoints.setActive(id, false, principal);
  }
}
