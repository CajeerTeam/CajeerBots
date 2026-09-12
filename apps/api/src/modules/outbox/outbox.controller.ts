import { Controller, Get, Param, Post, Query, UseGuards } from '@nestjs/common';
import { permissions, type Principal } from '@cajeerbots/security';
import { CurrentPrincipal } from '../shared/auth-context.js';
import { RequireScopes } from '../shared/scopes.decorator.js';
import { ScopedAuthGuard } from '../shared/scoped-auth.guard.js';
import { OutboxService } from './outbox.service.js';

@Controller('outbox')
@UseGuards(ScopedAuthGuard)
export class OutboxController {
  constructor(private readonly outbox: OutboxService) {}

  @Get()
  @RequireScopes(permissions.outboxRead)
  list(@Query('status') status?: string, @Query('take') take?: string) {
    return this.outbox.list(status, Number(take ?? 50));
  }

  @Get(':id')
  @RequireScopes(permissions.outboxRead)
  get(@Param('id') id: string) {
    return this.outbox.get(id);
  }

  @Post(':id/retry')
  @RequireScopes(permissions.outboxWrite)
  retry(@Param('id') id: string, @CurrentPrincipal() principal: Principal) {
    return this.outbox.retry(id, principal);
  }
}
