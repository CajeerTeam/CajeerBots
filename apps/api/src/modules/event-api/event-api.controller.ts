import { Body, Controller, Get, Param, Post, Put, Query, UseGuards } from '@nestjs/common';
import { permissions, type Principal } from '@cajeerbots/security';
import { CurrentPrincipal } from '../shared/auth-context.js';
import { RequireScopes } from '../shared/scopes.decorator.js';
import { ScopedAuthGuard } from '../shared/scoped-auth.guard.js';
import { parseBody } from '../shared/zod.js';
import { EventApiService } from './event-api.service.js';
import { createEventSubscriptionSchema, listEventsQuerySchema, publishEventSchema, retryDeliverySchema, updateEventSubscriptionSchema, upsertEventTopicSchema } from './event-api.schemas.js';

@Controller('events')
@UseGuards(ScopedAuthGuard)
export class EventApiController {
  constructor(private readonly events: EventApiService) {}

  @Get('topics')
  @RequireScopes(permissions.eventApiRead)
  topics() {
    return this.events.topics();
  }

  @Put('topics/:name')
  @RequireScopes(permissions.eventApiWrite)
  upsertTopic(@Param('name') name: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.events.upsertTopic(name, parseBody(upsertEventTopicSchema, body), principal);
  }

  @Post('publish')
  @RequireScopes(permissions.eventApiPublish)
  publish(@Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.events.publish(parseBody(publishEventSchema, body), principal);
  }

  @Get('records')
  @RequireScopes(permissions.eventApiRead)
  records(@Query() query: Record<string, unknown>) {
    return this.events.listEvents(listEventsQuerySchema.parse(query));
  }

  @Get('records/:id')
  @RequireScopes(permissions.eventApiRead)
  record(@Param('id') id: string) {
    return this.events.getEvent(id);
  }

  @Post('records/:id/replay')
  @RequireScopes(permissions.eventApiReplay)
  replay(@Param('id') id: string, @CurrentPrincipal() principal: Principal) {
    return this.events.replayEvent(id, principal);
  }

  @Get('subscriptions')
  @RequireScopes(permissions.eventApiRead)
  subscriptions() {
    return this.events.subscriptions();
  }

  @Post('subscriptions')
  @RequireScopes(permissions.eventApiWrite)
  createSubscription(@Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.events.createSubscription(parseBody(createEventSubscriptionSchema, body), principal);
  }

  @Put('subscriptions/:id')
  @RequireScopes(permissions.eventApiWrite)
  updateSubscription(@Param('id') id: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.events.updateSubscription(id, parseBody(updateEventSubscriptionSchema, body), principal);
  }

  @Post('subscriptions/:id/enable')
  @RequireScopes(permissions.eventApiWrite)
  enableSubscription(@Param('id') id: string, @CurrentPrincipal() principal: Principal) {
    return this.events.setSubscriptionState(id, true, principal);
  }

  @Post('subscriptions/:id/disable')
  @RequireScopes(permissions.eventApiWrite)
  disableSubscription(@Param('id') id: string, @CurrentPrincipal() principal: Principal) {
    return this.events.setSubscriptionState(id, false, principal);
  }

  @Get('deliveries')
  @RequireScopes(permissions.eventApiRead)
  deliveries(@Query('subscriptionId') subscriptionId?: string, @Query('status') status?: string, @Query('take') take?: string) {
    return this.events.deliveries(subscriptionId, status, Number(take ?? 50));
  }

  @Post('deliveries/:id/retry')
  @RequireScopes(permissions.eventApiReplay)
  retryDelivery(@Param('id') id: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    const input = parseBody(retryDeliverySchema, body ?? {});
    return this.events.retryDelivery(id, input.resetAttempts, principal);
  }

  @Get('dead-letter')
  @RequireScopes(permissions.eventApiRead)
  deadLetters(@Query('status') status?: string, @Query('take') take?: string) {
    return this.events.deadLetters(status, Number(take ?? 50));
  }

  @Post('dead-letter/:id/resolve')
  @RequireScopes(permissions.eventApiWrite)
  resolveDeadLetter(@Param('id') id: string, @CurrentPrincipal() principal: Principal) {
    return this.events.resolveDeadLetter(id, principal);
  }
}
