import { BadRequestException, Body, Controller, Headers, HttpCode, Post, Req, UseGuards } from '@nestjs/common';
import { permissions } from '@cajeerbots/security';
import { RequireScopes } from '../shared/scopes.decorator.js';
import { ScopedAuthGuard } from '../shared/scoped-auth.guard.js';
import { parseBody } from '../shared/zod.js';
import { WebhooksService } from './webhooks.service.js';
import { incomingEventSchema } from './webhooks.schemas.js';

@Controller('webhooks')
export class WebhooksController {
  constructor(private readonly webhooks: WebhooksService) {}

  @Post('events')
  @HttpCode(202)
  receiveSignedEvent(
    @Body() body: unknown,
    @Req() request: { rawBody?: Buffer },
    @Headers('x-cajeer-signature') signature?: string,
    @Headers('idempotency-key') idempotencyKey?: string,
  ) {
    if (!request.rawBody) throw new BadRequestException('Missing raw webhook body');
    const event = parseBody(incomingEventSchema, body);
    return this.webhooks.receive(
      { ...event, occurredAt: event.occurredAt ?? new Date().toISOString() },
      request.rawBody.toString('utf8'),
      signature,
      idempotencyKey,
    );
  }

  @Post('sign-example')
  @UseGuards(ScopedAuthGuard)
  @RequireScopes(permissions.webhooksWrite)
  signExample(@Body() body: unknown) {
    return this.webhooks.signExample(body);
  }
}
