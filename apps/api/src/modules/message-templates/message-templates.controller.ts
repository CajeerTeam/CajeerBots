import { Body, Controller, Get, Param, Post, Put, Query, UseGuards } from '@nestjs/common';
import { permissions, type Principal } from '@cajeerbots/security';
import { CurrentPrincipal } from '../shared/auth-context.js';
import { RequireScopes } from '../shared/scopes.decorator.js';
import { ScopedAuthGuard } from '../shared/scoped-auth.guard.js';
import { parseBody } from '../shared/zod.js';
import { MessageTemplatesService } from './message-templates.service.js';
import { renderMessageTemplateSchema, upsertMessageTemplateSchema } from './message-templates.schemas.js';

@Controller('message-templates')
@UseGuards(ScopedAuthGuard)
export class MessageTemplatesController {
  constructor(private readonly templates: MessageTemplatesService) {}

  @Get()
  @RequireScopes(permissions.messagesRead)
  list(@Query('locale') locale?: string) {
    return this.templates.list(locale);
  }

  @Get(':key')
  @RequireScopes(permissions.messagesRead)
  get(@Param('key') key: string, @Query('locale') locale?: string) {
    return this.templates.get(key, locale ?? 'ru');
  }

  @Put()
  @RequireScopes(permissions.messagesWrite)
  upsert(@Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.templates.upsert(parseBody(upsertMessageTemplateSchema, body), principal);
  }

  @Post(':key/render')
  @RequireScopes(permissions.messagesRead)
  render(@Param('key') key: string, @Body() body: unknown) {
    const parsed = parseBody(renderMessageTemplateSchema, body);
    return this.templates.render(key, parsed.locale, parsed.variables);
  }
}
