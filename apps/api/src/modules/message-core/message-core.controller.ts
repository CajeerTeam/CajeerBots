import { Body, Controller, Get, Param, Post, Put, Query, UseGuards } from '@nestjs/common';
import { permissions, type Principal } from '@cajeerbots/security';
import { CurrentPrincipal } from '../shared/auth-context.js';
import { RequireScopes } from '../shared/scopes.decorator.js';
import { ScopedAuthGuard } from '../shared/scoped-auth.guard.js';
import { parseBody } from '../shared/zod.js';
import { MessageCoreService } from './message-core.service.js';
import { renderMessageSchema, submitFormSchema, upsertButtonLayoutSchema, upsertCatalogSchema, upsertFormSchema } from './message-core.schemas.js';

@Controller('communities/:communityId/message-core')
@UseGuards(ScopedAuthGuard)
export class MessageCoreController {
  constructor(private readonly messages: MessageCoreService) {}

  @Get('catalogs')
  @RequireScopes(permissions.messageCoreRead)
  catalogs(@Param('communityId') communityId: string, @Query('locale') locale?: string, @Query('namespace') namespace?: string) {
    return this.messages.catalogs(communityId, locale, namespace);
  }

  @Put('catalogs')
  @RequireScopes(permissions.messageCoreWrite)
  upsertCatalog(@Param('communityId') communityId: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.messages.upsertCatalog(communityId, parseBody(upsertCatalogSchema, body), principal);
  }

  @Get('button-layouts')
  @RequireScopes(permissions.messageCoreRead)
  buttonLayouts(@Param('communityId') communityId: string, @Query('code') code?: string, @Query('provider') provider?: 'telegram' | 'vk' | 'discord', @Query('locale') locale?: string) {
    return this.messages.buttonLayouts(communityId, code, provider, locale);
  }

  @Put('button-layouts')
  @RequireScopes(permissions.messageCoreWrite)
  upsertButtonLayout(@Param('communityId') communityId: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.messages.upsertButtonLayout(communityId, parseBody(upsertButtonLayoutSchema, body), principal);
  }

  @Get('forms')
  @RequireScopes(permissions.messageCoreRead)
  forms(@Param('communityId') communityId: string, @Query('locale') locale?: string, @Query('activeOnly') activeOnly?: string) {
    return this.messages.forms(communityId, locale, activeOnly === 'true');
  }

  @Put('forms')
  @RequireScopes(permissions.messageCoreWrite)
  upsertForm(@Param('communityId') communityId: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.messages.upsertForm(communityId, parseBody(upsertFormSchema, body), principal);
  }

  @Post('forms/:code/submit')
  @RequireScopes(permissions.messageCoreWrite)
  submitForm(@Param('communityId') communityId: string, @Param('code') code: string, @Query('locale') locale: string | undefined, @Body() body: unknown) {
    return this.messages.submitForm(communityId, code, locale ?? 'ru', parseBody(submitFormSchema, body));
  }

  @Get('form-submissions')
  @RequireScopes(permissions.messageCoreRead)
  formSubmissions(@Param('communityId') communityId: string, @Query('formId') formId?: string, @Query('take') take?: string) {
    return this.messages.formSubmissions(communityId, formId, take ? Number(take) : 50);
  }

  @Post('render')
  @RequireScopes(permissions.messageCoreRead)
  render(@Param('communityId') communityId: string, @Body() body: unknown) {
    return this.messages.render(communityId, parseBody(renderMessageSchema, body));
  }

  @Get('render-logs')
  @RequireScopes(permissions.messageCoreRead)
  renderLogs(@Param('communityId') communityId: string, @Query('take') take?: string) {
    return this.messages.renderLogs(communityId, take ? Number(take) : 50);
  }
}
