import { Body, Controller, Get, Param, Put, UseGuards } from '@nestjs/common';
import { permissions, type Principal } from '@cajeerbots/security';
import { CurrentPrincipal } from '../shared/auth-context.js';
import { RequireScopes } from '../shared/scopes.decorator.js';
import { ScopedAuthGuard } from '../shared/scoped-auth.guard.js';
import { parseBody } from '../shared/zod.js';
import { SettingsService } from './settings.service.js';
import { upsertSettingSchema } from './settings.schemas.js';

@Controller('communities/:communityId/settings')
@UseGuards(ScopedAuthGuard)
export class SettingsController {
  constructor(private readonly settings: SettingsService) {}

  @Get()
  @RequireScopes(permissions.settingsRead)
  list(@Param('communityId') communityId: string) {
    return this.settings.list(communityId);
  }

  @Get(':key')
  @RequireScopes(permissions.settingsRead)
  get(@Param('communityId') communityId: string, @Param('key') key: string) {
    return this.settings.get(communityId, key);
  }

  @Get(':key/history')
  @RequireScopes(permissions.settingsRead)
  history(@Param('communityId') communityId: string, @Param('key') key: string) {
    return this.settings.history(communityId, key);
  }

  @Put(':key')
  @RequireScopes(permissions.settingsWrite)
  upsert(@Param('communityId') communityId: string, @Param('key') key: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    const parsed = parseBody(upsertSettingSchema, body);
    return this.settings.upsert(communityId, key, parsed.value, principal, parsed.reason);
  }
}
