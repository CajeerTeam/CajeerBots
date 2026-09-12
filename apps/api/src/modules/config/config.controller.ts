import { Controller, Get, UseGuards } from '@nestjs/common';
import { permissions } from '@cajeerbots/security';
import { RequireScopes } from '../shared/scopes.decorator.js';
import { ScopedAuthGuard } from '../shared/scoped-auth.guard.js';
import { RuntimeConfigService } from './config.service.js';

@Controller('config')
@UseGuards(ScopedAuthGuard)
export class RuntimeConfigController {
  constructor(private readonly config: RuntimeConfigService) {}

  @Get('validate')
  @RequireScopes(permissions.configRead)
  validate() {
    return this.config.validate();
  }

  @Get('summary')
  @RequireScopes(permissions.configRead)
  summary() {
    return this.config.summary();
  }

  @Get('env')
  @RequireScopes(permissions.configRead)
  env() {
    return this.config.redactedEnv();
  }
}
