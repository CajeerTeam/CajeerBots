import { Body, Controller, Delete, Get, Param, Post, Put, UseGuards } from '@nestjs/common';
import { permissions, type Principal } from '@cajeerbots/security';
import { CurrentPrincipal } from '../shared/auth-context.js';
import { RequireScopes } from '../shared/scopes.decorator.js';
import { ScopedAuthGuard } from '../shared/scoped-auth.guard.js';
import { parseBody, parseParam } from '../shared/zod.js';
import { SecretsService } from './secrets.service.js';
import { createSecretSchema, rotateSecretSchema, secretKeySchema, updateSecretSchema } from './secrets.schemas.js';

@Controller('secrets')
@UseGuards(ScopedAuthGuard)
export class SecretsController {
  constructor(private readonly secrets: SecretsService) {}

  @Get()
  @RequireScopes(permissions.secretsRead)
  list() {
    return this.secrets.list();
  }

  @Get(':key')
  @RequireScopes(permissions.secretsRead)
  get(@Param('key') key: string) {
    return this.secrets.get(parseParam(secretKeySchema, key));
  }

  @Get(':key/value')
  @RequireScopes(permissions.secretsReveal)
  reveal(@Param('key') key: string, @CurrentPrincipal() principal: Principal) {
    return this.secrets.reveal(parseParam(secretKeySchema, key), principal);
  }

  @Get(':key/history')
  @RequireScopes(permissions.secretsRead)
  history(@Param('key') key: string) {
    return this.secrets.history(parseParam(secretKeySchema, key));
  }

  @Post()
  @RequireScopes(permissions.secretsWrite)
  create(@Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.secrets.upsert(parseBody(createSecretSchema, body), principal);
  }

  @Put(':key')
  @RequireScopes(permissions.secretsWrite)
  update(@Param('key') key: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.secrets.upsert({ key: parseParam(secretKeySchema, key), ...parseBody(updateSecretSchema, body) }, principal);
  }

  @Post(':key/rotate')
  @RequireScopes(permissions.secretsWrite)
  rotate(@Param('key') key: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.secrets.rotate(parseParam(secretKeySchema, key), parseBody(rotateSecretSchema, body), principal);
  }

  @Delete(':key')
  @RequireScopes(permissions.secretsWrite)
  remove(@Param('key') key: string, @CurrentPrincipal() principal: Principal) {
    return this.secrets.remove(parseParam(secretKeySchema, key), principal);
  }
}
