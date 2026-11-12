import { Body, Controller, Get, Param, Post, Query, UseGuards } from '@nestjs/common';
import { permissions, type Principal } from '@cajeerbots/security';
import { CurrentPrincipal } from '../shared/auth-context.js';
import { RequireScopes } from '../shared/scopes.decorator.js';
import { ScopedAuthGuard } from '../shared/scoped-auth.guard.js';
import { parseBody } from '../shared/zod.js';
import { applyUpdatePlanSchema, listUpdatePlansQuerySchema, planCoreUpdateSchema, planExtensionUpdateSchema, rollbackUpdatePlanSchema } from './updates.schemas.js';
import { UpdatesService } from './updates.service.js';

@Controller('updates')
@UseGuards(ScopedAuthGuard)
export class UpdatesController {
  constructor(private readonly updates: UpdatesService) {}

  @Get('plans')
  @RequireScopes(permissions.updatesRead)
  plans(@Query() query: Record<string, unknown>) {
    return this.updates.plans(listUpdatePlansQuerySchema.parse(query));
  }

  @Get('plans/:id')
  @RequireScopes(permissions.updatesRead)
  plan(@Param('id') id: string) {
    return this.updates.getPlan(id);
  }

  @Post('plans/core')
  @RequireScopes(permissions.updatesPlan)
  planCore(@Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.updates.planCoreUpdate(parseBody(planCoreUpdateSchema, body), principal);
  }

  @Post('plans/extensions/:id')
  @RequireScopes(permissions.updatesPlan)
  planExtension(@Param('id') id: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.updates.planExtensionUpdate(id, parseBody(planExtensionUpdateSchema, body ?? {}), principal);
  }

  @Post('plans/:id/apply')
  @RequireScopes(permissions.updatesApply)
  apply(@Param('id') id: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.updates.applyPlan(id, parseBody(applyUpdatePlanSchema, body ?? {}), principal);
  }

  @Post('plans/:id/rollback')
  @RequireScopes(permissions.updatesApply)
  rollback(@Param('id') id: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.updates.rollback(id, principal, parseBody(rollbackUpdatePlanSchema, body ?? {}).discardPending);
  }

  @Get('history')
  @RequireScopes(permissions.updatesRead)
  history(@Query('take') take?: string) {
    return this.updates.history(Number(take ?? 100));
  }
}
