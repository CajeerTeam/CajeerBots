import { Body, Controller, Get, Param, Post, Put, Query, UseGuards } from '@nestjs/common';
import { permissions, type Principal } from '@cajeerbots/security';
import { CurrentPrincipal } from '../shared/auth-context.js';
import { ScopedAuthGuard } from '../shared/scoped-auth.guard.js';
import { RequireScopes } from '../shared/scopes.decorator.js';
import { parseBody } from '../shared/zod.js';
import { ObservabilityService } from './observability.service.js';
import { collectSnapshotSchema, listAlertRulesQuerySchema, listIncidentsQuerySchema, listProbesQuerySchema, listSamplesQuerySchema, resolveIncidentSchema, upsertAlertRuleSchema, upsertProbeSchema } from './observability.schemas.js';

@Controller('observability')
@UseGuards(ScopedAuthGuard)
export class ObservabilityController {
  constructor(private readonly observability: ObservabilityService) {}

  @Get('summary')
  @RequireScopes(permissions.observabilityRead)
  summary() {
    return this.observability.summary();
  }

  @Post('collect')
  @RequireScopes(permissions.observabilityWrite)
  collect(@Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.observability.collect(parseBody(collectSnapshotSchema, body ?? {}), principal);
  }

  @Get('samples')
  @RequireScopes(permissions.observabilityRead)
  samples(@Query() query: unknown) {
    return this.observability.samples(parseBody(listSamplesQuerySchema, query));
  }

  @Get('probes')
  @RequireScopes(permissions.observabilityRead)
  probes(@Query() query: unknown) {
    return this.observability.probes(parseBody(listProbesQuerySchema, query));
  }

  @Put('probes')
  @RequireScopes(permissions.observabilityWrite)
  upsertProbe(@Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.observability.upsertProbe(parseBody(upsertProbeSchema, body), principal);
  }

  @Post('probes/:idOrCode/run')
  @RequireScopes(permissions.observabilityWrite)
  runProbe(@Param('idOrCode') idOrCode: string, @CurrentPrincipal() principal: Principal) {
    return this.observability.runProbe(idOrCode, principal);
  }

  @Get('alert-rules')
  @RequireScopes(permissions.observabilityRead)
  alertRules(@Query() query: unknown) {
    return this.observability.alertRules(parseBody(listAlertRulesQuerySchema, query));
  }

  @Put('alert-rules')
  @RequireScopes(permissions.observabilityWrite)
  upsertAlertRule(@Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.observability.upsertAlertRule(parseBody(upsertAlertRuleSchema, body), principal);
  }

  @Post('alert-rules/evaluate')
  @RequireScopes(permissions.observabilityWrite)
  evaluate(@CurrentPrincipal() principal: Principal) {
    return this.observability.evaluateAlerts(principal);
  }

  @Get('incidents')
  @RequireScopes(permissions.observabilityRead)
  incidents(@Query() query: unknown) {
    return this.observability.incidents(parseBody(listIncidentsQuerySchema, query));
  }

  @Post('incidents/:idOrCode/resolve')
  @RequireScopes(permissions.observabilityManage)
  resolveIncident(@Param('idOrCode') idOrCode: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.observability.resolveIncident(idOrCode, parseBody(resolveIncidentSchema, body ?? {}), principal);
  }
}
