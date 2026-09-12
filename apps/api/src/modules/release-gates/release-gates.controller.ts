import { Body, Controller, Get, Param, Post, Put, Query, UseGuards } from '@nestjs/common';
import { permissions, type Principal } from '@cajeerbots/security';
import { CurrentPrincipal } from '../shared/auth-context.js';
import { ScopedAuthGuard } from '../shared/scoped-auth.guard.js';
import { RequireScopes } from '../shared/scopes.decorator.js';
import { parseBody } from '../shared/zod.js';
import { ReleaseGatesService } from './release-gates.service.js';
import { createReportSchema, listBlockersQuerySchema, listFreezesQuerySchema, listQualityGatesQuerySchema, listReportsQuerySchema, listRunsQuerySchema, resolveBlockerSchema, runGatesSchema, upsertFreezeSchema, upsertQualityGateSchema } from './release-gates.schemas.js';

@Controller('release-gates')
@UseGuards(ScopedAuthGuard)
export class ReleaseGatesController {
  constructor(private readonly releaseGates: ReleaseGatesService) {}

  @Get('summary')
  @RequireScopes(permissions.releaseGatesRead)
  summary() { return this.releaseGates.summary(); }

  @Get('freezes')
  @RequireScopes(permissions.releaseGatesRead)
  freezes(@Query() query: unknown) { return this.releaseGates.listFreezes(parseBody(listFreezesQuerySchema, query)); }

  @Put('freezes')
  @RequireScopes(permissions.releaseGatesWrite)
  upsertFreeze(@Body() body: unknown, @CurrentPrincipal() principal: Principal) { return this.releaseGates.upsertFreeze(parseBody(upsertFreezeSchema, body ?? {}), principal); }

  @Get('quality-gates')
  @RequireScopes(permissions.releaseGatesRead)
  qualityGates(@Query() query: unknown) { return this.releaseGates.listQualityGates(parseBody(listQualityGatesQuerySchema, query)); }

  @Post('quality-gates/defaults')
  @RequireScopes(permissions.releaseGatesWrite)
  seedDefaults(@CurrentPrincipal() principal: Principal) { return this.releaseGates.seedDefaultGates(principal); }

  @Put('quality-gates')
  @RequireScopes(permissions.releaseGatesWrite)
  upsertQualityGate(@Body() body: unknown, @CurrentPrincipal() principal: Principal) { return this.releaseGates.upsertQualityGate(parseBody(upsertQualityGateSchema, body), principal); }

  @Post('runs')
  @RequireScopes(permissions.releaseGatesRun)
  runGates(@Body() body: unknown, @CurrentPrincipal() principal: Principal) { return this.releaseGates.runGates(parseBody(runGatesSchema, body ?? {}), principal); }

  @Get('runs')
  @RequireScopes(permissions.releaseGatesRead)
  runs(@Query() query: unknown) { return this.releaseGates.listRuns(parseBody(listRunsQuerySchema, query)); }

  @Get('runs/:id')
  @RequireScopes(permissions.releaseGatesRead)
  run(@Param('id') id: string) { return this.releaseGates.run(id); }

  @Get('blockers')
  @RequireScopes(permissions.releaseGatesRead)
  blockers(@Query() query: unknown) { return this.releaseGates.listBlockers(parseBody(listBlockersQuerySchema, query)); }

  @Put('blockers/:id')
  @RequireScopes(permissions.releaseGatesWrite)
  resolveBlocker(@Param('id') id: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) { return this.releaseGates.resolveBlocker(id, parseBody(resolveBlockerSchema, body ?? {}), principal); }

  @Post('reports')
  @RequireScopes(permissions.releaseGatesReport)
  createReport(@Body() body: unknown, @CurrentPrincipal() principal: Principal) { return this.releaseGates.createReport(parseBody(createReportSchema, body ?? {}), principal); }

  @Get('reports')
  @RequireScopes(permissions.releaseGatesRead)
  reports(@Query() query: unknown) { return this.releaseGates.listReports(parseBody(listReportsQuerySchema, query)); }
}
