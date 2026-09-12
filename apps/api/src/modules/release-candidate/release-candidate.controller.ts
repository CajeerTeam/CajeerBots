import { Body, Controller, Get, Param, Post, Put, Query, UseGuards } from '@nestjs/common';
import { permissions, type Principal } from '@cajeerbots/security';
import { CurrentPrincipal } from '../shared/auth-context.js';
import { ScopedAuthGuard } from '../shared/scoped-auth.guard.js';
import { RequireScopes } from '../shared/scopes.decorator.js';
import { parseBody } from '../shared/zod.js';
import { ReleaseCandidateService } from './release-candidate.service.js';
import { buildMatrixSchema, createApiFreezeSchema, createUpgradePlanSchema, listQuerySchema, runReadinessSchema, runUpgradePlanSchema, upsertCompatibilityTargetSchema, verifyApiFreezeSchema } from './release-candidate.schemas.js';

@Controller('release-candidate')
@UseGuards(ScopedAuthGuard)
export class ReleaseCandidateController {
  constructor(private readonly rc: ReleaseCandidateService) {}

  @Get('summary')
  @RequireScopes(permissions.releaseCandidateRead)
  summary() { return this.rc.summary(); }

  @Post('api-freeze/snapshots')
  @RequireScopes(permissions.releaseCandidateWrite)
  createApiFreeze(@Body() body: unknown, @CurrentPrincipal() principal: Principal) { return this.rc.createApiFreeze(parseBody(createApiFreezeSchema, body ?? {}), principal); }

  @Get('api-freeze/snapshots')
  @RequireScopes(permissions.releaseCandidateRead)
  apiFreezeSnapshots(@Query() query: unknown) { return this.rc.listApiFreezes(parseBody(listQuerySchema, query)); }

  @Post('api-freeze/verify')
  @RequireScopes(permissions.releaseCandidateVerify)
  verifyApiFreeze(@Body() body: unknown, @CurrentPrincipal() principal: Principal) { return this.rc.verifyApiFreeze(parseBody(verifyApiFreezeSchema, body ?? {}), principal); }

  @Post('compatibility/defaults')
  @RequireScopes(permissions.releaseCandidateWrite)
  seedCompatibilityDefaults(@CurrentPrincipal() principal: Principal) { return this.rc.seedCompatibilityDefaults(principal); }

  @Get('compatibility/targets')
  @RequireScopes(permissions.releaseCandidateRead)
  compatibilityTargets(@Query() query: unknown) { return this.rc.listCompatibilityTargets(parseBody(listQuerySchema, query)); }

  @Put('compatibility/targets')
  @RequireScopes(permissions.releaseCandidateWrite)
  upsertCompatibilityTarget(@Body() body: unknown, @CurrentPrincipal() principal: Principal) { return this.rc.upsertCompatibilityTarget(parseBody(upsertCompatibilityTargetSchema, body), principal); }

  @Post('compatibility/matrix')
  @RequireScopes(permissions.releaseCandidateVerify)
  buildCompatibilityMatrix(@Body() body: unknown, @CurrentPrincipal() principal: Principal) { return this.rc.buildCompatibilityMatrix(parseBody(buildMatrixSchema, body ?? {}), principal); }

  @Get('compatibility/matrix')
  @RequireScopes(permissions.releaseCandidateRead)
  matrices(@Query() query: unknown) { return this.rc.listMatrices(parseBody(listQuerySchema, query)); }

  @Post('upgrade-plans')
  @RequireScopes(permissions.releaseCandidateWrite)
  createUpgradePlan(@Body() body: unknown, @CurrentPrincipal() principal: Principal) { return this.rc.createUpgradePlan(parseBody(createUpgradePlanSchema, body ?? {}), principal); }

  @Get('upgrade-plans')
  @RequireScopes(permissions.releaseCandidateRead)
  upgradePlans(@Query() query: unknown) { return this.rc.listUpgradePlans(parseBody(listQuerySchema, query)); }

  @Post('upgrade-plans/:id/run')
  @RequireScopes(permissions.releaseCandidateVerify)
  runUpgradePlan(@Param('id') id: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) { return this.rc.runUpgradePlan(id, parseBody(runUpgradePlanSchema, body ?? {}), principal); }

  @Get('upgrade-runs')
  @RequireScopes(permissions.releaseCandidateRead)
  upgradeRuns(@Query() query: unknown) { return this.rc.listUpgradeRuns(parseBody(listQuerySchema, query)); }

  @Post('readiness-runs')
  @RequireScopes(permissions.releaseCandidateVerify)
  runReadiness(@Body() body: unknown, @CurrentPrincipal() principal: Principal) { return this.rc.runReadiness(parseBody(runReadinessSchema, body ?? {}), principal); }

  @Get('readiness-runs')
  @RequireScopes(permissions.releaseCandidateRead)
  readinessRuns(@Query() query: unknown) { return this.rc.listReadinessRuns(parseBody(listQuerySchema, query)); }
}
