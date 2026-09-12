import { Body, Controller, Get, Param, Post, Put, Query, UseGuards } from '@nestjs/common';
import { permissions, type Principal } from '@cajeerbots/security';
import { CurrentPrincipal } from '../shared/auth-context.js';
import { RequireScopes } from '../shared/scopes.decorator.js';
import { ScopedAuthGuard } from '../shared/scoped-auth.guard.js';
import { parseBody } from '../shared/zod.js';
import { SecurityService } from './security.service.js';
import { listRiskSnapshotsQuerySchema, listSecurityAuditRunsQuerySchema, listSecurityFindingsQuerySchema, listSecurityPoliciesQuerySchema, resolveSecurityFindingSchema, runSecurityAuditSchema, upsertSecurityPolicySchema } from './security.schemas.js';

@Controller('security')
@UseGuards(ScopedAuthGuard)
export class SecurityController {
  constructor(private readonly security: SecurityService) {}

  @Get('hardening')
  @RequireScopes(permissions.securityRead)
  hardeningStatus() {
    return this.security.hardeningStatus();
  }

  @Get('rate-limit-violations')
  @RequireScopes(permissions.securityRead)
  rateLimitViolations(@Query('take') take?: string) {
    return this.security.rateLimitViolations(take ? Number(take) : 100);
  }

  @Get('audit/summary')
  @RequireScopes(permissions.securityAuditRead)
  auditSummary() {
    return this.security.auditSummary();
  }

  @Post('audit/defaults')
  @RequireScopes(permissions.securityAuditWrite)
  seedAuditDefaults(@CurrentPrincipal() principal: Principal) {
    return this.security.seedAuditDefaults(principal);
  }

  @Get('audit/policies')
  @RequireScopes(permissions.securityAuditRead)
  auditPolicies(@Query() query: unknown) {
    return this.security.listAuditPolicies(parseBody(listSecurityPoliciesQuerySchema, query));
  }

  @Put('audit/policies')
  @RequireScopes(permissions.securityAuditWrite)
  upsertAuditPolicy(@Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.security.upsertAuditPolicy(parseBody(upsertSecurityPolicySchema, body), principal);
  }

  @Post('audit/runs')
  @RequireScopes(permissions.securityAuditRun)
  runAudit(@Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.security.runAudit(parseBody(runSecurityAuditSchema, body ?? {}), principal);
  }

  @Get('audit/runs')
  @RequireScopes(permissions.securityAuditRead)
  auditRuns(@Query() query: unknown) {
    return this.security.listAuditRuns(parseBody(listSecurityAuditRunsQuerySchema, query));
  }

  @Get('audit/runs/:id')
  @RequireScopes(permissions.securityAuditRead)
  auditRun(@Param('id') id: string) {
    return this.security.getAuditRun(id);
  }

  @Get('audit/findings')
  @RequireScopes(permissions.securityAuditRead)
  auditFindings(@Query() query: unknown) {
    return this.security.listAuditFindings(parseBody(listSecurityFindingsQuerySchema, query));
  }

  @Put('audit/findings/:id')
  @RequireScopes(permissions.securityAuditWrite)
  resolveFinding(@Param('id') id: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.security.resolveFinding(id, parseBody(resolveSecurityFindingSchema, body ?? {}), principal);
  }

  @Get('risk-snapshots')
  @RequireScopes(permissions.securityAuditRead)
  riskSnapshots(@Query() query: unknown) {
    return this.security.listRiskSnapshots(parseBody(listRiskSnapshotsQuerySchema, query));
  }
}
