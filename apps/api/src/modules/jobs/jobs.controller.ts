import { Body, Controller, Get, Param, Post, Put, Query, UseGuards } from '@nestjs/common';
import { permissions, type Principal } from '@cajeerbots/security';
import { CurrentPrincipal } from '../shared/auth-context.js';
import { ScopedAuthGuard } from '../shared/scoped-auth.guard.js';
import { RequireScopes } from '../shared/scopes.decorator.js';
import { parseBody } from '../shared/zod.js';
import { JobsService } from './jobs.service.js';
import { listJobsQuerySchema, listRunsQuerySchema, maintenanceSnapshotQuerySchema, runNowSchema, upsertScheduledJobSchema } from './jobs.schemas.js';

@Controller('jobs')
@UseGuards(ScopedAuthGuard)
export class JobsController {
  constructor(private readonly jobs: JobsService) {}

  @Get()
  @RequireScopes(permissions.schedulerRead)
  list(@Query() query: Record<string, unknown>) {
    return this.jobs.list(listJobsQuerySchema.parse(query));
  }

  @Get('queue-summary')
  @RequireScopes(permissions.schedulerRead)
  queueSummary() {
    return this.jobs.queueSummary();
  }

  @Get('runs')
  @RequireScopes(permissions.schedulerRead)
  runs(@Query() query: Record<string, unknown>) {
    return this.jobs.runs(listRunsQuerySchema.parse(query));
  }

  @Get('maintenance/snapshots')
  @RequireScopes(permissions.schedulerRead)
  snapshots(@Query() query: Record<string, unknown>) {
    return this.jobs.snapshots(maintenanceSnapshotQuerySchema.parse(query));
  }

  @Get(':idOrCode')
  @RequireScopes(permissions.schedulerRead)
  get(@Param('idOrCode') idOrCode: string) {
    return this.jobs.get(idOrCode);
  }

  @Put()
  @RequireScopes(permissions.schedulerWrite)
  upsert(@Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.jobs.upsert(parseBody(upsertScheduledJobSchema, body), principal);
  }

  @Post(':idOrCode/enable')
  @RequireScopes(permissions.schedulerWrite)
  enable(@Param('idOrCode') idOrCode: string, @CurrentPrincipal() principal: Principal) {
    return this.jobs.setEnabled(idOrCode, true, principal);
  }

  @Post(':idOrCode/disable')
  @RequireScopes(permissions.schedulerWrite)
  disable(@Param('idOrCode') idOrCode: string, @CurrentPrincipal() principal: Principal) {
    return this.jobs.setEnabled(idOrCode, false, principal);
  }

  @Post(':idOrCode/run')
  @RequireScopes(permissions.schedulerRun)
  runNow(@Param('idOrCode') idOrCode: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.jobs.runNow(idOrCode, parseBody(runNowSchema, body ?? {}), principal);
  }
}
