import { Body, Controller, Get, Param, Post, Put, Query, UseGuards } from '@nestjs/common';
import { permissions, type Principal } from '@cajeerbots/security';
import { CurrentPrincipal } from '../shared/auth-context.js';
import { ScopedAuthGuard } from '../shared/scoped-auth.guard.js';
import { RequireScopes } from '../shared/scopes.decorator.js';
import { parseBody } from '../shared/zod.js';
import { IntegrationTestsService } from './integration-tests.service.js';
import { coverageSchema, listCasesQuerySchema, listCoverageQuerySchema, listRunsQuerySchema, listStepRunsQuerySchema, listSuitesQuerySchema, queueRunSchema, runCaseSchema, runSuiteSchema, upsertCaseSchema, upsertSuiteSchema } from './integration-tests.schemas.js';

@Controller('integration-tests')
@UseGuards(ScopedAuthGuard)
export class IntegrationTestsController {
  constructor(private readonly integrationTests: IntegrationTestsService) {}

  @Get('summary')
  @RequireScopes(permissions.integrationTestsRead)
  summary() { return this.integrationTests.summary(); }

  @Post('defaults')
  @RequireScopes(permissions.integrationTestsWrite)
  seedDefaults(@CurrentPrincipal() principal: Principal) { return this.integrationTests.seedDefaults(principal); }

  @Get('suites')
  @RequireScopes(permissions.integrationTestsRead)
  suites(@Query() query: unknown) { return this.integrationTests.listSuites(parseBody(listSuitesQuerySchema, query)); }

  @Put('suites')
  @RequireScopes(permissions.integrationTestsWrite)
  upsertSuite(@Body() body: unknown, @CurrentPrincipal() principal: Principal) { return this.integrationTests.upsertSuite(parseBody(upsertSuiteSchema, body), principal); }

  @Get('cases')
  @RequireScopes(permissions.integrationTestsRead)
  cases(@Query() query: unknown) { return this.integrationTests.listCases(parseBody(listCasesQuerySchema, query)); }

  @Put('cases')
  @RequireScopes(permissions.integrationTestsWrite)
  upsertCase(@Body() body: unknown, @CurrentPrincipal() principal: Principal) { return this.integrationTests.upsertCase(parseBody(upsertCaseSchema, body), principal); }

  @Get('suites/:suiteCode/cases/:caseCode')
  @RequireScopes(permissions.integrationTestsRead)
  case(@Param('suiteCode') suiteCode: string, @Param('caseCode') caseCode: string) { return this.integrationTests.getCase(suiteCode, caseCode); }

  @Post('suites/:suiteCode/run')
  @RequireScopes(permissions.integrationTestsRun)
  runSuite(@Param('suiteCode') suiteCode: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) { return this.integrationTests.runSuite(suiteCode, parseBody(runSuiteSchema, body ?? {}), principal); }

  @Post('suites/:suiteCode/cases/:caseCode/run')
  @RequireScopes(permissions.integrationTestsRun)
  runCase(@Param('suiteCode') suiteCode: string, @Param('caseCode') caseCode: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) { return this.integrationTests.runCase(suiteCode, caseCode, parseBody(runCaseSchema, body ?? {}), principal); }

  @Post('runs/queue')
  @RequireScopes(permissions.integrationTestsRun)
  queueRun(@Body() body: unknown, @CurrentPrincipal() principal: Principal) { return this.integrationTests.queueRun(parseBody(queueRunSchema, body ?? {}), principal); }

  @Get('runs')
  @RequireScopes(permissions.integrationTestsRead)
  runs(@Query() query: unknown) { return this.integrationTests.listRuns(parseBody(listRunsQuerySchema, query)); }

  @Get('runs/:id')
  @RequireScopes(permissions.integrationTestsRead)
  run(@Param('id') id: string) { return this.integrationTests.getRun(id); }

  @Get('steps')
  @RequireScopes(permissions.integrationTestsRead)
  steps(@Query() query: unknown) { return this.integrationTests.listSteps(parseBody(listStepRunsQuerySchema, query)); }

  @Post('coverage')
  @RequireScopes(permissions.integrationTestsRun)
  coverage(@Body() body: unknown, @CurrentPrincipal() principal: Principal) { return this.integrationTests.coverage(parseBody(coverageSchema, body ?? {}), principal); }

  @Get('coverage')
  @RequireScopes(permissions.integrationTestsRead)
  coverageList(@Query() query: unknown) { return this.integrationTests.listCoverage(parseBody(listCoverageQuerySchema, query)); }
}
