import { Body, Controller, Get, Param, Post, Put, Query, UseGuards } from '@nestjs/common';
import { permissions, type Principal } from '@cajeerbots/security';
import { CurrentPrincipal } from '../shared/auth-context.js';
import { ScopedAuthGuard } from '../shared/scoped-auth.guard.js';
import { RequireScopes } from '../shared/scopes.decorator.js';
import { parseBody } from '../shared/zod.js';
import { ContractsService } from './contracts.service.js';
import { createSnapshotSchema, generateSdkSchema, listChecksQuerySchema, listDeprecationsQuerySchema, listSdkRunsQuerySchema, listSnapshotsQuerySchema, runContractCheckSchema, upsertDeprecationSchema } from './contracts.schemas.js';

@Controller('contracts')
@UseGuards(ScopedAuthGuard)
export class ContractsController {
  constructor(private readonly contracts: ContractsService) {}

  @Get('snapshots')
  @RequireScopes(permissions.contractsRead)
  snapshots(@Query() query: unknown) {
    return this.contracts.snapshots(parseBody(listSnapshotsQuerySchema, query));
  }

  @Get('snapshots/:id')
  @RequireScopes(permissions.contractsRead)
  snapshot(@Param('id') id: string) {
    return this.contracts.snapshot(id);
  }

  @Post('snapshots')
  @RequireScopes(permissions.contractsWrite)
  createSnapshot(@Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.contracts.createSnapshot(parseBody(createSnapshotSchema, body ?? {}), principal);
  }

  @Post('checks/run')
  @RequireScopes(permissions.contractsCheck)
  runCheck(@Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.contracts.runCheck(parseBody(runContractCheckSchema, body ?? {}), principal);
  }

  @Get('checks')
  @RequireScopes(permissions.contractsRead)
  checks(@Query() query: unknown) {
    return this.contracts.checks(parseBody(listChecksQuerySchema, query));
  }

  @Post('sdk/generate')
  @RequireScopes(permissions.contractsSdk)
  generateSdk(@Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.contracts.generateSdk(parseBody(generateSdkSchema, body ?? {}), principal);
  }

  @Get('sdk/runs')
  @RequireScopes(permissions.contractsRead)
  sdkRuns(@Query() query: unknown) {
    return this.contracts.sdkRuns(parseBody(listSdkRunsQuerySchema, query));
  }

  @Get('deprecations')
  @RequireScopes(permissions.contractsRead)
  deprecations(@Query() query: unknown) {
    return this.contracts.deprecations(parseBody(listDeprecationsQuerySchema, query));
  }

  @Put('deprecations')
  @RequireScopes(permissions.contractsWrite)
  upsertDeprecation(@Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.contracts.upsertDeprecation(parseBody(upsertDeprecationSchema, body), principal);
  }
}
