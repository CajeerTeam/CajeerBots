import { Body, Controller, Get, Param, Post, Put, Query, UseGuards } from '@nestjs/common';
import { permissions, type Principal } from '@cajeerbots/security';
import { CurrentPrincipal } from '../shared/auth-context.js';
import { ScopedAuthGuard } from '../shared/scoped-auth.guard.js';
import { RequireScopes } from '../shared/scopes.decorator.js';
import { parseBody } from '../shared/zod.js';
import { DevtoolsService } from './devtools.service.js';
import { createDebugSessionSchema, listBotScenariosQuerySchema, listDebugEventsQuerySchema, listDebugSessionsQuerySchema, listScenarioRunsQuerySchema, recordDebugEventSchema, runScenarioSchema, simulateProviderUpdateSchema, upsertBotScenarioSchema } from './devtools.schemas.js';

@Controller('devtools')
@UseGuards(ScopedAuthGuard)
export class DevtoolsController {
  constructor(private readonly devtools: DevtoolsService) {}

  @Get('debug/sessions')
  @RequireScopes(permissions.devtoolsRead)
  sessions(@Query() query: unknown) {
    return this.devtools.listSessions(parseBody(listDebugSessionsQuerySchema, query));
  }

  @Post('debug/sessions')
  @RequireScopes(permissions.devtoolsWrite)
  createSession(@Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.devtools.createSession(parseBody(createDebugSessionSchema, body), principal);
  }

  @Post('debug/sessions/:id/end')
  @RequireScopes(permissions.devtoolsWrite)
  endSession(@Param('id') id: string, @CurrentPrincipal() principal: Principal) {
    return this.devtools.endSession(id, principal);
  }

  @Get('debug/events')
  @RequireScopes(permissions.devtoolsRead)
  events(@Query() query: unknown) {
    return this.devtools.listEvents(parseBody(listDebugEventsQuerySchema, query));
  }

  @Post('debug/events')
  @RequireScopes(permissions.devtoolsWrite)
  recordEvent(@Body() body: unknown) {
    return this.devtools.recordEvent(parseBody(recordDebugEventSchema, body));
  }

  @Get('communities/:communityId/scenarios')
  @RequireScopes(permissions.devtoolsRead)
  scenarios(@Param('communityId') communityId: string, @Query() query: unknown) {
    return this.devtools.listScenarios(communityId, parseBody(listBotScenariosQuerySchema, query));
  }

  @Get('communities/:communityId/scenarios/:idOrCode')
  @RequireScopes(permissions.devtoolsRead)
  scenario(@Param('communityId') communityId: string, @Param('idOrCode') idOrCode: string) {
    return this.devtools.getScenario(communityId, idOrCode);
  }

  @Put('communities/:communityId/scenarios')
  @RequireScopes(permissions.devtoolsWrite)
  upsertScenario(@Param('communityId') communityId: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.devtools.upsertScenario(communityId, parseBody(upsertBotScenarioSchema, body), principal);
  }

  @Post('communities/:communityId/scenarios/:idOrCode/run')
  @RequireScopes(permissions.devtoolsRun)
  runScenario(@Param('communityId') communityId: string, @Param('idOrCode') idOrCode: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.devtools.runScenario(communityId, idOrCode, parseBody(runScenarioSchema, body ?? {}), principal);
  }

  @Get('communities/:communityId/scenario-runs')
  @RequireScopes(permissions.devtoolsRead)
  runs(@Param('communityId') communityId: string, @Query() query: unknown) {
    return this.devtools.listRuns(communityId, parseBody(listScenarioRunsQuerySchema, query));
  }

  @Post('communities/:communityId/simulate-update')
  @RequireScopes(permissions.devtoolsRun)
  simulate(@Param('communityId') communityId: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.devtools.simulateUpdate(communityId, parseBody(simulateProviderUpdateSchema, body), principal);
  }
}
