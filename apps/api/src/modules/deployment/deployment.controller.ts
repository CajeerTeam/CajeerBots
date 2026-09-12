import { Body, Controller, Get, Param, Post, Put, Query, UseGuards } from '@nestjs/common';
import { permissions, type Principal } from '@cajeerbots/security';
import { CurrentPrincipal } from '../shared/auth-context.js';
import { ScopedAuthGuard } from '../shared/scoped-auth.guard.js';
import { RequireScopes } from '../shared/scopes.decorator.js';
import { parseBody } from '../shared/zod.js';
import { DeploymentService } from './deployment.service.js';
import { buildReleaseManifestSchema, createDeploymentRunSchema, finishDeploymentRunSchema, listDeploymentChecksQuerySchema, listDeploymentTargetsQuerySchema, renderEnvSchema, renderNginxSchema, renderSystemdSchema, runDeploymentCheckSchema, upsertDeploymentTargetSchema, validateEnvSchema } from './deployment.schemas.js';

@Controller('deployment')
@UseGuards(ScopedAuthGuard)
export class DeploymentController {
  constructor(private readonly deployment: DeploymentService) {}

  @Get('targets')
  @RequireScopes(permissions.deploymentRead)
  targets(@Query() query: unknown) {
    return this.deployment.targets(parseBody(listDeploymentTargetsQuerySchema, query));
  }

  @Put('targets')
  @RequireScopes(permissions.deploymentWrite)
  upsertTarget(@Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.deployment.upsertTarget(parseBody(upsertDeploymentTargetSchema, body), principal);
  }

  @Post('targets/:targetId/check')
  @RequireScopes(permissions.deploymentRun)
  runTargetChecks(@Param('targetId') targetId: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.deployment.runChecks(targetId, parseBody(runDeploymentCheckSchema, body ?? {}), principal);
  }

  @Post('checks/run')
  @RequireScopes(permissions.deploymentRun)
  runAdHocChecks(@Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.deployment.runChecks(undefined, parseBody(runDeploymentCheckSchema, body ?? {}), principal);
  }

  @Get('checks')
  @RequireScopes(permissions.deploymentRead)
  checks(@Query() query: unknown) {
    return this.deployment.checks(parseBody(listDeploymentChecksQuerySchema, query));
  }

  @Post('runs')
  @RequireScopes(permissions.deploymentRun)
  createRun(@Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.deployment.createRun(parseBody(createDeploymentRunSchema, body), principal);
  }

  @Post('runs/:id/finish')
  @RequireScopes(permissions.deploymentRun)
  finishRun(@Param('id') id: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.deployment.finishRun(id, parseBody(finishDeploymentRunSchema, body), principal);
  }

  @Post('env/render')
  @RequireScopes(permissions.deploymentWrite)
  renderEnv(@Body() body: unknown) {
    return this.deployment.renderEnv(parseBody(renderEnvSchema, body ?? {}));
  }

  @Post('env/validate')
  @RequireScopes(permissions.deploymentRead)
  validateEnv(@Body() body: unknown) {
    return this.deployment.validateEnv(parseBody(validateEnvSchema, body).envText);
  }

  @Post('nginx/render')
  @RequireScopes(permissions.deploymentWrite)
  renderNginx(@Body() body: unknown) {
    return this.deployment.renderNginx(parseBody(renderNginxSchema, body));
  }

  @Post('systemd/render')
  @RequireScopes(permissions.deploymentWrite)
  renderSystemd(@Body() body: unknown) {
    return this.deployment.renderSystemd(parseBody(renderSystemdSchema, body ?? {}));
  }

  @Post('release-manifest')
  @RequireScopes(permissions.deploymentRelease)
  buildReleaseManifest(@Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.deployment.buildReleaseManifest(parseBody(buildReleaseManifestSchema, body), principal);
  }

  @Get('release-artifacts')
  @RequireScopes(permissions.deploymentRead)
  artifacts(@Query() query: { version?: string; channel?: string; status?: string; take?: string }) {
    return this.deployment.artifacts({ ...query, take: query.take ? Number(query.take) : undefined });
  }
}
