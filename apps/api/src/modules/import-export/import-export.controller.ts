import { Body, Controller, Get, Param, Post, UseGuards } from '@nestjs/common';
import { permissions, type Principal } from '@cajeerbots/security';
import { CurrentPrincipal } from '../shared/auth-context.js';
import { RequireScopes } from '../shared/scopes.decorator.js';
import { ScopedAuthGuard } from '../shared/scoped-auth.guard.js';
import { parseBody } from '../shared/zod.js';
import { ImportExportService } from './import-export.service.js';
import { importCommunitySchema } from './import-export.schemas.js';

@Controller('communities/:communityId/import-export')
@UseGuards(ScopedAuthGuard)
export class ImportExportController {
  constructor(private readonly service: ImportExportService) {}

  @Get('export')
  @RequireScopes(permissions.importExportRead)
  export(@Param('communityId') communityId: string, @CurrentPrincipal() principal: Principal) {
    return this.service.exportCommunity(communityId, principal);
  }

  @Post('import')
  @RequireScopes(permissions.importExportWrite)
  import(@Param('communityId') communityId: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.service.importCommunity(communityId, parseBody(importCommunitySchema, body), principal);
  }
}
