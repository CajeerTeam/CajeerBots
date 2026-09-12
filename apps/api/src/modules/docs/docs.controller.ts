import { Body, Controller, Get, Param, Post, Put, Query, UseGuards } from '@nestjs/common';
import { permissions, type Principal } from '@cajeerbots/security';
import { CurrentPrincipal } from '../shared/auth-context.js';
import { ScopedAuthGuard } from '../shared/scoped-auth.guard.js';
import { RequireScopes } from '../shared/scopes.decorator.js';
import { parseBody } from '../shared/zod.js';
import { DocsService } from './docs.service.js';
import { createDocsExportSchema, listDocCollectionsQuerySchema, listDocsChecksQuerySchema, listDocsExportsQuerySchema, listDocsSnapshotsQuerySchema, listPagesQuerySchema, runDocsCheckSchema, searchDocsQuerySchema, syncDocsSchema, upsertDocCollectionSchema } from './docs.schemas.js';

@Controller('docs')
@UseGuards(ScopedAuthGuard)
export class DocsController {
  constructor(private readonly docs: DocsService) {}

  @Get('collections')
  @RequireScopes(permissions.docsRead)
  collections(@Query() query: unknown) {
    return this.docs.collections(parseBody(listDocCollectionsQuerySchema, query));
  }

  @Put('collections')
  @RequireScopes(permissions.docsWrite)
  upsertCollection(@Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.docs.upsertCollection(parseBody(upsertDocCollectionSchema, body), principal);
  }

  @Post('collections/:collectionId/sync')
  @RequireScopes(permissions.docsSync)
  syncCollection(@Param('collectionId') collectionId: string, @Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.docs.syncCollection(collectionId, parseBody(syncDocsSchema, body ?? {}), principal);
  }

  @Get('pages')
  @RequireScopes(permissions.docsRead)
  pages(@Query() query: unknown) {
    return this.docs.pages(parseBody(listPagesQuerySchema, query));
  }

  @Get('pages/:idOrSlug')
  @RequireScopes(permissions.docsRead)
  page(@Param('idOrSlug') idOrSlug: string, @Query('collectionCode') collectionCode?: string) {
    return this.docs.page(idOrSlug, collectionCode);
  }

  @Get('search')
  @RequireScopes(permissions.docsRead)
  search(@Query() query: unknown) {
    return this.docs.search(parseBody(searchDocsQuerySchema, query));
  }

  @Post('checks/run')
  @RequireScopes(permissions.docsCheck)
  runCheck(@Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.docs.runCheck(parseBody(runDocsCheckSchema, body ?? {}), principal);
  }

  @Get('checks')
  @RequireScopes(permissions.docsRead)
  checks(@Query() query: unknown) {
    return this.docs.checks(parseBody(listDocsChecksQuerySchema, query));
  }

  @Get('snapshots')
  @RequireScopes(permissions.docsRead)
  snapshots(@Query() query: unknown) {
    return this.docs.snapshots(parseBody(listDocsSnapshotsQuerySchema, query));
  }

  @Post('exports')
  @RequireScopes(permissions.docsExport)
  createExport(@Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.docs.createExport(parseBody(createDocsExportSchema, body ?? {}), principal);
  }

  @Get('exports')
  @RequireScopes(permissions.docsRead)
  exports(@Query() query: unknown) {
    return this.docs.exports(parseBody(listDocsExportsQuerySchema, query));
  }
}
