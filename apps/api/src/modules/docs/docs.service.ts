import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '@cajeerbots/db';
import { buildDocumentationExportBundle, bundleToMarkdownIndex, checkDocumentationPages, scanWikiDirectory, searchDocumentationPages, sha256Hex } from '@cajeerbots/docs';
import type { Principal } from '@cajeerbots/security';
import type { z } from 'zod';
import type { createDocsExportSchema, listDocCollectionsQuerySchema, listDocsChecksQuerySchema, listDocsExportsQuerySchema, listDocsSnapshotsQuerySchema, listPagesQuerySchema, runDocsCheckSchema, searchDocsQuerySchema, syncDocsSchema, upsertDocCollectionSchema } from './docs.schemas.js';

type ListCollections = z.infer<typeof listDocCollectionsQuerySchema>;
type UpsertCollection = z.infer<typeof upsertDocCollectionSchema>;
type SyncDocs = z.infer<typeof syncDocsSchema>;
type RunCheck = z.infer<typeof runDocsCheckSchema>;
type CreateExport = z.infer<typeof createDocsExportSchema>;

@Injectable()
export class DocsService {
  constructor(private readonly prisma: PrismaService) {}

  collections(query: ListCollections) {
    return this.prisma.documentationCollection.findMany({
      where: { ...(query.code ? { code: query.code } : {}), ...(query.locale ? { locale: query.locale } : {}), ...(query.sourceType ? { sourceType: query.sourceType } : {}) },
      orderBy: [{ isActive: 'desc' }, { code: 'asc' }],
      take: query.take,
    });
  }

  async upsertCollection(input: UpsertCollection, principal: Principal) {
    const collection = await this.prisma.documentationCollection.upsert({
      where: { code: input.code },
      create: { code: input.code, title: input.title, locale: input.locale, sourceType: input.sourceType, sourcePath: input.sourcePath, sourceUrl: input.sourceUrl, metadata: (input.metadata ?? {}) as never },
      update: { title: input.title, locale: input.locale, sourceType: input.sourceType, sourcePath: input.sourcePath, sourceUrl: input.sourceUrl, metadata: (input.metadata ?? {}) as never, isActive: true },
    });
    await this.audit(principal, 'docs.collection.upsert', 'documentation_collection', collection.id, { code: collection.code });
    return collection;
  }

  async syncCollection(collectionId: string, input: SyncDocs, principal: Principal) {
    const collection = await this.prisma.documentationCollection.findUnique({ where: { id: collectionId } });
    if (!collection) throw new NotFoundException('Documentation collection не найдена');
    const rootDir = safeProjectPath(input.rootDir || collection.sourcePath || 'wiki');
    const scanned = await scanWikiDirectory({ rootDir, includeDrafts: input.includeDrafts });
    const existing = await this.prisma.documentationPage.findMany({ where: { collectionId: collection.id } });
    const existingBySlug = new Map(existing.map((page) => [page.slug, page]));
    const seen = new Set<string>();
    let created = 0;
    let updated = 0;
    let unchanged = 0;

    for (const page of scanned.pages) {
      seen.add(page.slug);
      const previous = existingBySlug.get(page.slug);
      const data = {
        title: page.title,
        path: page.path,
        locale: collection.locale,
        status: 'published',
        body: page.body,
        excerpt: page.excerpt,
        headings: page.headings as never,
        links: page.links as never,
        frontmatter: page.frontmatter as never,
        wordCount: page.wordCount,
        sizeBytes: page.sizeBytes,
        sha256: page.sha256,
        sourceMtime: page.mtime,
        syncedAt: scanned.scannedAt,
        metadata: { sourcePath: page.sourcePath } as never,
      };
      if (!previous) {
        await this.prisma.documentationPage.create({ data: { collectionId: collection.id, slug: page.slug, ...data } });
        created += 1;
      } else if (previous.sha256 !== page.sha256) {
        await this.prisma.documentationPage.update({ where: { id: previous.id }, data: data as never });
        updated += 1;
      } else {
        await this.prisma.documentationPage.update({ where: { id: previous.id }, data: { syncedAt: scanned.scannedAt, status: 'published' } });
        unchanged += 1;
      }
    }

    const missing = existing.filter((page) => !seen.has(page.slug));
    if (missing.length > 0) {
      await this.prisma.documentationPage.updateMany({ where: { id: { in: missing.map((page) => page.id) } }, data: { status: 'missing', syncedAt: scanned.scannedAt } });
    }
    const sourceHash = sha256Hex(scanned.pages.map((page) => `${page.slug}:${page.sha256}`).join('\n'));
    const snapshot = input.createSnapshot
      ? await this.prisma.documentationSnapshot.create({ data: { collectionId: collection.id, version: collection.version + 1, sourceHash, pageCount: scanned.pages.length, totalWords: scanned.pages.reduce((sum, page) => sum + page.wordCount, 0), totalBytes: scanned.totalBytes, toc: scanned.pages.map((page) => ({ slug: page.slug, title: page.title, path: page.path })) as never, metadata: { rootDir: input.rootDir, created, updated, unchanged, missing: missing.length } as never, createdBy: principal.sub } })
      : null;
    await this.prisma.documentationCollection.update({ where: { id: collection.id }, data: { lastSyncedAt: scanned.scannedAt, version: { increment: 1 }, metadata: { ...(collection.metadata as Record<string, unknown> | null ?? {}), lastSync: { created, updated, unchanged, missing: missing.length, sourceHash } } as never } });
    await this.audit(principal, 'docs.collection.sync', 'documentation_collection', collection.id, { created, updated, unchanged, missing: missing.length, snapshotId: snapshot?.id });
    return { collectionId: collection.id, scanned: scanned.pages.length, created, updated, unchanged, missing: missing.length, snapshot };
  }

  pages(query: z.infer<typeof listPagesQuerySchema>) {
    return this.prisma.documentationPage.findMany({
      where: {
        ...(query.collectionId ? { collectionId: query.collectionId } : {}),
        ...(query.collectionCode ? { collection: { code: query.collectionCode } } : {}),
        ...(query.locale ? { locale: query.locale } : {}),
        ...(query.status ? { status: query.status } : {}),
        ...(query.tag ? { tags: { has: query.tag } } : {}),
      },
      orderBy: [{ collectionId: 'asc' }, { path: 'asc' }],
      take: query.take,
      include: { collection: { select: { code: true, title: true } } },
    });
  }

  async page(idOrSlug: string, collectionCode?: string) {
    const page = await this.prisma.documentationPage.findFirst({ where: { OR: [{ id: idOrSlug }, { slug: idOrSlug }], ...(collectionCode ? { collection: { code: collectionCode } } : {}) }, include: { collection: true } });
    if (!page) throw new NotFoundException('Documentation page не найдена');
    return page;
  }

  async search(query: z.infer<typeof searchDocsQuerySchema>) {
    const pages = await this.prisma.documentationPage.findMany({ where: { status: 'published', ...(query.collectionCode ? { collection: { code: query.collectionCode } } : {}), ...(query.locale ? { locale: query.locale } : {}) }, include: { collection: { select: { code: true, title: true } } }, take: 2000 });
    const normalized = pages.map((page) => ({ ...page, sourcePath: String((page.metadata as Record<string, unknown> | null)?.sourcePath ?? page.path), mtime: page.sourceMtime ?? page.updatedAt, headings: page.headings as never, links: page.links as never, frontmatter: page.frontmatter as never, sizeBytes: page.sizeBytes, sha256: page.sha256 }));
    return searchDocumentationPages(normalized as never, query.q, query.take).map((result) => ({ ...result, page: pages.find((page) => page.slug === result.slug) }));
  }

  async runCheck(input: RunCheck, principal: Principal) {
    const collection = await this.ensureCollection(input.collectionCode, input.rootDir);
    const scanned = await scanWikiDirectory({ rootDir: safeProjectPath(input.rootDir), includeDrafts: input.includeDrafts });
    const result = checkDocumentationPages(scanned.pages);
    const row = await this.prisma.documentationCheck.create({ data: { collectionId: collection.id, checkType: 'local_wiki', status: result.status, summary: result.summary as never, issues: result.issues as never, createdBy: principal.sub } });
    await this.audit(principal, 'docs.check.run', 'documentation_check', row.id, { collectionCode: collection.code, status: result.status, issues: result.issues.length });
    return { check: row, ...result };
  }

  checks(query: z.infer<typeof listDocsChecksQuerySchema>) {
    return this.prisma.documentationCheck.findMany({ where: { ...(query.collectionId ? { collectionId: query.collectionId } : {}), ...(query.status ? { status: query.status } : {}), ...(query.checkType ? { checkType: query.checkType } : {}) }, orderBy: { createdAt: 'desc' }, take: query.take, include: { collection: { select: { code: true, title: true } } } });
  }

  snapshots(query: z.infer<typeof listDocsSnapshotsQuerySchema>) {
    return this.prisma.documentationSnapshot.findMany({ where: { ...(query.collectionId ? { collectionId: query.collectionId } : {}), ...(query.collectionCode ? { collection: { code: query.collectionCode } } : {}) }, orderBy: { createdAt: 'desc' }, take: query.take, include: { collection: { select: { code: true, title: true } } } });
  }

  async createExport(input: CreateExport, principal: Principal) {
    const collection = await this.ensureCollection(input.collectionCode, input.rootDir);
    const scanned = await scanWikiDirectory({ rootDir: safeProjectPath(input.rootDir), includeDrafts: false });
    const bundle = buildDocumentationExportBundle({ collection: { code: collection.code, title: collection.title, locale: collection.locale, sourceType: collection.sourceType }, pages: scanned.pages });
    const content = input.format === 'markdown_index' ? bundleToMarkdownIndex(bundle) : JSON.stringify(input.includeBody ? bundle : { ...bundle, pages: bundle.pages.map(({ body, ...page }) => page) }, null, 2);
    const checksum = sha256Hex(content);
    const row = await this.prisma.documentationExport.create({ data: { collectionId: collection.id, format: input.format, status: 'created', checksum, sizeBytes: Buffer.byteLength(content), pageCount: scanned.pages.length, payload: { content } as never, createdBy: principal.sub } });
    await this.audit(principal, 'docs.export.create', 'documentation_export', row.id, { collectionCode: collection.code, format: input.format, pageCount: scanned.pages.length });
    return row;
  }

  exports(query: z.infer<typeof listDocsExportsQuerySchema>) {
    return this.prisma.documentationExport.findMany({ where: { ...(query.collectionId ? { collectionId: query.collectionId } : {}), ...(query.format ? { format: query.format } : {}) }, orderBy: { createdAt: 'desc' }, take: query.take, include: { collection: { select: { code: true, title: true } } } });
  }

  private async ensureCollection(code: string, sourcePath: string) {
    return this.prisma.documentationCollection.upsert({
      where: { code },
      create: { code, title: code === 'gitflic-wiki' ? 'GitFlic Wiki' : code, locale: 'ru', sourceType: 'local_wiki', sourcePath },
      update: { sourcePath, isActive: true },
    });
  }

  private audit(principal: Principal, action: string, resource: string, resourceId: string, metadata: Record<string, unknown>) {
    return this.prisma.auditLog.create({ data: { actorType: principal.type, actorId: principal.sub, action, resource, resourceId, metadata: metadata as never } });
  }
}

function safeProjectPath(relativePath: string): string {
  const cleaned = relativePath.replace(/^\/+/, '');
  const absolute = resolve(process.cwd(), cleaned);
  if (!absolute.startsWith(process.cwd())) throw new Error(`Недопустимый путь: ${relativePath}`);
  return absolute;
}
