import { Command } from 'commander';
import { PrismaService } from '@cajeerbots/db';
import { buildDocumentationExportBundle, bundleToMarkdownIndex, checkDocumentationPages, scanWikiDirectory, searchDocumentationPages, sha256Hex } from '@cajeerbots/docs';
import { resolve } from 'node:path';

export function docsCommand(): Command {
  const command = new Command('docs').description('GitFlic Wiki documentation operations: sync, check, search, export');

  command.command('collections:list')
    .option('--code <code>')
    .option('--take <n>', 'limit', '100')
    .action(async (options: { code?: string; take: string }) => withPrisma(async (prisma) => {
      const rows = await prisma.documentationCollection.findMany({ where: options.code ? { code: options.code } : {}, orderBy: [{ isActive: 'desc' }, { code: 'asc' }], take: Math.min(Number(options.take), 500) });
      console.log(JSON.stringify(rows, null, 2));
    }));

  command.command('collections:set')
    .requiredOption('--code <code>')
    .requiredOption('--title <title>')
    .option('--locale <locale>', 'locale', 'ru')
    .option('--source-type <type>', 'local_wiki | gitflic_wiki | manual', 'local_wiki')
    .option('--source-path <path>', 'local wiki path', 'wiki')
    .option('--source-url <url>')
    .action(async (options: { code: string; title: string; locale: string; sourceType: string; sourcePath: string; sourceUrl?: string }) => withPrisma(async (prisma) => {
      const row = await prisma.documentationCollection.upsert({ where: { code: options.code }, create: { code: options.code, title: options.title, locale: options.locale, sourceType: options.sourceType, sourcePath: options.sourcePath, sourceUrl: options.sourceUrl }, update: { title: options.title, locale: options.locale, sourceType: options.sourceType, sourcePath: options.sourcePath, sourceUrl: options.sourceUrl, isActive: true } });
      console.log(JSON.stringify(row, null, 2));
    }));

  command.command('sync')
    .option('--collection <code>', 'collection code', 'gitflic-wiki')
    .option('--root <path>', 'wiki root', 'wiki')
    .option('--include-drafts')
    .action(async (options: { collection: string; root: string; includeDrafts?: boolean }) => withPrisma(async (prisma) => {
      const collection = await ensureCollection(prisma, options.collection, options.root);
      const scanned = await scanWikiDirectory({ rootDir: resolve(process.cwd(), options.root), includeDrafts: Boolean(options.includeDrafts) });
      const existing = await prisma.documentationPage.findMany({ where: { collectionId: collection.id } });
      const existingBySlug = new Map(existing.map((page) => [page.slug, page]));
      const seen = new Set<string>();
      let created = 0, updated = 0, unchanged = 0;
      for (const page of scanned.pages) {
        seen.add(page.slug);
        const previous = existingBySlug.get(page.slug);
        const data = { title: page.title, path: page.path, locale: collection.locale, status: 'published', body: page.body, excerpt: page.excerpt, headings: page.headings as never, links: page.links as never, frontmatter: page.frontmatter as never, wordCount: page.wordCount, sizeBytes: page.sizeBytes, sha256: page.sha256, sourceMtime: page.mtime, syncedAt: scanned.scannedAt, metadata: { sourcePath: page.sourcePath } as never };
        if (!previous) { await prisma.documentationPage.create({ data: { collectionId: collection.id, slug: page.slug, ...data } }); created++; }
        else if (previous.sha256 !== page.sha256) { await prisma.documentationPage.update({ where: { id: previous.id }, data }); updated++; }
        else { await prisma.documentationPage.update({ where: { id: previous.id }, data: { status: 'published', syncedAt: scanned.scannedAt } }); unchanged++; }
      }
      const missing = existing.filter((page) => !seen.has(page.slug));
      if (missing.length) await prisma.documentationPage.updateMany({ where: { id: { in: missing.map((page) => page.id) } }, data: { status: 'missing', syncedAt: scanned.scannedAt } });
      const sourceHash = sha256Hex(scanned.pages.map((page) => `${page.slug}:${page.sha256}`).join('\n'));
      const snapshot = await prisma.documentationSnapshot.create({ data: { collectionId: collection.id, version: collection.version + 1, sourceHash, pageCount: scanned.pages.length, totalWords: scanned.pages.reduce((sum, page) => sum + page.wordCount, 0), totalBytes: scanned.totalBytes, toc: scanned.pages.map((page) => ({ slug: page.slug, title: page.title, path: page.path })) as never, metadata: { rootDir: options.root, created, updated, unchanged, missing: missing.length } as never, createdBy: 'cli' } });
      await prisma.documentationCollection.update({ where: { id: collection.id }, data: { version: { increment: 1 }, lastSyncedAt: scanned.scannedAt } });
      console.log(JSON.stringify({ collection: collection.code, snapshotId: snapshot.id, scanned: scanned.pages.length, created, updated, unchanged, missing: missing.length }, null, 2));
    }));

  command.command('pages:list')
    .option('--collection <code>')
    .option('--status <status>')
    .option('--take <n>', 'limit', '200')
    .action(async (options: { collection?: string; status?: string; take: string }) => withPrisma(async (prisma) => {
      const rows = await prisma.documentationPage.findMany({ where: { ...(options.collection ? { collection: { code: options.collection } } : {}), ...(options.status ? { status: options.status } : {}) }, orderBy: [{ collectionId: 'asc' }, { path: 'asc' }], take: Math.min(Number(options.take), 1000), include: { collection: { select: { code: true, title: true } } } });
      console.log(JSON.stringify(rows, null, 2));
    }));

  command.command('pages:get').argument('<idOrSlug>').option('--collection <code>').action(async (idOrSlug: string, options: { collection?: string }) => withPrisma(async (prisma) => {
    const page = await prisma.documentationPage.findFirst({ where: { OR: [{ id: idOrSlug }, { slug: idOrSlug }], ...(options.collection ? { collection: { code: options.collection } } : {}) }, include: { collection: true } });
    if (!page) throw new Error('Documentation page не найдена');
    console.log(JSON.stringify(page, null, 2));
  }));

  command.command('search').argument('<query>').option('--collection <code>').option('--take <n>', 'limit', '20').action(async (query: string, options: { collection?: string; take: string }) => withPrisma(async (prisma) => {
    const pages = await prisma.documentationPage.findMany({ where: { status: 'published', ...(options.collection ? { collection: { code: options.collection } } : {}) }, take: 2000 });
    const normalized = pages.map((page) => ({ ...page, sourcePath: String((page.metadata as Record<string, unknown> | null)?.sourcePath ?? page.path), mtime: page.sourceMtime ?? page.updatedAt, headings: page.headings as never, links: page.links as never, frontmatter: page.frontmatter as never, sizeBytes: page.sizeBytes, sha256: page.sha256 }));
    console.log(JSON.stringify(searchDocumentationPages(normalized as never, query, Math.min(Number(options.take), 100)), null, 2));
  }));

  command.command('check')
    .option('--collection <code>', 'collection code', 'gitflic-wiki')
    .option('--root <path>', 'wiki root', 'wiki')
    .option('--include-drafts')
    .action(async (options: { collection: string; root: string; includeDrafts?: boolean }) => withPrisma(async (prisma) => {
      const collection = await ensureCollection(prisma, options.collection, options.root);
      const scanned = await scanWikiDirectory({ rootDir: resolve(process.cwd(), options.root), includeDrafts: Boolean(options.includeDrafts) });
      const result = checkDocumentationPages(scanned.pages);
      const check = await prisma.documentationCheck.create({ data: { collectionId: collection.id, checkType: 'cli_local_wiki', status: result.status, summary: result.summary as never, issues: result.issues as never, createdBy: 'cli' } });
      console.log(JSON.stringify({ check, ...result }, null, 2));
      if (result.status === 'failed') process.exitCode = 1;
    }));

  command.command('checks:list').option('--status <status>').option('--take <n>', 'limit', '100').action(async (options: { status?: string; take: string }) => withPrisma(async (prisma) => {
    const rows = await prisma.documentationCheck.findMany({ where: options.status ? { status: options.status } : {}, orderBy: { createdAt: 'desc' }, take: Math.min(Number(options.take), 1000), include: { collection: { select: { code: true, title: true } } } });
    console.log(JSON.stringify(rows, null, 2));
  }));

  command.command('snapshots:list').option('--collection <code>').option('--take <n>', 'limit', '100').action(async (options: { collection?: string; take: string }) => withPrisma(async (prisma) => {
    const rows = await prisma.documentationSnapshot.findMany({ where: options.collection ? { collection: { code: options.collection } } : {}, orderBy: { createdAt: 'desc' }, take: Math.min(Number(options.take), 500), include: { collection: { select: { code: true, title: true } } } });
    console.log(JSON.stringify(rows, null, 2));
  }));

  command.command('export')
    .option('--collection <code>', 'collection code', 'gitflic-wiki')
    .option('--root <path>', 'wiki root', 'wiki')
    .option('--format <format>', 'json_bundle | markdown_index', 'json_bundle')
    .action(async (options: { collection: string; root: string; format: string }) => withPrisma(async (prisma) => {
      const collection = await ensureCollection(prisma, options.collection, options.root);
      const scanned = await scanWikiDirectory({ rootDir: resolve(process.cwd(), options.root) });
      const bundle = buildDocumentationExportBundle({ collection: { code: collection.code, title: collection.title, locale: collection.locale, sourceType: collection.sourceType }, pages: scanned.pages });
      const content = options.format === 'markdown_index' ? bundleToMarkdownIndex(bundle) : JSON.stringify(bundle, null, 2);
      const row = await prisma.documentationExport.create({ data: { collectionId: collection.id, format: options.format, status: 'created', checksum: sha256Hex(content), sizeBytes: Buffer.byteLength(content), pageCount: scanned.pages.length, payload: { content } as never, createdBy: 'cli' } });
      console.log(JSON.stringify(row, null, 2));
    }));

  command.command('exports:list').option('--collection <code>').option('--take <n>', 'limit', '100').action(async (options: { collection?: string; take: string }) => withPrisma(async (prisma) => {
    const rows = await prisma.documentationExport.findMany({ where: options.collection ? { collection: { code: options.collection } } : {}, orderBy: { createdAt: 'desc' }, take: Math.min(Number(options.take), 500), include: { collection: { select: { code: true, title: true } } } });
    console.log(JSON.stringify(rows, null, 2));
  }));

  return command;
}

async function ensureCollection(prisma: PrismaService, code: string, sourcePath: string) {
  return prisma.documentationCollection.upsert({
    where: { code },
    create: { code, title: code === 'gitflic-wiki' ? 'GitFlic Wiki' : code, locale: 'ru', sourceType: 'local_wiki', sourcePath },
    update: { sourcePath, isActive: true },
  });
}

async function withPrisma<T>(callback: (prisma: PrismaService) => Promise<T>): Promise<T> {
  const prisma = new PrismaService();
  await prisma.connect();
  try { return await callback(prisma); } finally { await prisma.close(); }
}
