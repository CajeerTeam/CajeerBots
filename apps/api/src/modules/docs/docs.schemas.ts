import { z } from 'zod';

export const listDocCollectionsQuerySchema = z.object({
  code: z.string().optional(),
  locale: z.string().optional(),
  sourceType: z.string().optional(),
  take: z.coerce.number().int().min(1).max(500).default(100),
});

export const upsertDocCollectionSchema = z.object({
  code: z.string().min(1).max(120).regex(/^[a-z0-9][a-z0-9._-]*$/i),
  title: z.string().min(1).max(200),
  locale: z.string().min(2).max(12).default('ru'),
  sourceType: z.enum(['local_wiki', 'gitflic_wiki', 'manual']).default('local_wiki'),
  sourcePath: z.string().min(1).default('wiki'),
  sourceUrl: z.string().url().optional().nullable(),
  metadata: z.record(z.unknown()).optional(),
});

export const syncDocsSchema = z.object({
  rootDir: z.string().min(1).default('wiki'),
  includeDrafts: z.boolean().default(false),
  createSnapshot: z.boolean().default(true),
});

export const listPagesQuerySchema = z.object({
  collectionId: z.string().optional(),
  collectionCode: z.string().optional(),
  locale: z.string().optional(),
  status: z.string().optional(),
  tag: z.string().optional(),
  take: z.coerce.number().int().min(1).max(1000).default(200),
});

export const searchDocsQuerySchema = z.object({
  q: z.string().min(1),
  collectionCode: z.string().optional(),
  locale: z.string().optional(),
  take: z.coerce.number().int().min(1).max(100).default(20),
});

export const runDocsCheckSchema = z.object({
  collectionCode: z.string().default('gitflic-wiki'),
  rootDir: z.string().min(1).default('wiki'),
  includeDrafts: z.boolean().default(false),
});

export const listDocsChecksQuerySchema = z.object({
  collectionId: z.string().optional(),
  status: z.string().optional(),
  checkType: z.string().optional(),
  take: z.coerce.number().int().min(1).max(1000).default(200),
});

export const listDocsSnapshotsQuerySchema = z.object({
  collectionId: z.string().optional(),
  collectionCode: z.string().optional(),
  take: z.coerce.number().int().min(1).max(500).default(100),
});

export const createDocsExportSchema = z.object({
  collectionCode: z.string().default('gitflic-wiki'),
  rootDir: z.string().min(1).default('wiki'),
  format: z.enum(['json_bundle', 'markdown_index']).default('json_bundle'),
  includeBody: z.boolean().default(true),
});

export const listDocsExportsQuerySchema = z.object({
  collectionId: z.string().optional(),
  format: z.string().optional(),
  take: z.coerce.number().int().min(1).max(500).default(100),
});
