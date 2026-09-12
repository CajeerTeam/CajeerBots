import { createHash } from 'node:crypto';
import type { ScannedWikiPage } from './scanner.js';

export interface DocumentationExportBundle {
  format: 'cajeerbots.docs.bundle.v1';
  generatedAt: string;
  collection: { code: string; title: string; locale: string; sourceType: string };
  pages: Array<{
    slug: string;
    title: string;
    path: string;
    sha256: string;
    wordCount: number;
    headings: ScannedWikiPage['headings'];
    links: ScannedWikiPage['links'];
    body: string;
  }>;
  toc: Array<{ slug: string; title: string; path: string }>;
  checksum: string;
}

export function buildDocumentationExportBundle(input: { collection: { code: string; title: string; locale: string; sourceType: string }; pages: ScannedWikiPage[]; generatedAt?: Date }): DocumentationExportBundle {
  const pages = input.pages.map((page) => ({ slug: page.slug, title: page.title, path: page.path, sha256: page.sha256, wordCount: page.wordCount, headings: page.headings, links: page.links, body: page.body }));
  const generatedAt = (input.generatedAt ?? new Date()).toISOString();
  const checksumSource = JSON.stringify({ collection: input.collection, pages: pages.map((page) => ({ slug: page.slug, sha256: page.sha256 })) });
  return {
    format: 'cajeerbots.docs.bundle.v1',
    generatedAt,
    collection: input.collection,
    pages,
    toc: pages.map((page) => ({ slug: page.slug, title: page.title, path: page.path })),
    checksum: createHash('sha256').update(checksumSource).digest('hex'),
  };
}

export function bundleToMarkdownIndex(bundle: DocumentationExportBundle): string {
  const lines = [`# ${bundle.collection.title}`, '', `Формат: ${bundle.format}`, `Сгенерировано: ${bundle.generatedAt}`, `Checksum: ${bundle.checksum}`, '', '## Страницы', ''];
  for (const page of bundle.pages) lines.push(`- [${page.title}](${page.path}) — ${page.wordCount} слов`);
  return `${lines.join('\n')}\n`;
}
