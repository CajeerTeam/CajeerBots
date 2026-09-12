import { normalizeSlug, slugifyAnchor } from './markdown.js';
import type { ScannedWikiPage } from './scanner.js';

export interface DocumentationIssue {
  severity: 'info' | 'warning' | 'error';
  code: string;
  pageSlug?: string;
  message: string;
  details?: Record<string, unknown>;
}

export interface DocumentationCheckResult {
  status: 'passed' | 'warning' | 'failed';
  summary: {
    pages: number;
    errors: number;
    warnings: number;
    infos: number;
    internalLinks: number;
    externalLinks: number;
    words: number;
  };
  issues: DocumentationIssue[];
}

export function checkDocumentationPages(pages: ScannedWikiPage[]): DocumentationCheckResult {
  const issues: DocumentationIssue[] = [];
  const bySlug = new Map(pages.map((page) => [page.slug, page]));
  const byPath = new Map(pages.map((page) => [page.path.replace(/\.md$/i, ''), page]));
  let internalLinks = 0;
  let externalLinks = 0;
  let words = 0;

  for (const page of pages) {
    words += page.wordCount;
    if (!page.title || page.title.length < 3) issues.push({ severity: 'warning', code: 'missing_title', pageSlug: page.slug, message: 'Страница без читаемого заголовка.' });
    if (page.wordCount < 20) issues.push({ severity: 'info', code: 'short_page', pageSlug: page.slug, message: 'Страница слишком короткая для пользовательской документации.', details: { wordCount: page.wordCount } });
    const anchors = new Set(page.headings.map((heading) => heading.anchor));
    for (const link of page.links) {
      if (link.type === 'external') {
        externalLinks += 1;
        continue;
      }
      if (link.type === 'anchor') {
        internalLinks += 1;
        const anchor = slugifyAnchor(link.href.slice(1));
        if (anchor && !anchors.has(anchor)) issues.push({ severity: 'warning', code: 'broken_anchor', pageSlug: page.slug, message: `Якорь ${link.href} не найден на странице.`, details: { href: link.href } });
        continue;
      }
      if (link.type !== 'internal') continue;
      internalLinks += 1;
      const [rawTarget, rawAnchor] = link.href.split('#');
      const targetSlug = normalizeSlug(resolveRelative(page.slug, rawTarget));
      const target = bySlug.get(targetSlug) ?? byPath.get(targetSlug);
      if (!target) {
        issues.push({ severity: 'error', code: 'broken_internal_link', pageSlug: page.slug, message: `Внутренняя ссылка не найдена: ${link.href}`, details: { href: link.href, targetSlug } });
        continue;
      }
      if (rawAnchor) {
        const anchorsOnTarget = new Set(target.headings.map((heading) => heading.anchor));
        const expected = slugifyAnchor(rawAnchor);
        if (!anchorsOnTarget.has(expected)) issues.push({ severity: 'warning', code: 'broken_target_anchor', pageSlug: page.slug, message: `Якорь ${rawAnchor} не найден в ${target.slug}.`, details: { href: link.href, targetSlug: target.slug } });
      }
    }
  }

  const errors = issues.filter((issue) => issue.severity === 'error').length;
  const warnings = issues.filter((issue) => issue.severity === 'warning').length;
  const infos = issues.filter((issue) => issue.severity === 'info').length;
  return {
    status: errors > 0 ? 'failed' : warnings > 0 ? 'warning' : 'passed',
    summary: { pages: pages.length, errors, warnings, infos, internalLinks, externalLinks, words },
    issues,
  };
}

function resolveRelative(fromSlug: string, href: string): string {
  if (!href || href.startsWith('/')) return href.replace(/^\/+/, '');
  if (!href.startsWith('.')) return href;
  const base = fromSlug.split('/').slice(0, -1);
  for (const part of href.split('/')) {
    if (!part || part === '.') continue;
    if (part === '..') base.pop();
    else base.push(part);
  }
  return base.join('/');
}
