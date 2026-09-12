import type { ScannedWikiPage } from './scanner.js';

export interface DocumentationSearchResult {
  slug: string;
  title: string;
  path: string;
  excerpt: string;
  score: number;
  matches: string[];
}

export function searchDocumentationPages(pages: ScannedWikiPage[], query: string, limit = 20): DocumentationSearchResult[] {
  const terms = tokenize(query);
  if (terms.length === 0) return [];
  const results = pages.map((page) => scorePage(page, terms)).filter((result) => result.score > 0);
  results.sort((a, b) => b.score - a.score || a.title.localeCompare(b.title, 'ru'));
  return results.slice(0, limit);
}

function scorePage(page: ScannedWikiPage, terms: string[]): DocumentationSearchResult {
  const title = page.title.toLowerCase();
  const body = page.body.toLowerCase();
  const slug = page.slug.toLowerCase();
  let score = 0;
  const matches: string[] = [];
  for (const term of terms) {
    if (title.includes(term)) { score += 10; matches.push(`title:${term}`); }
    if (slug.includes(term)) { score += 4; matches.push(`slug:${term}`); }
    const occurrences = body.split(term).length - 1;
    if (occurrences > 0) { score += Math.min(occurrences, 10); matches.push(`body:${term}:${occurrences}`); }
  }
  return { slug: page.slug, title: page.title, path: page.path, excerpt: buildExcerpt(page.body, terms), score, matches };
}

function tokenize(query: string): string[] {
  return query.toLowerCase().split(/[\s,.;:!?]+/).map((term) => term.trim()).filter((term) => term.length >= 2);
}

function buildExcerpt(body: string, terms: string[]): string {
  const normalized = body.replace(/\s+/g, ' ').trim();
  const lower = normalized.toLowerCase();
  const first = terms.map((term) => lower.indexOf(term)).filter((idx) => idx >= 0).sort((a, b) => a - b)[0] ?? 0;
  const start = Math.max(0, first - 80);
  return normalized.slice(start, start + 220);
}
