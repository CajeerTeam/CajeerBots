import { readdir, readFile, stat } from 'node:fs/promises';
import { join, relative } from 'node:path';
import { normalizeSlug, parseMarkdownPage, type ParsedMarkdownPage } from './markdown.js';

export interface WikiScanOptions {
  rootDir: string;
  includeDrafts?: boolean;
}

export interface ScannedWikiPage extends ParsedMarkdownPage {
  path: string;
  sourcePath: string;
  sizeBytes: number;
  mtime: Date;
}

export interface WikiScanResult {
  rootDir: string;
  pages: ScannedWikiPage[];
  totalBytes: number;
  scannedAt: Date;
}

export async function scanWikiDirectory(options: WikiScanOptions): Promise<WikiScanResult> {
  const files = await listMarkdownFiles(options.rootDir);
  const pages: ScannedWikiPage[] = [];
  let totalBytes = 0;
  for (const file of files) {
    const content = await readFile(file, 'utf8');
    const info = await stat(file);
    totalBytes += info.size;
    const path = relative(options.rootDir, file).replace(/\\/g, '/');
    const parsed = parseMarkdownPage({ path, content, slug: normalizeSlug(path) });
    if (!options.includeDrafts && parsed.frontmatter.draft === true) continue;
    pages.push({ ...parsed, path, sourcePath: file, sizeBytes: info.size, mtime: info.mtime });
  }
  pages.sort((a, b) => a.path.localeCompare(b.path, 'ru'));
  return { rootDir: options.rootDir, pages, totalBytes, scannedAt: new Date() };
}

export function buildWikiToc(pages: ScannedWikiPage[]): Array<{ slug: string; title: string; path: string; depth: number; headings: number }> {
  return pages.map((page) => ({ slug: page.slug, title: page.title, path: page.path, depth: page.slug.split('/').length, headings: page.headings.length }));
}

async function listMarkdownFiles(rootDir: string): Promise<string[]> {
  const files: string[] = [];
  async function walk(dir: string): Promise<void> {
    const entries = await readdir(dir, { withFileTypes: true });
    for (const entry of entries) {
      const path = join(dir, entry.name);
      if (entry.isDirectory()) {
        if (entry.name === 'node_modules' || entry.name === '.git' || entry.name === 'dist') continue;
        await walk(path);
      } else if (entry.isFile() && entry.name.endsWith('.md')) {
        files.push(path);
      }
    }
  }
  await walk(rootDir);
  return files;
}
