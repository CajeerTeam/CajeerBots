import { createHash } from 'node:crypto';

export interface ParsedMarkdownPage {
  title: string;
  slug: string;
  frontmatter: Record<string, string | boolean | number>;
  body: string;
  excerpt: string;
  headings: Array<{ level: number; title: string; anchor: string }>;
  links: Array<{ label: string; href: string; type: 'internal' | 'external' | 'anchor' | 'asset' }>;
  wordCount: number;
  sha256: string;
}

export function parseMarkdownPage(input: { path: string; content: string; slug?: string }): ParsedMarkdownPage {
  const { frontmatter, body } = splitFrontmatter(input.content);
  const headings = extractHeadings(body);
  const title = String(frontmatter.title ?? headings[0]?.title ?? titleFromPath(input.path));
  const slug = normalizeSlug(input.slug ?? frontmatter.slug?.toString() ?? input.path.replace(/\\.md$/i, ''));
  const links = extractLinks(body);
  const plain = body
    .replace(/```[\s\S]*?```/g, ' ')
    .replace(/`[^`]+`/g, ' ')
    .replace(/!\[[^\]]*\]\([^)]*\)/g, ' ')
    .replace(/\[[^\]]*\]\([^)]*\)/g, ' ')
    .replace(/[#>*_~\-|]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  return {
    title,
    slug,
    frontmatter,
    body,
    excerpt: plain.slice(0, 240),
    headings,
    links,
    wordCount: plain ? plain.split(/\s+/).length : 0,
    sha256: sha256Hex(input.content),
  };
}

export function splitFrontmatter(content: string): { frontmatter: Record<string, string | boolean | number>; body: string } {
  if (!content.startsWith('---\n')) return { frontmatter: {}, body: content };
  const end = content.indexOf('\n---', 4);
  if (end === -1) return { frontmatter: {}, body: content };
  const raw = content.slice(4, end).trim();
  const frontmatter: Record<string, string | boolean | number> = {};
  for (const line of raw.split(/\r?\n/)) {
    const separator = line.indexOf(':');
    if (separator === -1) continue;
    const key = line.slice(0, separator).trim();
    const value = parseScalar(line.slice(separator + 1).trim());
    if (key) frontmatter[key] = value;
  }
  return { frontmatter, body: content.slice(end + 4).replace(/^\r?\n/, '') };
}

export function extractHeadings(content: string): Array<{ level: number; title: string; anchor: string }> {
  const headings: Array<{ level: number; title: string; anchor: string }> = [];
  for (const line of content.split(/\r?\n/)) {
    const match = /^(#{1,6})\s+(.+?)\s*$/.exec(line);
    if (!match) continue;
    const title = match[2]!.replace(/#+$/, '').trim();
    headings.push({ level: match[1]!.length, title, anchor: slugifyAnchor(title) });
  }
  return headings;
}

export function extractLinks(content: string): Array<{ label: string; href: string; type: 'internal' | 'external' | 'anchor' | 'asset' }> {
  const links: Array<{ label: string; href: string; type: 'internal' | 'external' | 'anchor' | 'asset' }> = [];
  const regex = /!?\[([^\]]*)\]\(([^)\s]+)(?:\s+"[^"]*")?\)/g;
  let match: RegExpExecArray | null;
  while ((match = regex.exec(content))) {
    const href = match[2]!.trim();
    links.push({ label: match[1]!.trim(), href, type: classifyHref(href) });
  }
  return links;
}

export function normalizeSlug(value: string): string {
  return value
    .replace(/\\/g, '/')
    .replace(/^\.?\/?/, '')
    .replace(/\.md$/i, '')
    .replace(/\/index$/i, '')
    .replace(/\s+/g, '-')
    .replace(/[^\p{Letter}\p{Number}/_-]+/gu, '')
    .replace(/\/+/g, '/')
    .toLowerCase();
}

export function slugifyAnchor(value: string): string {
  return value
    .toLowerCase()
    .replace(/`([^`]+)`/g, '$1')
    .replace(/[^\p{Letter}\p{Number}\s_-]+/gu, '')
    .trim()
    .replace(/\s+/g, '-');
}

export function sha256Hex(value: string | Buffer): string {
  return createHash('sha256').update(value).digest('hex');
}

function classifyHref(href: string): 'internal' | 'external' | 'anchor' | 'asset' {
  if (href.startsWith('#')) return 'anchor';
  if (/^https?:\/\//i.test(href) || href.startsWith('mailto:')) return 'external';
  if (/\.(png|jpe?g|webp|gif|svg|pdf|zip|tar\.gz)$/i.test(href)) return 'asset';
  return 'internal';
}

function parseScalar(value: string): string | boolean | number {
  const unquoted = value.replace(/^['"]|['"]$/g, '');
  if (unquoted === 'true') return true;
  if (unquoted === 'false') return false;
  if (/^-?\d+(\.\d+)?$/.test(unquoted)) return Number(unquoted);
  return unquoted;
}

function titleFromPath(path: string): string {
  const filename = path.split(/[\\/]/).pop() ?? path;
  return filename.replace(/\.md$/i, '').replace(/^\d+[-_]/, '').replace(/[-_]+/g, ' ');
}
