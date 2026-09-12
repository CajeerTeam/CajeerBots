import { describe, expect, it } from 'vitest';
import { checkDocumentationPages } from './checker.js';
import { parseMarkdownPage } from './markdown.js';

it('parses frontmatter, headings and internal links', () => {
  const page = parseMarkdownPage({ path: '01-test.md', content: '---\ntitle: Тест\n---\n# Тест\n\n[Внутренняя](02-next.md)\n' });
  expect(page.title).toBe('Тест');
  expect(page.links[0]?.type).toBe('internal');
  expect(page.headings[0]?.anchor).toBe('тест');
});

describe('checkDocumentationPages', () => {
  it('reports broken internal links', () => {
    const page = parseMarkdownPage({ path: '01-test.md', content: '# Тестовая страница\n\n[Битая](missing.md)\nКонтент страницы с достаточным количеством слов для проверки документации и поиска.' });
    const result = checkDocumentationPages([{ ...page, path: '01-test.md', sourcePath: 'wiki/01-test.md', sizeBytes: 1, mtime: new Date() }]);
    expect(result.status).toBe('failed');
    expect(result.issues.some((issue) => issue.code === 'broken_internal_link')).toBe(true);
  });
});
