# Docs / GitFlic Wiki Operations

CajeerBots `0.17.0` добавляет рабочий слой управления документацией без внешней панели. Он нужен, чтобы русскоязычная GitFlic Wiki не была отдельным неуправляемым набором Markdown-файлов, а проходила синхронизацию, проверку, поиск и экспорт через API/CLI/worker.

## Что хранится в БД

- `documentation_collections` — источники документации: локальная wiki, GitFlic Wiki, manual collection.
- `documentation_pages` — страницы с body, excerpt, headings, links, frontmatter, sha256, статусом и метаданными источника.
- `documentation_snapshots` — слепки состояния wiki после sync.
- `documentation_checks` — результаты проверки качества документации.
- `documentation_exports` — JSON/Markdown export bundles.

## CLI

```bash
pnpm cajeer docs collections:set --code gitflic-wiki --title "GitFlic Wiki" --source-path wiki
pnpm cajeer docs sync --collection gitflic-wiki --root wiki
pnpm cajeer docs check --collection gitflic-wiki --root wiki
pnpm cajeer docs search "registry" --collection gitflic-wiki
pnpm cajeer docs export --collection gitflic-wiki --root wiki --format json_bundle
```

## API

Основные endpoints:

- `GET /api/docs/collections`
- `PUT /api/docs/collections`
- `POST /api/docs/collections/:collectionId/sync`
- `GET /api/docs/pages`
- `GET /api/docs/pages/:idOrSlug`
- `GET /api/docs/search?q=...`
- `POST /api/docs/checks/run`
- `GET /api/docs/checks`
- `GET /api/docs/snapshots`
- `POST /api/docs/exports`
- `GET /api/docs/exports`

## Scheduled maintenance

Seed добавляет две встроенные задачи:

- `maintenance.docs_sync` — ежедневная синхронизация `wiki/*.md` в БД.
- `maintenance.docs_check` — ежедневная проверка внутренних ссылок и anchors.

## Проверки

Documentation checker находит:

- битые внутренние ссылки;
- битые anchors внутри текущей или целевой страницы;
- страницы без заголовка;
- слишком короткие страницы;
- summary по количеству страниц, слов, warnings/errors.

## Export format

JSON bundle имеет формат `cajeerbots.docs.bundle.v1`. Он содержит collection metadata, страницы, TOC и checksum. Это можно использовать для release artifacts, офлайн-документации или публикации в GitFlic Wiki.
