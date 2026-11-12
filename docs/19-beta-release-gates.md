# Beta release gates и feature-freeze

`0.18.0-beta.1` переводит CajeerBots из alpha-линейки в beta feature-freeze. С этого момента крупные новые подсистемы не добавляются без отдельного release decision; основной фокус — багфиксы, тесты, документация, безопасность и подготовка RC.

## Что проверяет слой release gates

- наличие обязательных product/release файлов;
- синхронизацию версии в `VERSION`, `package.json`, workspace packages и OpenAPI;
- наличие Prisma migration и seed permissions для текущей версии;
- отсутствие blocker-маркеров в runtime-коде;
- parseability OpenAPI 3.1.1;
- наличие и базовую проверку GitFlic Wiki документации;
- наличие production deployment artifacts.

## CLI

```bash
cajeer release-gates defaults:seed
cajeer release-gates freeze --version 0.18.0-beta.1 --channel beta
cajeer release-gates run --version 0.18.0-beta.1 --channel beta
cajeer release-gates blockers:list --status open
cajeer release-gates report:create --version 0.18.0-beta.1 --out var/release-gate-report.md
```

## API

Базовый namespace: `/api/release-gates`.

- `GET /summary`
- `GET/PUT /freezes`
- `GET/PUT /quality-gates`
- `POST /quality-gates/defaults`
- `GET/POST /runs`
- `GET /runs/:id`
- `GET /blockers`
- `PUT /blockers/:id`
- `GET/POST /reports`

## Scheduled maintenance

Seed добавляет job `maintenance.release_gates`. Он запускает gates по расписанию и сохраняет результаты в `release_gate_runs` и `release_blockers`.
