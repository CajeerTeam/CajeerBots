# Registry & Updates

Версия `0.10.0` добавляет рабочий слой registry/store и планирования обновлений.

## Что хранится в БД

- `registry_sources` — источники registry manifest, включая GitFlic Registry/Releases URL.
- `registry_packages` — modules/plugins/core/sdk packages.
- `registry_package_versions` — версии, artifact URL, sha256, signature, channel, release notes и embedded manifest.
- `update_plans` — рассчитанные планы обновления.
- `update_history` — история применения планов.

## CLI

```bash
cajeer registry sources:add --name local --url ./registry/local.registry.json
cajeer registry sources:sync local
cajeer registry packages:list
cajeer registry packages:install @cajeerbots/example-extension --auto-grant

cajeer update check --package @cajeerbots/example-extension --current-version 0.1.0
cajeer update plans:list
cajeer update apply <planId> --dry-run
cajeer update history:list
```

## API

- `GET /api/registry/sources`
- `POST /api/registry/sources`
- `POST /api/registry/sources/:id/sync`
- `GET /api/registry/packages`
- `GET /api/registry/packages/:name`
- `POST /api/registry/packages/:name/install`
- `POST /api/updates/plans/core`
- `POST /api/updates/plans/extensions/:id`
- `POST /api/updates/plans/:id/apply`
- `GET /api/updates/history`

Core self-update через API не переписывает исходный код процесса. API фиксирует проверенный plan/history, а фактическое применение core artifact должно делать CLI/CI/Docker job на хосте.
