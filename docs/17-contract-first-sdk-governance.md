# Contract-first API Governance и SDK Sync

CajeerBots 0.16.0 добавляет runtime-слой контроля REST API и TypeScript SDK.

## Что хранится в БД

- `api_contract_snapshots` — снимки OpenAPI: версия, sha256, операции, теги, unsecured/deprecated операции.
- `api_contract_checks` — результаты проверок: breaking changes, OpenAPI issues, SDK coverage.
- `sdk_generation_runs` — история генерации SDK artifacts.
- `api_deprecations` — политика устаревания operationId с replacement/removeAfter.

## CLI

```bash
cajeer contracts snapshot:create --baseline
cajeer contracts snapshots:list
cajeer contracts check --min-sdk-coverage 50
cajeer contracts checks:list
cajeer contracts sdk:generate --write
cajeer contracts deprecations:set --operation-id oldMethod --method GET --path /old --status deprecated --replacement newMethod
```

## API

- `GET /api/contracts/snapshots`
- `POST /api/contracts/snapshots`
- `POST /api/contracts/checks/run`
- `GET /api/contracts/checks`
- `POST /api/contracts/sdk/generate`
- `GET /api/contracts/sdk/runs`
- `GET /api/contracts/deprecations`
- `PUT /api/contracts/deprecations`

## Scheduled job

Seed создаёт задачу `maintenance.contracts_check`. Она регулярно читает OpenAPI, создаёт snapshot, сравнивает его с baseline и фиксирует SDK coverage.
