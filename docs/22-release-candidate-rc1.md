# 1.0.0-rc.1: Release Candidate workflow

`1.0.0-rc.1` переводит CajeerBots из beta-стабилизации в release-candidate. Главная задача версии — зафиксировать публичные контракты и дать воспроизводимый механизм проверки готовности.

## Что фиксируется

- REST API и OpenAPI 3.1.1;
- TypeScript SDK surface;
- Extension manifest / hooks / permission scopes;
- Compatibility targets для runtime и deployment;
- Upgrade path с `0.18.0-beta.3` на `1.0.0-rc.1`.

## CLI

```bash
cajeer rc summary
cajeer rc api-freeze:create --version 1.0.0-rc.1
cajeer rc api-freeze:verify --version 1.0.0-rc.1
cajeer rc compatibility:defaults
cajeer rc compatibility:matrix --version 1.0.0-rc.1
cajeer rc upgrade-plan:create --from 0.18.0-beta.3 --to 1.0.0-rc.1 --has-backup
cajeer rc upgrade-plan:run <planId> --mode dry-run
cajeer rc readiness:run --version 1.0.0-rc.1
```

## API

Все endpoints находятся под `/api/release-candidate/*` и требуют scopes `release_candidate:*`.

Основные группы:

- `/summary` — состояние RC;
- `/api-freeze/*` — snapshot и verification OpenAPI;
- `/compatibility/*` — compatibility targets/matrix;
- `/upgrade-plans/*` — upgrade plan и rehearsal;
- `/readiness-runs` — итоговая RC readiness проверка.

## Что считается blocker

- отсутствует API freeze snapshot;
- удалена операция REST API после freeze;
- изменён `operationId`;
- есть duplicate `operationId`;
- required compatibility target имеет статус `blocked`;
- открыты release/security blockers;
- последний integration smoke не прошёл.

## Scheduled maintenance

Seed создаёт job `maintenance.rc_readiness`. Он собирает compatibility matrix и создаёт readiness run по расписанию.
