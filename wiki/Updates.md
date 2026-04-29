# Обновления Cajeer Bots

Production-обновления выполняются из GitHub Releases, а не через `git pull`.

## CLI

```bash
cajeer-bots update status
cajeer-bots update check
cajeer-bots update stage ./dist/CajeerBots-0.10.1.tar.gz --manifest ./dist/CajeerBots-0.10.1.release.json
cajeer-bots update apply --version 0.10.1 --staged-path runtime/updates/staging/CajeerBots-0.10.1
cajeer-bots update rollback
cajeer-bots update history
```

## API

```text
GET  /updates/status
GET  /updates/history
POST /updates/check
POST /updates/apply
POST /updates/rollback
```

Все write-маршруты требуют admin token.

## Staged layout

```text
runtime/updates/
├── current -> releases/<version>
├── previous -> releases/<version>
├── releases/
├── staging/
└── history.jsonl
```

## Правила безопасности

- основной production-источник — GitHub Releases;
- release artifact проверяется по SHA256;
- preflight запускает `doctor --offline` и проверяет версии контрактов;
- rollback переключает symlink `current` на `previous`;
- автоматический downgrade БД не выполняется.
