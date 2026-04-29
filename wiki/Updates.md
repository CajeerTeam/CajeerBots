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

## Безопасный контур обновления

Production-обновление выполняется через GitHub Releases, release manifest и staged install. `git pull` допускается только для development/repo-root режима.

Команды:

```bash
cajeer-bots update check
cajeer-bots update download
cajeer-bots update stage-latest
cajeer-bots update apply --version latest
cajeer-bots update rollback
cajeer-bots update history
```

Updater использует `runtime/updates/update.lock`, чтобы запретить параллельные обновления. Для production можно включить systemd-менеджер:

```env
CAJEER_UPDATE_SERVICE_MANAGER=systemd
CAJEER_UPDATE_SERVICES=cajeer-bots-api,cajeer-bots-bridge,cajeer-bots-telegram
CAJEER_UPDATE_REQUIRE_SIGNATURE=true
```

Rollback проверяет service health gate. Если проверка не прошла, запись истории получает `result=error`.

## Улучшения update lifecycle

Update subsystem поддерживает безопасный цикл `check -> plan -> download -> verify -> stage -> apply -> healthcheck -> rollback`.

### План обновления

```bash
cajeer-bots update plan --version latest
```

План содержит текущую и целевую версии, канал, artifact, sha256, признаки миграций, версии контрактов, список сервисов для рестарта, preflight checks и наличие rollback target.

### Безопасное извлечение artifact

`stage_local_artifact` запрещает absolute paths, `..` и небезопасные symlink/hardlink entries. После распаковки staging root нормализуется: если tar.gz содержит единственный каталог `CajeerBots-*`, updater применяет именно его как корень релиза.

### Подпись релиза

Для stable-контуров включите:

```env
CAJEER_UPDATE_REQUIRE_SIGNATURE=true
CAJEER_UPDATE_PUBLIC_KEY=runtime/secrets/release-public.pem
```

Проверка выполняется через `openssl dgst -sha256 -verify` для файла `CajeerBots-*.tar.gz.sig`.

### Migration gate

```env
CAJEER_UPDATE_AUTO_MIGRATE=false
CAJEER_UPDATE_BLOCK_ON_REQUIRED_MIGRATION=true
```

Если `release.json` содержит `requires_migration=true`, применение блокируется до явного запуска миграций оператором или включения `CAJEER_UPDATE_AUTO_MIGRATE=true`.

### Workspace UI contract

Для Cajeer Workspace доступны:

```text
GET  /updates/status
GET  /updates/plan
POST /updates/check
POST /updates/plan
POST /updates/apply
POST /updates/rollback
GET  /updates/history
```

`POST /updates/apply {"version":"latest","auto_stage":true}` выполняет download, verify, stage и apply одной операцией с audit/update events.
