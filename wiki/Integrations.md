# Интеграции

## Cajeer Workspace

Интеграция отправляет heartbeat и события жизненного цикла сервиса.

```env
CAJEER_WORKSPACE_ENABLED=true
CAJEER_WORKSPACE_URL=https://workspace.cajeer.ru/api/v1
CAJEER_WORKSPACE_TOKEN=...
CAJEER_WORKSPACE_PROJECT_ID=...
CAJEER_WORKSPACE_TEAM_ID=...
CAJEER_WORKSPACE_SERVICE_ID=...
```

## Cajeer Logs

Интеграция использует ingest API Cajeer Logs:

```env
REMOTE_LOGS_ENABLED=true
REMOTE_LOGS_URL=https://logs.example.com/api/v1/ingest
REMOTE_LOGS_TOKEN=clog_...
REMOTE_LOGS_PROJECT=CajeerBots
REMOTE_LOGS_BOT=CajeerBots
REMOTE_LOGS_SIGN_REQUESTS=true
```

При `REMOTE_LOGS_SIGN_REQUESTS=true` отправляются заголовки `X-Log-Timestamp`, `X-Log-Nonce`, `X-Log-Signature`.
