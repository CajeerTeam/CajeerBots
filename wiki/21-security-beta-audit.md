# Security Beta Audit

`1.0.0-rc.1` добавляет рабочий security-beta слой для проверки CajeerBots перед RC.

## Что проверяется

- активные admin accounts без подтверждённой TOTP 2FA;
- устаревшие refresh sessions;
- истёкшие, но не отозванные API keys;
- API keys без scopes;
- secrets, требующие ротации;
- активные webhook endpoints без `secretRef`;
- превышение beta-порога rate-limit violations;
- открытые release/security blockers.

## API

Основные endpoints:

- `GET /api/security/audit/summary`
- `POST /api/security/audit/defaults`
- `GET /api/security/audit/policies`
- `PUT /api/security/audit/policies`
- `POST /api/security/audit/runs`
- `GET /api/security/audit/runs`
- `GET /api/security/audit/runs/:id`
- `GET /api/security/audit/findings`
- `PUT /api/security/audit/findings/:id`
- `GET /api/security/risk-snapshots`

## CLI

```bash
cajeer security summary
cajeer security audit:defaults
cajeer security audit:run --version 1.0.0-rc.1
cajeer security policies:list
cajeer security findings:list --status open
cajeer security findings:resolve <id> --status fixed
cajeer security sessions:revoke-stale --older-than-days 30 --dry-run
```

## Scheduler

Seed создаёт job `maintenance.security_audit` с ежедневным запуском. Handler создаёт audit run, findings и risk snapshot.

## RC gate

Перед `1.0.0-rc.1` открытые `critical`/`error` findings должны быть исправлены или явно приняты как риск через `accepted`.
