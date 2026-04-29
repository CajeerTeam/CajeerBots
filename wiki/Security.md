# Безопасность

Ключевые требования: секреты только через окружение, подпись событий, минимальные права доступа, аудит действий.

## Scoped API tokens

Помимо env-токенов поддерживается file-backed registry `runtime/secrets/api_tokens.json`, где хранятся только SHA-256 хэши токенов.

```bash
cajeer-bots tokens create --id admin-2026-04 --scope system.admin --prefix cb_admin_
cajeer-bots tokens revoke admin-2026-04
cajeer-bots tokens list
```
