# Проверка переменных окружения

`core.config.Settings` проверяет типы и допустимые значения основных переменных окружения до запуска runtime.

| Переменная | Проверка |
|---|---|
| `API_PORT` | целое число от 1 до 65535 |
| `TELEGRAM_MODE` | `polling` или `webhook` |
| `CAJEER_BOTS_MODE` | `all`, `telegram`, `discord`, `vkontakte`, `worker`, `api`, `bridge` |
| `DATABASE_SSLMODE` | `disable`, `allow`, `prefer`, `require`, `verify-ca`, `verify-full` |
| `EVENT_BUS_BACKEND` | `memory`, `postgres`, `redis` |
| `CAJEER_BOTS_LOG_LEVEL` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |

Секреты `EVENT_SIGNING_SECRET` и `API_TOKEN` не должны использовать демонстрационные значения `change-me*` в боевом окружении. Это проверяется командой:

```bash
cajeer-bots doctor --offline
```
