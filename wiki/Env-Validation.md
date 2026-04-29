# Проверка переменных окружения

## Обязательные переменные для боевого запуска

| Переменная | Обязательность | Секрет | Назначение |
|---|---:|---:|---|
| `DATABASE_URL` | да | да | подключение к PostgreSQL |
| `EVENT_SIGNING_SECRET` | да | да | подпись внутренних событий |
| `API_TOKEN` | да | да | доступ к административному API |

## Переменные адаптеров

| Переменная | Когда нужна | Назначение |
|---|---|---|
| `TELEGRAM_BOT_TOKEN` | если `TELEGRAM_ENABLED=true` | токен Telegram-бота |
| `DISCORD_TOKEN` | если `DISCORD_ENABLED=true` | токен Discord-бота |
| `VK_GROUP_TOKEN` | если `VKONTAKTE_ENABLED=true` | токен группы ВКонтакте |

## Демонстрационные значения

Значения вида `change-me`, `change-me-admin-token` и `change-me-long-random-secret` допустимы только для локального каркаса. `doctor` считает их проблемой.

## Безопасная проверка

```bash
cajeer-bots doctor --offline
```

Проверка без `--offline` дополнительно обращается к PostgreSQL и проверяет токены включённых адаптеров.
