# Проверка `.env`

## Основной local mode

| Переменная | Обязательность | Описание |
|---|---:|---|
| `CAJEER_BOTS_MODE` | да | `local` или `distributed`. По умолчанию `local`. |
| `CAJEER_BOTS_DEFAULT_TARGET` | нет | Цель запуска по умолчанию: `all`, `telegram`, `discord`, `vkontakte`, `api`, `worker`, `bridge`. |
| `EVENT_SIGNING_SECRET` | да | Секрет подписи событий. Не должен быть демонстрационным. |
| `API_TOKEN` | да для API | Административный API-токен. |
| `API_TOKEN_READONLY` | нет | Read-only API-токен. |
| `API_TOKEN_METRICS` | нет | Токен только для `/metrics`. |
| `METRICS_PUBLIC` | нет | Делает `/metrics` публичным при значении `true`. |
| `EVENT_BUS_BACKEND` | нет | `memory`, `redis` или `postgres`. |
| `REDIS_URL` | если backend `redis` | URL Redis. |
| `DATABASE_URL` | если backend `postgres` или online doctor | URL PostgreSQL. |

## Distributed mode

Эти переменные нужны только если включён distributed mode:

| Переменная | Описание |
|---|---|
| `DISTRIBUTED_ENABLED` | Включает распределённый режим. |
| `DISTRIBUTED_ROLE` | `server`, `agent`, `gateway`, `worker`. |
| `CORE_SERVER_URL` | Адрес Core Server для роли `agent`. |
| `NODE_ID` | Идентификатор runtime-ноды. |
| `NODE_SECRET` | Секрет runtime-ноды. |
| `DISTRIBUTED_TRANSPORT` | `http`, `websocket`, `grpc`, `broker`. |
