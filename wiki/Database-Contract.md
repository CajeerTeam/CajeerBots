# Database Contract

Cajeer Bots использует PostgreSQL как общий эксплуатационный слой. Runtime **не выполняет DDL автоматически**. Проект поставляет reference Alembic migrations, а оператор применяет их явно через Alembic.

Текущая версия DB contract:

```text
cajeer.bots.db.v1
```

Обязательная схема по умолчанию:

```text
shared
```

Обязательные таблицы:

```text
shared.platform_schema
shared.event_bus
shared.delivery_queue
shared.dead_letters
shared.idempotency_keys
shared.audit_log
shared.adapter_state
```

## platform_schema

Хранит версии эксплуатационных контрактов.

```text
component
version
updated_at
```

Для Cajeer Bots должна быть запись:

```text
component = cajeer-bots-db
version   = cajeer.bots.db.v1
```

## event_bus

```text
event_id
trace_id
source
event_type
payload
status
created_at
locked_at
delivered_at
```

## delivery_queue

```text
delivery_id
adapter
target
payload
status
attempts
max_attempts
trace_id
created_at
locked_at
sent_at
last_error
```

## dead_letters

```text
dead_letter_id
event_id
trace_id
payload
reason
created_at
retried_at
```

## idempotency_keys

```text
key
created_at
expires_at
```

## audit_log

```text
audit_id
actor_type
actor_id
action
resource
result
trace_id
ip
user_agent
message
created_at
```

## adapter_state

```text
adapter
instance_id
state
last_error
updated_at
```

## Проверка

```bash
cajeer-bots db contract
cajeer-bots db check
```

## Runtime persistence 0.10.0

Delivery, dead letters, idempotency и audit имеют memory/redis/postgres backend-и. PostgreSQL-реализации используют SQLAlchemy 2.x async и таблицы `delivery_queue`, `dead_letters`, `idempotency_keys`, `audit_log` из Alembic-контракта.

Для `EVENT_BUS_BACKEND=postgres` требуется `DATABASE_ASYNC_URL`; sync `psycopg` больше не используется в event bus runtime path.

## Дополнительные production-таблицы 0.10.x

Для identity/RBAC/support/moderation/announcements/scheduler foundation добавлены контракты:

```text
users
platform_accounts
user_profiles
workspace_links
roles
role_permissions
user_roles
support_tickets
moderation_actions
announcements
scheduled_jobs
```

`platform_accounts` связывает `telegram/discord/vkontakte` идентификаторы с внутренним `user_id`. `workspace_links` хранит связь с Cajeer Workspace. `scheduled_jobs` используется для отложенных задач: heartbeat, logs flush, update checks, cleanup и scheduled announcements.
