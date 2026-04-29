# Контракт базы данных

Этот документ описывает ожидаемую модель PostgreSQL без встроенных миграций.

## Принципы

1. Одна база данных обслуживает все адаптеры, модули и плагины.
2. Общие сущности находятся в `shared`.
3. Состояние конкретного адаптера находится в его собственной схеме.
4. Данные модулей и плагинов изолируются по отдельным схемам.
5. Cajeer Bots не выполняет DDL-команды в runtime.

## Обязательные общие таблицы

```text
shared.platform_schema       версия внешнего контракта БД
shared.event_bus             журнал входящих событий
shared.event_outbox          события на доставку
shared.event_inbox           дедупликация входящих событий
shared.event_dead_letters    события, которые не удалось обработать
shared.audit_log             аудит административных действий
shared.runtime_locks         блокировки фоновых процессов
shared.idempotency_keys      ключи идемпотентности
```

## Обязательные поля событий

```text
event_id
contract_version
source
type
trace_id
payload
created_at
processed_at
status
last_error
```

## Проверка эксплуатации

`doctor` должен проверять подключение к PostgreSQL, но не должен создавать таблицы. Внешний эксплуатационный слой отвечает за создание и обновление схемы.


## Контракт `shared.event_bus` для backend `postgres`

Минимальный набор полей для `EVENT_BUS_BACKEND=postgres`:

```text
event_id      уникальный идентификатор события
trace_id      идентификатор трассировки
source        источник события: telegram, discord, vkontakte, system, module, plugin
event_type    тип события, например adapter.started
payload       JSONB-представление CajeerEvent
status        состояние доставки: new, processing, done, failed
created_at    время создания записи
processed_at  время обработки, если применимо
last_error    последняя ошибка обработки, если применимо
```

Индексы и блокировки выбираются эксплуатационным слоем. Платформа не создаёт таблицы автоматически.

## Redis Streams

Для `EVENT_BUS_BACKEND=redis` используется stream `cajeer-bots:events`. Поле `payload` содержит JSON-представление `CajeerEvent`.
