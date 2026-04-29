# Шина событий

Шина событий связывает адаптеры, модули и плагины через единый контракт `CajeerEvent`.

## Backend

Платформа поддерживает несколько вариантов backend через переменную `EVENT_BUS_BACKEND`:

| Значение | Назначение |
|---|---|
| `memory` | один процесс, разработка, тесты, простой local mode |
| `postgres` | несколько процессов через внешний PostgreSQL-контракт |
| `redis` | несколько процессов через Redis Streams |

Каталог `migrations` в проект не входит. Для `postgres` и `redis` инфраструктурные объекты создаются внешним эксплуатационным слоем.

## Inline routing и bridge routing

`LOCAL_INLINE_ROUTING=true` позволяет адаптеру сразу передать событие в router внутри одного процесса.

`BRIDGE_ROUTING=true` позволяет отдельному bridge-процессу читать события из шины и маршрутизировать их.

Для многопроцессного режима рекомендуется:

```env
EVENT_BUS_BACKEND=redis
LOCAL_INLINE_ROUTING=false
BRIDGE_ROUTING=true
```

Идемпотентность защищает от повторной обработки одного `event_id`.

## Режим bridge

`cajeer-bots run bridge` читает события из шины, маршрутизирует их через `core.router` и отправляет ошибки в dead letters.

## Режим worker

`cajeer-bots run worker` выполняет фоновые задачи: обработку очереди доставки и будущие задачи модулей.
