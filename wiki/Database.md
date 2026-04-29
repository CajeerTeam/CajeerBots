# База данных

Платформа использует единую PostgreSQL-базу. Встроенные миграции в Cajeer Bots не поставляются: управление схемой БД выполняется внешним эксплуатационным слоем.

## Правило владения схемой

Cajeer Bots описывает контракт БД, но не создаёт и не изменяет таблицы самостоятельно. Это позволяет использовать любые корпоративные инструменты управления схемой: SQL-пакеты, отдельный мигратор, Terraform, Ansible, Flyway, Liquibase или внутренний процесс эксплуатации.

## Рекомендуемые схемы PostgreSQL

```text
shared              общие сущности платформы
telegram            состояние Telegram-адаптера
discord             состояние Discord-адаптера
vkontakte           состояние адаптера ВКонтакте
modules_identity    данные модуля идентификации
modules_rbac        роли и права доступа
modules_logs        журналы
modules_bridge      шина событий
modules_support     обращения пользователей
modules_announcements объявления
modules_scheduler   фоновые задачи
modules_moderation  модерация
plugins_example     пример плагина
```

## Минимальный контракт `shared`

```text
shared.platform_schema
shared.event_bus
shared.event_outbox
shared.event_inbox
shared.event_dead_letters
shared.audit_log
shared.runtime_locks
shared.idempotency_keys
```

## Контракт без миграций

Команда `cajeer-bots db-status` не меняет базу данных. Она только сообщает, что схема управляется внешним слоем. Проверка доступности PostgreSQL выполняется через `cajeer-bots doctor` без флага `--offline`.

## Совместимость

Совместимость схемы фиксируется в `compatibility.yaml` через поле:

```yaml
db_contract: external
```

Если проекту потребуется встроенный мигратор, его нужно добавлять отдельным решением, а не смешивать с runtime-слоем платформы.
