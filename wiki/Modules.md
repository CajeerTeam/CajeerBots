# Модули

Модули содержат официальную бизнес-логику платформы.


## Strict persistence

Для `support`, `announcements`, `moderation`, `scheduler` доступен режим строгой записи: `MODULE_STRICT_PERSISTENCE=true` или отдельные `*_STRICT_PERSISTENCE=true`. В этом режиме ошибка записи в БД возвращает ошибку команды, а не скрытый warning.

## Права модулей

Support разделяет `bots.support.create`, `bots.support.assign`, `bots.support.manage`; moderation — `bots.moderation.warn/mute/ban/kick/manage`; announcements — `bots.announce.create/publish/manage`.
