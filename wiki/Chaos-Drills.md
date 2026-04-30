# Chaos / fault drills

## Проверки

- Убить worker во время delivery processing.
- Перезапустить Redis и проверить reconnect.
- Перезапустить PostgreSQL и проверить health degradation.
- Отправить duplicate webhook update.
- Выполнить failed update apply и rollback.
- Смоделировать slow adapter send.

Каждый drill должен завершаться проверкой `/readyz`, audit и dead letters.
