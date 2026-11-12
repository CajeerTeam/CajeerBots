# Roadmap CajeerBots до production

Текущая ветка: `1.0.0-rc.6` — Release Hardening (реализация, допуск после полного CI/rehearsal).

| Релиз | Работы | Критерий допуска |
|---|---|---|
| 1.0.0-rc.2 | Воспроизводимая сборка, миграции, запуск API/bot/worker/CLI | Подлинный lockfile, зелёный CI, clean install и upgrade PostgreSQL, Compose healthy |
| 1.0.0-rc.3 | Security & Reliability | Проверка provider webhook signatures, community-aware RBAC, SSRF, Redis rate limiting, атомарный захват задач и retry только неуспешных deliveries |
| 1.0.0-rc.4 | Product Completion | Полноценные commands/menus/callbacks/forms, signed extensions install/update/rollback, CLI/SDK parity |
| 1.0.0-rc.5 | Production Rehearsal | Сквозные и нагрузочные тесты, clean install, upgrade, logical backup/restore (PITR — отдельный эксплуатационный допуск), failure tests |
| 1.0.0-rc.6 | Release Hardening | Security review, SBOM, подписанные артефакты, compatibility и эксплуатационные инструкции |
| 1.0.0 | Stable / limited production | Все блокеры закрыты, ограниченный pilot, мониторинг и rollback |
| 1.0.x | Production stabilization / GA | Исправления пилота, SLO, проверка восстановления, эксплуатационный допуск |

Проверки и оставшиеся блокеры: [RELEASE_VALIDATION_1.0.0-rc.6.md](../RELEASE_VALIDATION_1.0.0-rc.6.md). Не публиковать stable до прохождения release gates и ограниченного production pilot.
