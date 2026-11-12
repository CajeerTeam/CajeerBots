# Integration Tests / Beta Regression

`0.18.0-beta.2` добавляет рабочий слой интеграционного тестирования CajeerBots.

## Назначение

Слой нужен для beta regression и MVP smoke-проверок перед RC:

- проверить API и БД;
- проверить durable Event API;
- проверить outbox/dead-letter отсутствие критичных ошибок;
- проверить bot scenario dry-run;
- связать integration coverage с release gates.

## Основные сущности

- `integration_test_suites` — наборы проверок;
- `integration_test_cases` — отдельные test cases;
- `integration_test_runs` — запуски suite/case;
- `integration_test_step_runs` — результаты каждого шага;
- `integration_test_coverage` — снимки покрытия MVP-областей.

## Поддерживаемые step kinds

- `api_health`;
- `http_request`;
- `database_count`;
- `event_publish`;
- `outbox_probe`;
- `bot_scenario`;
- `release_gate_summary`.

## CLI

```bash
cajeer integration-tests defaults:seed
cajeer integration-tests suites:list
cajeer integration-tests cases:list --suite mvp-smoke
cajeer integration-tests run:suite mvp-smoke --dry-run
cajeer integration-tests run:case --suite mvp-smoke api-db-event-smoke
cajeer integration-tests queue --suite mvp-smoke
cajeer integration-tests runs:list
cajeer integration-tests coverage
```

Алиас: `cajeer it`.

## Worker

`IntegrationTestsWorker` обрабатывает queued runs из `integration_test_runs`. Интервал настраивается через:

```env
INTEGRATION_TESTS_WORKER_INTERVAL_MS=15000
INTEGRATION_TESTS_WORKER_BATCH_SIZE=3
```

## Scheduled smoke

Seed добавляет job `maintenance.integration_smoke`, который запускает suite `mvp-smoke` по расписанию.

## Практический смысл

Beta 2 считается не новым feature-freeze расширением, а стабилизационным слоем: каждый критичный MVP-путь должен иметь воспроизводимый integration case и историю запусков.
