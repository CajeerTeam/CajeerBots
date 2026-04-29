# Эксплуатация

## Проверка платформы

```bash
cajeer-bots doctor --offline
cajeer-bots doctor
```

Проверка без `--offline` обращается к PostgreSQL и проверяет токены включённых адаптеров.

## Release preflight

Перед сборкой релиза `scripts/release.sh` выполняет:

- проверку запрещённых проектных и устаревших терминов;
- проверку executable-bit у shell-скриптов;
- компиляцию Python-файлов;
- `doctor --offline`;
- проверку registry для адаптеров, модулей, плагинов и команд.

## HTTP API

```bash
cajeer-bots run api
```

Маршруты:

```text
/healthz
/readyz
/version
/adapters
/modules
/plugins
/events
/commands
/config/summary
/adapter-status
```
