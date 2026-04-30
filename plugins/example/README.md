# Пример плагина

Этот каталог показывает минимальную структуру расширения платформы.

## Что демонстрирует

- `plugin.json` с явным `entrypoint`.
- `runtime.py` с компонентом `ExamplePlugin`.
- Обработку команды `/example`.
- Обработку события `plugin.example.ping`.

Плагин выключен по умолчанию и подключается через:

```env
PLUGINS_ENABLED=example_plugin
```
