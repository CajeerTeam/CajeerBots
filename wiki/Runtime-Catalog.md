# Runtime Catalog

Cajeer Bots использует модель B + C с fallback A:

- B: встроенные `bots`, `modules`, `plugins` входят в Python package и доступны после wheel-install.
- C: кастомные бизнес-модули и плагины подключаются из runtime catalog.
- A fallback: в development registry может читать repo-root каталоги напрямую.

## Переменные

```env
RUNTIME_CATALOG_PATHS=runtime/catalog
REGISTRY_REPO_ROOT_FALLBACK=true
```

## Структура

```text
runtime/catalog/
├─ modules/
│  └─ custom_support/
│     ├─ module.json
│     └─ runtime.py
└─ plugins/
   └─ customer_plugin/
      ├─ plugin.json
      └─ runtime.py
```

`module.json` и `plugin.json` могут содержать `entrypoint` в формате:

```json
{
  "entrypoint": "runtime:CustomModule"
}
```
