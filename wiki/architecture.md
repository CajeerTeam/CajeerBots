# Архитектура

Cajeer Bots = platform core + transport adapters + modules + plugins.

```text
External event -> Adapter -> CajeerEvent -> Event bus -> Module/Plugin -> Outbox -> Adapter delivery
```
