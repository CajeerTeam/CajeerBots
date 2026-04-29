# Alembic

Cajeer Bots поставляет reference Alembic migrations для DB contract, но runtime не запускает миграции автоматически.

Оператор выполняет миграции отдельно:

```bash
alembic -c alembic.ini upgrade head
```

После применения миграций нужно проверить контракт:

```bash
cajeer-bots db check
```

Текущая reference revision:

```text
0001_core
```

Текущий DB contract:

```text
cajeer.bots.db.v1
```
