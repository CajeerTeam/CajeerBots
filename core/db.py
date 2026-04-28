from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


class Database:
    """Минимальный слой проверки подключения к общей PostgreSQL-базе.

    В этом каркасе миграции намеренно не встроены: платформа ожидает, что схема
    БД управляется отдельным эксплуатационным процессом или будущим модулем.
    """

    def __init__(self, dsn: str, sslmode: str = "prefer") -> None:
        self.dsn = dsn
        self.sslmode = sslmode

    def connect(self):
        if not self.dsn:
            raise RuntimeError("DATABASE_URL не задан")
        import psycopg

        kwargs = {"sslmode": self.sslmode} if self.sslmode else {}
        return psycopg.connect(self.dsn, **kwargs)

    def ping(self) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
