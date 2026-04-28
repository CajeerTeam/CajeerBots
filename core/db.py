from __future__ import annotations
import logging
from pathlib import Path
logger = logging.getLogger(__name__)

class Database:
    def __init__(self, dsn: str, sslmode: str = "prefer") -> None:
        self.dsn = dsn
        self.sslmode = sslmode

    def connect(self):
        if not self.dsn:
            raise RuntimeError("DATABASE_URL is empty")
        import psycopg
        kwargs = {"sslmode": self.sslmode} if self.sslmode else {}
        return psycopg.connect(self.dsn, **kwargs)

    def ping(self) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()

    def apply_sql_file(self, path: Path) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(path.read_text(encoding="utf-8"))
            conn.commit()
        logger.info("applied migration file: %s", path)

class MigrationManager:
    def __init__(self, db: Database, root: Path) -> None:
        self.db = db
        self.root = root

    def list_migration_files(self) -> list[Path]:
        return sorted(self.root.rglob("*.sql")) if self.root.exists() else []

    def apply_all(self) -> int:
        count = 0
        for path in self.list_migration_files():
            self.db.apply_sql_file(path)
            count += 1
        return count
