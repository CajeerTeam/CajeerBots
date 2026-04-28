from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

from nmbot.config import BotConfig, ConfigValidationError
from nmbot.database import Database
from nmbot.postgres_backend import PostgresDatabase


def create_database(config: BotConfig):
    if not config.database_url:
        return Database(config.sqlite_path)
    parsed = urlparse(config.database_url)
    if parsed.scheme == 'sqlite' and parsed.path:
        return Database(Path(parsed.path))
    if parsed.scheme in {'postgres', 'postgresql'}:
        return PostgresDatabase(config.database_url)
    raise ConfigValidationError('DATABASE_URL должен быть sqlite://<path> или postgresql://user:pass@host:port/dbname')
