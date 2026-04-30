from __future__ import annotations

import re

_SCHEMA_RE = re.compile(r"^[a-z_][a-z0-9_]*$")


def validate_schema_name(value: str | None, *, default: str = "shared") -> str:
    """Вернуть безопасное имя PostgreSQL-схемы для raw SQL фрагментов.

    В проекте имя схемы подставляется в SQL как идентификатор, поэтому оно не
    может передаваться как bind-параметр. Ограничиваем формат нижним регистром,
    цифрами и подчёркиванием, чтобы исключить SQL injection и проблемы quoting.
    """

    schema = (value or default).strip() or default
    if not _SCHEMA_RE.fullmatch(schema):
        raise ValueError(
            "DATABASE_SCHEMA_SHARED должен соответствовать ^[a-z_][a-z0-9_]*$ "
            "и использовать только нижний регистр, цифры и подчёркивание"
        )
    return schema
