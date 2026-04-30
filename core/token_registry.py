from __future__ import annotations

import hashlib
import hmac
import json
import secrets
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from core.api_routes import KNOWN_SCOPES, canonical_scope


@dataclass
class ApiTokenRecord:
    id: str
    prefix: str
    sha256: str
    scopes: list[str] = field(default_factory=list)
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    last_used_at: str | None = None
    last_used_ip: str | None = None
    last_used_user_agent: str | None = None
    created_by: str | None = None
    expires_at: str | None = None
    revoked_at: str | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class ApiTokenRegistry:
    """File-backed registry для scoped API-токенов.

    Env-токены остаются compatibility fallback, но production может хранить
    только sha256-хэши токенов в runtime/secrets/api_tokens.json.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self.records: list[ApiTokenRecord] = []
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            self.records = [ApiTokenRecord(**item) for item in data.get("tokens", [])]
        except Exception:
            self.records = []

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps({"tokens": [item.to_dict() for item in self.records]}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    @staticmethod
    def hash_token(token: str) -> str:
        return hashlib.sha256(token.encode("utf-8")).hexdigest()

    def _normalize_scopes(self, scopes: Iterable[str]) -> list[str]:
        normalized = sorted({canonical_scope(scope) for scope in scopes if scope})
        unknown = [scope for scope in normalized if scope not in KNOWN_SCOPES and scope != "*"]
        if unknown:
            raise ValueError("неизвестные scopes: " + ", ".join(unknown))
        return normalized

    def authenticate(self, bearer: str, *, ip: str | None = None, user_agent: str | None = None) -> tuple[str | None, set[str], str | None]:
        token = bearer.removeprefix("Bearer ").strip()
        if not token:
            return None, set(), None
        digest = self.hash_token(token)
        now = datetime.now(timezone.utc).isoformat()
        for record in self.records:
            if record.revoked_at:
                continue
            if record.expires_at and record.expires_at < now:
                continue
            if record.prefix and not token.startswith(record.prefix):
                continue
            if hmac.compare_digest(record.sha256, digest):
                record.last_used_at = now
                record.last_used_ip = ip or record.last_used_ip
                record.last_used_user_agent = user_agent or record.last_used_user_agent
                self._save()
                return record.id, set(record.scopes), record.prefix
        return None, set(), None

    def create_token(self, *, token_id: str, scopes: Iterable[str], prefix: str = "cb_", created_by: str | None = None, expires_at: str | None = None) -> tuple[str, ApiTokenRecord]:
        if any(record.id == token_id for record in self.records):
            raise ValueError(f"token id уже существует: {token_id}")
        token = prefix + secrets.token_urlsafe(32)
        record = ApiTokenRecord(token_id, prefix, self.hash_token(token), self._normalize_scopes(scopes), created_by=created_by, expires_at=expires_at)
        self.records.append(record)
        self._save()
        return token, record

    def rotate(self, token_id: str) -> tuple[str, ApiTokenRecord]:
        old = next((item for item in self.records if item.id == token_id and not item.revoked_at), None)
        if old is None:
            raise KeyError(token_id)
        token = old.prefix + secrets.token_urlsafe(32)
        old.sha256 = self.hash_token(token)
        old.last_used_at = None
        old.last_used_ip = None
        old.last_used_user_agent = None
        self._save()
        return token, old

    def revoke(self, token_id: str) -> bool:
        for record in self.records:
            if record.id == token_id and not record.revoked_at:
                record.revoked_at = datetime.now(timezone.utc).isoformat()
                self._save()
                return True
        return False

    def inspect(self, token_id: str) -> dict[str, object] | None:
        for record in self.records:
            if record.id == token_id:
                return record.to_dict()
        return None

    def export_redacted(self) -> dict[str, object]:
        return {"tokens": [{**record.to_dict(), "sha256": "<redacted>"} for record in self.records]}

    def snapshot(self) -> list[dict[str, object]]:
        return [record.to_dict() for record in self.records]
