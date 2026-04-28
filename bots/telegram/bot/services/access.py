from __future__ import annotations

import time
import os
from dataclasses import dataclass

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None

from telegram import Chat
from telegram.ext import ContextTypes

from nmbot.config import BotConfig
from nmbot.database import Database


class PermissionDeniedError(PermissionError):
    pass


class RateLimitExceededError(RuntimeError):
    def __init__(self, retry_after: float) -> None:
        super().__init__(f"rate_limit:{retry_after:.2f}")
        self.retry_after = retry_after


@dataclass(slots=True)
class RateLimitState:
    hits: int = 0
    last_seen_at: float = 0.0


_ROLE_ORDER = {"user": 0, "mod": 1, "admin": 2, "owner": 3}


class RateLimiter:
    def __init__(self, db: Database) -> None:
        self._state: dict[str, RateLimitState] = {}
        self._db = db
        self._redis = None
        self.redis_url = os.getenv('REDIS_URL', '').strip()
        self.redis_configured = bool(self.redis_url)
        self.redis_last_error = ''
        if self.redis_url and redis is not None:
            try:
                self._redis = redis.Redis.from_url(self.redis_url, decode_responses=True)
                self._redis.ping()
            except Exception as exc:
                self.redis_last_error = str(exc)
                self._redis = None
        elif self.redis_url and redis is None:
            self.redis_last_error = 'redis package not installed'
        self._db.set_runtime_value("rate_limit_rejections", self._db.runtime_value("rate_limit_rejections", "0"))
        self.backend_mode = "redis" if self._redis is not None else ("sqlite-fallback" if self.redis_url else "sqlite")


    def diagnostics(self) -> dict[str, object]:
        return {
            "backend_mode": self.backend_mode,
            "redis_configured": self.redis_configured,
            "redis_connected": self._redis is not None,
            "redis_last_error": self.redis_last_error,
        }

    @property
    def total_rejections(self) -> int:
        return int(self._db.runtime_value("rate_limit_rejections", "0") or "0")

    def hit(self, key: str, *, cooldown_seconds: float) -> None:
        if cooldown_seconds <= 0:
            return
        if self._redis is not None:
            try:
                created = self._redis.set(f"nmtg:rl:{key}", '1', nx=True, ex=max(1, int(cooldown_seconds)))
                if not created:
                    ttl = self._redis.ttl(f"nmtg:rl:{key}")
                    self._db.increment_runtime_counter("rate_limit_rejections", 1)
                    raise RateLimitExceededError(float(ttl if ttl and ttl > 0 else cooldown_seconds))
                return
            except RateLimitExceededError:
                raise
            except Exception as exc:
                self.redis_last_error = str(exc)
                self._redis = None
                self.backend_mode = "sqlite-fallback" if self.redis_url else "sqlite"
        now = time.monotonic()
        state = self._state.get(key)
        if state is not None and now - state.last_seen_at < cooldown_seconds:
            self._db.increment_runtime_counter("rate_limit_rejections", 1)
            raise RateLimitExceededError(cooldown_seconds - (now - state.last_seen_at))
        self._state[key] = RateLimitState(hits=(state.hits + 1 if state else 1), last_seen_at=now)



def _effective_role(cfg: BotConfig, db: Database, user_id: int | None) -> str:
    base = cfg.role_for_user(user_id)
    if user_id is None:
        return base
    override = db.get_user_role_override(user_id)
    if override and override in _ROLE_ORDER and _ROLE_ORDER[override] >= _ROLE_ORDER.get(base, 0):
        return override
    return base


def _feature_enabled(db: Database, chat_id: int, feature: str, default: bool = True) -> bool:
    flags = db.get_chat_feature_flags(chat_id) if hasattr(db, 'get_chat_feature_flags') else {}
    return bool(flags.get(feature, default))


def guard_access(
    cfg: BotConfig,
    db: Database,
    *,
    chat: Chat | None,
    user_id: int | None,
    required_role: str = "user",
    command: str | None = None,
) -> None:
    if chat is None or user_id is None:
        raise PermissionDeniedError("no_chat_or_user")
    if cfg.telegram_allowed_chat_ids and chat.id not in cfg.telegram_allowed_chat_ids:
        raise PermissionDeniedError("chat_not_allowed")
    if cfg.telegram_chat_scope == "private" and chat.type != Chat.PRIVATE:
        raise PermissionDeniedError("private_only")
    if cfg.telegram_chat_scope == "groups" and chat.type == Chat.PRIVATE:
        raise PermissionDeniedError("groups_only")

    effective_required_role = db.get_required_role_override(command or '', chat_id=chat.id) or required_role
    if _ROLE_ORDER[_effective_role(cfg, db, user_id)] < _ROLE_ORDER[effective_required_role]:
        raise PermissionDeniedError(f"role_required:{effective_required_role}")

    settings = db.get_chat_settings(chat.id)
    if settings is None:
        return
    if command in {"status", "online"} and (not settings.allow_status or not _feature_enabled(db, chat.id, 'status', True)):
        raise PermissionDeniedError("chat_status_disabled")
    if command in {"announce", "broadcast", "schedule"} and (not settings.allow_broadcasts or not _feature_enabled(db, chat.id, 'broadcasts', True)):
        raise PermissionDeniedError("chat_broadcasts_disabled")
    if command in {"metrics"} and not _feature_enabled(db, chat.id, 'metrics', True):
        raise PermissionDeniedError("chat_metrics_disabled")
    if command in {"security"} and not _feature_enabled(db, chat.id, 'security_notifications', True):
        raise PermissionDeniedError("chat_security_disabled")


def enforce_rate_limit(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    user_id: int,
    chat_id: int,
    command: str,
    cooldown_seconds: float,
) -> None:
    limiter: RateLimiter = context.application.bot_data["rate_limiter"]
    limiter.hit(f"user:{user_id}:{command}", cooldown_seconds=cooldown_seconds)
    limiter.hit(f"chat:{chat_id}:{command}", cooldown_seconds=cooldown_seconds)
