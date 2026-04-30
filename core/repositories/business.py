from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from core.schema import validate_schema_name


def _sql_text(statement: str):
    from sqlalchemy import text
    return text(statement)


class BusinessStateRepository:
    def __init__(self, async_dsn: str, schema: str = "shared") -> None:
        self.async_dsn = async_dsn
        self.schema = validate_schema_name(schema)
        self._engine: Any | None = None

    def _engine_obj(self) -> Any:
        if self._engine is None:
            from sqlalchemy.ext.asyncio import create_async_engine
            self._engine = create_async_engine(self.async_dsn, pool_pre_ping=True)
        return self._engine

    async def upsert_identity(self, *, user_id: str, platform: str, platform_user_id: str, display_name: str = "", profile: dict[str, Any] | None = None) -> None:
        async with self._engine_obj().begin() as conn:
            await conn.execute(_sql_text(f"INSERT INTO {self.schema}.users(user_id, display_name, created_at, updated_at) VALUES(:user_id,:display_name,NOW(),NOW()) ON CONFLICT(user_id) DO UPDATE SET display_name=COALESCE(EXCLUDED.display_name,{self.schema}.users.display_name), updated_at=NOW()"), {"user_id": user_id, "display_name": display_name})
            await conn.execute(_sql_text(f"INSERT INTO {self.schema}.platform_accounts(platform, platform_user_id, user_id, display_name, profile, created_at, updated_at) VALUES(:platform,:platform_user_id,:user_id,:display_name,CAST(:profile AS jsonb),NOW(),NOW()) ON CONFLICT(platform, platform_user_id) DO UPDATE SET user_id=EXCLUDED.user_id, display_name=EXCLUDED.display_name, profile=EXCLUDED.profile, updated_at=NOW()"), {"platform": platform, "platform_user_id": platform_user_id, "user_id": user_id, "display_name": display_name, "profile": json.dumps(profile or {}, ensure_ascii=False)})
            await conn.execute(_sql_text(f"INSERT INTO {self.schema}.user_profiles(user_id, profile, updated_at) VALUES(:user_id,CAST(:profile AS jsonb),NOW()) ON CONFLICT(user_id) DO UPDATE SET profile=EXCLUDED.profile, updated_at=NOW()"), {"user_id": user_id, "profile": json.dumps(profile or {}, ensure_ascii=False)})

    async def create_support_ticket(self, *, ticket_id: str, user_id: str | None, platform: str, platform_chat_id: str, subject: str, history: dict[str, Any]) -> None:
        async with self._engine_obj().begin() as conn:
            await conn.execute(_sql_text(f"INSERT INTO {self.schema}.support_tickets(ticket_id,user_id,platform,platform_chat_id,status,subject,history,created_at,updated_at) VALUES(:ticket_id,:user_id,:platform,:platform_chat_id,'open',:subject,CAST(:history AS jsonb),NOW(),NOW()) ON CONFLICT(ticket_id) DO NOTHING"), {"ticket_id": ticket_id, "user_id": user_id, "platform": platform, "platform_chat_id": platform_chat_id, "subject": subject, "history": json.dumps(history, ensure_ascii=False)})

    async def update_support_ticket(self, *, ticket_id: str, status: str | None = None, assigned_to: str | None = None, event: dict[str, Any] | None = None) -> None:
        # history is JSONB; append event in a simple array field when possible.
        async with self._engine_obj().begin() as conn:
            await conn.execute(_sql_text(f"UPDATE {self.schema}.support_tickets SET status=COALESCE(:status,status), assigned_to=COALESCE(:assigned_to,assigned_to), history = jsonb_set(COALESCE(history,'{{}}'::jsonb), '{{events}}', COALESCE(history->'events','[]'::jsonb) || CAST(:event AS jsonb), true), updated_at=NOW() WHERE ticket_id=:ticket_id"), {"ticket_id": ticket_id, "status": status, "assigned_to": assigned_to, "event": json.dumps([event or {}], ensure_ascii=False)})

    async def create_moderation_action(self, *, action_id: str, platform: str, target_id: str, action: str, reason: str, actor_id: str | None, trace_id: str | None) -> None:
        async with self._engine_obj().begin() as conn:
            await conn.execute(_sql_text(f"INSERT INTO {self.schema}.moderation_actions(action_id,platform,target_id,action,reason,actor_id,trace_id,created_at) VALUES(:action_id,:platform,:target_id,:action,:reason,:actor_id,:trace_id,NOW()) ON CONFLICT(action_id) DO NOTHING"), locals())

    async def create_announcement(self, *, announcement_id: str, status: str, title: str, body: str, targets: list[str], scheduled_at: str | None) -> None:
        async with self._engine_obj().begin() as conn:
            await conn.execute(_sql_text(f"INSERT INTO {self.schema}.announcements(announcement_id,status,title,body,targets,scheduled_at,created_at) VALUES(:announcement_id,:status,:title,:body,CAST(:targets AS jsonb),CAST(:scheduled_at AS timestamptz),NOW()) ON CONFLICT(announcement_id) DO NOTHING"), {"announcement_id": announcement_id, "status": status, "title": title, "body": body, "targets": json.dumps({"items": targets}, ensure_ascii=False), "scheduled_at": scheduled_at})

    async def create_scheduled_job(self, *, job_id: str, job_type: str, payload: dict[str, Any], run_at: str) -> None:
        async with self._engine_obj().begin() as conn:
            await conn.execute(_sql_text(f"INSERT INTO {self.schema}.scheduled_jobs(job_id,job_type,payload,status,run_at,created_at) VALUES(:job_id,:job_type,CAST(:payload AS jsonb),'pending',CAST(:run_at AS timestamptz),NOW()) ON CONFLICT(job_id) DO NOTHING"), {"job_id": job_id, "job_type": job_type, "payload": json.dumps(payload, ensure_ascii=False), "run_at": run_at or datetime.now(timezone.utc).isoformat()})
