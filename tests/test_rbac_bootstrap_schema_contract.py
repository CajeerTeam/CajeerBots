from __future__ import annotations

import inspect

from core.rbac_store import bootstrap_owner_db


def test_db_rbac_bootstrap_sql_matches_current_schema():
    source = inspect.getsource(bootstrap_owner_db)
    assert "users(user_id, display_name" in source
    assert "ON CONFLICT (user_id)" in source
    assert "platform_accounts(platform, platform_user_id, user_id" in source
    assert "ON CONFLICT (platform, platform_user_id)" in source
    assert "roles(role_id, title, source" in source
    assert "role_permissions(role_id, permission)" in source
    assert "audit_log(audit_id, actor_type" in source
    assert ".permissions" not in source
    assert "permission_id" not in source
