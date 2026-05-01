from core.audit import AuditLog


def test_audit_counters_are_monotonic_and_not_snapshot_derived():
    audit = AuditLog(max_size=1)
    audit.write(actor_type="user", actor_id="1", action="rbac.denied", resource="x", result="denied")
    audit.write(actor_type="webhook", actor_id="telegram", action="webhook.auth_failed", resource="/webhooks/telegram", result="denied")
    audit.write(actor_type="user", actor_id="2", action="rbac.denied", resource="y", result="denied")
    assert len(audit.snapshot()) == 1
    assert audit.counter("rbac_denied_total") == 2
    assert audit.counter("webhook_rejected_total") == 1
