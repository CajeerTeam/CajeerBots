from core.events import CajeerEvent, sign_event, validate_event, verify_event_signature


def test_event_signature_roundtrip():
    event = CajeerEvent.create(source="system", type="system.test", payload={"ok": True})
    signature = sign_event(event, "secret")
    assert verify_event_signature(event, "secret", signature)
    assert not verify_event_signature(event, "other", signature)
    assert validate_event(event) == []
