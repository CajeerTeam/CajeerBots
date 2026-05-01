from __future__ import annotations

from core.events import extract_command, message_event, command_event_from_message


def test_adapter_incoming_message_contract_is_transport_neutral():
    event = message_event(source="fake", platform_user_id="u1", platform_chat_id="c1", text="/ping")
    parsed = extract_command(event.payload["text"])
    assert parsed == ("ping", "")
    command = command_event_from_message(event, *parsed)
    assert command.type == "command.received"
    assert command.trace_id == event.trace_id
