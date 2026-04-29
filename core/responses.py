from __future__ import annotations

from dataclasses import asdict, dataclass

from core.events import CajeerEvent


@dataclass(frozen=True)
class CommandResponse:
    adapter: str
    chat_id: str
    text: str
    trace_id: str
    command: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def response_from_result(event: CajeerEvent, result: dict[str, object]) -> CommandResponse | None:
    if event.chat is None:
        return None
    text = result.get("message") or result.get("text")
    if text is None:
        return None
    text_value = str(text).strip()
    if not text_value:
        return None
    return CommandResponse(
        adapter=event.chat.platform,
        chat_id=event.chat.platform_chat_id,
        text=text_value,
        trace_id=event.trace_id,
        command=str(event.payload.get("command") or ""),
    )
