from __future__ import annotations

from typing import Final

RU_GLOSSARY: Final[dict[str, str]] = {
    "verification": "привязка",
    "approval": "согласование",
    "identity_card": "карточка связей",
    "bridge_policy": "политика синхронизации",
    "guild_recruitment": "набор в гильдию",
    "report": "жалоба",
    "support": "поддержка",
    "event": "событие",
    "devlog": "дневник разработки",
}

HELP_TOPIC_ALIASES: Final[dict[str, str]] = {
    "start": "старт",
    "старт": "старт",
    "начало": "старт",
    "rules": "правила",
    "правила": "правила",
    "roles": "роли",
    "роль": "роли",
    "роли": "роли",
    "faq": "вопросы",
    "вопросы": "вопросы",
    "faq-discord": "вопросы",
    "support": "поддержка",
    "поддержка": "поддержка",
    "bugs": "баги",
    "bug": "баги",
    "баг": "баги",
    "баги": "баги",
    "suggestions": "предложения",
    "suggestion": "предложения",
    "предложение": "предложения",
    "предложения": "предложения",
    "guilds": "гильдии",
    "guild": "гильдии",
    "гильдии": "гильдии",
    "events": "события",
    "event": "события",
    "событие": "события",
    "события": "события",
    "appeals": "апелляции",
    "appeal": "апелляции",
    "апелляция": "апелляции",
    "апелляции": "апелляции",
}

SUPPORT_AREA_ALIASES: Final[dict[str, str]] = {
    "general": "general",
    "общая": "general",
    "общее": "general",
    "launcher": "launcher",
    "лаунчер": "launcher",
    "launcher-tech": "launcher",
    "account": "account",
    "аккаунт": "account",
    "учетная-запись": "account",
    "учётная-запись": "account",
    "appeal": "appeal",
    "апелляция": "appeal",
}

APPROVAL_STATUS_ALIASES: Final[dict[str, str]] = {
    "pending": "pending",
    "ожидает": "pending",
    "в-ожидании": "pending",
    "approved": "approved",
    "одобрено": "approved",
    "reject": "rejected",
    "rejected": "rejected",
    "отклонено": "rejected",
}

APPROVAL_DECISION_ALIASES: Final[dict[str, str]] = {
    "approve": "approved",
    "approved": "approved",
    "одобрить": "approved",
    "одобрено": "approved",
    "reject": "rejected",
    "rejected": "rejected",
    "отклонить": "rejected",
    "отклонено": "rejected",
}

TRIAGE_STATUS_ALIASES: Final[dict[str, str]] = {
    "open": "open",
    "открыто": "open",
    "открыта": "open",
    "in_review": "in_review",
    "in-review": "in_review",
    "на-рассмотрении": "in_review",
    "на рассмотрении": "in_review",
    "resolved": "resolved",
    "решено": "resolved",
    "resolved/archived": "resolved",
    "closed": "closed",
    "закрыто": "closed",
    "закрыта": "closed",
}

TRIAGE_STATUS_LABELS: Final[dict[str, str]] = {
    "open": "открыто",
    "in_review": "на рассмотрении",
    "resolved": "решено",
    "closed": "закрыто",
}

PANEL_TYPE_LABELS: Final[dict[str, str]] = {
    "onboarding": "панель входа",
    "interest_roles": "панель ролей интересов",
    "help": "панель навигации",
}

BOOL_LABELS: Final[dict[bool, str]] = {True: "включено", False: "выключено"}


def normalize_help_topic(value: str | None) -> str:
    raw = (value or "старт").strip().lower()
    return HELP_TOPIC_ALIASES.get(raw, "старт")


def normalize_support_area(value: str | None) -> str | None:
    raw = (value or "").strip().lower()
    return SUPPORT_AREA_ALIASES.get(raw)


def normalize_approval_status(value: str | None) -> str | None:
    raw = (value or "").strip().lower()
    return APPROVAL_STATUS_ALIASES.get(raw)


def normalize_approval_decision(value: str | None) -> str | None:
    raw = (value or "").strip().lower()
    return APPROVAL_DECISION_ALIASES.get(raw)


def normalize_triage_status(value: str | None) -> str | None:
    raw = (value or "").strip().lower()
    return TRIAGE_STATUS_ALIASES.get(raw)


def triage_status_label(value: str) -> str:
    return TRIAGE_STATUS_LABELS.get(value, value)


def panel_type_label(value: str) -> str:
    return PANEL_TYPE_LABELS.get(value, value)


def bool_label(value: bool) -> str:
    return BOOL_LABELS[bool(value)]


AUDIT_CATEGORY_LABELS: Final[dict[str, str]] = {
    "security": "безопасность",
    "business": "сообщество",
    "ops": "операции",
}

AUDIT_STATUS_LABELS: Final[dict[str, str]] = {
    "success": "успешно",
    "warning": "предупреждение",
    "error": "ошибка",
    "degraded": "ограниченный режим",
    "pending": "ожидает",
    "conflict": "конфликт",
}


def audit_category_label(value: str) -> str:
    return AUDIT_CATEGORY_LABELS.get(value, value)


def audit_status_label(value: str) -> str:
    return AUDIT_STATUS_LABELS.get(value, value)


STATUS_SOURCE_LABELS: Final[dict[str, str]] = {
    "tags": "только теги",
    "title": "только префикс в названии",
    "hybrid": "теги и префикс в названии",
}

VISIBLE_ENGLISH_FRAGMENTS: Final[tuple[str, ...]] = (
    "manage roles",
    "hierarchy",
    "member",
    "visitor",
    "job #",
    "server members intent",
    "message content intent",
    "manage roles",
    "open",
    "closed",
    "in_review",
    "identity card",
    "bridge policy",
)

ROLE_LABELS: Final[dict[str, str]] = {
    "member": "участник",
    "visitor": "гость",
    "verified": "подтверждённый участник",
}


def status_source_label(value: str) -> str:
    return STATUS_SOURCE_LABELS.get(value, value)


def role_label(value: str) -> str:
    return ROLE_LABELS.get(value, value)


def find_visible_english_fragments(payload: object) -> list[str]:
    fragments: list[str] = []
    lowered = json_like_to_text(payload).lower()
    for token in VISIBLE_ENGLISH_FRAGMENTS:
        if token in lowered:
            fragments.append(token)
    return fragments


def json_like_to_text(payload: object) -> str:
    if isinstance(payload, dict):
        return ' '.join(f"{k} {json_like_to_text(v)}" for k, v in payload.items())
    if isinstance(payload, (list, tuple, set)):
        return ' '.join(json_like_to_text(v) for v in payload)
    return str(payload)

OPERATIONAL_RUSSIAN_HINTS = {'schema': 'схема', 'warning': 'предупреждение', 'storage': 'хранилище'}
