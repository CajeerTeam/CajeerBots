DEGRADED_RESPONSE = "Сервис временно недоступен. Попробуйте позже."


def fallback_response() -> str:
    return DEGRADED_RESPONSE
