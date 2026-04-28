FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DATA_DIR=/app/data \
    SHARED_DIR=/app/shared \
    NMBOT_RUNTIME_DIR=/app/data \
    BOT_MODE=webhook \
    PUBLIC_HTTP_SERVER_URL=https://nmtelegrambot.bothost.ru/ \
    WEBHOOK_URL=https://nmtelegrambot.bothost.ru/ \
    WEBHOOK_LISTEN=0.0.0.0 \
    WEBHOOK_PORT=8080 \
    PORT=8080 \
    HEALTH_HTTP_PORT=0

WORKDIR /app

COPY requirements.lock ./requirements.lock
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.lock \
    && useradd -r -u 10001 -g root appuser

COPY . .
RUN python3 -m compileall -q nmbot main.py db_tools.py setup_wizard.py preflight_check.py qa_smoke.py qa_contract_nm_auth.py
RUN python3 - <<'PY'
import nmbot.main
import nmbot.database
import nmbot.health_http
print('import smoke ok')
PY
RUN TELEGRAM_BOT_TOKEN=docker-build-token \
    TELEGRAM_OWNER_IDS=1 \
    DATABASE_URL= \
    SQLITE_PATH=storage/docker-build.db \
    WEBHOOK_URL= \
    BOT_MODE=polling \
    python3 qa_smoke.py
RUN mkdir -p /app/data/logs /app/data/storage /app/data/artifacts/backups /app/data/artifacts/exports /app/templates && chown -R appuser:root /app
USER appuser

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 CMD python3 -m nmbot.main --readiness-check || exit 1

CMD ["python3", "-m", "nmbot.main"]
