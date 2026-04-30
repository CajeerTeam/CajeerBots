FROM python:3.12-slim AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
ARG CAJEER_BOTS_EXTRAS=api,adapters,redis
COPY pyproject.toml README.md VERSION LICENSE alembic.ini ./
COPY core ./core
COPY bots ./bots
COPY modules ./modules
COPY plugins ./plugins
COPY distributed ./distributed
COPY scripts ./scripts
COPY alembic ./alembic
COPY schemas ./schemas
RUN addgroup --system cajeer && adduser --system --ingroup cajeer --home /app cajeer \
    && pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -e ".[${CAJEER_BOTS_EXTRAS}]" \
    && chmod +x /app/scripts/*.sh \
    && mkdir -p /app/runtime /tmp/cajeer-bots \
    && chown -R cajeer:cajeer /app /tmp/cajeer-bots
USER cajeer:cajeer
CMD ["cajeer-bots", "run", "all"]

FROM runtime AS test
USER root
COPY tests ./tests
RUN pip install --no-cache-dir -e ".[dev]" \
    && chown -R cajeer:cajeer /app
USER cajeer:cajeer
CMD ["python", "-m", "pytest", "-q", "tests/integration"]
