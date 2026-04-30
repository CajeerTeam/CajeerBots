FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY pyproject.toml README.md VERSION LICENSE ./
COPY core ./core
COPY bots ./bots
COPY modules ./modules
COPY plugins ./plugins
COPY distributed ./distributed
COPY scripts ./scripts
RUN addgroup --system cajeer && adduser --system --ingroup cajeer --home /app cajeer \
    && pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir '.[api,adapters,redis]' \
    && chmod +x /app/scripts/*.sh \
    && mkdir -p /app/runtime /tmp/cajeer-bots \
    && chown -R cajeer:cajeer /app /tmp/cajeer-bots
USER cajeer:cajeer
CMD ["cajeer-bots", "run", "all"]
