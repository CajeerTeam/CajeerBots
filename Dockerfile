FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY pyproject.toml README.md VERSION LICENSE ./
COPY cajeer_bots ./cajeer_bots
COPY bots ./bots
COPY modules ./modules
COPY plugins ./plugins
COPY migrations ./migrations
COPY scripts ./scripts
RUN pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir .[api,adapters] && chmod +x /app/scripts/*.sh
CMD ["python", "-m", "cajeer_bots", "run", "all"]
