FROM python:3.11-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV DATA_DIR=/app/data
ENV SHARED_DIR=/app/shared
ENV LOG_DIR=/app/data/logs
ENV BACKUP_DIR=/app/data/backups
ENV SQLITE_PATH=/app/data/nmdiscordbot.sqlite3
ENV INGRESS_HOST=0.0.0.0
ENV INGRESS_PORT=8080
ENV PORT=8080
ENV APP_PUBLIC_URL=https://nmdiscordbot.bothost.ru

RUN addgroup --system app && adduser --system --ingroup app --home /app app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN mkdir -p /app/data/logs /app/data/backups && chown -R app:app /app

EXPOSE 8080

USER app

HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 CMD ["python", "-m", "nmbot.healthcheck", "--mode", "liveness"]

CMD ["python", "-m", "nmbot.main"]
