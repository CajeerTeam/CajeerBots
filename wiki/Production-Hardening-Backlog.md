# Production hardening backlog

В этот patch-pack добавлены практические изменения для пунктов 1–15:

1. `RBAC_BACKEND=cache|postgres|hybrid` и DB-backed runtime reader.
2. Redis EventBus retry через delayed sorted set и terminal DLQ.
3. Bridge больше не пишет transient ошибки сразу в DLQ.
4. `/readyz` разделяет production errors и dev warnings.
5. Добавлен общий `RuntimeDbResources` для постепенной централизации SQLAlchemy engine lifecycle.
6. API получил request validation по `RouteSpec.request_schema`.
7. Добавлена модель доверенных reverse proxy: `TRUSTED_PROXY_CIDRS`, `REAL_IP_HEADER`.
8. Разделены direct/gateway webhook env-профили.
9. Добавлен docker compose integration profile и Redis/Postgres contract tests.
10. Добавлен adapter conformance contract.
11. Distributed registry/queue получили heartbeat, TTL, leases и ack/nack.
12. Добавлена schema plugin package и enable/disable lifecycle в catalog lock.
13. Добавлена минимальная русская web admin panel на `/admin`.
14. Добавлены JSON logs, Prometheus alerts и Grafana dashboard.
15. Ужесточены systemd/nginx шаблоны.
