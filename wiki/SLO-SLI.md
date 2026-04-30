# SLO / SLI

## SLI

- `cajeerbots_events_total`
- `cajeerbots_events_failed_total`
- `cajeerbots_delivery_failed_total`
- `cajeerbots_dead_letters_total`
- `cajeerbots_outbound_trace_failed_total`

## SLO

- Event ingest p95 < 500 ms в local/staging профиле.
- Delivery failed rate < 1% без внешней деградации адаптеров.
- Dead letters не растут дольше 15 минут без operator action.
- `/readyz` возвращает ok после штатного rolling restart.
