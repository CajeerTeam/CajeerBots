# Security Hardening

## API tokens
Use API_TOKENS_FILE and scoped tokens. Store only hashes.

## Release signatures
Use CAJEER_UPDATE_REQUIRE_SIGNATURE=true and CAJEER_UPDATE_PUBLIC_KEY.

## Webhooks
Set Telegram/VK webhook secrets and rate limits.

## Redis/PostgreSQL
Keep backends private, firewall-protected, and minimally privileged.

## Runtime catalog
Install only trusted catalog entries with sha256/signature.

## Backup and rollback
Run cajeer-bots db backup before migrations. Application rollback is not DB rollback.
