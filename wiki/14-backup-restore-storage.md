
# Backup/Restore & Storage Catalog

`0.13.0` добавляет рабочий слой резервного копирования CajeerBots.

## Что хранится

- `storage_objects` — каталог файлов local/S3 storage.
- `backup_artifacts` — backup artifacts в формате `cajeerbots.backup.bundle.v1`.
- `backup_restore_runs` — история dry-run и apply restore.

## Формат backup bundle

Backup хранится как JSON-документ:

```json
{
  "kind": "cajeerbots.backup.bundle",
  "version": 1,
  "format": "cajeerbots.backup.bundle.v1",
  "manifest": {
    "payloadSha256": "...",
    "includedResources": ["settings", "roles"]
  },
  "payload": {}
}
```

Проверяется sha256 всего объекта и sha256 payload.

## CLI

```bash
cajeer storage diagnose
cajeer storage objects:list
cajeer backup create --community <communityId> --title "Перед обновлением"
cajeer backup verify <artifactId>
cajeer backup restore <artifactId> --community <communityId>
cajeer backup restore <artifactId> --community <communityId> --apply
```

## API

- `GET /api/storage/objects`
- `DELETE /api/storage/objects/:id`
- `GET /api/backups`
- `POST /api/communities/:communityId/backups`
- `POST /api/backups/:id/verify`
- `POST /api/backups/:id/restore`
- `GET /api/backups/restore-runs`

## Storage drivers

Используется существующая абстракция `@cajeerbots/storage`:

- `STORAGE_DRIVER=local`
- `STORAGE_DRIVER=s3`

Для S3-compatible storage нужны `S3_ENDPOINT`, `S3_BUCKET`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`.
