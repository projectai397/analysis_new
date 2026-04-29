# /trigger_backup_upload

Manually runs the same pipeline as the daily scheduler: **MongoDB dump → folder → ZIP → upload to S3**. Telegram-style success/failure messages are still sent via `NOTIFICATION_URL` → `/send_notifications` when upload runs (same as the scheduled upload job).

## Method

`POST`

## Header

- `X-Auth-Token`: must match server-side `STATIC_TOKEN` (same as `/send_notifications`)
- `Content-Type`: `application/json` (body can be empty `{}`)

## Rate limit

Same rules as `/send_notifications` (per IP, `API_RATE_LIMIT` per `RATE_WINDOW_SECONDS`).

## Body

Optional. No fields are required today. Example:

```json
{}
```

## Response

`202 Accepted` immediately; work runs in a background thread.

```json
{
  "status": "accepted",
  "message": "Backup and S3 upload started."
}
```

## Example

```bash
curl -X POST http://127.0.0.1:8015/trigger_backup_upload \
  -H "X-Auth-Token: YOUR_STATIC_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{}"
```

## Environment (same as scheduled backup/upload)

- `SOURCE_MONGO_URI`, `SOURCE_DB_NAME` (or DB name inferred from URI)
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `S3_BUCKET`
- Optional: `NOTIFICATION_URL`, `STATIC_TOKEN` for upload result messages
