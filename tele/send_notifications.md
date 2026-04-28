# /send_notifications

## Header
- `X-Auth-Token`: API token (must match the server-side `STATIC_TOKEN` env var)
- `Content-Type`: `application/json`

## Body
JSON object (optional keys):

- `message` (string): HTML-formatted message to broadcast to all subscribers.
  - Default: `✅ <b>Notification sent successfully!</b>`

Example `message` body:
```json
{
  "message": "✅ <b>Notification sent successfully!</b>"
}
```

Example (database upload success):
```json
{
  "message": "✅ <b>Database upload complete</b>\nTime: 2026-04-28 11:29:03 +0530\nHost: server-01\nSize: 2.50 GB\nS3: <code>s3://my-bucket/mongo_backup/2026-04-28.zip</code>"
}
```

Example (database upload failure):
```json
{
  "message": "🔴 <b>Database upload failed</b>\nTime: 2026-04-28 11:29:03 +0530\nHost: server-01\nError: <code>AWS credentials not found/invalid</code>\nArchive: <code>/var/www/html/analysis_new/tele/backups/2026-04-28.zip</code>"
}
```
