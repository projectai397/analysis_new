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
