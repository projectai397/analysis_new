import logging
import os
import socket
from datetime import datetime, timedelta, timezone

import requests

from src.config import config

logger = logging.getLogger(__name__)

_IST = timezone(timedelta(hours=5, minutes=30))


def _human_bytes(num_bytes: int | None) -> str:
    if num_bytes is None:
        return "unknown"
    try:
        n = float(num_bytes)
    except Exception:
        return "unknown"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024.0 or unit == "TB":
            if unit == "B":
                return f"{int(n)} {unit}"
            return f"{n:.2f} {unit}"
        n /= 1024.0
    return "unknown"


def _host_name() -> str:
    try:
        return socket.gethostname()
    except Exception:
        return "unknown-host"


def post_notification(message: str) -> bool:
    url = os.environ.get("NOTIFICATION_URL") or getattr(config, "NOTIFICATION_URL", "") or ""
    token = os.environ.get("STATIC_TOKEN") or getattr(config, "STATIC_TOKEN", "") or ""
    if not url or not token:
        logger.warning("NOTIFICATION_URL or STATIC_TOKEN not set, skipping notification")
        return False

    try:
        response = requests.post(
            url,
            json={"message": message},
            headers={
                "X-Auth-Token": token,
                "Content-Type": "application/json",
            },
            timeout=60,
        )
        if response.status_code in (200, 202):
            logger.info("Notification sent to NOTIFICATION_URL")
            return True
        logger.warning("Notification failed (status=%s): %s", response.status_code, response.text)
        return False
    except requests.exceptions.RequestException as exc:
        logger.warning("Notification error: %s", exc)
        return False


def db_upload_success_message(size_bytes: int | None) -> str:
    ts = datetime.now(_IST).strftime("%Y-%m-%d %H:%M:%S %z")
    return (
        "✅ <b>Database upload complete</b>\n"
        f"Time: {ts}\n"
        f"Host: {_host_name()}\n"
        f"Size: {_human_bytes(size_bytes)}"
    )
