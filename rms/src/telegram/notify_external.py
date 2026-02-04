import logging
import httpx

from src.config import config

logger = logging.getLogger(__name__)


def post_cron_notification(message: str) -> bool:
    url = getattr(config, "NOTIFICATION_URL", None) or ""
    token = getattr(config, "STATIC_TOKEN", None) or ""
    if not url or not token:
        logger.debug("NOTIFICATION_URL or STATIC_TOKEN not set, skipping cron notification")
        return False
    try:
        with httpx.Client(timeout=30.0) as client:
            r = client.post(
                url,
                json={"message": message},
                headers={
                    "X-Auth-Token": token,
                    "Content-Type": "application/json",
                },
            )
            if r.status_code == 200:
                logger.info("Cron notification sent to NOTIFICATION_URL")
                return True
            logger.warning(f"Cron notification failed: {r.status_code} {r.text}")
            return False
    except Exception as e:
        logger.warning(f"Cron notification error: {e}")
        return False
