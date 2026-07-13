import logging
import html
from datetime import datetime, timezone
from typing import List, Optional

import httpx
from dateutil.parser import isoparse
from dateutil.relativedelta import relativedelta

from src.config import config, users, notification, ADMIN_ROLE_ID

logger = logging.getLogger(__name__)


def _get_superadmin_chat_ids() -> List[int]:
    try:
        chat_ids = []
        for doc in notification.find({"role": "superadmin"}):
            for cid in doc.get("chat_ids", []):
                try:
                    chat_ids.append(int(cid))
                except (ValueError, TypeError):
                    continue
        return list(set(chat_ids))
    except Exception as e:
        logger.error(f"get_superadmin_chat_ids: {e}")
        return []


def _created_to_date(created_at) -> Optional[datetime]:
    if created_at is None:
        return None
    if hasattr(created_at, "date"):
        return created_at if getattr(created_at, "tzinfo", None) else created_at.replace(tzinfo=timezone.utc)
    if isinstance(created_at, str):
        try:
            return isoparse(created_at)
        except (ValueError, TypeError):
            return None
    return None


def _send_saas_notification(chat_ids: List[int], user_name: str, month_num: int, amount) -> int:
    if not chat_ids or not config.TELEGRAM_BOT_TOKEN:
        return 0
    month_text = "1 month complete" if month_num == 1 else f"{month_num} months complete"
    amount_str = str(amount) if amount is not None else "0"
    message = (
        f"📋 <b>SaaS milestone</b>\n"
        f"👤 <b>userName:</b> {html.escape(user_name or '—')}\n"
        f"📅 <b>{month_text}</b>\n"
        f"💰 <b>amount:</b> {html.escape(amount_str)}"
    )
    api_url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    sent = 0
    with httpx.Client(timeout=10.0) as client:
        for chat_id in chat_ids:
            try:
                r = client.post(
                    api_url,
                    json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
                )
                r.raise_for_status()
                sent += 1
            except Exception as e:
                logger.warning(f"saas notify chat_id={chat_id}: {e}")
    return sent


def run_daily_saas_check() -> int:
    """
    Find admins with saas=True whose createdAt anniversary is today (1 month, 2 months, ...),
    and send one notification per such admin per completed month to all superadmins.
    Returns number of notifications sent.
    """
    try:
        today = datetime.now(timezone.utc).date()
        chat_ids = _get_superadmin_chat_ids()
        if not chat_ids:
            logger.debug("No superadmin chat IDs for SaaS notifications")
            return 0
        if not config.TELEGRAM_BOT_TOKEN:
            logger.warning("TELEGRAM_BOT_TOKEN not set, skipping SaaS check")
            return 0

        cursor = users.find(
            {"role": ADMIN_ROLE_ID, "saas": True, "createdAt": {"$exists": True}},
            {"userName": 1, "createdAt": 1, "saasAmount": 1},
        )
        total_sent = 0
        for doc in cursor:
            created_dt = _created_to_date(doc.get("createdAt"))
            if not created_dt:
                continue
            user_name = doc.get("userName") or "—"
            amount = doc.get("saasAmount")
            for month_num in range(1, 61):
                anniversary = (created_dt + relativedelta(months=month_num)).date()
                if anniversary > today:
                    break
                if anniversary == today:
                    sent = _send_saas_notification(chat_ids, user_name, month_num, amount)
                    total_sent += sent
                    logger.info(f"SaaS notification: userName={user_name}, {month_num} month(s), amount={amount}")
                    break
        return total_sent
    except Exception as e:
        logger.exception(f"run_daily_saas_check: {e}")
        return 0
