import html
import logging
import re
from typing import Optional, Tuple

import httpx

from src.config import config
from src.telegram.notify_external import post_cron_notification

logger = logging.getLogger(__name__)

NSE_POSITION_LIMITS_URL = "https://www.nseclearing.in/risk-management/equity-derivatives/position-limits"
NSE_SECBAN_CSV_URL = "https://nsearchives.nseindia.com/content/fo/fo_secban.csv"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}
CSV_REFERER = "https://www.nseclearing.in/"


def _extract_csv_url_from_html(html_text: str) -> Optional[str]:
    m = re.search(r'href="(https://nsearchives\.nseindia\.com/content/fo/fo_secban\.csv)"', html_text)
    return m.group(1) if m else None


def fetch_page() -> Optional[str]:
    try:
        with httpx.Client(timeout=15.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
            r = client.get(NSE_POSITION_LIMITS_URL)
            r.raise_for_status()
            return r.text
    except Exception as e:
        logger.warning(f"fetch_page: {e}")
        return None


def download_csv(csv_url: str) -> Optional[str]:
    try:
        headers = {**DEFAULT_HEADERS, "Referer": CSV_REFERER}
        with httpx.Client(timeout=15.0, follow_redirects=True, headers=headers) as client:
            r = client.get(csv_url)
            r.raise_for_status()
            return r.text
    except Exception as e:
        logger.warning(f"download_csv: {e}")
        return None


def csv_has_data(content: str) -> bool:
    if not content or not content.strip():
        return False
    if "Securities in Ban" not in content and "Security in Ban" not in content:
        return False
    lines = [ln.strip() for ln in content.strip().splitlines() if ln.strip()]
    for line in lines:
        if "Securities in Ban" in line or "Security in Ban" in line:
            continue
        if re.search(r"\d+\s+[A-Z0-9]+", line):
            return True
    return False


def count_script_lines(content: str) -> int:
    if not content or not content.strip():
        return 0
    count = 0
    for line in content.strip().splitlines():
        line = line.strip()
        if not line or "Securities in Ban" in line or "Security in Ban" in line:
            continue
        if re.search(r"\d+\s+[A-Z0-9]+", line):
            count += 1
    return count


def get_superadmin_chat_ids():
    from src.config import notification
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


def send_secban_to_superadmins(csv_content: str) -> Tuple[int, bool]:
    chat_ids = get_superadmin_chat_ids()
    if not chat_ids:
        logger.info("No superadmin chat IDs for NSE secban notification")
        return 0, False
    if not config.TELEGRAM_BOT_TOKEN:
        logger.warning("TELEGRAM_BOT_TOKEN not set, skipping NSE secban notification")
        return 0, False

    api_base = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}"
    max_msg_len = 4096
    sent = 0

    with httpx.Client(timeout=15.0) as client:
        for chat_id in chat_ids:
            try:
                if len(csv_content) <= max_msg_len:
                    escaped = html.escape(csv_content)
                    r = client.post(
                        f"{api_base}/sendMessage",
                        json={
                            "chat_id": chat_id,
                            "text": f"<b>Securities in Ban (F&amp;O) – next trade date</b>\n\n<pre>{escaped}</pre>",
                            "parse_mode": "HTML",
                        },
                    )
                else:
                    r = client.post(
                        f"{api_base}/sendDocument",
                        data={"chat_id": chat_id, "caption": "Securities in Ban (F&O) – next trade date"},
                        files={"document": ("fo_secban.csv", csv_content.encode("utf-8"), "text/csv")},
                    )
                r.raise_for_status()
                sent += 1
            except Exception as e:
                logger.warning(f"NSE secban notify chat_id={chat_id}: {e}")
    return sent, sent > 0


def run_daily_nse_secban() -> bool:
    html = fetch_page()
    csv_url = _extract_csv_url_from_html(html) if html else None
    if not csv_url:
        csv_url = NSE_SECBAN_CSV_URL
    content = download_csv(csv_url)
    if not content or not csv_has_data(content):
        logger.info("NSE secban CSV empty or no data, skipping notification")
        post_cron_notification("job done 0 scripts ban notification sent")
        return False
    script_count = count_script_lines(content)
    sent, ok = send_secban_to_superadmins(content)
    if ok:
        logger.info(f"NSE secban notification sent to {sent} superadmin(s)")
    post_cron_notification(f"job done {script_count} scripts ban notification sent")
    return ok
