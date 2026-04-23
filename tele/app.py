import os
import requests
import gspread
import boto3
import shutil
import subprocess
import glob
import logging
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
import calendar
from dotenv import load_dotenv
from flask import Flask, request, jsonify
import schedule
import time
from threading import Thread
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlparse
from botocore.exceptions import NoCredentialsError, ClientError

# ================= LOAD ENV =================
load_dotenv()

GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON")
SHEET_ID = os.getenv("SHEET_ID")
WEBSITE_SHEET_NAME = os.getenv("WEBSITE_SHEET_NAME", "website")  # Website sheet name
BILLING_SHEET_NAME = os.getenv("BILLING_SHEET_NAME", "billing_info")  # Billing info sheet name
API_RATE_LIMIT = int(os.getenv("API_RATE_LIMIT", 10))
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "10"))

if not all([GOOGLE_SA_JSON, SHEET_ID, BOT_TOKEN]):
    raise RuntimeError("Missing required .env values")

TG_API = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
logger = logging.getLogger(__name__)

# ================= FLASK APP =================
app = Flask(__name__)
rate_limit = defaultdict(int)
RATE_WINDOW_SECONDS = 60
rate_state = {}  # ip -> {"count": int, "window_start": float}
# ================= HELPERS =================
def send_telegram(chat_id: str, text: str):
    try:
        response = requests.post(
            TG_API,
            json={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",  # Added this line
                "disable_web_page_preview": True,
            },
            timeout=TIMEOUT,
        )
        print(f"Telegram response: {response.status_code}")
    except Exception as e:
        print(f"Error sending message: {str(e)}")


def send_broadcast(subscribers, message):
    for chat_id in subscribers:
        send_telegram(chat_id, message)


def check_website(url: str):
    try:
        r = requests.get(url, timeout=TIMEOUT, allow_redirects=True)
        if 200 <= r.status_code < 400:
            return True, f"HTTP {r.status_code}"
        return False, f"HTTP {r.status_code}"
    except requests.exceptions.RequestException:
        return False, "Connection error / timeout"


# ================= MAIN =================

# Store the last status and time of each website to track changes
website_status_cache = {}
billing_notification_cache = {}


def _infer_db_name(uri: str):
    if not uri:
        return None
    try:
        p = urlparse(uri)
        return p.path.lstrip("/") or None
    except Exception:
        return None


PRO_DB = os.getenv("SOURCE_DB_NAME") or _infer_db_name(os.getenv("SOURCE_MONGO_URI"))


def backup_mongo_to_archive(
    db_names: list[str],
    mongo_uri: str | None = None,
    out_root: str = "backups",
) -> dict:
    date_str = datetime.now().strftime("%Y-%m-%d")
    root = Path(out_root).resolve()
    dump_dir = root / date_str
    dump_dir.mkdir(parents=True, exist_ok=True)

    uri = mongo_uri or os.environ.get("SOURCE_MONGO_URI")
    if not uri:
        return {
            "ok": False,
            "date": date_str,
            "dump_dir": str(dump_dir),
            "archive_path": None,
            "used_format": None,
            "error": "Mongo URI not set (SOURCE_MONGO_URI).",
        }

    mongodump = shutil.which("mongodump")
    if not mongodump:
        return {
            "ok": False,
            "date": date_str,
            "dump_dir": str(dump_dir),
            "archive_path": None,
            "used_format": None,
            "error": "mongodump not found in PATH. Install MongoDB Database Tools.",
        }

    try:
        for db in db_names:
            cmd = [mongodump, f"--uri={uri}", f"--db={db}", f"--out={str(dump_dir)}"]
            logger.info("[backup] Running: %s", " ".join(cmd))
            subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        return {
            "ok": False,
            "date": date_str,
            "dump_dir": str(dump_dir),
            "archive_path": None,
            "used_format": None,
            "error": f"mongodump failed for db '{db}': {e}",
        }

    zip_base = root / date_str
    archive_path = zip_base.with_suffix(".zip")
    logger.info("[backup] Creating ZIP archive -> %s", archive_path)
    shutil.make_archive(str(zip_base), "zip", str(dump_dir))

    return {
        "ok": True,
        "date": date_str,
        "dump_dir": str(dump_dir),
        "archive_path": str(archive_path),
        "used_format": "zip",
        "error": None,
    }


def _s3_client():
    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    region = os.environ.get("AWS_REGION", "ap-south-1")
    if not (access_key and secret_key):
        raise RuntimeError("AWS creds not set (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY).")
    return boto3.client(
        "s3",
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )


def upload_backup_to_s3(
    date_str: str | None = None,
    out_root: str = "backups",
    bucket: str | None = None,
    s3_prefix: str = "mongo_backup",
) -> dict:
    date_str = date_str or datetime.now().strftime("%Y-%m-%d")
    root = Path(out_root).resolve()
    archive_path = root / f"{date_str}.zip"

    if not archive_path.exists():
        return {
            "ok": False,
            "date": date_str,
            "archive_path": None,
            "bucket": bucket,
            "key": None,
            "error": f"No ZIP archive found for {date_str} at {archive_path}",
        }

    bucket = bucket or os.environ.get("S3_BUCKET")
    if not bucket:
        return {
            "ok": False,
            "date": date_str,
            "archive_path": str(archive_path),
            "bucket": None,
            "key": None,
            "error": "S3 bucket not set (S3_BUCKET)",
        }

    key = f"{s3_prefix}/{archive_path.name}"

    try:
        s3 = _s3_client()
        logger.info("[backup] Uploading %s -> s3://%s/%s", archive_path, bucket, key)
        s3.upload_file(str(archive_path), bucket, key)

        notif_url = os.environ.get("NOTIFICATION_URL")
        auth_token = os.environ.get("STATIC_TOKEN")
        if notif_url and auth_token:
            headers = {"X-Auth-Token": auth_token, "Content-Type": "application/json"}
            payload = {"message": "database upload complete"}
            try:
                notif_res = requests.post(
                    notif_url,
                    json=payload,
                    headers=headers,
                    timeout=60,
                )
                if notif_res.status_code == 200:
                    logger.info("Database backup upload complete notification sent.")
                else:
                    logger.warning(
                        "Notification failed (Status %s): %s",
                        notif_res.status_code,
                        notif_res.text,
                    )
            except requests.exceptions.Timeout:
                logger.error("Notification timeout at %s.", notif_url)
            except requests.exceptions.RequestException as req_e:
                logger.error("Notification network error: %s", req_e)
        else:
            logger.error("Missing NOTIFICATION_URL or STATIC_TOKEN in .env")

        return {
            "ok": True,
            "date": date_str,
            "archive_path": str(archive_path),
            "bucket": bucket,
            "key": key,
            "error": None,
        }
    except FileNotFoundError:
        return {
            "ok": False,
            "date": date_str,
            "archive_path": str(archive_path),
            "bucket": bucket,
            "key": key,
            "error": "Archive file not found",
        }
    except NoCredentialsError:
        return {
            "ok": False,
            "date": date_str,
            "archive_path": str(archive_path),
            "bucket": bucket,
            "key": key,
            "error": "AWS credentials not found/invalid",
        }
    except ClientError as e:
        return {
            "ok": False,
            "date": date_str,
            "archive_path": str(archive_path),
            "bucket": bucket,
            "key": key,
            "error": f"AWS error: {e}",
        }
    except Exception as e:
        return {
            "ok": False,
            "date": date_str,
            "archive_path": str(archive_path),
            "bucket": bucket,
            "key": key,
            "error": str(e),
        }

def send_notifications():
    print("Starting to send notifications...")
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    creds = Credentials.from_service_account_file(GOOGLE_SA_JSON, scopes=scopes)
    gc = gspread.authorize(creds)
    website_ws = gc.open_by_key(SHEET_ID).worksheet(WEBSITE_SHEET_NAME)
    billing_ws = gc.open_by_key(SHEET_ID).worksheet(BILLING_SHEET_NAME)

    # Fetching website details and subscribers
    website_rows = website_ws.get_all_records()
    websites = []
    subscribers = set()

    for r in website_rows:
        name = str(r.get("name", "")).strip()
        url = str(r.get("url", "")).strip()
        sub = str(r.get("subscriber", "")).strip()

        if name and url:
            websites.append((name, url))

        if sub:
            subscribers.add(sub)

    # Website status check notifications
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    for name, url in websites:
        up, detail = check_website(url)

        # Get last status and time of first "down" notification
        last_status = website_status_cache.get(url, {}).get("status", None)
        last_down_time = website_status_cache.get(url, {}).get("time", None)

        # If website was down and time passed is greater than 10 minutes, send a new "down" notification
        if last_status == "down" and last_down_time and (datetime.now() - last_down_time).total_seconds() >= 600:
            if not up:
                msg = f"❌ <b>{name}</b> is STILL DOWN\n{url}\nChecked: {now}"
                for chat_id in subscribers:
                    send_telegram(chat_id, msg)
                website_status_cache[url]["time"] = datetime.now()  # Update the time of last down notification
        elif last_status != "down" and not up:
            # Send the first "down" notification if it wasn't previously down
            msg = f"❌ <b>{name}</b> is NOW DOWN\n{url}\nChecked: {now}"
            for chat_id in subscribers:
                send_telegram(chat_id, msg)
            website_status_cache[url] = {"status": "down", "time": datetime.now()}
        elif last_status == "down" and up:
            # If website is back up, send the "working" notification
            msg = f"✅ <b>{name}</b> is NOW WORKING\n{url}\nChecked: {now}"
            for chat_id in subscribers:
                send_telegram(chat_id, msg)
            website_status_cache[url] = {"status": "working", "time": None}  # Reset status when it's working

    # --- BILLING SECTION ---
    billing_rows = billing_ws.get_all_records()

    # Normalize today to midnight
    today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    one_day_ahead = today + timedelta(days=1)
    two_days_ahead = today + timedelta(days=2)

    # Track time for when each notification is sent for each provider
    for billing_row in billing_rows:
        # Match uppercase headers from your sheet
        provider = str(billing_row.get("PROVIDER", "")).strip()
        billing_date_str = str(billing_row.get("BILLING DATE", "")).strip()

        if not provider or not billing_date_str:
            continue

        try:
            # Extract day number
            day_num_str = "".join(filter(str.isdigit, billing_date_str))
            if not day_num_str:
                continue
            
            day_num = int(day_num_str)
            _, last_day = calendar.monthrange(today.year, today.month)
            day_num = min(day_num, last_day)
            billing_day = today.replace(day=day_num)

            # Check if 8 hours have passed since the last reminder for this billing day
            last_reminder_time = billing_notification_cache.get(provider, {}).get(billing_day.strftime('%Y-%m-%d'), None)
            if last_reminder_time and (datetime.now() - last_reminder_time).total_seconds() < 28800:
                continue  # Skip if a notification was already sent in the last 8 hours

            # Formatting the text with HTML <b> tags
            formatted_date = f"<b>{billing_day.strftime('%d %B, %Y')}</b>"
            provider_bold = f"<b>{provider}</b>"

            # Reminders with bolded status and date
            if billing_day == today:
                msg = f"🔔 Reminder: The billing date for {provider_bold} is <b>TODAY</b>, {formatted_date}"
                for chat_id in subscribers:
                    send_telegram(chat_id, msg)

            elif billing_day == one_day_ahead:
                msg = f"⏳ Reminder: The billing date for {provider_bold} is <b>TOMORROW</b>, {formatted_date}"
                for chat_id in subscribers:
                    send_telegram(chat_id, msg)

            elif billing_day == two_days_ahead:
                msg = f"🗓️ Reminder: The billing date for {provider_bold} is in <b>2 days</b>, {formatted_date}"
                for chat_id in subscribers:
                    send_telegram(chat_id, msg)
            else:
                continue

            billing_notification_cache.setdefault(provider, {})[billing_day.strftime('%Y-%m-%d')] = datetime.now()

        except Exception:
            print(f"Skipping invalid billing date for {provider}: {billing_date_str}")
            continue

    print("Notifications sent.")


# ================= API ENDPOINT =================
@app.route('/send_notifications', methods=['POST'])
def trigger_success_notification():
    # 1. Token Verification
    provided_token = request.headers.get("X-Auth-Token")
    secret_token = os.getenv("STATIC_TOKEN")

    if not provided_token or provided_token != secret_token:
        print("Unauthorized access attempt: Invalid Token")
        return jsonify({"error": "Unauthorized"}), 401

    print("API hit to send success notification...")

    # 2. Rate limiting logic (non-blocking, time window; no sleep)
    ip = request.remote_addr or "unknown"
    now = time.time()

    st = rate_state.get(ip)
    if not st or (now - st["window_start"]) >= RATE_WINDOW_SECONDS:
        st = {"count": 0, "window_start": now}

    st["count"] += 1
    rate_state[ip] = st

    if st["count"] > API_RATE_LIMIT:
        return jsonify({"error": "Rate limit exceeded."}), 429

    # 3. Fetching subscribers
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(GOOGLE_SA_JSON, scopes=scopes)
    gc = gspread.authorize(creds)
    website_ws = gc.open_by_key(SHEET_ID).worksheet(WEBSITE_SHEET_NAME)

    website_rows = website_ws.get_all_records()
    subscribers = {str(r.get("subscriber", "")).strip() for r in website_rows if r.get("subscriber")}

    if not subscribers:
        return jsonify({"message": "No subscribers found."}), 404

    # 4. Get Message Body
    data = request.get_json(silent=True) or {}
    message = data.get('message', "✅ <b>Notification sent successfully!</b>")

    # 5. Send Notification in background (instant response)
    t = Thread(target=send_broadcast, args=(subscribers, message), daemon=True)
    t.start()

    # Respond immediately
    return jsonify(
        {
            "status": "accepted",
            "message": "Notification dispatch started.",
            "subscriber_count": len(subscribers),
        }
    ), 202


# ================= SCHEDULE =================
def check_websites_periodically():
    print("Checking websites periodically...")  # Debugging line
    send_notifications()  # This will run your website checking function

# Schedule the job every 1 minute (to check websites)
schedule.every(1).minute.do(check_websites_periodically)


def _run_async(fn, label: str = "job"):
    def runner():
        try:
            fn()
        except Exception:
            logger.exception("[%s] failed", label)
    t = Thread(target=runner, daemon=True)
    t.start()


def _daily_backup_job():
    try:
        db_name = PRO_DB or os.getenv("SOURCE_DB_NAME")
        if not db_name:
            logger.error("Daily backup failed -> SOURCE_DB_NAME (or URI DB name) not resolved")
            return
        res = backup_mongo_to_archive([db_name], out_root="backups")
        if res.get("ok"):
            logger.info("Daily backup done -> %s (%s)", res["archive_path"], res["used_format"])
        else:
            logger.error("Daily backup failed -> %s", res.get("error"))
    except Exception as e:
        logger.exception("Daily backup crashed: %s", e)


def _daily_upload_job():
    try:
        res_up = upload_backup_to_s3(
            date_str=None,
            out_root="backups",
            bucket=None,
            s3_prefix="mongo_backup",
        )
        if res_up.get("ok"):
            logger.info("Daily upload done -> s3://%s/%s", res_up["bucket"], res_up["key"])
        else:
            logger.error("Daily upload failed -> %s", res_up.get("error"))
    except Exception as e:
        logger.exception("Daily upload crashed: %s", e)


def _daily_cleanup_job():
    try:
        backup_dir = "backups"
        deleted = 0
        for f in glob.glob(f"{backup_dir}/*"):
            try:
                if os.path.isdir(f):
                    shutil.rmtree(f)
                else:
                    os.remove(f)
                deleted += 1
            except Exception as e:
                logger.warning("Could not delete %s: %s", f, e)
        logger.info("Daily cleanup done -> deleted %s files/folders from %s", deleted, backup_dir)
    except Exception as e:
        logger.exception("Daily cleanup crashed: %s", e)


schedule.every().day.at("00:00").do(lambda: _run_async(_daily_backup_job, "backup-03"))
schedule.every().day.at("00:10").do(lambda: _run_async(_daily_upload_job, "upload-04"))
schedule.every().day.at("00:15").do(lambda: _run_async(_daily_cleanup_job, "cleanup-05"))

def run_schedule():
    while True:
        schedule.run_pending()
        time.sleep(1)  # Sleep for a while to prevent CPU overload


# ================= RUN THE FLASK SERVER =================
if __name__ == "__main__":
    # Run Flask in a separate thread to allow scheduling to work concurrently
    schedule_thread = Thread(target=run_schedule)
    schedule_thread.start()

    # Start the Flask API server
    app.run(host="0.0.0.0", port=8015)

# import os
# import requests
# import gspread
# from google.oauth2.service_account import Credentials
# from datetime import datetime, timedelta
# from dotenv import load_dotenv
# from flask import Flask, request, jsonify
# import schedule
# import time
# from threading import Thread
# from collections import defaultdict

# # ================= LOAD ENV =================
# load_dotenv()

# GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON")
# SHEET_ID = os.getenv("SHEET_ID")
# WEBSITE_SHEET_NAME = os.getenv("WEBSITE_SHEET_NAME", "website")  # Website sheet name
# BILLING_SHEET_NAME = os.getenv("BILLING_SHEET_NAME", "billing_info")  # Billing info sheet name
# API_RATE_LIMIT = int(os.getenv("API_RATE_LIMIT", 10))
# BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
# TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "10"))

# if not all([GOOGLE_SA_JSON, SHEET_ID, BOT_TOKEN]):
#     raise RuntimeError("Missing required .env values")

# TG_API = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

# # ================= FLASK APP =================
# app = Flask(__name__)
# rate_limit = defaultdict(int)
# RATE_WINDOW_SECONDS = 60
# rate_state = {}  # ip -> {"count": int, "window_start": float}

# def send_broadcast(subscribers, message):
#     for chat_id in subscribers:
#         send_telegram(chat_id, message)

# # ================= HELPERS =================
# def send_telegram(chat_id: str, text: str):
#     try:
#         response = requests.post(
#             TG_API,
#             json={
#                 "chat_id": chat_id,
#                 "text": text,
#                 "parse_mode": "HTML",  # Added this line
#                 "disable_web_page_preview": True,
#             },
#             timeout=TIMEOUT,
#         )
#         print(f"Telegram response: {response.status_code}")
#     except Exception as e:
#         print(f"Error sending message: {str(e)}")


# def check_website(url: str):
#     try:
#         r = requests.get(url, timeout=TIMEOUT, allow_redirects=True)
#         if 200 <= r.status_code < 400:
#             return True, f"HTTP {r.status_code}"
#         return False, f"HTTP {r.status_code}"
#     except requests.exceptions.RequestException:
#         return False, "Connection error / timeout"


# # ================= MAIN =================

# # Store the last status and time of each website to track changes
# website_status_cache = {}
# billing_notification_cache = {}

# def send_notifications():
#     print("Starting to send notifications...")
#     scopes = [
#         "https://www.googleapis.com/auth/spreadsheets",
#         "https://www.googleapis.com/auth/drive",
#     ]

#     creds = Credentials.from_service_account_file(GOOGLE_SA_JSON, scopes=scopes)
#     gc = gspread.authorize(creds)
#     website_ws = gc.open_by_key(SHEET_ID).worksheet(WEBSITE_SHEET_NAME)
#     billing_ws = gc.open_by_key(SHEET_ID).worksheet(BILLING_SHEET_NAME)

#     # Fetching website details and subscribers
#     website_rows = website_ws.get_all_records()
#     websites = []
#     subscribers = set()

#     for r in website_rows:
#         name = str(r.get("name", "")).strip()
#         url = str(r.get("url", "")).strip()
#         sub = str(r.get("subscriber", "")).strip()

#         if name and url:
#             websites.append((name, url))

#         if sub:
#             subscribers.add(sub)

#     # Website status check notifications
#     now = datetime.now().strftime("%Y-%m-%d %H:%M")
#     for name, url in websites:
#         up, detail = check_website(url)

#         # Get last status and time of first "down" notification
#         last_status = website_status_cache.get(url, {}).get("status", None)
#         last_down_time = website_status_cache.get(url, {}).get("time", None)

#         # If website was down and time passed is greater than 10 minutes, send a new "down" notification
#         if last_status == "down" and last_down_time and (datetime.now() - last_down_time).total_seconds() >= 600:
#             if not up:
#                 msg = f"❌ <b>{name}</b> is STILL DOWN\n{url}\nChecked: {now}"
#                 for chat_id in subscribers:
#                     send_telegram(chat_id, msg)
#                 website_status_cache[url]["time"] = datetime.now()  # Update the time of last down notification
#         elif last_status != "down" and not up:
#             # Send the first "down" notification if it wasn't previously down
#             msg = f"❌ <b>{name}</b> is NOW DOWN\n{url}\nChecked: {now}"
#             for chat_id in subscribers:
#                 send_telegram(chat_id, msg)
#             website_status_cache[url] = {"status": "down", "time": datetime.now()}
#         elif last_status == "down" and up:
#             # If website is back up, send the "working" notification
#             msg = f"✅ <b>{name}</b> is NOW WORKING\n{url}\nChecked: {now}"
#             for chat_id in subscribers:
#                 send_telegram(chat_id, msg)
#             website_status_cache[url] = {"status": "working", "time": None}  # Reset status when it's working

#     # --- BILLING SECTION ---
#     billing_rows = billing_ws.get_all_records()

#     # Normalize today to midnight
#     today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
#     one_day_ahead = today + timedelta(days=1)
#     two_days_ahead = today + timedelta(days=2)

#     # Track time for when each notification is sent for each provider
#     for billing_row in billing_rows:
#         # Match uppercase headers from your sheet
#         provider = str(billing_row.get("PROVIDER", "")).strip()
#         billing_date_str = str(billing_row.get("BILLING DATE", "")).strip()

#         if not provider or not billing_date_str:
#             continue

#         try:
#             # Extract day number
#             day_num_str = "".join(filter(str.isdigit, billing_date_str))
#             if not day_num_str:
#                 continue
            
#             day_num = int(day_num_str)
#             billing_day = today.replace(day=day_num)

#             # Check if 8 hours have passed since the last reminder for this billing day
#             last_reminder_time = billing_notification_cache.get(provider, {}).get(billing_day.strftime('%Y-%m-%d'), None)
#             if last_reminder_time and (datetime.now() - last_reminder_time).total_seconds() < 28800:
#                 continue  # Skip if a notification was already sent in the last 8 hours

#             # Formatting the text with HTML <b> tags
#             formatted_date = f"<b>{billing_day.strftime('%d %B, %Y')}</b>"
#             provider_bold = f"<b>{provider}</b>"

#             # Reminders with bolded status and date
#             if billing_day == today:
#                 msg = f"🔔 Reminder: The billing date for {provider_bold} is <b>TODAY</b>, {formatted_date}"
#                 for chat_id in subscribers:
#                     send_telegram(chat_id, msg)

#             elif billing_day == one_day_ahead:
#                 msg = f"⏳ Reminder: The billing date for {provider_bold} is <b>TOMORROW</b>, {formatted_date}"
#                 for chat_id in subscribers:
#                     send_telegram(chat_id, msg)

#             elif billing_day == two_days_ahead:
#                 msg = f"🗓️ Reminder: The billing date for {provider_bold} is in <b>2 days</b>, {formatted_date}"
#                 for chat_id in subscribers:
#                     send_telegram(chat_id, msg)

#             # Update the time of last reminder for this provider and date
#             billing_notification_cache.setdefault(provider, {})[billing_day.strftime('%Y-%m-%d')] = datetime.now()

#         except Exception as e:
#             print(f"Skipping invalid billing date for {provider}: {billing_date_str}")
#             continue

#     print("Notifications sent.")


# # ================= API ENDPOINT =================
# @app.route('/send_notifications', methods=['POST'])
# def trigger_success_notification():
#     # 1. Token Verification
#     provided_token = request.headers.get("X-Auth-Token")
#     secret_token = os.getenv("STATIC_TOKEN")

#     if not provided_token or provided_token != secret_token:
#         print("Unauthorized access attempt: Invalid Token")
#         return jsonify({"error": "Unauthorized"}), 401

#     print("API hit to send success notification...")

#     # 2. Rate limiting logic (non-blocking, time window; no sleep)
#     ip = request.remote_addr or "unknown"
#     now = time.time()

#     st = rate_state.get(ip)
#     if not st or (now - st["window_start"]) >= RATE_WINDOW_SECONDS:
#         st = {"count": 0, "window_start": now}

#     st["count"] += 1
#     rate_state[ip] = st

#     if st["count"] > API_RATE_LIMIT:
#         return jsonify({"error": "Rate limit exceeded."}), 429

#     # 3. Fetching subscribers (unchanged)
#     scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
#     creds = Credentials.from_service_account_file(GOOGLE_SA_JSON, scopes=scopes)
#     gc = gspread.authorize(creds)
#     website_ws = gc.open_by_key(SHEET_ID).worksheet(WEBSITE_SHEET_NAME)

#     website_rows = website_ws.get_all_records()
#     subscribers = {str(r.get("subscriber", "")).strip() for r in website_rows if r.get("subscriber")}

#     if not subscribers:
#         return jsonify({"message": "No subscribers found."}), 404

#     # 4. Get Message Body (unchanged)
#     data = request.get_json(silent=True) or {}
#     message = data.get('message', "✅ <b>Notification sent successfully!</b>")

#     # 5. Send Notification in background (instant response)
#     t = Thread(target=send_broadcast, args=(subscribers, message), daemon=True)
#     t.start()

#     # respond immediately
#     return jsonify({
#         "status": "accepted",
#         "message": "Notification dispatch started.",
#         "subscriber_count": len(subscribers)
#     }), 202


# # ================= SCHEDULE =================
# def check_websites_periodically():
#     print("Checking websites periodically...")  # Debugging line
#     send_notifications()  # This will run your website checking function

# # Schedule the job every 1 minute (to check websites)
# schedule.every(1).minute.do(check_websites_periodically)

# def run_schedule():
#     while True:
#         schedule.run_pending()
#         time.sleep(1)  # Sleep for a while to prevent CPU overload


# # ================= RUN THE FLASK SERVER =================
# if __name__ == "__main__":
#     # Run Flask in a separate thread to allow scheduling to work concurrently
#     schedule_thread = Thread(target=run_schedule)
#     schedule_thread.start()

#     # Start the Flask API server
#     app.run(host="0.0.0.0", port=8015)
