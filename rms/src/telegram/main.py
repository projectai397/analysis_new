import asyncio
import json
import re
import csv
from pathlib import Path
from io import StringIO
from difflib import SequenceMatcher
from zoneinfo import ZoneInfo
import threading
from typing import Dict, Any, List, Tuple
import logging
import time
import html
from datetime import datetime, timedelta, timezone
import os
import logging
import httpx
from bson import ObjectId
import schedule
from telegram import BotCommand
from telegram import Update
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.ext import CallbackQueryHandler
from telegram.ext import CallbackContext
from telegram.constants import BotCommandScopeType
from telegram import BotCommandScopeDefault, BotCommandScopeAllPrivateChats
from telegram.ext import (
    ApplicationBuilder,
    ConversationHandler,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)
# 🔹 REVISED IMPORT: Only import the necessary external functions
from src.telegram.notification import ( 
    send_message_to_chats,
    subscribe_user, unsubscribe_user,
    get_subscribed_parents   # Kept from original code
)
from .session_store import remember_bot_message_from_message, start_session_timer
from src.config import config, users, notification, positions ,tele_notification,SUPERADMIN_ROLE_ID, ADMIN_ROLE_ID, MASTER_ROLE_ID
from .summarize import process_all_documents
from src.helpers.hierarchy_service import (
    get_admins_for_superadmin,
    get_masters_for_superadmin,
    get_users_for_superadmin,
    get_masters_for_admin,
    get_users_for_admin,
    get_users_for_master,
)

# 🔹 NEW: use the shared session store
from . import session_store


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# ============================================================
#  SESSION / SHARED HELPERS
# ============================================================

ASK_USERNAME, ASK_PASSWORD = range(2)

# NOTE: these dicts are now unused, but kept so nothing else breaks.
USER_TOKENS: Dict[int, str] = {}
USER_INFO: Dict[int, dict] = {}
USER_SESSION_EXPIRES: Dict[int, float] = {}

SESSION_TIMEOUT_SECONDS = 600 

def build_login_payload(login_id: str, password: str) -> dict:
    raw = login_id.strip()
    digits = raw[1:] if raw.startswith("+") else raw
    if digits.isdigit() and 8 <= len(digits) <= 15:
        return {"phone": raw, "password": password}
    return {"username": raw, "password": password}

def load_bots_config(json_path: str) -> list[dict]:
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("bots.json must contain a JSON array of bot objects")

    required = {"token", "name", "logo_path"}
    for i, item in enumerate(data):
        if not isinstance(item, dict):
            raise ValueError(f"bots.json item #{i} must be an object")
        missing = required - set(item.keys())
        if missing:
            raise ValueError(f"bots.json item #{i} missing keys: {sorted(missing)}")
        
        if "url" in item and item["url"] is not None:
            if not isinstance(item["url"], str) or not item["url"].strip():
                raise ValueError(f"bots.json item #{i} has invalid 'url' (must be a non-empty string)")

    return data

def _normalize_oid_str(x) -> str | None:
    """
    Supports:
    - ObjectId("...")        -> "..."
    - {"$oid": "..."}        -> "..."
    - "..."                  -> "..."
    """
    if not x:
        return None
    if isinstance(x, ObjectId):
        return str(x)
    if isinstance(x, dict) and "$oid" in x:
        return str(x["$oid"])
    return str(x)

def role_name_from_user(user: dict) -> str:
    if not user:
        return ""

    # Check the ObjectId stored in the 'role' field
    role_id = user.get("role")
    
    # Ensure role_id is an ObjectId before comparison
    if isinstance(role_id, dict) and '$oid' in role_id:
        role_id = ObjectId(role_id['$oid'])
    
    if role_id == SUPERADMIN_ROLE_ID:
        return "superadmin"
    elif role_id == ADMIN_ROLE_ID:
        return "admin"
    elif role_id == MASTER_ROLE_ID:
        return "master"
    else:
        # Fallback to the original logic if the role is a string/other
        rn = user.get("role_name") or user.get("role") or ""
        return str(rn).lower()

def get_trading_button_from_context(context: ContextTypes.DEFAULT_TYPE) -> InlineKeyboardButton | None:
    url = (context.application.bot_data.get("trading_url") or "").strip()
    if not url:
        return None
    return InlineKeyboardButton("▶️ Start Trading", web_app=WebAppInfo(url=url))

async def safe_delete_message(bot, chat_id: int, message_id: int):
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass

def mark_skip_faq(context: ContextTypes.DEFAULT_TYPE) -> None:
    # Skip FAQ handler for the next non-command text update (used for username/password)
    context.user_data["skip_next_faq"] = True

def get_user_by_id(user_id: ObjectId):
    # Assuming you're using MongoDB and pymongo
    user = users.find_one({"_id": ObjectId(user_id)})
    return user


def clear_session(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Wrapper around the shared session_store.clear_session
    so existing imports from main.py still work.
    """
    session_store.clear_session(update, context)


def get_logged_in(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Wrapper around the shared session_store.get_logged_in.
    All commands now read the SAME session data.
    """
    return session_store.get_logged_in(update, context)


async def require_login(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "⚠ You must <b>login first</b>.\nUse /start.",
        parse_mode="HTML",
    )


async def require_login_from_query(query, context: ContextTypes.DEFAULT_TYPE):
    await query.message.reply_text(
        "⚠ You must <b>login first</b>.\nUse /start.",
        parse_mode="HTML",
    )

def display_name(u: Dict[str, Any]) -> str:
    return (
        u.get("userName")
        or u.get("username")
        or u.get("name")
        or str(u.get("phone") or "Unknown")
    )

IST = ZoneInfo("Asia/Kolkata")

# key -> {"date": "YYYY-MM-DD", "count": int}
FAQ_DAILY_USAGE: dict[str, dict] = {}

def _today_ist_str() -> str:
    return datetime.now(IST).date().isoformat()

def _get_daily_limit() -> int:
    # default 20 if not set
    try:
        return int(os.getenv("FAQ_DAILY_LIMIT", "20"))
    except Exception:
        return 20

def _get_daily_limit_message() -> str:
    return os.getenv(
        "FAQ_DAILY_LIMIT_MESSAGE",
        "⚠️ Daily limit reached for today. Please try again tomorrow."
    )

def _faq_user_key(update: Update, context: ContextTypes.DEFAULT_TYPE) -> str:
    """
    Prefer Mongo user id if logged in; else fall back to Telegram user id.
    """
    token, user = get_logged_in(update, context)
    if user:
        uid = user.get("id") or user.get("_id")
        if uid:
            return f"mongo:{str(uid)}"
    tg_id = update.effective_user.id if update.effective_user else "unknown"
    return f"tg:{tg_id}"

def check_and_increment_daily_faq_limit(update: Update, context: ContextTypes.DEFAULT_TYPE) -> tuple[bool, int, int]:
    """
    Returns: (allowed, used, limit)
    If allowed=True, this function increments usage.
    """
    limit = _get_daily_limit()
    if limit <= 0:
        # treat <=0 as unlimited
        return True, 0, limit

    key = _faq_user_key(update, context)
    today = _today_ist_str()

    entry = FAQ_DAILY_USAGE.get(key)
    if not entry or entry.get("date") != today:
        entry = {"date": today, "count": 0}
        FAQ_DAILY_USAGE[key] = entry

    used = int(entry.get("count", 0))
    if used >= limit:
        return False, used, limit

    entry["count"] = used + 1
    return True, used + 1, limit

def build_all_accessible_users(user: dict) -> List[Dict[str, Any]]:
    rn = role_name_from_user(user)
    results: List[Dict[str, Any]] = []

    try:
        oid = ObjectId(user.get("id"))
    except Exception:
        return results

    try:
        if rn == "superadmin":
            admins = get_admins_for_superadmin(oid)
            masters = get_masters_for_superadmin(oid)
            clients = get_users_for_superadmin(oid)
            for a in admins:
                a["_category"] = "admin"
                results.append(a)
            for m in masters:
                m["_category"] = "master"
                results.append(m)
            for c in clients:
                c["_category"] = "client"
                results.append(c)
        elif rn == "admin":
            masters = get_masters_for_admin(oid)
            clients = get_users_for_admin(oid)
            for m in masters:
                m["_category"] = "master"
                results.append(m)
            for c in clients:
                c["_category"] = "client"
                results.append(c)
        elif rn == "master":
            clients = get_users_for_master(oid)
            for c in clients:
                c["_category"] = "client"
                results.append(c)
    except Exception as e:
        logger.error(f"build_all_accessible_users error: {e}")

    return results


def today_utc_range() -> Tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)
    start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start, end


def get_client_object_ids(user: dict) -> List[ObjectId]:
    all_users = build_all_accessible_users(user)
    client_ids: List[ObjectId] = []
    for u in all_users:
        if u.get("_category") != "client":
            continue
        uid = u.get("id") or u.get("_id")
        if not uid:
            continue
        try:
            client_ids.append(ObjectId(uid))
        except Exception:
            continue
    return client_ids

# ============================================================
#  NEW SUBSCRIPTION HELPERS
# ============================================================

async def get_subscription_button(chat_id: int, role: str) -> Tuple[InlineKeyboardButton, str]:
    """Determines the correct button (Subscribe/Unsubscribe) based on DB status."""
    
    # Check subscription status in MongoDB (synchronous call is fine here)
    role_doc = notification.find_one({"role": role})

    # Check if the chat_id is already in the list for this role
    is_subscribed = role_doc and chat_id in role_doc.get("chat_ids", [])

    if is_subscribed:
        button_text = "✅ Unsubscribe from Notifications"
        callback_data = f"unsubscribe_{role}"
        status_text = f"You are currently **subscribed** to **{role.capitalize()}** notifications."
    else:
        button_text = "🔔 Subscribe to Notifications"
        callback_data = f"subscribe_{role}"
        status_text = f"You are currently **not subscribed** to **{role.capitalize()}** notifications."

    button = InlineKeyboardButton(button_text, callback_data=callback_data)
    return button, status_text


# ============================================================
#  LOGIN FLOW
# ============================================================


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    clear_session(update, context)

    context.user_data["is_logging_in"] = True
    mark_skip_faq(context)

    chat = update.effective_chat

    # Per-bot config
    bot_name = context.application.bot_data.get("bot_name", "Bot")
    logo_path = context.application.bot_data.get("logo_path")

    # ✅ Resolve logo path relative to THIS file's folder: src/telegram/
    resolved_logo_path = None
    if logo_path:
        p = Path(logo_path)
        if p.is_absolute():
            resolved_logo_path = p
        else:
            resolved_logo_path = Path(__file__).resolve().parent / p

    # ---- SEND IMAGE + CAPTION ----
    if resolved_logo_path and resolved_logo_path.exists():
        try:
            with resolved_logo_path.open("rb") as f:
                msg = await chat.send_photo(
                    photo=f,
                    caption=f"🎯 Welcome to {html.escape(str(bot_name))}"
                )
                remember_bot_message_from_message(update, msg)
        except Exception as e:
            logger.warning(f"Logo image send failed for {bot_name}: {e}")
    else:
        logger.warning(f"Logo path missing/not found for {bot_name}: {logo_path}")

    trading_button = get_trading_button_from_context(context)
    if trading_button:
        keyboard = InlineKeyboardMarkup([[trading_button]])
        start_btn_msg = await chat.send_message(
            text="▶️ Tap below to open the trading platform:",
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        remember_bot_message_from_message(update, start_btn_msg)

    msg = await chat.send_message(
        text="Please enter your <b>username or phone number</b> to login.",
        parse_mode="HTML",
    )
    remember_bot_message_from_message(update, msg)

    context.user_data["login_prompt_msg_id"] = msg.message_id
    start_session_timer(update, context)
    return ASK_USERNAME

async def get_username(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    login_id = (update.message.text or "").strip()
    chat_id = update.effective_chat.id

    if not login_id:
        await update.message.reply_text("⚠ Username / phone cannot be empty.")
        return ASK_USERNAME

    try:
        await update.message.delete()
    except Exception:
        pass

    prompt_id = context.user_data.pop("login_prompt_msg_id", None)
    if prompt_id is not None:
        await safe_delete_message(context.bot, chat_id, prompt_id)

    context.user_data["login_id"] = login_id

    msg = await update.effective_chat.send_message(
        "🔐 <b>Enter your password.</b>",
        parse_mode="HTML",
    )
    context.user_data["password_prompt_msg_id"] = msg.message_id

    # ✅ IMPORTANT: the user's next text is password, do NOT let FAQ handle it
    mark_skip_faq(context)

    return ASK_PASSWORD


async def get_password(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    password = (update.message.text or "").strip()
    login_id = context.user_data.get("login_id")
    chat_id = update.effective_chat.id

    # ✅ IMPORTANT: do NOT let FAQ handler process this password text
    mark_skip_faq(context)

    if not login_id:
        context.user_data["is_logging_in"] = False
        await update.message.reply_text("❌ Session expired. Use /start again.")
        return ConversationHandler.END

    try:
        await update.message.delete()
    except Exception:
        pass

    pwd_prompt_id = context.user_data.pop("password_prompt_msg_id", None)
    if pwd_prompt_id is not None:
        await safe_delete_message(context.bot, chat_id, pwd_prompt_id)

    info_msg = await update.effective_chat.send_message("⏳ Logging you in...")

    payload = build_login_payload(login_id, password)

    try:
        async with httpx.AsyncClient(base_url=config.RMS_API_BASE_URL, timeout=10) as client:
            resp = await client.post("/auth/login", json=payload)
    except Exception as e:
        logger.error(e)
        context.user_data["is_logging_in"] = False
        context.user_data.pop("login_id", None)
        context.user_data.pop("login_prompt_msg_id", None)
        context.user_data.pop("password_prompt_msg_id", None)
        await info_msg.edit_text("❌ Cannot reach server. Try later.")
        return ConversationHandler.END

    if resp.status_code != 200:
        context.user_data["is_logging_in"] = False
        context.user_data.pop("login_id", None)
        context.user_data.pop("login_prompt_msg_id", None)
        context.user_data.pop("password_prompt_msg_id", None)
        await info_msg.edit_text("❌ Invalid credentials.")
        return ConversationHandler.END

    data = resp.json()
    if not data.get("ok"):
        context.user_data["is_logging_in"] = False
        context.user_data.pop("login_id", None)
        context.user_data.pop("login_prompt_msg_id", None)
        context.user_data.pop("password_prompt_msg_id", None)
        await info_msg.edit_text("❌ Invalid credentials.")
        return ConversationHandler.END

    token = data["access_token"]
    user = data["user"]

    session_store.set_session(update, context, token, user)

    mongo_user_id = str(user.get("id") or user.get("_id"))
    user_role_string = role_name_from_user(user)

    context.user_data["mongo_user_id"] = mongo_user_id
    context.user_data["user_role"] = user_role_string

    # ✅ login completed
    context.user_data["is_logging_in"] = False

    # ✅ cleanup login temp keys
    context.user_data.pop("login_id", None)
    context.user_data.pop("login_prompt_msg_id", None)
    context.user_data.pop("password_prompt_msg_id", None)

    await info_msg.edit_text(
    "🎉 <b>Login Successful!</b>\n"
    f"User: <code>{html.escape(str(user.get('username') or user.get('phone') or ''))}</code>\n\n"
    "Now you can use:\n"
    "• <code>/users</code>\n"
    "• <code>/position</code>\n"
    "• <code>/trades</code>\n"
    "• <code>/alerts</code>\n"
    "• <code>/transaction</code>\n"
    "• <code>/role_wise_position</code>\n"
    "• <code>/role_wise_trades</code>\n"
    "• <code>/role_wise_transaction</code>",
    parse_mode="HTML",
)
    remember_bot_message_from_message(update, info_msg)

    subscribe_button, status_text = await get_subscription_button(chat_id, user_role_string)
    keyboard = InlineKeyboardMarkup([[subscribe_button]])

    sub_msg = await update.effective_chat.send_message(
        text=status_text,
        reply_markup=keyboard,
        parse_mode="Markdown"
    )
    remember_bot_message_from_message(update, sub_msg)

    return ConversationHandler.END


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["is_logging_in"] = False
    context.user_data.pop("login_id", None)
    context.user_data.pop("login_prompt_msg_id", None)
    context.user_data.pop("password_prompt_msg_id", None)

    # skip FAQ for this cancel text if needed
    mark_skip_faq(context)

    clear_session(update, context)
    await update.message.reply_text("🛑 Login cancelled.\nUse /start to login again.")
    return ConversationHandler.END


async def me(update: Update, context: ContextTypes.DEFAULT_TYPE):
    token, user = get_logged_in(update, context)
    if not token or not user:
        return await require_login(update, context)

    # Extract the _id from the user (which is available in the JWT token)
    user_id = user.get("id")
    if not user_id:
        await update.message.reply_text("❌ User data not found in the database.")
        return

    try:
        # Ensure we are passing ObjectId to the query (not a string)
        user_data = users.find_one({"_id": ObjectId(user_id)})
    except Exception as e:
        logger.error(f"Error fetching user data: {e}")
        await update.message.reply_text("❌ Error fetching user data.")
        return

    if not user_data:
        await update.message.reply_text("❌ User data not found in the database.")
        return

    # Define the fields you want to show
    field_order = [
        ("Name", "name"),
        ("Username", "userName"),
        ("Phone", "phone"),
        ("Credit", "credit"),
        ("Balance", "balance"),
        ("Profit / Loss", "profitLoss"),
        ("Allowed Devices", "allowedDevices"),
        ("Created At", "createdAt"),
        ("Max Admin", "maxAdmin"),
        ("Max User", "maxUser"),
        ("Max Office", "maxOffice"),
        ("Max Master", "maxMaster"),
        ("Is B2B", "isB2B"),
        ("Device ID", "deviceId"),
        ("Forward Balance", "forwardBalance"),
    ]

    # Build the message content for all the details
    rows: List[str] = []
    label_width = max(len(label) for label, _ in field_order)

    for label, key in field_order:
        raw_val = user_data.get(key, "-")
        if isinstance(raw_val, ObjectId):
            raw_val = str(raw_val)
        if isinstance(raw_val, datetime):
            raw_val = raw_val.isoformat()
        if isinstance(raw_val, bool):
            raw_val = "Yes" if raw_val else "No"
        text_val = "-" if raw_val is None else str(raw_val)
        rows.append(f"{label.ljust(label_width)} : {text_val}")

    table_text = "\n".join(rows)
    table_text = html.escape(table_text)

    header = "👤 <b>User Summary</b>\n\n" "<pre>" f"{table_text}" "</pre>"

    # Send the message with user details
    await update.message.reply_text(
        header,
        parse_mode="HTML",
    )

    # Remember bot message
    remember_bot_message_from_message(update, update.message)

def resolve_user_display_by_id(user_id) -> str:
    """
    Returns a readable user display name for any user ObjectId/string.
    Format: "NameOrUsernameOrPhone"
    """
    try:
        oid = user_id if isinstance(user_id, ObjectId) else ObjectId(str(user_id))
    except Exception:
        return str(user_id or "")

    try:
        doc = users.find_one({"_id": oid})
    except Exception:
        doc = None

    if not doc:
        return str(oid)

    return (
        doc.get("name")
        or doc.get("userName")
        or doc.get("username")
        or doc.get("phone")
        or str(oid)
    )


def format_entity_header(entity_type: str, entity_id) -> str:
    """
    entity_type: "Admin" | "Master" | "Client" | etc.
    Output example: "Master: Ramesh (6943acbb...)".
    """
    name = resolve_user_display_by_id(entity_id)
    return f"{entity_type}: {html.escape(str(name))} (<code>{html.escape(str(entity_id))}</code>)"

# 🔹 NEW/REFACTORED: Consolidate subscribe/unsubscribe logic into one handler
async def handle_subscription_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handles subscription/unsubscription logic when a user clicks the inline button.
    
    CRITICAL FIX: This now relies entirely on the authenticated user data stored 
    in context.user_data after a successful login AND ensures the Web App button 
    is re-added to the message keyboard.
    """
    query = update.callback_query
    await query.answer() # Always answer the query to dismiss the loading icon

    # The button data is structured as "action_role" (e.g., "subscribe_superadmin")
    action, role = query.data.split("_")
    chat_id = update.effective_chat.id
    role = role.lower() # Ensure role is lowercase for consistency

    if role not in ["superadmin", "admin", "master"]:
        await query.edit_message_text(f"Invalid role specified: {role}")
        return

    # 1. 🚨 FIX: Retrieve stored user data from the login step.
    mongo_user_id = context.user_data.get('mongo_user_id')
    logged_in_role = context.user_data.get('user_role') 

    # Check 1: Must be logged in
    if not mongo_user_id or not logged_in_role:
        await query.edit_message_text("❌ Session required. Please use /start and log in first.")
        return

    # Check 2: The role on the button must match the user's actual role.
    if role != logged_in_role:
        await query.edit_message_text(
            f"❌ You are logged in as **{logged_in_role.upper()}**. You cannot subscribe to **{role.upper()}** alerts."
        )
        return
    
    # 2. Perform the Action
    if action == "subscribe":
        await subscribe_user(mongo_user_id, role, chat_id) 
        action_text = "Subscribed"

    elif action == "unsubscribe":
        await unsubscribe_user(mongo_user_id, chat_id) 
        action_text = "Unsubscribed"
    else:
        await query.edit_message_text("Invalid action.")
        return

    # 3. Update the Inline Button Message
    
    # Re-fetch the button after the action
    subscribe_button, status_text = await get_subscription_button(chat_id, role)

    rows = [[subscribe_button]]

    trading_button = get_trading_button_from_context(context)
    if trading_button:
        rows.append([trading_button])

    keyboard = InlineKeyboardMarkup(rows)

    await query.edit_message_text(
        text=f"✅ Successfully **{action_text}**.\n\n{status_text}",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )


def register_auth_handlers(app):
    """
    This will register all the handlers, including the subscribe button click handler
    """
    login_conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            ASK_USERNAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, get_username)
            ],
            ASK_PASSWORD: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, get_password)
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    app.add_handler(login_conv)
    # 🔹 REVISED HANDLER: Use a single pattern for both subscribe/unsubscribe
    app.add_handler(
        CallbackQueryHandler(handle_subscription_query, pattern="^(subscribe|unsubscribe)_")
    ) 
    app.add_handler(CommandHandler("cancel", cancel))
    app.add_handler(CommandHandler("me", me))
    app.add_handler(CommandHandler("subscribe_role", subscribe_command))
    app.add_handler(CommandHandler("unsubscribe", unsubscribe_command))
# NOTE: The in-memory 'subscriptions' dict from original code is kept temporarily for compatibility
subscriptions = {"superadmin": [], "admin": [], "master": []}


# --- Original Trade Notification/Listener Logic (Kept mostly intact) ---

# The original `send_trade_notification` is now imported.
# The original `handle_real_trade` and `notify_role_of_trade` were not called in the original code,
# but the logic for trade listening is below.

# NOTE: The original local definitions of subscribe_user, button_callback, and send_subscribe_button are REMOVED.


# 🔹 Original `send_trade_notification` (Assumed to be in src/telegram/notification.py now)
# async def send_trade_notification(trade_details, user_role, context):
#     ... (Implementation assumed to be external now)


async def handle_real_trade(trade_data: dict, context: CallbackContext):
    """
    Function that sends real trade notifications to all subscribed users based on the role of the user who made the trade.
    (This function was in the original code but wasn't fully connected, keeping it for structure)
    """
    user_role = trade_data.get("role") 
    user_id = trade_data.get("userId") 

    if not user_role or not user_id:
        print("Error: Missing user data in trade notification.")
        return

    # Create the message to notify about the real trade
    trade_message = (
        f"📊 <b>Trade Notification</b>\n"
        f"User {user_role}: <code>{user_id}</code> made a trade.\n\n"
        f"Symbol: {trade_data.get('symbolName', 'Unknown')}\n"
        f"Quantity: {trade_data.get('totalQuantity', 'N/A')}\n"
        f"Price: {trade_data.get('price', 'N/A')}\n"
        f"Order Type: {trade_data.get('orderType', 'N/A')}\n"
        f"Trade Type: {trade_data.get('tradeType', 'N/A')}"
    )

    # Send the notification to the corresponding role's subscribers
    # Note: `send_trade_notification` must be able to handle `context` or be passed `app.bot`.
    # Based on the original context, it's called with `context`.
    await send_message_to_chats(trade_message, user_role, context)


# NOTE: The original `notify_role_of_trade` is removed as it was redundant/unused.

def listen_for_trade_changes(app_instance):
    """
    Watches tele_notification and triggers notifications ONLY for new inserts.
    Fixed: Avoids ExtBot initialization error by using a safer bot check.
    """
    try:
        logger.info("🔄 Trade listener thread started. Waiting for bot...")
        
        bot_instance = None
        # Safer initialization check
        for _ in range(30): # Wait up to 30 seconds
            try:
                # We check if the bot object exists and attempt to access username
                # username is usually available once the app starts its loop
                if app_instance.bot:
                    bot_instance = app_instance.bot
                    # If this succeeds, the bot is initialized enough for our needs
                    logger.info("✅ Bot detected. Proceeding to watch stream.")
                    break
            except Exception:
                pass
            time.sleep(1)

        if not bot_instance:
            logger.error("❌ Bot failed to initialize in time. Listener thread stopping.")
            return

        # Watch collection only for new inserts
        with tele_notification.watch(full_document='updateLookup') as stream:
            logger.info(f"📡 Now watching for INSERTS in: {tele_notification.name}")
            
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            for change in stream:
                # 🚨 ONLY trigger on 'insert'
                if change.get('operationType') == 'insert':
                    logger.info("⚡ New Trade found. Sending pretty notification...")
                    loop.run_until_complete(handle_trade_update(change, bot_instance))

    except Exception as e:
        logger.error(f"❌ Listener Error: {e}", exc_info=True)

def start_trade_listener(app_instance):
    """
    Start the MongoDB Change Stream listener in a separate thread,
    passing the specific app instance.
    """
    # 🚨 FIX: We pass app_instance via the 'args' parameter of the Thread
    listener_thread = threading.Thread(
        target=listen_for_trade_changes, 
        args=(app_instance,)
    )
    listener_thread.daemon = True 
    listener_thread.start()


# NOTE: The original `handle_trade_update` is removed as it was moved/simplified in the earlier update.
# I'll re-add the minimal original functionality back into `main.py` since you wanted to retain old logic.
# Alternatively, I'll rely on the existing import `handle_trade_update` from `notification.py` if it was moved there.

# Assuming the logic remains local, but using the imported trade logic:

async def handle_trade_update(change: dict, bot_instance):
    """
    Formats notification to match pretty style and adds Quantity field.
    """
    doc = change.get("fullDocument")
    if not doc:
        return

    # 1. Extract Fields from Document
    user_id_str = str(doc.get("userId", ""))
    symbol = doc.get("symbolName", "N/A")
    quantity = doc.get("quantity", 0)  # 🆕 Added Quantity
    price = doc.get("price", 0)
    order_type = doc.get("orderType", "N/A")
    trade_type = doc.get("tradeType", "N/A")
    
    # Determine the status for the bold title
    status_raw = doc.get("status", "Executed")
    title = f"Trade {status_raw.capitalize()}"

    # 2. Get subscribed parents (IDs and Roles)
    parent_subscriptions = await get_subscribed_parents(user_id_str)
    if not parent_subscriptions:
        logger.info(f"No subscribers for client {user_id_str}")
        return

    # 3. Look up Client Name
    client_name = user_id_str
    try:
        client_doc = await asyncio.to_thread(users.find_one, {"_id": ObjectId(user_id_str)})
        if client_doc:
            client_name = client_doc.get("userName") or client_doc.get("name") or user_id_str
    except Exception as e:
        logger.error(f"⚠️ Client name lookup failed: {e}")

    # 4. Construct the Body (Matching image_026ae3.png)
    # Note: Labels and values are NOT bolded for the 'pretty' look.
    body = (
        f"🔔 <b>{html.escape(str(title))}</b>\n"
        f"Client Name: {html.escape(str(client_name))}\n"
        f"Symbol: {html.escape(str(symbol))}\n"
        f"Quantity: {quantity}\n"
        f"Price: {price}\n"
        f"Order Type: {html.escape(str(order_type))}\n"
        f"Trade Type: {html.escape(str(trade_type))}"
    )

    # 5. Add Footer Message (Upper Case, with blank line spacing)
    footer_text = doc.get("comment") or doc.get("message")
    if footer_text:
        body += f"\n\n{html.escape(str(footer_text)).upper()}"

    # 6. Send to each parent with dynamic "To: ROLE" bold header
    for item in parent_subscriptions:
        if isinstance(item, (tuple, list)):
            chat_id, role = item[0], item[1]
        else:
            chat_id = item
            sub_doc = await asyncio.to_thread(notification.find_one, {"chat_ids": chat_id})
            role = sub_doc.get("role", "ADMIN").upper() if sub_doc else "ADMIN"

        await send_message_to_chats(body, chat_id, bot_instance, role)

async def get_chat_ids_for_role(role: str):
    """
    Fetch chat_ids for the role from the MongoDB collection.
    (Kept from original code, although not used in the new flow, still required by other parts of old logic)
    """
    role_doc = notification.find_one({"role": role})

    # If role document exists, return the chat_ids array
    if role_doc:
        return role_doc.get("chat_ids", [])
    else:
        return []


def start_trade_listener(app_instance): 
    listener_thread = threading.Thread(target=listen_for_trade_changes, args=(app_instance,))
    listener_thread.daemon = True 
    listener_thread.start()

async def subscribe_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handler for /subscribe_role <user_role> <mongo_user_id>
    E.g., /subscribe_role superadmin 69317f5cc51a7fed13eaed02
    """
    chat_id = update.effective_chat.id
    args = context.args # Get arguments passed after the command

    if len(args) != 2:
        await update.message.reply_text(
            "Usage Error: Please use the format `/subscribe_role <role> <Your_MongoDB_ID>`.\n"
            "Example: `/subscribe_role superadmin 69317f5cc51a7fed13eaed02`"
        )
        return

    role = args[0].lower()
    mongo_user_id = args[1] # This is the unique MongoDB _id we need

    if role not in ["superadmin", "admin", "master"]:
        await update.message.reply_text(f"Invalid role specified: {role}")
        return

    # 1. Verify the MongoDB ID actually belongs to the claimed role (Optional but highly recommended)
    try:
        user_doc = users.find_one({"_id": ObjectId(mongo_user_id), "role": role})
        if not user_doc:
            await update.message.reply_text(
                f"❌ Verification failed. The ID `{mongo_user_id}` does not match the role `{role}` in the user database."
            )
            return
    except Exception:
        await update.message.reply_text("❌ Invalid MongoDB ID format. Please check your ID.")
        return

    # 2. Call the correct subscription function
    if await subscribe_user(mongo_user_id, role, chat_id):
        await update.message.reply_text(
            f"✅ Success! You are now subscribed as **{role.upper()}**.\n"
            f"Your Telegram Chat ID ({chat_id}) is linked to MongoDB ID `{mongo_user_id}`."
        )
    else:
        await update.message.reply_text("Subscription failed. Please contact support.")

async def unsubscribe_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    
    # We must find out which MongoDB user_id this chat_id belongs to.
    # The subscription is saved in notification_collection.
    
    # 1. Search the notification collection for the document containing this chat_id
    notification_doc = notification.find_one({"chat_ids": chat_id})
    
    if not notification_doc:
        await update.message.reply_text("You don't appear to be subscribed to any notifications.")
        return

    mongo_user_id = notification_doc['user_id']
    
    # 2. Call the correct unsubscription function
    if await unsubscribe_user(mongo_user_id, chat_id):
        await update.message.reply_text(f"✅ Successfully unsubscribed all notifications linked to your ID `{mongo_user_id}`.")
    else:
        await update.message.reply_text("Unsubscription failed.")

FAQ_CACHE = {"ts": 0.0, "rows": []}
FAQ_CACHE_TTL_SECONDS = 60

def _faq_norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^\w\s]", "", s)
    return s

async def fetch_faqs_from_google_sheet() -> List[Dict[str, str]]:
    """
    Option 1 (recommended, simple): Use a published CSV URL.
    Set config.FAQ_SHEET_CSV_URL in src/config.py
    Example:
      https://docs.google.com/spreadsheets/d/<SHEET_ID>/gviz/tq?tqx=out:csv&sheet=FAQ
    Sheet should have columns: question, answer
    """
    csv_url = getattr(config, "FAQ_SHEET_CSV_URL", None)
    if not csv_url:
        return []

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(csv_url)
            r.raise_for_status()
            text = r.text
    except Exception as e:
        logger.error(f"FAQ sheet fetch failed: {e}")
        return []

    rows: List[Dict[str, str]] = []
    try:
        reader = csv.DictReader(StringIO(text))
        for row in reader:
            q = (row.get("questions") or row.get("Questions") or row.get("question") or row.get("Question") or "").strip()
            a = (row.get("answers")   or row.get("Answers")   or row.get("answer")   or row.get("Answer")   or "").strip()
            if q and a:
                rows.append({"q": q, "a": a})
    except Exception as e:
        logger.error(f"FAQ CSV parse failed: {e}")
        return []

    return rows

async def get_cached_faqs() -> List[Dict[str, str]]:
    now = time.time()
    if FAQ_CACHE["rows"] and (now - FAQ_CACHE["ts"] < FAQ_CACHE_TTL_SECONDS):
        return FAQ_CACHE["rows"]

    rows = await fetch_faqs_from_google_sheet()
    cleaned: List[Dict[str, str]] = []
    for r in rows:
        q = (r.get("q") or "").strip()
        a = (r.get("a") or "").strip()
        if q and a:
            cleaned.append({"q": q, "a": a, "q_norm": _faq_norm(q)})

    FAQ_CACHE["rows"] = cleaned
    FAQ_CACHE["ts"] = now
    return cleaned

def find_best_faq_match(user_text: str, faqs: List[Dict[str, str]]) -> Tuple[Dict[str, str] | None, int]:
    u = _faq_norm(user_text)
    if not u or not faqs:
        return None, 0

    best_row = None
    best_score = 0
    for row in faqs:
        qn = row.get("q_norm") or ""
        if not qn:
            continue
        score = int(SequenceMatcher(None, u, qn).ratio() * 100)
        if score > best_score:
            best_score = score
            best_row = row

    return best_row, best_score

async def summarize_with_ollama_phi(answer_text: str) -> str:
    # 1. Fetch URL from .env (LLM_URL)
    llm_url = os.getenv("LLM_URL", "http://127.0.0.1:11434")
    model = getattr(config, "OLLAMA_PHI_MODEL", "phi:2.7b")

    # 2. Strict Prompt to prevent the AI from adding introductions or "TradingX" 
    # unless it is in the original text.
    prompt = (
        "Instructions: Summarize the text below into exactly 3 to 4 lines.\n"
        "Rules:\n"
        "- Do not say 'Hi' or 'I am an assistant'.\n"
        "- Do not add any information not found in the text.\n"
        "- Start the summary immediately.\n\n"
        f"Text to summarize:\n{answer_text}\n"
    )

    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {
            "num_ctx": 2048,
            "temperature": 0.1, # Lower temperature makes it more literal/strict
            "num_predict": 100   # Limit output length at the model level
        },
        "keep_alive": "10m",
    }

    try:
        # 3. Safe URL handling to prevent /api/generate/api/generate
        target_url = llm_url.rstrip('/')
        if "/api/generate" not in target_url:
            target_url = f"{target_url}/api/generate"

        async with httpx.AsyncClient(timeout=httpx.Timeout(180.0), trust_env=False) as client:
            r = await client.post(target_url, json=payload)
            r.raise_for_status()
            
            data = r.json()
            out = (data.get("response") or "").strip()
            
            # If the LLM still adds a prefix like "Summary:", we strip it
            if ":" in out[:15]:
                out = out.split(":", 1)[-1].strip()

            return out or answer_text

    except Exception as e:
        logger.error(f"Ollama summarize error: {repr(e)}")
        return answer_text
   
def _normalize_chat_id(x) -> int | None:
    """
    Handles chat_ids saved as:
    - int
    - {"$numberLong": "..."}  (mongo export style)
    - string
    """
    try:
        if isinstance(x, dict) and "$numberLong" in x:
            return int(x["$numberLong"])
        return int(x)
    except Exception:
        return None


def to_object_id(v) -> ObjectId | None:
    try:
        if isinstance(v, ObjectId):
            return v
        if isinstance(v, dict) and "$oid" in v:
            return ObjectId(v["$oid"])
        if isinstance(v, str):
            return ObjectId(v.strip())
        return None
    except Exception:
        return None

def oid_to_str(v) -> str | None:
    oid = to_object_id(v)
    return str(oid) if oid else None

async def notify_parent_on_unanswered_faq(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    question_text: str,
    asked_by_user: dict,
) -> None:
    logger.info("[ESCALATE] ===== Escalation started =====")

    # session gives id as string; users._id is ObjectId
    asker_id_str = (asked_by_user.get("id") or asked_by_user.get("_id") or "")
    asker_oid = to_object_id(asker_id_str)

    logger.info(f"[ESCALATE] session asker_id_str={asker_id_str} asker_oid={asker_oid}")

    if not asker_oid:
        logger.info("[ESCALATE] invalid asker_oid -> stop")
        return

    # Fetch FULL user doc from Mongo (source of truth for parentId)
    asker_doc = users.find_one({"_id": asker_oid})
    if not asker_doc:
        logger.info(f"[ESCALATE] users doc not found for _id={asker_oid}")
        return

    logger.info(f"[ESCALATE] users doc found userName={asker_doc.get('userName')} role={asker_doc.get('role')}")

    # IMPORTANT: parent can be stored as parentId OR addedBy in your schema
    parent_oid = asker_doc.get("parentId") or asker_doc.get("addedBy")
    parent_id_str = oid_to_str(parent_oid)

    logger.info(f"[ESCALATE] parent_oid={parent_oid} parent_id_str={parent_id_str}")

    if not parent_id_str:
        logger.info("[ESCALATE] No parentId/addedBy found in DB user doc -> stop")
        return

    # notification.user_id is STRING
    notif_doc = notification.find_one({"user_id": parent_id_str})
    if not notif_doc:
        logger.info(f"[ESCALATE] No notification doc for user_id={parent_id_str}")
        return

    raw_chat_ids = notif_doc.get("chat_ids") or []
    chat_ids: list[int] = []
    for c in raw_chat_ids:
        cid = _normalize_chat_id(c)
        if cid is not None:
            chat_ids.append(cid)

    logger.info(f"[ESCALATE] notif _id={notif_doc.get('_id')} chat_ids={chat_ids}")

    if not chat_ids:
        logger.info("[ESCALATE] notification chat_ids empty -> stop")
        return

    asker_name = asker_doc.get("userName") or asker_doc.get("name") or asker_doc.get("phone") or ""
    asker_role = role_name_from_user(asker_doc)

    msg = (
        "❓ <b>Unanswered Question Escalation</b>\n"
        f"From: <b>{html.escape(str(asker_name))}</b>\n"
        f"Role: <b>{html.escape(str(asker_role))}</b>\n"
        f"User ID: <code>{html.escape(str(asker_oid))}</code>\n\n"
        f"Question:\n<b>{html.escape(question_text)}</b>"
    )

    for cid in chat_ids:
        try:
            sent = await context.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML")
            remember_bot_message_from_message(update, sent)
            logger.info(f"[ESCALATE] sent to chat_id={cid}")
        except Exception as e:
            logger.warning(f"[ESCALATE] send failed chat_id={cid}: {repr(e)}")

async def handle_free_text_faq(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    if update.message.via_bot is not None:
        return

    if context.user_data.pop("skip_next_faq", False):
        return

    text = (update.message.text or "").strip()
    if not text:
        return

    if context.user_data.get("is_logging_in"):
        return

    # ✅ DAILY LIMIT CHECK (before sheet/ollama)
    allowed, used, limit = check_and_increment_daily_faq_limit(update, context)
    if not allowed:
        msg = await update.message.reply_text(_get_daily_limit_message())
        remember_bot_message_from_message(update, msg)
        return

    # existing logic continues...
    faqs = await get_cached_faqs()
    match, score = find_best_faq_match(text, faqs)

    threshold = int(getattr(config, "FAQ_MATCH_THRESHOLD", 80))
    if not match or score < threshold:
        msg = await update.message.reply_text(
            "Sorry for that, I'm not able to provide such data. "
            "You can ask anything related to our trading platform. "
            "If you have any query related, let me know."
        )
        remember_bot_message_from_message(update, msg)

        token, user = get_logged_in(update, context)
        if token and user:
            rn = role_name_from_user(user)
            if rn in ("master", "admin"):
                await notify_parent_on_unanswered_faq(
                    update,
                    context,
                    question_text=text,
                    asked_by_user=user,
                )
        return

    summarized = await summarize_with_ollama_phi(match["a"])
    reply = await update.message.reply_text(summarized)
    remember_bot_message_from_message(update, reply)
    
# ============================================================
#  BUILD APP + RUN BOT
# ============================================================


async def _set_bot_commands(app):
    bot_name = app.bot_data.get("bot_name", "Bot")

    commands = [
    BotCommand("start", f"Login to {bot_name} bot"),
    BotCommand("users", "View/manage users"),
    BotCommand("position", "View positions summary"),
    BotCommand("role_wise_position", "Role-wise positions (Admin/Master/Client)"),
    BotCommand("transaction", "View transactions summary"),
    BotCommand("role_wise_transaction", "Role-wise transactions (Admin/Master/Client)"),
    BotCommand("role_wise_trades", "Role-wise trades (Admin/Master/Client)"),
    BotCommand("trades", "View trades summary"),
    BotCommand("alerts", "View alerts summary"),
    BotCommand("report", "Generate M2M reports"),
    BotCommand("summarization", "View WhatsApp chat summaries (Superadmin only)"),
    BotCommand("me", "Who am I / role info"),
    BotCommand("cancel", "Cancel current operation"),
]

    try:
        await app.bot.delete_my_commands(scope=BotCommandScopeDefault())
        await app.bot.delete_my_commands(scope=BotCommandScopeAllPrivateChats())
    except Exception as e:
        logger.warning(f"Failed to delete bot commands (non-critical): {e}")
    
    try:
        await app.bot.set_my_commands(commands, scope=BotCommandScopeDefault())
        await app.bot.set_my_commands(commands, scope=BotCommandScopeAllPrivateChats())
    except Exception as e:
        logger.warning(f"Failed to set bot commands (non-critical): {e}")
def build_application(token: str, bot_name: str, logo_path: str, trading_url: str | None):
    """
    Initializes the bot application and registers various handlers.
    Fixed to pass the 'app' instance to the trade listener to avoid global errors.
    """
    # 1. Create the bot application
    app = ApplicationBuilder().token(token).build()
    app.bot_data["bot_name"] = bot_name
    app.bot_data["logo_path"] = logo_path
    app.bot_data["trading_url"] = trading_url or ""
    # 2. Register authentication handlers (Login flow)
    register_auth_handlers(app)

    # 3. Import and register feature modules
    from .users import register_user_handlers
    from .positions import register_position_handlers
    from .role_wise_positions import register_role_wise_position_handlers  # ✅ NEW
    from .trades import register_trade_handlers
    from .alerts import register_alert_handlers
    from .transactions import register_transaction_handlers
    from .role_wise_transactions import register_role_wise_transaction_handlers
    from .role_wise_trades import register_role_wise_trade_handlers
    from .report import register_report_handlers
    from .summarization import register_summarization_handlers

    register_user_handlers(app)
    register_position_handlers(app)
    register_role_wise_position_handlers(app)  # ✅ NEW
    register_trade_handlers(app)
    register_alert_handlers(app)
    register_transaction_handlers(app)
    register_role_wise_transaction_handlers(app)
    register_role_wise_trade_handlers(app)
    register_report_handlers(app)
    register_summarization_handlers(app)

    # 4. 🚨 CRITICAL FIX: Pass 'app' to the listener
    # This prevents the "Global 'app' not found" error in your threads.
    start_trade_listener(app)

    # 5. Add FAQ handler
    # We use a separate group so it doesn't conflict with ConversationHandlers
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_free_text_faq), 
        group=99
    )

    return app


async def start_bot(token: str, bot_name: str, logo_path: str, trading_url: str | None):
    app = build_application(token, bot_name, logo_path, trading_url)

    try:
        await app.initialize()
    except Exception as e:
        logger.warning(f"Bot initialization timeout/error (will retry): {e}")
        # Try to continue - sometimes the bot can still work

    # ✅ Explicitly set commands (no post_init)
    try:
        await _set_bot_commands(app)
    except Exception as e:
        logger.warning(f"Failed to set bot commands (non-critical): {e}")

    try:
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)
    except Exception as e:
        logger.error(f"Failed to start bot polling: {e}")
        raise  # Re-raise as this is critical

    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        await app.stop()
        await app.shutdown()

def run_bot_instance(token: str, bot_name: str, logo_path: str, trading_url: str | None):
    try:
        logger.info(f"🚀 Starting bot: {bot_name}")

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        loop.run_until_complete(start_bot(token, bot_name, logo_path, trading_url))
    except Exception as e:
        logger.exception(f"Bot '{bot_name}' crashed: {e}")


def _daily_summarize_job():
    """Daily job to summarize WhatsApp messages at 11:45 PM"""
    try:
        logger.info("📝 Starting daily summarize job at 11:45 PM")
        success_count, error_count = process_all_documents()
        logger.info(
            f"✔ Daily summarize done → {success_count} successful, {error_count} errors"
        )
    except Exception as e:
        logger.exception(f"✖ Daily summarize crashed: {e}")


def _summarize_scheduler_loop():
    """Background thread that runs the scheduler for daily summarize job"""
    schedule.every().day.at("09:50").do(_daily_summarize_job)
    logger.info("📅 Summarize scheduler started - will run daily at 11:45 PM")
    
    while True:
        schedule.run_pending()
        time.sleep(60)


def start_summarize_scheduler():
    """Start the summarize scheduler in a background thread"""
    scheduler_thread = threading.Thread(
        target=_summarize_scheduler_loop,
        daemon=True
    )
    scheduler_thread.start()
    logger.info("✅ Summarize scheduler thread started")


def run_multiple_bots():
    threads = []

    json_path = Path(__file__).resolve().parent / "bots.json"
    bots = load_bots_config(str(json_path))

    start_summarize_scheduler()

    for b in bots:
        token = b["token"]
        bot_name = b["name"]
        logo_path = b["logo_path"]
        trading_url = b.get("url")

        thread = threading.Thread(
            target=run_bot_instance,
            args=(token, bot_name, logo_path, trading_url),
            daemon=True
        )
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

if __name__ == "__main__":
    run_multiple_bots()
