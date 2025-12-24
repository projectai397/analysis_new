from typing import Dict, Any, List, Tuple
import logging
import html
from datetime import datetime

from bson import ObjectId
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ContextTypes,
    CommandHandler,
    CallbackQueryHandler,
    Application,
)

# ⚠️ Assuming 'users' collection is also available in src.config (or defined globally in main context)
# You need to ensure this import is present in the final file:
from src.config import alerts, users # <-- ADDED 'users' collection here (assuming it's in src.config)
from .main import (
    get_logged_in,
    require_login,
    require_login_from_query,
    today_utc_range,
    get_client_object_ids,
    # NOTE: Need access to role_name_from_user or similar for users to work if used elsewhere
)

# 🔹 NEW: track bot messages so they can be deleted when session clears
from .session_store import remember_bot_message_from_message

logger = logging.getLogger(__name__)

# cache: tg_id -> { items, page }
ALERT_LIST_CACHE: Dict[int, Dict[str, Any]] = {}
ALERT_PAGE_SIZE = 10


def _build_alert_page_text_and_keyboard(
    tg_id: int,
) -> Tuple[str, List[List[InlineKeyboardButton]]]:
    """
    Build message text + keyboard for alerts pagination.
    (This function remains unchanged)
    """
    cache = ALERT_LIST_CACHE.get(tg_id)
    if not cache:
        return "📢 No alerts cached.", []

    items: List[Dict[str, Any]] = cache["items"]
    page: int = cache["page"]

    if not items:
        return "📢 No alerts for today.", []

    total = len(items)
    max_page = (total - 1) // ALERT_PAGE_SIZE

    if page < 0:
        page = 0
    if page > max_page:
        page = max_page

    cache["page"] = page

    start = page * ALERT_PAGE_SIZE
    end = start + ALERT_PAGE_SIZE
    chunk = items[start:end]

    text = (
        "📢 <b>Alerts (today)</b>\n"
        f"Page {page + 1} / {max_page + 1}\n\n"
        "Select an alert:"
    )

    keyboard: List[List[InlineKeyboardButton]] = []

    for doc in chunk:
        aid = str(doc.get("_id"))
        level = doc.get("level", "INFO")
        msg = doc.get("message", "") or ""
        short_msg = (msg[:35] + "…") if len(msg) > 35 else msg
        label = f"[{level}] {short_msg}"
        keyboard.append(
            [
                InlineKeyboardButton(
                    label or "Alert",
                    callback_data=f"alert_detail:{aid}",
                )
            ]
        )

    nav_row: List[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(
            InlineKeyboardButton("⬅ Prev", callback_data="alerts_page:prev")
        )
    if end < total:
        nav_row.append(
            InlineKeyboardButton("Next ➡", callback_data="alerts_page:next")
        )
    if nav_row:
        keyboard.append(nav_row)

    return text, keyboard


async def alerts_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /alerts:
      Show today's alerts for all clients under this user,
      paginated with inline buttons.
    (This function remains unchanged)
    """
    token, user = get_logged_in(update, context)
    if not token or not user:
        # require_login already sends + tracks its own message
        return await require_login(update, context)

    client_ids = get_client_object_ids(user)
    if not client_ids:
        msg = await update.message.reply_text(
            "📢 No clients found under you."
        )
        remember_bot_message_from_message(update, msg)
        return

    start, end = today_utc_range()

    try:
        cursor = alerts.find(
            {
                "userId": {"$in": client_ids},
                "createdAt": {"$gte": start, "$lt": end},
            }
        ).sort("createdAt", -1)
        items = list(cursor)
    except Exception as e:
        logger.error(f"/alerts query error: {e}")
        msg = await update.message.reply_text(
            "⚠ Error while loading alerts."
        )
        remember_bot_message_from_message(update, msg)
        return

    # If there are no alerts, show explicit message and return
    if not items:
        msg = await update.message.reply_text("📢 No alerts for today.")
        remember_bot_message_from_message(update, msg)
        return

    tg_id = update.effective_user.id
    ALERT_LIST_CACHE[tg_id] = {
        "items": items,
        "page": 0,
    }

    text, keyboard = _build_alert_page_text_and_keyboard(tg_id)
    msg = await update.message.reply_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
    )
    remember_bot_message_from_message(update, msg)


async def alerts_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Pagination for alerts list.
    (This function remains unchanged)
    """
    query = update.callback_query
    await query.answer()

    tg_id = query.from_user.id
    cache = ALERT_LIST_CACHE.get(tg_id)
    if not cache:
        return

    _, direction = query.data.split(":", 1)
    if direction == "next":
        cache["page"] += 1
    elif direction == "prev":
        cache["page"] -= 1

    text, keyboard = _build_alert_page_text_and_keyboard(tg_id)
    # Editing existing message only → no new message to track
    await query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
    )


async def alert_detail_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Show detail view for one alert, including the username.
    """
    query = update.callback_query
    await query.answer()

    token, _ = get_logged_in(update, context)
    if not token:
        return await require_login_from_query(query, context)

    _, aid = query.data.split(":", 1)

    from pymongo.errors import PyMongoError

    try:
        doc = alerts.find_one({"_id": ObjectId(aid)})
    except PyMongoError as e:
        logger.error(f"alert_detail find_one error: {e}")
        msg = await query.message.reply_text("❌ Could not load alert details.")
        remember_bot_message_from_message(update, msg)
        return
    except Exception as e:
        logger.error(f"alert_detail unknown error: {e}")
        msg = await query.message.reply_text("❌ Could not load alert details.")
        remember_bot_message_from_message(update, msg)
        return

    if not doc:
        msg = await query.message.reply_text("❌ Alert not found.")
        remember_bot_message_from_message(update, msg)
        return

    # 1. 🚨 NEW: Fetch Username
    user_id_obj = doc.get("userId")
    username = str(user_id_obj) # Default to the ObjectId string
    
    if user_id_obj:
        try:
            # Look up the user in the users collection
            user_doc = users.find_one({"_id": ObjectId(user_id_obj)})
            if user_doc:
                # Prioritize userName, then name, then phone
                username = user_doc.get("userName") or user_doc.get("name") or user_doc.get("phone") or str(user_id_obj)
        except Exception as e:
            logger.warning(f"Failed to fetch username for {user_id_obj}: {e}")
    # End New: Fetch Username
    
    # 2. 🚨 NEW: Define the desired fields for display
    # (Removed 'type', 'isSeen', 'isResolved', 'level', 'message', 'updatedAt')
    field_order = [
        # 🔹 NEW/REVISED ORDER
        ("Username", "username", username), # This field is custom-fetched
        ("Credit", "credit"),
        ("Balance", "balance"),
        ("Cut Off Balance", "cutOffBalance"),
        ("Cut Off For Alert", "cutOffBalanceForAlert"),
        ("Profit / Loss", "profitLoss"),
        ("Date / Time", "dateTime"),
        ("Alert Status", "alertStatus"),
        ("Auto SQ Status", "autoSQStatus"),
        ("Alert Percentage", "alertPercentage"),
        ("Auto SQ Percentage", "autoSQPercentage"),
        ("Created At", "createdAt"),
        # Keeping User ID for reference
        ("User ID (Raw)", "userId"),
    ]

    rows: List[str] = []
    # Calculate label width based on the longest custom or hardcoded label
    label_width = max(len(label) for label, _, *__ in field_order)

    for item in field_order:
        label = item[0]
        key = item[1]
        
        # Handle the custom 'Username' field
        if key == "username":
            raw_val = item[2] # Use the pre-fetched username
        else:
            raw_val = doc.get(key, "-")

        if isinstance(raw_val, ObjectId):
            raw_val = str(raw_val)
        if isinstance(raw_val, datetime):
            # Format datetime to a cleaner string for display
            raw_val = raw_val.strftime("%Y-%m-%d %H:%M:%S UTC")
        if isinstance(raw_val, bool):
            raw_val = "Yes" if raw_val else "No"
            
        # Ensure numbers/strings are formatted
        text_val = "-" if raw_val is None else str(raw_val)
        
        rows.append(f"{label.ljust(label_width)} : {text_val}")

    table_text = "\n".join(rows)
    table_text = html.escape(table_text)

    header = (
        "📢 <b>Alert Summary</b>\n\n"
        "<pre>"
        f"{table_text}"
        "</pre>"
    )

    msg = await query.message.reply_text(
        header,
        parse_mode="HTML",
    )
    remember_bot_message_from_message(update, msg)


def register_alert_handlers(app: Application):
    """
    Register /alerts and related callbacks on the Application.
    (This function remains unchanged)
    """
    app.add_handler(CommandHandler("alerts", alerts_cmd))
    app.add_handler(
        CallbackQueryHandler(alerts_page_callback, pattern=r"^alerts_page:")
    )
    app.add_handler(
        CallbackQueryHandler(alert_detail_callback, pattern=r"^alert_detail:")
    )