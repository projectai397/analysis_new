# src/telegram/positions.py

from typing import Dict, Any, List, Tuple
import html
from datetime import datetime
from bson import ObjectId
import logging

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)

from src.config import trade_market
from .main import (
    get_logged_in,
    require_login,
    require_login_from_query,
    today_utc_range,
    get_client_object_ids,
)
# 🔹 NEW: track bot messages so they can be deleted on session clear
from .session_store import remember_bot_message_from_message

logger = logging.getLogger(__name__)

POSITION_LIST_CACHE: Dict[int, Dict[str, Any]] = {}
POSITION_PAGE_SIZE = 10


def build_position_page_text_and_keyboard(tg_id: int) -> Tuple[str, List[List[InlineKeyboardButton]]]:
    cache = POSITION_LIST_CACHE.get(tg_id)
    if not cache:
        return "No open positions cached.", []

    items: List[Dict[str, Any]] = cache["items"]
    page: int = cache["page"]

    if not items:
        return "📊 No open positions for today.", []

    total = len(items)
    max_page = (total - 1) // POSITION_PAGE_SIZE
    if page < 0:
        page = 0
    if page > max_page:
        page = max_page
    cache["page"] = page

    start = page * POSITION_PAGE_SIZE
    end = start + POSITION_PAGE_SIZE
    chunk = items[start:end]

    text = (
        "📊 <b>Open Positions (today)</b>\n"
        f"Page {page + 1} / {max_page + 1}\n\n"
        "Select a position:"
    )

    keyboard: List[List[InlineKeyboardButton]] = []
    for pos in chunk:
        pid = str(pos.get("_id"))
        symbol = pos.get("symbolName") or pos.get("symbolTitle") or "—"
        qty = pos.get("totalQuantity") or pos.get("quantity") or 0
        price = pos.get("price") or 0
        label = f"{symbol} | Qty {qty} @ {price}"
        keyboard.append([
            InlineKeyboardButton(
                label,
                callback_data=f"position_detail:{pid}",
            )
        ])

    nav_row: List[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅ Prev", callback_data="positions_page:prev"))
    if end < total:
        nav_row.append(InlineKeyboardButton("Next ➡", callback_data="positions_page:next"))
    if nav_row:
        keyboard.append(nav_row)

    return text, keyboard


async def positions_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    token, user = get_logged_in(update, context)
    if not token or not user:
        # require_login() already sends a message (and should track it)
        return await require_login(update, context)

    client_ids = get_client_object_ids(user)
    if not client_ids:
        msg = await update.message.reply_text("📊 No clients found under you.")
        remember_bot_message_from_message(update, msg)
        return

    start, end = today_utc_range()

    try:
        cursor = trade_market.find(
            {
                "userId": {"$in": client_ids},
                "squareOff": False,
                "createdAt": {"$gte": start, "$lt": end},
            }
        ).sort("createdAt", -1)
        items = list(cursor)
    except Exception as e:
        logger.error(f"/positions query error: {e}")
        msg = await update.message.reply_text("⚠ Error while loading positions.")
        remember_bot_message_from_message(update, msg)
        return

    tg_id = update.effective_user.id
    POSITION_LIST_CACHE[tg_id] = {
        "items": items,
        "page": 0,
    }

    text, keyboard = build_position_page_text_and_keyboard(tg_id)
    msg = await update.message.reply_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
    )
    remember_bot_message_from_message(update, msg)


async def positions_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    tg_id = query.from_user.id
    cache = POSITION_LIST_CACHE.get(tg_id)
    if not cache:
        return

    _, direction = query.data.split(":", 1)
    if direction == "next":
        cache["page"] += 1
    elif direction == "prev":
        cache["page"] -= 1

    text, keyboard = build_position_page_text_and_keyboard(tg_id)
    # Editing existing message → no new message id to track
    await query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
    )


async def position_detail_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    token, _ = get_logged_in(update, context)
    if not token:
        return await require_login_from_query(query, context)

    _, pid = query.data.split(":", 1)

    try:
        doc = trade_market.find_one({"_id": ObjectId(pid)})
    except Exception as e:
        logger.error(f"position_detail find_one error: {e}")
        msg = await query.message.reply_text("❌ Could not load position details.")
        remember_bot_message_from_message(update, msg)
        return

    if not doc:
        msg = await query.message.reply_text("❌ Position not found.")
        remember_bot_message_from_message(update, msg)
        return

    field_order = [
        ("User ID", "userId"),
        ("Symbol", "symbolName"),
        ("Quantity", "quantity"),
        ("Price", "price"),
        ("Lot Size", "lotSize"),
        ("Total Quantity", "totalQuantity"),
        ("Total", "total"),
        ("Product Type", "productType"),
        ("Trade Type", "tradeType"),
        ("Trade Margin", "tradeMargin"),
        ("Trade Margin Price", "tradeMarginPrice"),
        ("Trade Margin Total", "tradeMarginTotal"),
        ("Created At", "createdAt"),
        ("Updated At", "updatedAt"),
    ]

    rows: List[str] = []
    label_width = max(len(label) for label, _ in field_order)
    for label, key in field_order:
        raw_val = doc.get(key, "-")
        if isinstance(raw_val, ObjectId):
            raw_val = str(raw_val)
        if isinstance(raw_val, datetime):
            raw_val = raw_val.isoformat()
        text_val = "-" if raw_val is None else str(raw_val)
        rows.append(f"{label.ljust(label_width)} : {text_val}")

    table_text = "\n".join(rows)
    table_text = html.escape(table_text)

    header = (
        "📊 <b>Position Summary</b>\n\n"
        "<pre>"
        f"{table_text}"
        "</pre>"
    )

    msg = await query.message.reply_text(header, parse_mode="HTML")
    remember_bot_message_from_message(update, msg)


def register_position_handlers(app):
    app.add_handler(CommandHandler(["positions", "position"], positions_cmd))
    app.add_handler(CallbackQueryHandler(positions_page_callback, pattern=r"^positions_page:"))
    app.add_handler(CallbackQueryHandler(position_detail_callback, pattern=r"^position_detail:"))
