# src/telegram/trades.py

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

from src.config import positions
from .main import (
    get_logged_in,
    require_login,
    require_login_from_query,
    today_utc_range,
    get_client_object_ids,
)

# 🔹 NEW: track messages for auto-delete when session clears
from .session_store import remember_bot_message_from_message

logger = logging.getLogger(__name__)

TRADE_LIST_CACHE: Dict[int, Dict[str, Any]] = {}
TRADE_PAGE_SIZE = 10


def build_trade_page_text_and_keyboard(tg_id: int) -> Tuple[str, List[List[InlineKeyboardButton]]]:
    cache = TRADE_LIST_CACHE.get(tg_id)
    if not cache:
        return "No trades cached.", []

    items: List[Dict[str, Any]] = cache["items"]
    page: int = cache["page"]

    if not items:
        return "💹 No trades for today.", []

    total = len(items)
    max_page = (total - 1) // TRADE_PAGE_SIZE
    if page < 0:
        page = 0
    if page > max_page:
        page = max_page
    cache["page"] = page

    start = page * TRADE_PAGE_SIZE
    end = start + TRADE_PAGE_SIZE
    chunk = items[start:end]

    text = (
        "💹 <b>Trades (today)</b>\n"
        f"Page {page + 1} / {max_page + 1}\n\n"
        "Select a trade:"
    )

    keyboard: List[List[InlineKeyboardButton]] = []
    for tr in chunk:
        tid = str(tr.get("_id"))
        symbol = tr.get("symbolName") or tr.get("symbolTitle") or "—"
        qty = tr.get("totalQuantity") or tr.get("quantity") or 0
        price = tr.get("price") or 0
        side = tr.get("tradeType") or tr.get("orderType") or ""
        label = f"{symbol} | {side} {qty} @ {price}"
        keyboard.append([
            InlineKeyboardButton(
                label,
                callback_data=f"trade_detail:{tid}",
            )
        ])

    nav_row: List[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅ Prev", callback_data="trades_page:prev"))
    if end < total:
        nav_row.append(InlineKeyboardButton("Next ➡", callback_data="trades_page:next"))
    if nav_row:
        keyboard.append(nav_row)

    return text, keyboard


async def trades_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    token, user = get_logged_in(update, context)
    if not token or not user:
        # require_login already sends and auto-tracks the message
        return await require_login(update, context)

    client_ids = get_client_object_ids(user)
    if not client_ids:
        msg = await update.message.reply_text("💹 No clients found under you.")
        remember_bot_message_from_message(update, msg)
        return

    start, end = today_utc_range()

    try:
        cursor = positions.find(
            {
                "userId": {"$in": client_ids},
                "createdAt": {"$gte": start, "$lt": end},
            }
        ).sort("createdAt", -1)
        items = list(cursor)
    except Exception as e:
        logger.error(f"/trades query error: {e}")
        msg = await update.message.reply_text("⚠ Error while loading trades.")
        remember_bot_message_from_message(update, msg)
        return

    tg_id = update.effective_user.id
    TRADE_LIST_CACHE[tg_id] = {
        "items": items,
        "page": 0,
    }

    text, keyboard = build_trade_page_text_and_keyboard(tg_id)
    msg = await update.message.reply_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
    )
    remember_bot_message_from_message(update, msg)


async def trades_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    tg_id = query.from_user.id
    cache = TRADE_LIST_CACHE.get(tg_id)
    if not cache:
        return

    _, direction = query.data.split(":", 1)
    if direction == "next":
        cache["page"] += 1
    elif direction == "prev":
        cache["page"] -= 1

    text, keyboard = build_trade_page_text_and_keyboard(tg_id)
    # Editing existing message; not new -> no tracking needed
    await query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
    )


async def trade_detail_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    token, _ = get_logged_in(update, context)
    if not token:
        return await require_login_from_query(query, context)

    _, tid = query.data.split(":", 1)

    try:
        doc = positions.find_one({"_id": ObjectId(tid)})
    except Exception as e:
        logger.error(f"trade_detail find_one error: {e}")
        msg = await query.message.reply_text("❌ Could not load trade details.")
        remember_bot_message_from_message(update, msg)
        return

    if not doc:
        msg = await query.message.reply_text("❌ Trade not found.")
        remember_bot_message_from_message(update, msg)
        return

    field_order = [
        ("User ID", "userId"),
        ("Symbol", "symbolName"),
        ("Quantity", "quantity"),
        ("Price", "price"),
        ("SL Price", "slPrice"),
        ("TP Price", "tpPrice"),
        ("Product Type", "productType"),
        ("Trade Type", "tradeType"),
        ("Exchange", "exchangeName"),
        ("Order Type", "orderType"),
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
        "💹 <b>Trade Summary</b>\n\n"
        "<pre>"
        f"{table_text}"
        "</pre>"
    )

    msg = await query.message.reply_text(header, parse_mode="HTML")
    remember_bot_message_from_message(update, msg)


def register_trade_handlers(app):
    app.add_handler(CommandHandler("trades", trades_cmd))
    app.add_handler(CallbackQueryHandler(trades_page_callback, pattern=r"^trades_page:"))
    app.add_handler(CallbackQueryHandler(trade_detail_callback, pattern=r"^trade_detail:"))
