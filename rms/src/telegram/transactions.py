# src/telegram/transactions.py

from typing import Dict, Any, List, Tuple
import logging
import html
from datetime import datetime

from bson import ObjectId
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)

from src.config import transactions  # collection name from config

# Re-use shared helpers from main.py
from .main import (
    get_logged_in,
    require_login,
    require_login_from_query,
    today_utc_range,
    get_client_object_ids,
)

# 🔹 NEW: import tracker so these messages get deleted when session expires
from .session_store import remember_bot_message_from_message

logger = logging.getLogger(__name__)

# Cache: tg_id -> { items, page }
TRANSACTION_LIST_CACHE: Dict[int, Dict[str, Any]] = {}
TRANSACTION_PAGE_SIZE = 10


def _build_transaction_page_text_and_keyboard(
    tg_id: int,
) -> Tuple[str, List[List[InlineKeyboardButton]]]:
    cache = TRANSACTION_LIST_CACHE.get(tg_id)
    if not cache:
        return "No transactions cached.", []

    items: List[Dict[str, Any]] = cache["items"]
    page: int = cache["page"]

    if not items:
        return "💰 No transactions for today.", []

    total = len(items)
    max_page = (total - 1) // TRANSACTION_PAGE_SIZE
    if page < 0:
        page = 0
    if page > max_page:
        page = max_page
    cache["page"] = page

    start = page * TRANSACTION_PAGE_SIZE
    end = start + TRANSACTION_PAGE_SIZE
    chunk = items[start:end]

    text = (
        "💰 <b>Transactions (today)</b>\n"
        f"Page {page + 1} / {max_page + 1}\n\n"
        "Select a transaction:"
    )

    keyboard: List[List[InlineKeyboardButton]] = []
    for tx in chunk:
        tid = str(tx.get("_id"))
        symbol = (
            tx.get("symbolName")
            or tx.get("symbolTitle")
            or str(tx.get("symbolId") or "—")
        )
        amount = tx.get("amount") or 0
        ttype = tx.get("transactionType") or tx.get("type") or ""
        label = f"{symbol} | {ttype} {amount}"
        keyboard.append(
            [
                InlineKeyboardButton(
                    label,
                    callback_data=f"transaction_detail:{tid}",
                )
            ]
        )

    nav_row: List[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(
            InlineKeyboardButton("⬅ Prev", callback_data="transactions_page:prev")
        )
    if end < total:
        nav_row.append(
            InlineKeyboardButton("Next ➡", callback_data="transactions_page:next")
        )
    if nav_row:
        keyboard.append(nav_row)

    return text, keyboard


async def transactions_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /transactions or /transaction:
      Show today's transactions for all clients under this user,
      paginated with inline buttons.
    """
    token, user = get_logged_in(update, context)
    if not token or not user:
        # require_login already tracks its own messages
        return await require_login(update, context)

    client_ids = get_client_object_ids(user)
    if not client_ids:
        msg = await update.message.reply_text("💰 No clients found under you.")
        remember_bot_message_from_message(update, msg)
        return

    start, end = today_utc_range()

    try:
        cursor = transactions.find(
            {
                "userId": {"$in": client_ids},
                "createdAt": {"$gte": start, "$lt": end},
            }
        ).sort("createdAt", -1)
        items = list(cursor)
    except Exception as e:
        logger.error(f"/transactions query error: {e}")
        msg = await update.message.reply_text(
            "⚠ Error while loading transactions."
        )
        remember_bot_message_from_message(update, msg)
        return

    tg_id = update.effective_user.id
    TRANSACTION_LIST_CACHE[tg_id] = {
        "items": items,
        "page": 0,
    }

    text, keyboard = _build_transaction_page_text_and_keyboard(tg_id)

    # Send and remember – so it gets deleted when session clears
    msg = await update.message.reply_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
    )
    remember_bot_message_from_message(update, msg)


async def transactions_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    tg_id = query.from_user.id
    cache = TRANSACTION_LIST_CACHE.get(tg_id)
    if not cache:
        return

    _, direction = query.data.split(":", 1)
    if direction == "next":
        cache["page"] += 1
    elif direction == "prev":
        cache["page"] -= 1

    text, keyboard = _build_transaction_page_text_and_keyboard(tg_id)
    # editing existing message → no need to track as "new"
    await query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
    )


async def transaction_detail_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
):
    """
    Show detailed info for a single transaction.

    Fields:
      userId, tradeId, exchangeId, symbolId, from, to,
      transactionType, amount, brokeragePoint, type, status, createdAt
    """
    query = update.callback_query
    await query.answer()

    token, _ = get_logged_in(update, context)
    if not token:
        return await require_login_from_query(query, context)

    _, tid = query.data.split(":", 1)

    try:
        doc = transactions.find_one({"_id": ObjectId(tid)})
    except Exception as e:
        logger.error(f"transaction_detail find_one error: {e}")
        msg = await query.message.reply_text("❌ Could not load transaction details.")
        remember_bot_message_from_message(update, msg)
        return

    if not doc:
        msg = await query.message.reply_text("❌ Transaction not found.")
        remember_bot_message_from_message(update, msg)
        return

    field_order = [
        ("User ID", "userId"),
        ("Trade ID", "tradeId"),
        ("Exchange ID", "exchangeId"),
        ("Symbol ID", "symbolId"),
        ("From", "from"),
        ("To", "to"),
        ("Transaction Type", "transactionType"),
        ("Amount", "amount"),
        ("Brokerage Point", "brokeragePoint"),
        ("Type", "type"),
        ("Status", "status"),
        ("Created At", "createdAt"),
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
        "💰 <b>Transaction Summary</b>\n\n"
        "<pre>"
        f"{table_text}"
        "</pre>"
    )

    msg = await query.message.reply_text(
        header,
        parse_mode="HTML",
    )
    remember_bot_message_from_message(update, msg)


def register_transaction_handlers(app):
    """
    Register command + callbacks with the Application.
    """
    # /transaction and /transactions
    app.add_handler(CommandHandler(["transaction", "transactions"], transactions_cmd))

    # Pagination
    app.add_handler(
        CallbackQueryHandler(
            transactions_page_callback, pattern=r"^transactions_page:"
        )
    )

    # Detail view
    app.add_handler(
        CallbackQueryHandler(
            transaction_detail_callback, pattern=r"^transaction_detail:"
        )
    )
