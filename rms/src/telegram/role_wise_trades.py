# src/telegram/role_wise_trades.py

from typing import Dict, Any, List, Tuple, Optional
import html
import logging
from datetime import datetime
from bson import ObjectId

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

from src.config import positions, users  # ✅ trades are coming from `positions` in your trades.py
from src.helpers.hierarchy_service import (
    get_admins_for_superadmin,
    get_masters_for_superadmin,
    get_masters_for_admin,
    get_users_for_superadmin,
    get_users_for_admin,
    get_users_for_master,
)

from .main import (
    get_logged_in,
    require_login,
    require_login_from_query,
    safe_delete_message,
    role_name_from_user,
    display_name,
    build_all_accessible_users,
    today_utc_range,
)

from .session_store import remember_bot_message_from_message

logger = logging.getLogger(__name__)

# ---------- caches ----------
RWT_ENTITY_CACHE: Dict[int, Dict[str, Any]] = {}
RWT_ENTITY_PAGE_SIZE = 5

RWT_TRADE_CACHE: Dict[int, Dict[str, Any]] = {}
RWT_TRADE_PAGE_SIZE = 5  # adjust if you want 10

RWT_SEARCH_CACHE: Dict[int, Dict[str, Any]] = {}
RWT_SEARCH_PAGE_SIZE = 10


# ---------- helpers ----------
def _oid_from_user_doc(u: Dict[str, Any]) -> Optional[ObjectId]:
    raw = u.get("id") or u.get("_id")
    if not raw:
        return None
    try:
        return ObjectId(str(raw))
    except Exception:
        return None


def resolve_user_display(user_id: ObjectId) -> str:
    """
    Convert positions.userId(ObjectId) to readable name/userName/username/phone.
    """
    try:
        doc = users.find_one({"_id": user_id})
        if not doc:
            return str(user_id)
        return (
            doc.get("name")
            or doc.get("userName")
            or doc.get("username")
            or doc.get("phone")
            or str(user_id)
        )
    except Exception:
        return str(user_id)
    
def format_entity_header(entity_type: str, entity_id: str) -> str:
    """
    Example: 'Master: John (<code>6943...</code>)'
    """
    name = resolve_user_display(ObjectId(entity_id)) if ObjectId.is_valid(entity_id) else entity_id
    return f"{entity_type}: {html.escape(str(name))} (<code>{html.escape(str(entity_id))}</code>)"

def _role_title(rn: str) -> str:
    if rn == "superadmin":
        return "Role-wise Trades (Superadmin)"
    if rn == "admin":
        return "Role-wise Trades (Admin)"
    if rn == "master":
        return "Role-wise Trades (Master)"
    return "Role-wise Trades"


def _load_entities_for_category(current_user: Dict[str, Any], category: str) -> Tuple[str, List[Dict[str, Any]]]:
    rn = role_name_from_user(current_user)
    me_oid = _oid_from_user_doc(current_user)
    if not me_oid:
        return "❌ Invalid user id.", []

    if category == "admin":
        if rn != "superadmin":
            return "❌ Only superadmin can view Admins.", []
        return "👑 Admins under you:", get_admins_for_superadmin(me_oid)

    if category == "master":
        if rn == "superadmin":
            return "🧩 Masters (all):", get_masters_for_superadmin(me_oid)
        if rn == "admin":
            return "🧩 Masters under you:", get_masters_for_admin(me_oid)
        return "❌ Only superadmin or admin can view Masters.", []

    if category == "client":
        if rn == "superadmin":
            return "👤 Clients (all):", get_users_for_superadmin(me_oid)
        if rn == "admin":
            return "👤 Clients under you:", get_users_for_admin(me_oid)
        if rn == "master":
            return "👤 Clients under you:", get_users_for_master(me_oid)
        return "❌ Only superadmin/admin/master can view Clients.", []

    return "❌ Unknown category.", []


def _load_clients_under_entity(entity_category: str, entity_oid: ObjectId) -> List[Dict[str, Any]]:
    if entity_category == "admin":
        return get_users_for_admin(entity_oid)
    if entity_category == "master":
        return get_users_for_master(entity_oid)
    return []


def _query_trades_for_user_ids(user_ids: List[ObjectId]) -> List[Dict[str, Any]]:
    start, end = today_utc_range()
    cursor = positions.find(
        {
            "userId": {"$in": user_ids},
            "createdAt": {"$gte": start, "$lt": end},
        }
    ).sort("createdAt", -1)
    return list(cursor)


def _build_trade_page(tg_id: int) -> Tuple[str, List[List[InlineKeyboardButton]]]:
    cache = RWT_TRADE_CACHE.get(tg_id)
    if not cache:
        return "No trades cached.", []

    items: List[Dict[str, Any]] = cache["items"]
    page: int = cache["page"]
    header_title: str = cache.get("header_title", "💹 <b>Trades (today)</b>")

    if not items:
        return f"{header_title}\n\n💹 No trades for today.", []

    total = len(items)
    max_page = (total - 1) // RWT_TRADE_PAGE_SIZE
    page = max(0, min(page, max_page))
    cache["page"] = page

    start_i = page * RWT_TRADE_PAGE_SIZE
    end_i = start_i + RWT_TRADE_PAGE_SIZE
    chunk = items[start_i:end_i]

    text = (
        f"{header_title}\n"
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
        keyboard.append([InlineKeyboardButton(label, callback_data=f"rwt_trade_detail:{tid}")])

    nav_row: List[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅ Prev", callback_data="rwt_trade_page:prev"))
    if end_i < total:
        nav_row.append(InlineKeyboardButton("Next ➡", callback_data="rwt_trade_page:next"))
    if nav_row:
        keyboard.append(nav_row)

    return text, keyboard


def _build_entity_list_page(tg_id: int) -> Tuple[str, List[List[InlineKeyboardButton]]]:
    cache = RWT_ENTITY_CACHE.get(tg_id)
    if not cache:
        return "No list cached.", []

    title: str = cache["title"]
    entities: List[Dict[str, Any]] = cache["entities"]
    page: int = cache["page"]
    category: str = cache["category"]

    if not entities:
        return f"{html.escape(title)}\n\nNo records found.", []

    total = len(entities)
    max_page = (total - 1) // RWT_ENTITY_PAGE_SIZE
    page = max(0, min(page, max_page))
    cache["page"] = page

    start_i = page * RWT_ENTITY_PAGE_SIZE
    end_i = start_i + RWT_ENTITY_PAGE_SIZE
    chunk = entities[start_i:end_i]

    text = (
        f"{html.escape(title)}\n"
        f"Page {page + 1} / {max_page + 1}\n\n"
        "Select one:"
    )

    keyboard: List[List[InlineKeyboardButton]] = []
    for u in chunk:
        uid = str(u.get("id") or u.get("_id"))
        label = f"{display_name(u)} ({u.get('phone') or ''})"
        keyboard.append([InlineKeyboardButton(label, callback_data=f"rwt_trade_entity:{category}:{uid}")])

    nav_row: List[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅ Prev", callback_data="rwt_trade_entity_page:prev"))
    if end_i < total:
        nav_row.append(InlineKeyboardButton("Next ➡", callback_data="rwt_trade_entity_page:next"))
    if nav_row:
        keyboard.append(nav_row)

    return text, keyboard


# ---------- /role_wise_trades ----------
async def role_wise_trades_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    token, user = get_logged_in(update, context)
    if not token or not user:
        return await require_login(update, context)

    rn = role_name_from_user(user)
    title = _role_title(rn)

    buttons: List[List[InlineKeyboardButton]] = []
    if rn == "superadmin":
        buttons = [
            [InlineKeyboardButton("👑 Admins", callback_data="rwt_trade_menu:admin")],
            [InlineKeyboardButton("🧩 Masters", callback_data="rwt_trade_menu:master")],
            [InlineKeyboardButton("👤 Clients", callback_data="rwt_trade_menu:client")],
        ]
    elif rn == "admin":
        buttons = [
            [InlineKeyboardButton("🧩 Masters", callback_data="rwt_trade_menu:master")],
            [InlineKeyboardButton("👤 Clients", callback_data="rwt_trade_menu:client")],
        ]
    elif rn == "master":
        buttons = [
            [InlineKeyboardButton("👤 Clients", callback_data="rwt_trade_menu:client")],
        ]
    else:
        msg = await update.message.reply_text(
            "❌ This command is only for <b>superadmin</b>, <b>admin</b> or <b>master</b>.",
            parse_mode="HTML",
        )
        remember_bot_message_from_message(update, msg)
        return

    buttons.append([InlineKeyboardButton("🔍 Search Client", callback_data="rwt_trade_search:start")])

    text = (
        f"💹 <b>{html.escape(title)}</b>\n\n"
        "Choose a category or search:\n"
        "Admins / Masters / Clients / Search Client\n"
        "Tap a button below."
    )

    msg = await update.message.reply_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(buttons),
    )
    remember_bot_message_from_message(update, msg)


async def rwt_trade_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    token, user = get_logged_in(update, context)
    if not token or not user:
        return await require_login_from_query(query, context)

    tg_id = query.from_user.id
    _, cat = query.data.split(":", 1)

    title, entities = _load_entities_for_category(user, cat)
    if not entities:
        return await query.edit_message_text(title, parse_mode="HTML")

    RWT_ENTITY_CACHE[tg_id] = {
        "category": cat,
        "title": title,
        "entities": entities,
        "page": 0,
    }

    text, keyboard = _build_entity_list_page(tg_id)
    await query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
    )


async def rwt_trade_entity_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    tg_id = query.from_user.id
    cache = RWT_ENTITY_CACHE.get(tg_id)
    if not cache:
        return

    _, direction = query.data.split(":", 1)
    if direction == "next":
        cache["page"] += 1
    elif direction == "prev":
        cache["page"] -= 1

    text, keyboard = _build_entity_list_page(tg_id)
    await query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
    )


async def rwt_trade_entity_select_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    token, user = get_logged_in(update, context)
    if not token or not user:
        return await require_login_from_query(query, context)

    tg_id = query.from_user.id
    _, entity_category, entity_id = query.data.split(":", 2)

    try:
        entity_oid = ObjectId(entity_id)
    except Exception:
        msg = await query.message.reply_text("❌ Invalid selection.")
        remember_bot_message_from_message(update, msg)
        return

    if entity_category == "client":
        client_ids = [entity_oid]
        header_title = "💹 <b>Trades (today)</b>\n" + format_entity_header("Client", entity_id)
    else:
        ...
        header_title = "💹 <b>Trades (today)</b>\n" + format_entity_header(entity_category.title(), entity_id)

        if not client_ids:
            msg = await query.message.reply_text("💹 No clients found under that selection.")
            remember_bot_message_from_message(update, msg)
            return

        try:
            items = _query_trades_for_user_ids(client_ids)
        except Exception as e:
            logger.error(f"trades query error: {e}")
            msg = await query.message.reply_text("⚠ Error while loading trades.")
            remember_bot_message_from_message(update, msg)
            return

    RWT_TRADE_CACHE[tg_id] = {
        "items": items,
        "page": 0,
        "header_title": header_title,
    }

    text, keyboard = _build_trade_page(tg_id)
    msg = await query.message.reply_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
    )
    remember_bot_message_from_message(update, msg)


async def rwt_trade_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    tg_id = query.from_user.id
    cache = RWT_TRADE_CACHE.get(tg_id)
    if not cache:
        return

    _, direction = query.data.split(":", 1)
    if direction == "next":
        cache["page"] += 1
    elif direction == "prev":
        cache["page"] -= 1

    text, keyboard = _build_trade_page(tg_id)
    await query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
    )


async def rwt_trade_detail_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    token, _ = get_logged_in(update, context)
    if not token:
        return await require_login_from_query(query, context)

    _, tid = query.data.split(":", 1)

    try:
        doc = positions.find_one({"_id": ObjectId(tid)})
    except Exception as e:
        logger.error(f"rwt_trade_detail find_one error: {e}")
        msg = await query.message.reply_text("❌ Could not load trade details.")
        remember_bot_message_from_message(update, msg)
        return

    if not doc:
        msg = await query.message.reply_text("❌ Trade not found.")
        remember_bot_message_from_message(update, msg)
        return

    # ✅ User name instead of userId
    field_order = [
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
    label_width = max(label_width, len("User"))

    uid = doc.get("userId")
    user_display = resolve_user_display(uid) if isinstance(uid, ObjectId) else str(uid or "-")
    rows.append(f"{'User'.ljust(label_width)} : {user_display}")

    for label, key in field_order:
        raw_val = doc.get(key, "-")
        if isinstance(raw_val, ObjectId):
            raw_val = str(raw_val)
        if isinstance(raw_val, datetime):
            raw_val = raw_val.isoformat()
        text_val = "-" if raw_val is None else str(raw_val)
        rows.append(f"{label.ljust(label_width)} : {text_val}")

    table_text = html.escape("\n".join(rows))

    header = "💹 <b>Trade Summary</b>\n\n<pre>" + table_text + "</pre>"
    msg = await query.message.reply_text(header, parse_mode="HTML")
    remember_bot_message_from_message(update, msg)


# ---------- search (clients only) ----------
async def rwt_trade_search_start_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    token, user = get_logged_in(update, context)
    if not token or not user:
        return await require_login_from_query(query, context)

    context.user_data["rwt_trade_search_mode"] = True

    msg = await query.message.reply_text(
        "🔍 <b>Search client</b>\nType <code>phone</code>, <code>userName</code> or <code>name</code>.",
        parse_mode="HTML",
    )
    remember_bot_message_from_message(update, msg)
    context.user_data["rwt_trade_search_prompt_msg_id"] = msg.message_id


def _build_trade_search_page(tg_id: int) -> Tuple[str, List[List[InlineKeyboardButton]]]:
    cache = RWT_SEARCH_CACHE.get(tg_id)
    if not cache:
        return "No active search.", []

    results: List[Dict[str, Any]] = cache["results"]
    page: int = cache["page"]
    query_str: str = cache["query"]

    if not results:
        return f"🔍 No clients found for \"{html.escape(query_str)}\".", []

    total = len(results)
    max_page = (total - 1) // RWT_SEARCH_PAGE_SIZE
    page = max(0, min(page, max_page))
    cache["page"] = page

    start_i = page * RWT_SEARCH_PAGE_SIZE
    end_i = start_i + RWT_SEARCH_PAGE_SIZE
    chunk = results[start_i:end_i]

    text = (
        f"🔍 Client results for \"{html.escape(query_str)}\"\n"
        f"Page {page + 1} / {max_page + 1}\n\n"
        "Select a client:"
    )

    keyboard: List[List[InlineKeyboardButton]] = []
    for u in chunk:
        uid = str(u.get("id") or u.get("_id"))
        label = f"{display_name(u)} ({u.get('phone') or ''})"
        keyboard.append([InlineKeyboardButton(label, callback_data=f"rwt_trade_entity:client:{uid}")])

    nav_row: List[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅ Prev", callback_data="rwt_trade_search_page:prev"))
    if end_i < total:
        nav_row.append(InlineKeyboardButton("Next ➡", callback_data="rwt_trade_search_page:next"))
    if nav_row:
        keyboard.append(nav_row)

    return text, keyboard


async def rwt_trade_search_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("rwt_trade_search_mode"):
        return

    context.user_data["rwt_trade_search_mode"] = False

    term = (update.message.text or "").strip()
    chat_id = update.effective_chat.id
    tg_id = update.effective_user.id

    try:
        await update.message.delete()
    except Exception:
        pass

    prompt_id = context.user_data.pop("rwt_trade_search_prompt_msg_id", None)
    if prompt_id is not None:
        await safe_delete_message(context.bot, chat_id, prompt_id)

    if not term:
        msg = await update.effective_chat.send_message(
            "⚠ Empty search. Use /role_wise_trades and tap <b>Search Client</b> again.",
            parse_mode="HTML",
        )
        remember_bot_message_from_message(update, msg)
        return

    token, user = get_logged_in(update, context)
    if not token or not user:
        msg = await update.effective_chat.send_message(
            "⚠ Session expired. Please /start and login again.",
            parse_mode="HTML",
        )
        remember_bot_message_from_message(update, msg)
        return

    all_users = build_all_accessible_users(user)
    term_l = term.lower()

    results: List[Dict[str, Any]] = []
    for u in all_users:
        # only clients
        if u.get("_category") != "client":
            continue
        for field in ("phone", "userName", "username", "name"):
            val = u.get(field)
            if val and term_l in str(val).lower():
                results.append(u)
                break

    RWT_SEARCH_CACHE[tg_id] = {
        "query": term,
        "results": results,
        "page": 0,
    }

    text, keyboard = _build_trade_search_page(tg_id)
    msg = await update.effective_chat.send_message(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
    )
    remember_bot_message_from_message(update, msg)


async def rwt_trade_search_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    tg_id = query.from_user.id
    cache = RWT_SEARCH_CACHE.get(tg_id)
    if not cache:
        return

    _, direction = query.data.split(":", 1)
    if direction == "next":
        cache["page"] += 1
    elif direction == "prev":
        cache["page"] -= 1

    text, keyboard = _build_trade_search_page(tg_id)
    await query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
    )


# ---------- register ----------
def register_role_wise_trade_handlers(app):
    app.add_handler(CommandHandler(["role_wise_trades", "role_wise_trade"], role_wise_trades_cmd))

    app.add_handler(CallbackQueryHandler(rwt_trade_menu_callback, pattern=r"^rwt_trade_menu:"))
    app.add_handler(CallbackQueryHandler(rwt_trade_entity_page_callback, pattern=r"^rwt_trade_entity_page:"))
    app.add_handler(CallbackQueryHandler(rwt_trade_entity_select_callback, pattern=r"^rwt_trade_entity:"))

    app.add_handler(CallbackQueryHandler(rwt_trade_page_callback, pattern=r"^rwt_trade_page:"))
    app.add_handler(CallbackQueryHandler(rwt_trade_detail_callback, pattern=r"^rwt_trade_detail:"))

    app.add_handler(CallbackQueryHandler(rwt_trade_search_start_callback, pattern=r"^rwt_trade_search:start$"))
    app.add_handler(CallbackQueryHandler(rwt_trade_search_page_callback, pattern=r"^rwt_trade_search_page:"))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, rwt_trade_search_text_handler))
