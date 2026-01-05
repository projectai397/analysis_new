# src/telegram/role_wise_positions.py

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

from src.config import trade_market
from src.helpers.hierarchy_service import (
    get_admins_for_superadmin,
    get_masters_for_superadmin,
    get_masters_for_admin,
    get_users_for_superadmin,
    get_users_for_admin,
    get_users_for_master,
    get_user_full_by_id,  # ✅ NEW
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
ENTITY_LIST_CACHE: Dict[int, Dict[str, Any]] = {}
ENTITY_PAGE_SIZE = 5

RWP_POS_CACHE: Dict[int, Dict[str, Any]] = {}
RWP_POS_PAGE_SIZE = 5

RWP_SEARCH_CACHE: Dict[int, Dict[str, Any]] = {}
RWP_SEARCH_PAGE_SIZE = 10


# ---------- helpers ----------
def resolve_user_display(user_id: ObjectId) -> str:
    """
    Convert a stored userId(ObjectId) into a readable name/userName/username/phone.
    Falls back to the raw ObjectId string if lookup fails.
    """
    try:
        user = get_user_full_by_id(user_id)
        if not user:
            return str(user_id)

        return (
            user.get("name")
            or user.get("userName")
            or user.get("username")
            or user.get("phone")
            or str(user_id)
        )
    except Exception:
        return str(user_id)

def format_entity_header(entity_type: str, entity_id: str) -> str:
    """
    Example: 'Master: John (<code>6943...</code>)'
    """
    try:
        name = resolve_user_display(ObjectId(entity_id))
    except Exception:
        name = entity_id
    return f"{entity_type}: {html.escape(str(name))} (<code>{html.escape(str(entity_id))}</code>)"

def _oid_from_user_doc(u: Dict[str, Any]) -> Optional[ObjectId]:
    raw = u.get("id") or u.get("_id")
    if not raw:
        return None
    try:
        return ObjectId(str(raw))
    except Exception:
        return None


def _role_title(rn: str) -> str:
    if rn == "superadmin":
        return "Role-wise Positions (Superadmin)"
    if rn == "admin":
        return "Role-wise Positions (Admin)"
    if rn == "master":
        return "Role-wise Positions (Master)"
    return "Role-wise Positions"


def _load_entities_for_category(current_user: Dict[str, Any], category: str) -> Tuple[str, List[Dict[str, Any]]]:
    rn = role_name_from_user(current_user)
    me_oid = _oid_from_user_doc(current_user)
    if not me_oid:
        return "❌ Invalid user id.", []

    if category == "admin":
        if rn != "superadmin":
            return "❌ Only superadmin can view Admins.", []
        title = "👑 Admins under you:"
        data = get_admins_for_superadmin(me_oid)
        return title, data

    if category == "master":
        if rn == "superadmin":
            title = "🧩 Masters (all):"
            data = get_masters_for_superadmin(me_oid)
            return title, data
        if rn == "admin":
            title = "🧩 Masters under you:"
            data = get_masters_for_admin(me_oid)
            return title, data
        return "❌ Only superadmin or admin can view Masters.", []

    if category == "client":
        if rn == "superadmin":
            title = "👤 Clients (all):"
            data = get_users_for_superadmin(me_oid)
            return title, data
        if rn == "admin":
            title = "👤 Clients under you:"
            data = get_users_for_admin(me_oid)
            return title, data
        if rn == "master":
            title = "👤 Clients under you:"
            data = get_users_for_master(me_oid)
            return title, data
        return "❌ Only superadmin/admin/master can view Clients.", []

    return "❌ Unknown category.", []


def _load_clients_under_entity(entity_category: str, entity_oid: ObjectId) -> List[Dict[str, Any]]:
    if entity_category == "admin":
        return get_users_for_admin(entity_oid)
    if entity_category == "master":
        return get_users_for_master(entity_oid)
    return []


def _query_positions_for_user_ids(user_ids: List[ObjectId]) -> List[Dict[str, Any]]:
    start, end = today_utc_range()
    cursor = trade_market.find(
        {
            "userId": {"$in": user_ids},
            "squareOff": False,
            "createdAt": {"$gte": start, "$lt": end},
        }
    ).sort("createdAt", -1)
    return list(cursor)


def _build_positions_page(tg_id: int) -> Tuple[str, List[List[InlineKeyboardButton]]]:
    cache = RWP_POS_CACHE.get(tg_id)
    if not cache:
        return "No positions cached.", []

    items: List[Dict[str, Any]] = cache["items"]
    page: int = cache["page"]
    header_title: str = cache.get("header_title", "📊 <b>Open Positions (today)</b>")

    if not items:
        return f"{header_title}\n\n📊 No open positions for today.", []

    total = len(items)
    max_page = (total - 1) // RWP_POS_PAGE_SIZE
    page = max(0, min(page, max_page))
    cache["page"] = page

    start_i = page * RWP_POS_PAGE_SIZE
    end_i = start_i + RWP_POS_PAGE_SIZE
    chunk = items[start_i:end_i]

    text = (
        f"{header_title}\n"
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
        keyboard.append([InlineKeyboardButton(label, callback_data=f"rwp_pos_detail:{pid}")])

    nav_row: List[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅ Prev", callback_data="rwp_pos_page:prev"))
    if end_i < total:
        nav_row.append(InlineKeyboardButton("Next ➡", callback_data="rwp_pos_page:next"))
    if nav_row:
        keyboard.append(nav_row)

    return text, keyboard


def _build_entity_list_page(tg_id: int) -> Tuple[str, List[List[InlineKeyboardButton]]]:
    cache = ENTITY_LIST_CACHE.get(tg_id)
    if not cache:
        return "No list cached.", []

    title: str = cache["title"]
    entities: List[Dict[str, Any]] = cache["entities"]
    page: int = cache["page"]
    category: str = cache["category"]

    if not entities:
        return f"{html.escape(title)}\n\nNo records found.", []

    total = len(entities)
    max_page = (total - 1) // ENTITY_PAGE_SIZE
    page = max(0, min(page, max_page))
    cache["page"] = page

    start_i = page * ENTITY_PAGE_SIZE
    end_i = start_i + ENTITY_PAGE_SIZE
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
        keyboard.append([InlineKeyboardButton(label, callback_data=f"rwp_entity:{category}:{uid}")])

    nav_row: List[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅ Prev", callback_data="rwp_entity_page:prev"))
    if end_i < total:
        nav_row.append(InlineKeyboardButton("Next ➡", callback_data="rwp_entity_page:next"))
    if nav_row:
        keyboard.append(nav_row)

    return text, keyboard


# ---------- /role_wise_position main ----------
async def role_wise_position_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    token, user = get_logged_in(update, context)
    if not token or not user:
        return await require_login(update, context)

    rn = role_name_from_user(user)
    title = _role_title(rn)

    buttons: List[List[InlineKeyboardButton]] = []
    if rn == "superadmin":
        buttons = [
            [InlineKeyboardButton("👑 Admins", callback_data="rwp_menu:admin")],
            [InlineKeyboardButton("🧩 Masters", callback_data="rwp_menu:master")],
            [InlineKeyboardButton("👤 Clients", callback_data="rwp_menu:client")],
        ]
    elif rn == "admin":
        buttons = [
            [InlineKeyboardButton("🧩 Masters", callback_data="rwp_menu:master")],
            [InlineKeyboardButton("👤 Clients", callback_data="rwp_menu:client")],
        ]
    elif rn == "master":
        buttons = [
            [InlineKeyboardButton("👤 Clients", callback_data="rwp_menu:client")],
        ]
    else:
        msg = await update.message.reply_text(
            "❌ This command is only for <b>superadmin</b>, <b>admin</b> or <b>master</b>.",
            parse_mode="HTML",
        )
        remember_bot_message_from_message(update, msg)
        return

    buttons.append([InlineKeyboardButton("🔍 Search Client", callback_data="rwp_search:start")])

    text = (
        f"📌 <b>{html.escape(title)}</b>\n\n"
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


# ---------- menu → list entities ----------
async def rwp_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

    ENTITY_LIST_CACHE[tg_id] = {
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


async def rwp_entity_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    tg_id = query.from_user.id
    cache = ENTITY_LIST_CACHE.get(tg_id)
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


# ---------- entity selected → load positions ----------
async def rwp_entity_select_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

    # If entity is client: positions only for that client
    if entity_category == "client":
        client_ids = [entity_oid]
        header_title = "📊 <b>Open Positions (today)</b>\n" + format_entity_header("Client", entity_id)
    else:
        try:
            clients = _load_clients_under_entity(entity_category, entity_oid)
        except Exception as e:
            logger.error(f"_load_clients_under_entity error: {e}")
            msg = await query.message.reply_text("⚠ Error while loading clients under selection.")
            remember_bot_message_from_message(update, msg)
            return

        client_ids = []
        for c in clients:
            cid = c.get("id") or c.get("_id")
            if cid:
                try:
                    client_ids.append(ObjectId(str(cid)))
                except Exception:
                    pass

        header_title = "📊 <b>Open Positions (today)</b>\n" + format_entity_header(entity_category.title(), entity_id)

    if not client_ids:
        msg = await query.message.reply_text("📊 No clients found under that selection.")
        remember_bot_message_from_message(update, msg)
        return

    try:
        items = _query_positions_for_user_ids(client_ids)
    except Exception as e:
        logger.error(f"positions query error: {e}")
        msg = await query.message.reply_text("⚠ Error while loading positions.")
        remember_bot_message_from_message(update, msg)
        return

    RWP_POS_CACHE[tg_id] = {
        "items": items,
        "page": 0,
        "header_title": header_title,
    }

    text, keyboard = _build_positions_page(tg_id)
    msg = await query.message.reply_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
    )
    remember_bot_message_from_message(update, msg)


async def rwp_pos_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    tg_id = query.from_user.id
    cache = RWP_POS_CACHE.get(tg_id)
    if not cache:
        return

    _, direction = query.data.split(":", 1)
    if direction == "next":
        cache["page"] += 1
    elif direction == "prev":
        cache["page"] -= 1

    text, keyboard = _build_positions_page(tg_id)
    await query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
    )


async def rwp_pos_detail_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    token, _ = get_logged_in(update, context)
    if not token:
        return await require_login_from_query(query, context)

    _, pid = query.data.split(":", 1)

    try:
        doc = trade_market.find_one({"_id": ObjectId(pid)})
    except Exception as e:
        logger.error(f"rwp_pos_detail find_one error: {e}")
        msg = await query.message.reply_text("❌ Could not load position details.")
        remember_bot_message_from_message(update, msg)
        return

    if not doc:
        msg = await query.message.reply_text("❌ Position not found.")
        remember_bot_message_from_message(update, msg)
        return

    # ✅ UPDATED: show readable User, not ObjectId
    field_order = [
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
    label_width = max(label_width, len("User"))

    user_display = "-"
    uid = doc.get("userId")
    if isinstance(uid, ObjectId):
        user_display = resolve_user_display(uid)

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
    header = "📊 <b>Position Summary</b>\n\n<pre>" + table_text + "</pre>"

    msg = await query.message.reply_text(header, parse_mode="HTML")
    remember_bot_message_from_message(update, msg)


# ---------- search flow (clients only) ----------
async def rwp_search_start_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    token, user = get_logged_in(update, context)
    if not token or not user:
        return await require_login_from_query(query, context)

    context.user_data["rwp_search_mode"] = True

    msg = await query.message.reply_text(
        "🔍 <b>Search client</b>\nType <code>phone</code>, <code>userName</code> or <code>name</code>.",
        parse_mode="HTML",
    )
    remember_bot_message_from_message(update, msg)
    context.user_data["rwp_search_prompt_msg_id"] = msg.message_id


def _build_search_page(tg_id: int) -> Tuple[str, List[List[InlineKeyboardButton]]]:
    cache = RWP_SEARCH_CACHE.get(tg_id)
    if not cache:
        return "No active search.", []

    results: List[Dict[str, Any]] = cache["results"]
    page: int = cache["page"]
    query_str: str = cache["query"]

    if not results:
        return f"🔍 No clients found for \"{html.escape(query_str)}\".", []

    total = len(results)
    max_page = (total - 1) // RWP_SEARCH_PAGE_SIZE
    page = max(0, min(page, max_page))
    cache["page"] = page

    start_i = page * RWP_SEARCH_PAGE_SIZE
    end_i = start_i + RWP_SEARCH_PAGE_SIZE
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
        keyboard.append([InlineKeyboardButton(label, callback_data=f"rwp_entity:client:{uid}")])

    nav_row: List[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅ Prev", callback_data="rwp_search_page:prev"))
    if end_i < total:
        nav_row.append(InlineKeyboardButton("Next ➡", callback_data="rwp_search_page:next"))
    if nav_row:
        keyboard.append(nav_row)

    return text, keyboard


async def rwp_search_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("rwp_search_mode"):
        return

    context.user_data["rwp_search_mode"] = False

    term = (update.message.text or "").strip()
    chat_id = update.effective_chat.id
    tg_id = update.effective_user.id

    try:
        await update.message.delete()
    except Exception:
        pass

    prompt_id = context.user_data.pop("rwp_search_prompt_msg_id", None)
    if prompt_id is not None:
        await safe_delete_message(context.bot, chat_id, prompt_id)

    if not term:
        msg = await update.effective_chat.send_message(
            "⚠ Empty search. Use /role_wise_position and tap <b>Search Client</b> again.",
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
        rn = role_name_from_user(u)
        if rn in ("superadmin", "admin", "master"):
            continue

        for field in ("phone", "userName", "username", "name"):
            val = u.get(field)
            if val and term_l in str(val).lower():
                results.append(u)
                break

    RWP_SEARCH_CACHE[tg_id] = {
        "query": term,
        "results": results,
        "page": 0,
    }

    text, keyboard = _build_search_page(tg_id)
    msg = await update.effective_chat.send_message(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
    )
    remember_bot_message_from_message(update, msg)


async def rwp_search_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    tg_id = query.from_user.id
    cache = RWP_SEARCH_CACHE.get(tg_id)
    if not cache:
        return

    _, direction = query.data.split(":", 1)
    if direction == "next":
        cache["page"] += 1
    elif direction == "prev":
        cache["page"] -= 1

    text, keyboard = _build_search_page(tg_id)
    await query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
    )


# ---------- register ----------
def register_role_wise_position_handlers(app):
    app.add_handler(CommandHandler(["role_wise_position", "role_wise_positions"], role_wise_position_cmd))

    app.add_handler(CallbackQueryHandler(rwp_menu_callback, pattern=r"^rwp_menu:"))
    app.add_handler(CallbackQueryHandler(rwp_entity_page_callback, pattern=r"^rwp_entity_page:"))
    app.add_handler(CallbackQueryHandler(rwp_entity_select_callback, pattern=r"^rwp_entity:"))

    app.add_handler(CallbackQueryHandler(rwp_pos_page_callback, pattern=r"^rwp_pos_page:"))
    app.add_handler(CallbackQueryHandler(rwp_pos_detail_callback, pattern=r"^rwp_pos_detail:"))

    app.add_handler(CallbackQueryHandler(rwp_search_start_callback, pattern=r"^rwp_search:start$"))
    app.add_handler(CallbackQueryHandler(rwp_search_page_callback, pattern=r"^rwp_search_page:"))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, rwp_search_text_handler))
