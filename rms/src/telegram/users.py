# src/telegram/users.py

from typing import Dict, Any, List, Tuple
import logging
import html
from datetime import datetime
from bson import ObjectId

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InlineQueryResultArticle,
    InputTextMessageContent,
)
from telegram.ext import (
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    InlineQueryHandler,
    ContextTypes,
    filters,
)
from src.helpers.hierarchy_service import (
    get_admins_for_superadmin,
    get_masters_for_superadmin,
    get_users_for_superadmin,
    get_masters_for_admin,
    get_users_for_admin,
    get_users_for_master,
    get_user_full_by_id,
)
from .main import (
    get_logged_in,
    require_login,
    require_login_from_query,
    safe_delete_message,
    role_name_from_user,
    display_name,
    build_all_accessible_users,
)
# 🔹 NEW: track bot messages so they can be auto-deleted on session clear
from .session_store import remember_bot_message_from_message

logger = logging.getLogger(__name__)

USER_LIST_CACHE: Dict[int, Dict[str, Any]] = {}
PAGE_SIZE = 5

USER_SEARCH_CACHE: Dict[int, Dict[str, Any]] = {}
SEARCH_PAGE_SIZE = 10


def format_user_list(title: str, users: List[Dict[str, Any]]) -> str:
    if not users:
        return f"{title}\n\nNo records found."

    lines = [title, ""]
    MAX_SHOW = 20
    for idx, u in enumerate(users[:MAX_SHOW], start=1):
        uid = str(u.get("id") or u.get("_id") or "")
        uname = u.get("userName") or u.get("username") or "—"
        phone = u.get("phone") or ""
        lines.append(f"{idx}. {uname} ({phone}) – `{uid}`")

    if len(users) > MAX_SHOW:
        lines.append(f"… and {len(users) - MAX_SHOW} more")

    return "\n".join(lines)


def build_user_summary_text(u: Dict[str, Any], category: str) -> str:
    field_order = [
        ("Name", "name"),
        ("Username", "userName"),
        ("Phone", "phone"),
        ("Credit", "credit"),
        ("Balance", "balance"),
        ("Profit / Loss", "profitLoss"),
        ("Allowed Devices", "allowedDevices"),
        ("Created At", "createdAt"),
        ("Is B2B", "isB2B"),
        ("Device ID", "deviceId"),
        ("Forward Balance", "forwardBalance"),
    ]

    rows: List[str] = []
    label_width = max(len(label) for label, _ in field_order)

    for label, key in field_order:
        raw_val = u.get(key, "-")
        if isinstance(raw_val, ObjectId):
            raw_val = str(raw_val)
        if key == "isB2B":
            raw_val = "Yes" if raw_val else "No"
        if isinstance(raw_val, datetime):
            raw_val = raw_val.isoformat()
        text_val = "-" if raw_val is None else str(raw_val)
        rows.append(f"{label.ljust(label_width)} : {text_val}")

    table_text = "\n".join(rows)
    table_text = html.escape(table_text)

    header = (
        "👤 <b>User Summary</b>\n"
        f"Category: <code>{html.escape(category.title())}</code>\n\n"
        "<pre>"
        f"{table_text}"
        "</pre>"
    )
    return header


# ---------- /users main ----------

async def users_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    token, user = get_logged_in(update, context)
    if not token or not user:
        # track this message too
        msg = await update.message.reply_text(
            "⚠ You must <b>login first</b>.\nUse /start.",
            parse_mode="HTML",
        )
        remember_bot_message_from_message(update, msg)
        return

    rn = role_name_from_user(user)
    buttons: List[List[InlineKeyboardButton]] = []
    title = ""

    if rn == "superadmin":
        title = "Users Module (Superadmin)"
        buttons = [
            [InlineKeyboardButton("👑 Admins", callback_data="users_menu:admin")],
            [InlineKeyboardButton("🧩 Masters", callback_data="users_menu:master")],
            [InlineKeyboardButton("👤 Clients", callback_data="users_menu:client")],
        ]
    elif rn == "admin":
        title = "Users Module (Admin)"
        buttons = [
            [InlineKeyboardButton("🧩 Masters", callback_data="users_menu:master")],
            [InlineKeyboardButton("👤 Clients", callback_data="users_menu:client")],
        ]
    elif rn == "master":
        title = "Users Module (Master)"
        buttons = [
            [InlineKeyboardButton("👤 Clients", callback_data="users_menu:client")],
        ]
    else:
        msg = await update.message.reply_text(
            "❌ This command is only for <b>superadmin</b>, <b>admin</b> or <b>master</b>.",
            parse_mode="HTML",
        )
        remember_bot_message_from_message(update, msg)
        return

    buttons.append(
        [InlineKeyboardButton("🔍 Search User", switch_inline_query_current_chat="")]
    )

    text = (
        f"👥 <b>{html.escape(title)}</b>\n\n"
        "Choose a category or search:\n"
        "Admins / Masters / Clients / Search User\n"
        "Tap a button below."
    )

    msg = await update.message.reply_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(buttons),
    )
    remember_bot_message_from_message(update, msg)


def load_user_list_for_category(user: dict, category: str) -> Tuple[str, List[Dict[str, Any]]]:
    rn = role_name_from_user(user)

    try:
        oid = ObjectId(user.get("id"))
    except Exception:
        return "❌ Invalid user id.", []

    if category == "admin":
        if rn != "superadmin":
            return "❌ Only superadmin can view Admins.", []
        title = "👑 Admins under you:"
        data = get_admins_for_superadmin(oid)
    elif category == "master":
        if rn == "superadmin":
            title = "🧩 Masters (all):"
            data = get_masters_for_superadmin(oid)
        elif rn == "admin":
            title = "🧩 Masters under you:"
            data = get_masters_for_admin(oid)
        else:
            return "❌ Only superadmin or admin can view Masters.", []
    elif category == "client":
        if rn == "superadmin":
            title = "👤 Clients (all):"
            data = get_users_for_superadmin(oid)
        elif rn == "admin":
            title = "👤 Clients under you:"
            data = get_users_for_admin(oid)
        elif rn == "master":
            title = "👤 Clients under you:"
            data = get_users_for_master(oid)
        else:
            return "❌ Only superadmin/admin/master can view Clients.", []
    else:
        return "❌ Unknown category.", []

    return title, data


async def send_user_list_page(query, tg_id: int):
    cache = USER_LIST_CACHE.get(tg_id)
    if not cache:
        return

    title = cache["title"]
    data: List[Dict[str, Any]] = cache["data"]
    page: int = cache["page"]

    total = len(data)
    if total == 0:
        # editing existing bot message, no new id – no need to track
        return await query.edit_message_text(
            f"{title}\n\nNo records found.",
            parse_mode="HTML",
        )

    max_page = (total - 1) // PAGE_SIZE
    if page < 0:
        page = 0
    if page > max_page:
        page = max_page
    cache["page"] = page

    start = page * PAGE_SIZE
    end = start + PAGE_SIZE
    chunk = data[start:end]

    text = (
        f"{html.escape(title)}\n"
        f"Page {page + 1} / {max_page + 1}\n\n"
        "Select a user:"
    )

    user_buttons: List[List[InlineKeyboardButton]] = []
    by_id: Dict[str, Any] = cache["by_id"]

    for u in chunk:
        uid = str(u.get("id") or u.get("_id"))
        by_id[uid] = u
        label = f"{display_name(u)} ({u.get('phone') or ''})"
        user_buttons.append([
            InlineKeyboardButton(
                label,
                callback_data=f"user_detail:{cache['category']}:{uid}",
            )
        ])

    nav_row: List[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅ Prev", callback_data="users_page:prev"))
    if end < total:
        nav_row.append(InlineKeyboardButton("Next ➡", callback_data="users_page:next"))

    keyboard = user_buttons
    if nav_row:
        keyboard.append(nav_row)

    await query.edit_message_text(
        text=text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def users_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    token, user = get_logged_in(update, context)
    if not token or not user:
        return await require_login_from_query(query, context)

    tg_id = query.from_user.id
    _, cat = query.data.split(":", 1)

    title, data = load_user_list_for_category(user, cat)
    if not data:
        # editing the existing message
        return await query.edit_message_text(
            title if title else "No records found.",
            parse_mode="HTML",
        )

    USER_LIST_CACHE[tg_id] = {
        "category": cat,
        "title": title,
        "data": data,
        "page": 0,
        "by_id": {},
    }

    await send_user_list_page(query, tg_id)


async def users_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    tg_id = query.from_user.id
    cache = USER_LIST_CACHE.get(tg_id)
    if not cache:
        return

    _, direction = query.data.split(":", 1)
    if direction == "next":
        cache["page"] += 1
    elif direction == "prev":
        cache["page"] -= 1

    await send_user_list_page(query, tg_id)


# ---------- search flow ----------

async def users_search_start_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    token, user = get_logged_in(update, context)
    if not token or not user:
        return await require_login_from_query(query, context)

    context.user_data["search_mode"] = True

    msg = await query.message.reply_text(
        "🔍 <b>Search user</b>\n"
        "Type <code>phone</code>, <code>userName</code> or <code>name</code>.",
        parse_mode="HTML",
    )
    remember_bot_message_from_message(update, msg)
    context.user_data["search_prompt_msg_id"] = msg.message_id


async def build_search_page_text_and_keyboard(tg_id: int) -> Tuple[str, List[List[InlineKeyboardButton]]]:
    cache = USER_SEARCH_CACHE.get(tg_id)
    if not cache:
        return "No active search.", []

    results: List[Dict[str, Any]] = cache["results"]
    page: int = cache["page"]
    query_str: str = cache["query"]

    if not results:
        text = f"🔍 No users found for \"{html.escape(query_str)}\"."
        return text, []

    total = len(results)
    max_page = (total - 1) // SEARCH_PAGE_SIZE
    if page < 0:
        page = 0
    if page > max_page:
        page = max_page
    cache["page"] = page

    start = page * SEARCH_PAGE_SIZE
    end = start + SEARCH_PAGE_SIZE
    chunk = results[start:end]

    text = (
        f"🔍 Search results for \"{html.escape(query_str)}\"\n"
        f"Page {page + 1} / {max_page + 1}\n\n"
        "Select a user:"
    )

    keyboard: List[List[InlineKeyboardButton]] = []
    for u in chunk:
        uid = str(u.get("id") or u.get("_id"))
        category = u.get("_category", "user")
        label = f"{display_name(u)} ({u.get('phone') or ''}) [{category}]"
        keyboard.append([
            InlineKeyboardButton(
                label,
                callback_data=f"user_detail:{category}:{uid}",
            )
        ])

    nav_row: List[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅ Prev", callback_data="users_search_page:prev"))
    if end < total:
        nav_row.append(InlineKeyboardButton("Next ➡", callback_data="users_search_page:next"))
    if nav_row:
        keyboard.append(nav_row)

    return text, keyboard


async def search_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("search_mode"):
        return

    context.user_data["search_mode"] = False

    term = (update.message.text or "").strip()
    chat_id = update.effective_chat.id
    tg_id = update.effective_user.id

    try:
        await update.message.delete()
    except Exception:
        pass

    prompt_id = context.user_data.pop("search_prompt_msg_id", None)
    if prompt_id is not None:
        await safe_delete_message(context.bot, chat_id, prompt_id)

    if not term:
        msg = await update.effective_chat.send_message(
            "⚠ Empty search. Use /users and tap <b>Search User</b> again.",
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
        for field in ("phone", "userName", "username", "name"):
            val = u.get(field)
            if val and term_l in str(val).lower():
                results.append(u)
                break

    USER_SEARCH_CACHE[tg_id] = {
        "query": term,
        "results": results,
        "page": 0,
    }

    text, keyboard = await build_search_page_text_and_keyboard(tg_id)
    msg = await update.effective_chat.send_message(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
    )
    remember_bot_message_from_message(update, msg)


async def users_search_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    tg_id = query.from_user.id
    cache = USER_SEARCH_CACHE.get(tg_id)
    if not cache:
        return

    _, direction = query.data.split(":", 1)
    if direction == "next":
        cache["page"] += 1
    elif direction == "prev":
        cache["page"] -= 1

    text, keyboard = await build_search_page_text_and_keyboard(tg_id)
    await query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
    )


# ---------- inline @bot search ----------

async def inline_user_search(update, context: ContextTypes.DEFAULT_TYPE):
    inline_query = update.inline_query
    q = (inline_query.query or "").strip()

    token, user = get_logged_in(update, context)
    if not token or not user:
        result = InlineQueryResultArticle(
            id="need_login",
            title="Login required",
            description="Open bot chat and use /start to login.",
            input_message_content=InputTextMessageContent(
                "⚠ Please open the bot chat and login with /start."
            ),
        )
        # inline results are a different flow; we don't track these for deletion
        await inline_query.answer([result], cache_time=1, is_personal=True)
        return

    if not q:
        await inline_query.answer([], cache_time=1, is_personal=True)
        return

    all_users = build_all_accessible_users(user)
    q_l = q.lower()

    matches: List[Dict[str, Any]] = []
    for u in all_users:
        for field in ("phone", "userName", "username", "name"):
            val = u.get(field)
            if val and q_l in str(val).lower():
                matches.append(u)
                break

    results = []
    for idx, u in enumerate(matches[:50]):
        uid = str(u.get("id") or u.get("_id"))
        category = u.get("_category", "user")
        title = f"{display_name(u)} ({u.get('phone') or ''}) [{category}]"

        full_doc = None
        try:
            full_doc = get_user_full_by_id(ObjectId(uid))
        except Exception as e:
            logger.error(f"inline_user_search full_doc error: {e}")
        user_doc = full_doc or u

        summary_text = build_user_summary_text(user_doc, category)

        results.append(
            InlineQueryResultArticle(
                id=f"{idx}-{uid}",
                title=title,
                description=f"ID: {uid}",
                input_message_content=InputTextMessageContent(
                    summary_text,
                    parse_mode="HTML",
                ),
            )
        )

    await inline_query.answer(results, cache_time=1, is_personal=True)


# ---------- user detail + old /admin /master /client ----------

async def user_detail_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    token, _ = get_logged_in(update, context)
    if not token:
        return await require_login_from_query(query, context)

    _, category, uid = query.data.split(":", 2)

    try:
        full_doc = get_user_full_by_id(ObjectId(uid))
    except Exception as e:
        logger.error(f"get_user_full_by_id error: {e}")
        msg = await query.message.reply_text("❌ Could not load user details.")
        remember_bot_message_from_message(update, msg)
        return

    if not full_doc:
        msg = await query.message.reply_text("❌ User not found in database.")
        remember_bot_message_from_message(update, msg)
        return

    header = build_user_summary_text(full_doc, category)
    msg = await query.message.reply_text(header, parse_mode="HTML")
    remember_bot_message_from_message(update, msg)


async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    token, user = get_logged_in(update, context)
    if not token or not user:
        msg = await update.message.reply_text(
            "⚠ You must <b>login first</b>.\nUse /start.",
            parse_mode="HTML",
        )
        remember_bot_message_from_message(update, msg)
        return

    rn = role_name_from_user(user)
    if rn != "superadmin":
        msg = await update.message.reply_text(
            "❌ Only <b>superadmin</b> can use <code>/admin</code>.",
            parse_mode="HTML",
        )
        remember_bot_message_from_message(update, msg)
        return

    try:
        sid = ObjectId(user.get("id"))
        admins = get_admins_for_superadmin(sid)
    except Exception as e:
        logger.error(e)
        msg = await update.message.reply_text("⚠ Error while listing admins.")
        remember_bot_message_from_message(update, msg)
        return

    text = format_user_list("👑 Admins under you:", admins)
    msg = await update.message.reply_text(text, parse_mode="Markdown")
    remember_bot_message_from_message(update, msg)


async def master_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    token, user = get_logged_in(update, context)
    if not token or not user:
        msg = await update.message.reply_text(
            "⚠ You must <b>login first</b>.\nUse /start.",
            parse_mode="HTML",
        )
        remember_bot_message_from_message(update, msg)
        return

    rn = role_name_from_user(user)

    try:
        oid = ObjectId(user.get("id"))
    except Exception:
        msg = await update.message.reply_text(
            "❌ Invalid user id.",
            parse_mode="Markdown",
        )
        remember_bot_message_from_message(update, msg)
        return

    try:
        if rn == "superadmin":
            masters = get_masters_for_superadmin(oid)
            title = "🧩 Masters (all):"
        elif rn == "admin":
            masters = get_masters_for_admin(oid)
            title = "🧩 Masters under you:"
        else:
            msg = await update.message.reply_text(
                "❌ Only *superadmin* or *admin* can use `/master`.",
                parse_mode="Markdown",
            )
            remember_bot_message_from_message(update, msg)
            return
    except Exception as e:
        logger.error(e)
        msg = await update.message.reply_text("⚠ Error while listing masters.")
        remember_bot_message_from_message(update, msg)
        return

    text = format_user_list(title, masters)
    msg = await update.message.reply_text(text, parse_mode="Markdown")
    remember_bot_message_from_message(update, msg)


async def client_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    token, user = get_logged_in(update, context)
    if not token or not user:
        msg = await update.message.reply_text(
            "⚠ You must <b>login first</b>.\nUse /start.",
            parse_mode="HTML",
        )
        remember_bot_message_from_message(update, msg)
        return

    rn = role_name_from_user(user)

    try:
        oid = ObjectId(user.get("id"))
    except Exception:
        msg = await update.message.reply_text(
            "❌ Invalid user id.",
            parse_mode="Markdown",
        )
        remember_bot_message_from_message(update, msg)
        return

    try:
        if rn == "superadmin":
            clients = get_users_for_superadmin(oid)
            title = "👤 Clients (all):"
        elif rn == "admin":
            clients = get_users_for_admin(oid)
            title = "👤 Clients under you:"
        elif rn == "master":
            clients = get_users_for_master(oid)
            title = "👤 Clients under you:"
        else:
            msg = await update.message.reply_text(
                "❌ Only *superadmin*, *admin* or *master* can use `/client`.",
                parse_mode="Markdown",
            )
            remember_bot_message_from_message(update, msg)
            return
    except Exception as e:
        logger.error(e)
        msg = await update.message.reply_text("⚠ Error while listing clients.")
        remember_bot_message_from_message(update, msg)
        return

    text = format_user_list(title, clients)
    msg = await update.message.reply_text(text, parse_mode="Markdown")
    remember_bot_message_from_message(update, msg)


def register_user_handlers(app):
    app.add_handler(CommandHandler("users", users_cmd))
    app.add_handler(CommandHandler("admin", admin_cmd))
    app.add_handler(CommandHandler("master", master_cmd))
    app.add_handler(CommandHandler("client", client_cmd))

    app.add_handler(CallbackQueryHandler(users_menu_callback, pattern=r"^users_menu:"))
    app.add_handler(CallbackQueryHandler(users_page_callback, pattern=r"^users_page:"))
    app.add_handler(CallbackQueryHandler(users_search_start_callback, pattern=r"^users_search:start$"))
    app.add_handler(CallbackQueryHandler(users_search_page_callback, pattern=r"^users_search_page:"))
    app.add_handler(CallbackQueryHandler(user_detail_callback, pattern=r"^user_detail:"))

    app.add_handler(InlineQueryHandler(inline_user_search))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, search_text_handler))
