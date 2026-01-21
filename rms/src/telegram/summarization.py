from typing import Dict, Any, List, Tuple
import logging
from datetime import datetime, timedelta
from bson import ObjectId

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

from src.config import summarize, SUPERADMIN_ROLE_ID
from .main import (
    get_logged_in,
    require_login,
    require_login_from_query,
    role_name_from_user,
)
from .session_store import remember_bot_message_from_message

logger = logging.getLogger(__name__)


def get_last_5_dates() -> List[str]:
    """
    Get the last 5 unique dates from summarize collection, sorted descending.
    """
    try:
        pipeline = [
            {"$group": {"_id": "$date"}},
            {"$sort": {"_id": -1}},
            {"$limit": 5}
        ]
        dates = [doc["_id"] for doc in summarize.aggregate(pipeline)]
        return dates
    except Exception as e:
        logger.error(f"Error getting last 5 dates: {e}")
        return []


def get_groups_for_date(date: str) -> List[str]:
    """
    Get all unique groups for a specific date from summarize collection.
    """
    try:
        pipeline = [
            {"$match": {"date": date}},
            {"$group": {"_id": "$group"}},
            {"$sort": {"_id": 1}}
        ]
        groups = [doc["_id"] for doc in summarize.aggregate(pipeline)]
        return groups
    except Exception as e:
        logger.error(f"Error getting groups for date {date}: {e}")
        return []


def get_summary(date: str, group: str, language: str) -> str:
    """
    Get summary for a specific date, group, and language.
    """
    try:
        doc = summarize.find_one({"date": date, "group": group})
        if not doc:
            return "Summary not found."
        
        summarization = doc.get("summarization", [])
        for item in summarization:
            if item.get("language", "").lower() == language.lower():
                return item.get("summary", "Summary not available.")
        
        return "Summary not available for this language."
    except Exception as e:
        logger.error(f"Error getting summary: {e}")
        return f"Error retrieving summary: {str(e)}"


async def summarization_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Command handler for /summarization - shows last 5 dates as buttons.
    Only accessible to superadmin.
    """
    token, user = get_logged_in(update, context)
    if not token or not user:
        return await require_login(update, context)
    
    rn = role_name_from_user(user)
    if rn != "superadmin":
        msg = await update.message.reply_text(
            "❌ Only <b>superadmin</b> can use <code>/summarization</code>.",
            parse_mode="HTML"
        )
        remember_bot_message_from_message(update, msg)
        return
    
    dates = get_last_5_dates()
    
    if not dates:
        msg = await update.message.reply_text(
            "📝 No summaries available yet. Summaries are generated daily at 11:45 PM.",
            parse_mode="HTML"
        )
        remember_bot_message_from_message(update, msg)
        return
    
    keyboard = []
    for date in dates:
        date_display = datetime.strptime(date, "%Y-%m-%d").strftime("%d %b %Y")
        button = InlineKeyboardButton(
            date_display,
            callback_data=f"summ_date_{date}"
        )
        keyboard.append([button])
    
    text = "📅 <b>Select a date to view summaries:</b>\n\nAvailable dates:"
    
    msg = await update.message.reply_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    remember_bot_message_from_message(update, msg)


async def summarization_date_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Callback handler when a date is clicked - shows groups for that date.
    """
    query = update.callback_query
    await query.answer()
    
    token, user = get_logged_in(update, context)
    if not token or not user:
        return await require_login_from_query(query, context)
    
    rn = role_name_from_user(user)
    if rn != "superadmin":
        await query.edit_message_text(
            "❌ Only superadmin can use this command.",
            parse_mode="HTML"
        )
        return
    
    date = query.data.replace("summ_date_", "")
    groups = get_groups_for_date(date)
    
    if not groups:
        await query.edit_message_text(
            f"📝 No groups found for date: <b>{date}</b>",
            parse_mode="HTML"
        )
        return
    
    keyboard = []
    for group in groups:
        button = InlineKeyboardButton(
            group,
            callback_data=f"summ_group_{date}|||{group}"
        )
        keyboard.append([button])
    
    back_button = InlineKeyboardButton(
        "◀️ Back to Dates",
        callback_data="summ_back_dates"
    )
    keyboard.append([back_button])
    
    date_display = datetime.strptime(date, "%Y-%m-%d").strftime("%d %b %Y")
    text = f"📅 <b>Date:</b> {date_display}\n\n<b>Select a group:</b>"
    
    await query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def summarization_group_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Callback handler when a group is clicked - shows language buttons.
    """
    query = update.callback_query
    await query.answer()
    
    token, user = get_logged_in(update, context)
    if not token or not user:
        return await require_login_from_query(query, context)
    
    rn = role_name_from_user(user)
    if rn != "superadmin":
        await query.edit_message_text(
            "❌ Only superadmin can use this command.",
            parse_mode="HTML"
        )
        return
    
    data_str = query.data.replace("summ_group_", "")
    parts = data_str.split("|||", 1)
    if len(parts) != 2:
        await query.edit_message_text("❌ Invalid data format.")
        return
    
    date = parts[0]
    group = parts[1]
    
    keyboard = [
        [InlineKeyboardButton("🇬🇧 English", callback_data=f"summ_lang_{date}|||{group}|||english")],
        [InlineKeyboardButton("🇮🇳 Hindi", callback_data=f"summ_lang_{date}|||{group}|||hindi")],
        [InlineKeyboardButton("🇮🇳 Gujarati", callback_data=f"summ_lang_{date}|||{group}|||gujarati")],
    ]
    
    back_button = InlineKeyboardButton(
        "◀️ Back to Groups",
        callback_data=f"summ_date_{date}"
    )
    keyboard.append([back_button])
    
    date_display = datetime.strptime(date, "%Y-%m-%d").strftime("%d %b %Y")
    text = f"📅 <b>Date:</b> {date_display}\n📱 <b>Group:</b> {group}\n\n<b>Select a language:</b>"
    
    await query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def summarization_language_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Callback handler when a language is clicked - shows the summary.
    """
    query = update.callback_query
    await query.answer()
    
    token, user = get_logged_in(update, context)
    if not token or not user:
        return await require_login_from_query(query, context)
    
    rn = role_name_from_user(user)
    if rn != "superadmin":
        await query.edit_message_text(
            "❌ Only superadmin can use this command.",
            parse_mode="HTML"
        )
        return
    
    data_str = query.data.replace("summ_lang_", "")
    parts = data_str.split("|||")
    if len(parts) != 3:
        await query.edit_message_text("❌ Invalid data format.")
        return
    
    date = parts[0]
    group = parts[1]
    language = parts[2]
    
    summary = get_summary(date, group, language)
    
    date_display = datetime.strptime(date, "%Y-%m-%d").strftime("%d %b %Y")
    language_display = language.capitalize()
    
    text = (
        f"📅 <b>Date:</b> {date_display}\n"
        f"📱 <b>Group:</b> {group}\n"
        f"🌐 <b>Language:</b> {language_display}\n\n"
        f"📝 <b>Summary:</b>\n\n{summary}"
    )
    
    keyboard = [
        [InlineKeyboardButton("◀️ Back to Languages", callback_data=f"summ_group_{date}|||{group}")]
    ]
    
    await query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def summarization_back_dates_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Callback handler for back to dates button.
    """
    query = update.callback_query
    await query.answer()
    
    token, user = get_logged_in(update, context)
    if not token or not user:
        return await require_login_from_query(query, context)
    
    rn = role_name_from_user(user)
    if rn != "superadmin":
        await query.edit_message_text(
            "❌ Only superadmin can use this command.",
            parse_mode="HTML"
        )
        return
    
    dates = get_last_5_dates()
    
    if not dates:
        await query.edit_message_text(
            "📝 No summaries available yet.",
            parse_mode="HTML"
        )
        return
    
    keyboard = []
    for date in dates:
        date_display = datetime.strptime(date, "%Y-%m-%d").strftime("%d %b %Y")
        button = InlineKeyboardButton(
            date_display,
            callback_data=f"summ_date_{date}"
        )
        keyboard.append([button])
    
    text = "📅 <b>Select a date to view summaries:</b>\n\nAvailable dates:"
    
    await query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


def register_summarization_handlers(app):
    """
    Register all summarization command and callback handlers.
    """
    app.add_handler(CommandHandler("summarization", summarization_cmd))
    app.add_handler(CallbackQueryHandler(summarization_date_callback, pattern="^summ_date_"))
    app.add_handler(CallbackQueryHandler(summarization_group_callback, pattern="^summ_group_"))
    app.add_handler(CallbackQueryHandler(summarization_language_callback, pattern="^summ_lang_"))
    app.add_handler(CallbackQueryHandler(summarization_back_dates_callback, pattern="^summ_back_dates$"))
