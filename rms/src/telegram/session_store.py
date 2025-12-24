from asyncio import sleep
from typing import Dict, List, Tuple
import time
from telegram import Update
from telegram.ext import ContextTypes

# -----------------------------
# Session data
# -----------------------------
USER_TOKENS: Dict[int, str] = {}
USER_INFO: Dict[int, dict] = {}
USER_SESSION_EXPIRES: Dict[int, float] = {}

# 600 seconds = 10 minutes
SESSION_TIMEOUT_SECONDS = 600  

# Bot message tracking (for auto-delete on session clear)
# key: telegram user id -> list of (chat_id, message_id)
BOT_MESSAGES: Dict[int, List[Tuple[int, int]]] = {}


def remember_bot_message_from_message(update: Update, msg) -> None:
    """
    Store bot message (chat_id, message_id) per Telegram user, so we can
    delete them later when the session expires or is cleared.
    """
    tg_id = update.effective_user.id
    BOT_MESSAGES.setdefault(tg_id, []).append((msg.chat_id, msg.message_id))


async def _delete_all_bot_messages_for_user(
    context: ContextTypes.DEFAULT_TYPE,
    tg_id: int,
) -> None:
    """
    Delete all bot messages that are associated with the user.
    """
    msgs = BOT_MESSAGES.pop(tg_id, [])
    if not msgs:
        return

    for chat_id, message_id in msgs:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        except Exception:
            # Ignore individual delete failures
            pass


async def session_expiry_task(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    tg_id: int,
    deadline: float,
) -> None:
    """
    Monitor session timeout and clear the session once expired.
    The 'deadline' is calculated at /start time.
    """
    while True:
        now = time.time()
        remaining = deadline - now
        if remaining <= 0:
            break
        # Sleep only as long as still remaining; if the process is delayed,
        # the loop will exit on the next iteration.
        await sleep(remaining)

    # Ensure this task is still valid (no newer /start call has updated the deadline)
    current_deadline = USER_SESSION_EXPIRES.get(tg_id)
    if current_deadline != deadline:
        # A newer session timer has been scheduled; this task is obsolete.
        return

    # Clear session and delete all messages
    clear_session(update, context)


# -----------------------------
# Core session helpers
# -----------------------------
def get_logged_in(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> Tuple[str | None, dict | None]:
    """
    Return (token, user) for this Telegram user if the session is still valid.
    If expired or not present, clear session and return (None, None).
    """
    tg_id = update.effective_user.id
    token = USER_TOKENS.get(tg_id)
    user = USER_INFO.get(tg_id)
    exp = USER_SESSION_EXPIRES.get(tg_id)

    if not token or not user or not exp or time.time() > exp:
        clear_session(update, context)
        return None, None

    return token, user


def set_session(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    token: str,
    user: dict,
) -> None:
    """
    Store token + user for this Telegram user.
    IMPORTANT: This no longer controls the timer. The timer is now started
    when the user sends /start, not here.
    """
    tg_id = update.effective_user.id
    USER_TOKENS[tg_id] = token
    USER_INFO[tg_id] = user
    context.user_data["user"] = user


def start_session_timer(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    """
    Start (or restart) the session timer when the user sends /start.
    After SESSION_TIMEOUT_SECONDS, clear_session will run and delete all bot messages.
    """
    tg_id = update.effective_user.id
    deadline = time.time() + SESSION_TIMEOUT_SECONDS

    # Save deadline so later we can detect if a newer /start has reset this
    USER_SESSION_EXPIRES[tg_id] = deadline

    # Launch async expiry monitor
    context.application.create_task(
        session_expiry_task(update, context, tg_id, deadline)
    )


def clear_session(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Clear the in-memory session for this Telegram user and schedule message deletion.
    """
    tg_id = update.effective_user.id

    USER_TOKENS.pop(tg_id, None)
    USER_INFO.pop(tg_id, None)
    USER_SESSION_EXPIRES.pop(tg_id, None)
    context.user_data.clear()

    # Schedule async message deletion
    context.application.create_task(
        _delete_all_bot_messages_for_user(context, tg_id)
    )
