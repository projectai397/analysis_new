import os
import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)

load_dotenv()

# ===== ENV =====
GOOGLE_SA_JSON = os.environ["GOOGLE_SA_JSON"]
SHEET_ID = os.environ["SHEET_ID"]
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "Sheet1")
BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]

# ===== Google Sheet client (cached) =====
def get_worksheet():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(GOOGLE_SA_JSON, scopes=scopes)
    gc = gspread.authorize(creds)
    ws = gc.open_by_key(SHEET_ID).worksheet(WORKSHEET_NAME)
    return ws

# Find if chat_id already exists in subscriber column (C)
def is_subscribed(ws, chat_id: str) -> bool:
    # Column C values (subscriber). get_all_values includes header row too.
    col = ws.col_values(3)  # 1-based, 3 => column C
    chat_id = str(chat_id).strip()
    return chat_id in [c.strip() for c in col if c.strip()]

# Append subscriber chat_id in a new row (A,B empty, C filled)
def add_subscriber(ws, chat_id: str) -> None:
    ws.append_row(["", "", str(chat_id)], value_input_option="RAW")

# Optional: remove subscriber (all matches)
def remove_subscriber(ws, chat_id: str) -> int:
    chat_id = str(chat_id).strip()
    # Get all values to find rows where column C equals chat_id
    values = ws.get_all_values()
    rows_to_delete = []
    for idx, row in enumerate(values, start=1):
        if idx == 1:
            continue  # header
        c = row[2].strip() if len(row) >= 3 else ""
        if c == chat_id:
            rows_to_delete.append(idx)
    # Delete bottom-up to avoid shifting indices
    for r in reversed(rows_to_delete):
        ws.delete_rows(r)
    return len(rows_to_delete)

# ===== Telegram handlers =====
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("✅ Subscribe", callback_data="subscribe")],
        [InlineKeyboardButton("❌ Unsubscribe", callback_data="unsubscribe")],
    ]
    text = (
        "Welcome.\n\n"
        "Use the buttons below to manage website status notifications."
    )
    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    chat_id = query.message.chat_id

    ws = get_worksheet()

    if query.data == "subscribe":
        if is_subscribed(ws, str(chat_id)):
            await query.edit_message_text("You are already subscribed.")
            return

        add_subscriber(ws, str(chat_id))
        await query.edit_message_text("✅ Subscribed successfully. You will receive website status notifications.")

    elif query.data == "unsubscribe":
        if not is_subscribed(ws, str(chat_id)):
            await query.edit_message_text("You are not subscribed.")
            return

        removed = remove_subscriber(ws, str(chat_id))
        await query.edit_message_text(f"❌ Unsubscribed successfully. Removed {removed} entry(s).")

def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(on_button))
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()
