import os
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional
from openai import OpenAI
from dotenv import load_dotenv
from zoneinfo import ZoneInfo
import httpx
import html
from src.config import messages, summarize, config, notification

load_dotenv()

logger = logging.getLogger(__name__)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

if not OPENAI_API_KEY:
    logger.error("OPENAI_API_KEY is missing in .env")
    raise ValueError("OPENAI_API_KEY is required")

openai_client = OpenAI(api_key=OPENAI_API_KEY)


def build_conversation_text(doc: Dict) -> str:
    """
    Build a conversation text from messages array.
    Format: "Sender: text\nSender: text\n..."
    """
    conversation_lines = []
    msg_array = doc.get("messages", [])
    
    for msg in msg_array:
        sender = msg.get("sender", "Unknown")
        text = msg.get("text", "")
        if text:
            conversation_lines.append(f"{sender}: {text}")
    
    return "\n".join(conversation_lines)


def summarize_with_openai(conversation_text: str, language: str) -> str:
    """
    Summarize conversation in the specified language.
    
    Args:
        conversation_text: The full conversation text
        language: 'english', 'hindi', or 'gujarati'
    
    Returns:
        Summary text in the specified language
    """
    language_prompts = {
        "english": "Summarize the following WhatsApp group chat conversation in English. Provide a concise summary of what actually happened and what conversation occurred:",
        "hindi": "निम्नलिखित WhatsApp ग्रुप चैट वार्तालाप को हिंदी में सारांशित करें। वास्तव में क्या हुआ और क्या बातचीत हुई, इसका संक्षिप्त सारांश प्रदान करें:",
        "gujarati": "નીચેની WhatsApp ગ્રુપ ચેટ વાતચીતને ગુજરાતીમાં સારાંશ આપો. ખરેખર શું થયું અને કઈ વાતચીત થઈ તેનો સંક્ષિપ્ત સારાંશ પ્રદાન કરો:"
    }
    
    prompt = language_prompts.get(language.lower(), language_prompts["english"])
    
    try:
        response = openai_client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": "You are a helpful assistant that summarizes WhatsApp group chat conversations. Provide clear and concise summaries."
                },
                {
                    "role": "user",
                    "content": f"{prompt}\n\n{conversation_text}"
                }
            ],
            temperature=0.3,
            max_tokens=500
        )
        
        summary = response.choices[0].message.content.strip()
        return summary
    
    except Exception as e:
        logger.error(f"Error summarizing in {language}: {e}")
        return f"Error generating summary in {language}"


def summarize_document(doc: Dict) -> Optional[Dict]:
    """
    Summarize a single document from messages collection.
    
    Args:
        doc: Document from messages collection with date, group, and messages array
    
    Returns:
        Dictionary with date, group, and summarization array, or None if error
    """
    try:
        date = doc.get("date")
        group = doc.get("group")
        msg_array = doc.get("messages", [])
        
        if not date or not group or not msg_array:
            logger.warning(f"Skipping document: missing required fields (date, group, or messages)")
            return None
        
        conversation_text = build_conversation_text(doc)
        
        if not conversation_text.strip():
            logger.warning(f"Skipping document: no conversation text found")
            return None
        
        logger.info(f"Summarizing document: date={date}, group={group}, messages={len(msg_array)}")
        
        english_summary = summarize_with_openai(conversation_text, "english")
        hindi_summary = summarize_with_openai(conversation_text, "hindi")
        gujarati_summary = summarize_with_openai(conversation_text, "gujarati")
        
        summary_doc = {
            "date": date,
            "group": group,
            "summarization": [
                {"language": "english", "summary": english_summary},
                {"language": "hindi", "summary": hindi_summary},
                {"language": "gujarati", "summary": gujarati_summary}
            ],
            "createdAt": datetime.now(timezone.utc),
            "updatedAt": datetime.now(timezone.utc)
        }
        
        return summary_doc
    
    except Exception as e:
        logger.error(f"Error summarizing document: {e}")
        return None


def get_superadmin_chat_ids() -> List[int]:
    """
    Get all Telegram chat IDs for superadmins from notification collection.
    """
    try:
        chat_ids = []
        superadmin_docs = notification.find({"role": "superadmin"})
        
        for doc in superadmin_docs:
            doc_chat_ids = doc.get("chat_ids", [])
            for chat_id in doc_chat_ids:
                try:
                    chat_ids.append(int(chat_id))
                except (ValueError, TypeError):
                    continue
        
        return list(set(chat_ids))
    except Exception as e:
        logger.error(f"Error getting superadmin chat IDs: {e}")
        return []


def send_summary_notification(date: str, group: str, english_summary: str) -> bool:
    """
    Send summary notification to all superadmins via Telegram Bot API.
    
    Args:
        date: Date string (e.g., "2026-01-21")
        group: Group name (e.g., "ProTrader5.Pro")
        english_summary: English summary text
    
    Returns:
        True if at least one notification was sent successfully
    """
    try:
        bot_token = config.TELEGRAM_BOT_TOKEN
        if not bot_token:
            logger.warning("TELEGRAM_BOT_TOKEN not configured, skipping notification")
            return False
        
        chat_ids = get_superadmin_chat_ids()
        if not chat_ids:
            logger.info("No superadmin chat IDs found, skipping notification")
            return False
        
        date_display = datetime.strptime(date, "%Y-%m-%d").strftime("%d %b %Y")
        
        message = (
            f"📅 <b>Date:</b> {date_display}\n"
            f"📱 <b>Group:</b> {html.escape(group)}\n"
            f"🌐 <b>Language:</b> English\n\n"
            f"📝 <b>Summary:</b>\n\n{html.escape(english_summary)}"
        )
        
        api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        success_count = 0
        
        with httpx.Client(timeout=10.0) as client:
            for chat_id in chat_ids:
                try:
                    response = client.post(
                        api_url,
                        json={
                            "chat_id": chat_id,
                            "text": message,
                            "parse_mode": "HTML"
                        }
                    )
                    response.raise_for_status()
                    success_count += 1
                except Exception as e:
                    logger.error(f"Error sending notification to chat_id {chat_id}: {e}")
                    continue
        
        if success_count > 0:
            logger.info(f"Summary notification sent to {success_count}/{len(chat_ids)} superadmins")
            return True
        else:
            logger.warning("Failed to send summary notification to any superadmin")
            return False
    
    except Exception as e:
        logger.error(f"Error sending summary notification: {e}")
        return False


def save_summary(summary_doc: Dict) -> bool:
    """
    Save summary document to summarize collection and send notification to superadmins.
    Uses upsert based on date and group to avoid duplicates.
    
    Args:
        summary_doc: Summary document to save
    
    Returns:
        True if successful, False otherwise
    """
    try:
        date = summary_doc.get("date")
        group = summary_doc.get("group")
        
        if not date or not group:
            logger.error("Cannot save summary: missing date or group")
            return False
        
        result = summarize.update_one(
            {"date": date, "group": group},
            {"$set": summary_doc},
            upsert=True
        )
        
        if result.upserted_id or result.modified_count > 0:
            logger.info(f"Summary saved: date={date}, group={group}")
            
            summarization = summary_doc.get("summarization", [])
            english_summary = None
            for item in summarization:
                if item.get("language", "").lower() == "english":
                    english_summary = item.get("summary", "")
                    break
            
            if english_summary:
                send_summary_notification(date, group, english_summary)
            
            return True
        else:
            logger.warning(f"Summary not saved: date={date}, group={group}")
            return False
    
    except Exception as e:
        logger.error(f"Error saving summary: {e}")
        return False


def get_today_date() -> str:
    """
    Get today's date in YYYY-MM-DD format using the app timezone.
    """
    app_tz = getattr(config, "APP_TZ", ZoneInfo("Asia/Kolkata"))
    today = datetime.now(app_tz).date()
    return today.isoformat()


def process_all_documents():
    """
    Process only today's documents from messages collection and create summaries.
    """
    try:
        today_date = get_today_date()
        logger.info(f"Processing documents for today's date: {today_date}")
        
        today_docs = list(messages.find({"date": today_date}))
        logger.info(f"Found {len(today_docs)} documents for today ({today_date})")
        
        if not today_docs:
            logger.info(f"No documents found for today ({today_date})")
            return 0, 0
        
        success_count = 0
        error_count = 0
        
        for doc in today_docs:
            summary_doc = summarize_document(doc)
            if summary_doc:
                if save_summary(summary_doc):
                    success_count += 1
                else:
                    error_count += 1
            else:
                error_count += 1
        
        logger.info(f"Processing complete: {success_count} successful, {error_count} errors")
        return success_count, error_count
    
    except Exception as e:
        logger.error(f"Error processing documents: {e}")
        return 0, 0


def process_document_by_date_and_group(date: str, group: str):
    """
    Process a specific document by date and group.
    
    Args:
        date: Date string (e.g., "2026-01-21")
        group: Group name (e.g., "ProTrader5.Pro")
    """
    try:
        doc = messages.find_one({"date": date, "group": group})
        if not doc:
            logger.warning(f"Document not found: date={date}, group={group}")
            return False
        
        summary_doc = summarize_document(doc)
        if summary_doc:
            return save_summary(summary_doc)
        return False
    
    except Exception as e:
        logger.error(f"Error processing document by date and group: {e}")
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    process_all_documents()
