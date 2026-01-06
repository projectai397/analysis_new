# src/helpers.py
import json
import logging
import os
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from threading import Lock
from typing import Any, Dict, Optional
from dotenv import load_dotenv
from src import config
import jwt
from collections import defaultdict
import uuid
from threading import Lock, Timer
from zoneinfo import ZoneInfo
import requests
from bson import ObjectId
from flask import request
from pymongo import MongoClient
from jwt import ExpiredSignatureError, InvalidTokenError

try:
    # Better similarity if present
    from rapidfuzz import fuzz, process

    _HAS_RAPIDFUZZ = True
except Exception:
    _HAS_RAPIDFUZZ = False
from src.db import (ADMIN_ROLE_ID, MASTER_ROLE_ID, PRO_USER_COLL,
                    SUPERADMIN_ROLE_ID, USER_ROLE_ID, demo_chatrooms_coll,
                    demo_messages_coll, demo_users_coll, faqs_coll,
                    support_users_coll)
from src.domain_guard import OOD_MESSAGE, guard_action, is_in_domain
from src.faq_router import answer_from_faq, load_faqs
from src.models import Chatroom, Message, ProUser, SCUser

load_dotenv()

PENDING_LOCK = Lock()
PENDING_BOT_TIMERS: dict[str, Timer] = {}
PENDING_USER_TEXT: dict[str, str] = {}   # optional: keep last user question
STAFF_ENGAGED: dict[str, bool] = {}
PING_INTERVAL_SECONDS = 300          # 5 min
BOT_REPLY_DELAY_SECONDS = 120
WS_IDLE_TIMEOUT_SECONDS = int(os.getenv("WS_IDLE_TIMEOUT_SECONDS", "300"))   # 5 min default
WS_DAILY_USER_LIMIT     = int(os.getenv("WS_DAILY_USER_LIMIT", "20"))       # 20 default
_DAILY_QA_COUNTS = {}
MASTER_SOCKETS = defaultdict(set)  # master_user_id(str) -> set(ws)
USER_SOCKETS   = defaultdict(set)  # user_id(str) -> set(ws)
ADMIN_SOCKETS = defaultdict(set)
SUPERADMIN_SOCKETS = defaultdict(set)
ACTIVE_CALLS = {}  # call_id -> {"chat_id": str, "user_id": str, "master_id": str, "state": str}
# MongoDB Setup
MONGO_URI = os.getenv("SOURCE_MONGO_URI")
DB_NAME = os.getenv("SOURCE_DB_NAME")

# Initialize global DB object
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
# ────────────────────── ObjectId / JWT helpers ──────────────────────
def _oid(v) -> Optional[ObjectId]:
    if not v:
        return None
    return v if isinstance(v, ObjectId) else ObjectId(str(v))


def _extract_bearer_token() -> str:
    """
    Try to get JWT in this order:
      1) Authorization: Bearer <token>
      2) access_token / token cookies
      3) ?token=<jwt> query param (for WebSocket URLs)
    """
    # 1) Authorization header
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        return auth.split(" ", 1)[1].strip()

    # 2) cookies (normal HTTP flow)
    cookie_token = request.cookies.get("access_token") or request.cookies.get("token")
    if cookie_token:
        return cookie_token

    # 3) query parameter (WebSocket: wss://.../ws?token=...)
    qs_token = request.args.get("token")
    if qs_token:
        return qs_token

    raise ValueError("no jwt token found in header, cookies, or query string")


def decode_jwt_id() -> ObjectId:
    """
    Decodes and verifies a Node-issued HS256 JWT and returns the user _id as ObjectId.
    Reads JWT_SECRET and JWT_ALG directly from environment variables (.env).
    """
    try:
        token = _extract_bearer_token()
    except Exception as e:
        raise ValueError(f"missing_or_invalid_token: {e}")

    # Read secret and algorithm directly from .env
    secret = os.getenv("JWT_SECRET")
    alg = os.getenv("JWT_ALG", "HS256")

    if not secret:
        raise ValueError("JWT_SECRET not set in environment")

    try:
        claims = jwt.decode(
            token,
            secret,
            algorithms=[alg],
            options={"require": ["exp", "iat"]},
        )
    except ExpiredSignatureError:
        raise ValueError("token_expired")
    except InvalidTokenError as e:
        raise ValueError(f"invalid_token: {e}")

    sub = (
        claims.get("_id")
        or claims.get("id")
        or claims.get("sub")
        or claims.get("user_id")
    )
    oid = _oid(sub)
    if not oid:
        raise ValueError(
            "invalid token claim: _id/id/sub/user_id must be a valid ObjectId string"
        )

    return oid

def now_ist_iso():
    ist = timezone(timedelta(hours=5, minutes=30))
    return datetime.now(ist).isoformat()


# ────────────────────── FAQ cache ──────────────────────
CACHE_SECONDS = 24 * 60 * 60
cache: Dict[str, tuple] = {}


def cache_get(q: str):
    d = cache.get((q or "").lower())
    if not d:
        return None
    ans, exp = d
    if time.time() > exp:
        cache.pop((q or "").lower(), None)
        return None
    return ans


def cache_set(q: str, a):
    cache[(q or "").lower()] = (a, time.time() + CACHE_SECONDS)


# ────────────────────── Text helpers ──────────────────────
ABBREV_MAP = {
    "sl": "stop loss",
    "tp": "take profit",
    "pnl": "profit and loss",
    "kyc": "kyc",
    "otp": "otp",
    "2fa": "two factor authentication",
    "mkt": "market order",
    "mt": "market order",
    "lmt": "limit order",
    "limit": "limit",
    "market": "market",
}

def _normalize(t: str) -> str:
    t = t or ""
    t = t.replace("’", "'").replace("‘", "'").replace("“", '"').replace("”", '"')
    t = t.lower().strip()

    # Replace punctuation incl Hindi danda "।"
    t = re.sub(r"[।\?\!\.\,]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()

    # Expand abbreviations token-wise (scales without thousands of rules)
    tokens = t.split()
    out = []
    for tok in tokens:
        out.append(ABBREV_MAP.get(tok, tok))
    t = " ".join(out)

    # Common variants
    t = re.sub(r"\bforgot\b", "forget", t)
    t = re.sub(r"\bpass\s+word\b", "password", t)
    t = re.sub(r"\breseting\b", "resetting", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _tokenize(t: str) -> set:
    return set(_normalize(t).split())


def _similar_ratio(a: str, b: str) -> float:
    """Return 0..100 similarity using rapidfuzz if present, else difflib 0..100."""
    if _HAS_RAPIDFUZZ:
        # token-based is robust to word order
        return float(fuzz.token_set_ratio(_normalize(a), _normalize(b)))
    # difflib returns 0..1, scale to 0..100
    return 100.0 * SequenceMatcher(None, _normalize(a), _normalize(b)).ratio()


_GREET_PAT = re.compile(
    r"^(hi|hii|hello|hey|heyy|good\s+morning|good\s+afternoon|good\s+evening)\b",
    re.I,
)

_BANNED_PHRASES = [
    "as an ai", "as a language model", "i am an ai", "i'm an ai", "language model",
    "i don't have access", "i cannot access", "i can't access",
    "imagine that you are", "market research analyst",
]

_BANNED_NAMES = [
    "protrader5", "pt5",
]

def _is_greeting(text: str) -> bool:
    return bool(_GREET_PAT.match((text or "").strip()))

def _clean_llm_text(text: str) -> str:
    t = (text or "").strip()
    if not t:
        return ""

    lines = [ln.strip() for ln in t.splitlines() if ln.strip()]
    kept = []
    for ln in lines:
        low = ln.lower()
        if any(p in low for p in _BANNED_PHRASES):
            continue
        if any(n in low for n in _BANNED_NAMES):
            # remove line containing your brand/platform name
            continue
        kept.append(ln)

    t = " ".join(kept)
    for n in _BANNED_NAMES:
        t = re.sub(re.escape(n), "", t, flags=re.I)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def _enforce_medium_length(text: str, min_words: int = 80, max_words: int = 130) -> str:
    words = (text or "").split()
    if len(words) > max_words:
        return " ".join(words[:max_words]).strip()
    return (text or "").strip()

def _to_lines(x) -> Optional[list[str]]:
    if not x:
        return None
    if isinstance(x, list):
        return [str(i).strip() for i in x if str(i).strip()]
    s = str(x).strip()
    if not s:
        return None
    return [ln.strip() for ln in s.splitlines() if ln.strip()]
def _similar(a, b, thr=0.7):
    return SequenceMatcher(None, a, b).ratio() > thr

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SupportBot")
# ────────────────────── FAQ + LLM fallback ──────────────────────
def faq_reply(user_msg: str):
    """
    Match user message against stored FAQ questions
    using fuzzy similarity.
    Returns the answer exactly as stored, or None.
    """

    if not user_msg:
        return None

    q = _normalize(user_msg)
    BEST = None  # (score, doc)

    try:
        THRESH = 72.0  # good balance for short questions like "what is sl?"

        cursor = faqs_coll.find({}, {"_id": 1, "question": 1, "answer": 1})
        for doc in cursor:
            qq = _normalize(doc.get("question", ""))
            if not qq:
                continue

            score = _similar_ratio(q, qq)

            if score >= THRESH and (BEST is None or score > BEST[0]):
                BEST = (score, doc)

        if BEST:
            _, doc = BEST
            return doc.get("answer")

    except Exception as e:
        print("faq_reply error:", e)

    return None

def _call_llm_internal(system_prompt: str, user_msg: str) -> str:
    # Get base URL and ensure it doesn't have a trailing slash or path
    base_url = os.getenv("LLM_URL")
    # Clean the URL to get ONLY the protocol and host:port
    if "/api/" in base_url:
        base_url = base_url.split("/api/")[0]
    
    endpoint = f"{base_url.rstrip('/')}/api/chat"

    payload = {
        "model": "mistral:latest",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_msg},
        ],
        "stream": False,
        "options": {"temperature": 0.1}
    }

    try:
        r = requests.post(endpoint, json=payload, timeout=60)
        r.raise_for_status()
        return r.json().get("message", {}).get("content", "").strip()
    except Exception as e:
        logger.error(f"LLM Connection Error: {e}")
        return ""
    
from datetime import datetime, time as datetime_time # Import alias to avoid conflict

IST = timezone(timedelta(hours=5, minutes=30))

def _extract_text(user_msg) -> str:
    """
    Supports:
      - dict payload: {"type":"message","text":"..."}
      - JSON string payload: '{"type":"message","text":"..."}'
      - plain string: "..."
    """
    if isinstance(user_msg, dict):
        return str(user_msg.get("text") or "").strip()

    if not isinstance(user_msg, str):
        return str(user_msg or "").strip()

    s = user_msg.strip()
    if not s:
        return s

    if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
        try:
            obj = json.loads(s)
            if isinstance(obj, dict) and "text" in obj:
                return str(obj.get("text") or "").strip()
        except Exception:
            pass

    return s

def _has_word(msg: str, word: str) -> bool:
    return re.search(rf"\b{re.escape(word)}\b", msg) is not None

def _parse_ddmmyyyy(s: str) -> datetime:
    s = s.replace("-", "/")
    return datetime.strptime(s, "%d/%m/%Y")

def _day_bounds_ist(dt_ist: datetime):
    start = dt_ist.replace(hour=0, minute=0, second=0, microsecond=0)
    end = dt_ist.replace(hour=23, minute=59, second=59, microsecond=999999)
    return start, end

def _ist_range_to_utc_filter(start_ist: datetime, end_ist: datetime):
    # If Mongo createdAt is stored in UTC (recommended), query in UTC
    start_utc = start_ist.astimezone(timezone.utc)
    end_utc = end_ist.astimezone(timezone.utc)
    return {"$gte": start_utc, "$lte": end_utc}

def _build_createdat_filter_and_label(text: str):
    """
    Returns: (date_filter_or_None, label_or_None)
    Handles:
      - range: 27/12/2025 to 31/12/2025 (two dates anywhere in msg)
      - single day: 27/12/2025
      - relative: today/yesterday/this week/last week/this month/last month
    """
    msg = (text or "").lower().strip()

    # Explicit date(s)
    date_pattern = r'(\d{1,2}[/-]\d{1,2}[/-]\d{4})'
    found_dates = re.findall(date_pattern, msg)

    if len(found_dates) >= 2:
        start_base = _parse_ddmmyyyy(found_dates[0]).replace(tzinfo=IST)
        end_base = _parse_ddmmyyyy(found_dates[1]).replace(tzinfo=IST)
        start_ist = start_base.replace(hour=0, minute=0, second=0, microsecond=0)
        end_ist = datetime.combine(end_base.date(), datetime_time.max, tzinfo=IST)
        return _ist_range_to_utc_filter(start_ist, end_ist), f"{found_dates[0]} to {found_dates[1]}"

    if len(found_dates) == 1:
        base_ist = _parse_ddmmyyyy(found_dates[0]).replace(tzinfo=IST)
        s_ist, e_ist = _day_bounds_ist(base_ist)
        return _ist_range_to_utc_filter(s_ist, e_ist), found_dates[0]

    # Relative date phrases
    now_ist = datetime.now(IST)

    if "today" in msg:
        s_ist, e_ist = _day_bounds_ist(now_ist)
        return _ist_range_to_utc_filter(s_ist, e_ist), "today"

    if "yesterday" in msg:
        s_ist, e_ist = _day_bounds_ist(now_ist - timedelta(days=1))
        return _ist_range_to_utc_filter(s_ist, e_ist), "yesterday"

    if "this week" in msg:
        start_of_week = (now_ist - timedelta(days=now_ist.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        end_ist = now_ist.replace(hour=23, minute=59, second=59, microsecond=999999)
        return _ist_range_to_utc_filter(start_of_week, end_ist), "this week"

    if "last week" in msg:
        start_this_week = (now_ist - timedelta(days=now_ist.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        start_last_week = start_this_week - timedelta(days=7)
        end_last_week = start_this_week - timedelta(microseconds=1)
        return _ist_range_to_utc_filter(start_last_week, end_last_week), "last week"

    if "this month" in msg:
        start_month = now_ist.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end_ist = now_ist.replace(hour=23, minute=59, second=59, microsecond=999999)
        return _ist_range_to_utc_filter(start_month, end_ist), "this month"

    if "last month" in msg:
        first_this_month = now_ist.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        last_month_end = first_this_month - timedelta(microseconds=1)
        last_month_start = last_month_end.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return _ist_range_to_utc_filter(last_month_start, last_month_end), "last month"

    return None, None


# ----------------------------
# DB Query (UPDATED)
# ----------------------------
def query_user_db(user_msg, user_id: str):
    logger.info(f" [1] DB_CHECK: Starting for user: {user_id}")

    text = _extract_text(user_msg)
    msg = text.lower().strip()

    # Date filter supports: today/yesterday/this week/this month/last week/last month/single date/range
    date_filter, date_label = _build_createdat_filter_and_label(text)

    # ID detection
    id_pattern = (
        r'[a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12}'
        r'|[a-zA-Z0-9]{20,40}'
    )
    id_match = re.search(id_pattern, text)
    found_id = id_match.group() if id_match else None

    # Intent flags
    is_last_query = any(w in msg for w in ["last", "latest", "recent"])
    is_count_query = any(w in msg for w in ["how many", "count", "total"])

    # Educational vs personal
    educational_phrases = [
        "what is", "meaning of", "define", "definition of", "explain",
        "how to", "how do i", "guide", "tutorial", "steps", "help me"
    ]
    is_educational = any(ph in msg for ph in educational_phrases)

    personal_words = ["my", "mine", "me", "account", "portfolio", "wallet"]
    is_personal = any(_has_word(msg, w) for w in personal_words)

    # Data nouns (used for implicit DB intent when date is present)
    data_nouns = [
        "trade", "trades", "order", "orders", "position", "positions", "holding", "holdings",
        "p&l", "pnl", "balance", "transaction", "transactions", "deposit", "withdraw", "payment"
    ]
    has_data_noun = any(w in msg for w in data_nouns)

    # If user gave a time window + asked for a data noun, treat as account-data even without "my"
    implicit_personal = bool(date_filter and has_data_noun)

    # If it's educational and not personal and not implicit and no ID, do not hit DB
    # e.g. "how to trade", "what is pnl"
    if is_educational and not is_personal and not implicit_personal and not found_id:
        logger.info(" [0] INTENT: Educational (non-personal). Skipping DB.")
        return None

    # Routing priority
    coll_name = None

    if any(w in msg for w in ["p&l", "pnl", "balance", "profit", "loss"]):
        coll_name = "user"

    elif found_id or any(w in msg for w in ["payment", "deposit", "withdraw", "request"]):
        coll_name = "paymentRequest"

    # "open" strongly correlates with positions; keep this before trade routing
    elif (is_personal or implicit_personal) and any(w in msg for w in ["position", "positions", "holding", "holdings", "open"]):
        coll_name = "position"

    elif (is_personal or implicit_personal) and any(w in msg for w in ["trade", "trades", "order", "orders"]):
        coll_name = "trade"

    elif any(w in msg for w in ["alert", "notification"]):
        coll_name = "alerts"

    else:
        # Avoid random DB hits; only query if personal or explicit time/id present
        coll_name = "transaction" if (is_personal or implicit_personal or found_id or date_filter) else None

    if not coll_name:
        logger.info(" [0.1] ROUTE: No DB route selected. Skipping DB.")
        return None

    # DB query
    try:
        if coll_name == "user":
            query_filter = {"_id": ObjectId(user_id)}
        else:
            query_filter = {"userId": ObjectId(user_id)}

        if found_id:
            ors = [{"transactionId": found_id}]
            if len(found_id) == 24:
                try:
                    ors.append({"_id": ObjectId(found_id)})
                except Exception:
                    pass
            query_filter["$or"] = ors

        if date_filter:
            query_filter["createdAt"] = date_filter

        if is_count_query:
            count = db[coll_name].count_documents(query_filter)
            return {"data": count, "collection": coll_name, "type": "count", "period": date_label}

        # Limit
        limit_val = 100 if date_filter else (1 if (is_last_query or found_id or coll_name == "user") else 5)

        results = list(db[coll_name].find(query_filter).sort("createdAt", -1).limit(limit_val))

        # ----------------------------
        # KEY FIX: return empty list for valid record lookups with explicit time window
        # so we don't fall back to FAQ/guard when there are zero rows.
        # ----------------------------
        is_record_lookup = coll_name in {"trade", "position", "transaction", "paymentRequest"}
        has_explicit_time = bool(date_filter)  # includes single-date and relative (today/yesterday/etc.)

        if results:
            return {"data": results, "collection": coll_name, "period": date_label}

        if is_record_lookup and has_explicit_time:
            return {"data": [], "collection": coll_name, "period": date_label}

        return None

    except Exception as e:
        logger.error(f" [1.7] DB_CHECK_ERROR: {e}")
        return None


# ----------------------------
# Formatting (UPDATED: explicit "no trades on <date>")
# ----------------------------
def format_db_results(data_list, collection_name: str, start_date=None, end_date=None) -> str:
    # If user asked a valid period but there are 0 records, say it explicitly
    if isinstance(data_list, list) and len(data_list) == 0:
        if start_date and not end_date:
            return f"<context><p>No <b>{collection_name}</b> records found for <b>{start_date}</b>.</p></context>"
        if start_date and end_date:
            return f"<context><p>No <b>{collection_name}</b> records found from <b>{start_date}</b> to <b>{end_date}</b>.</p></context>"
        return "<context><p>No records found for the selected period.</p></context>"

    # Preserve existing behavior for None/Falsey non-list cases
    if not data_list:
        return "<context><p>No records found for the selected period.</p></context>"

    if isinstance(data_list, int):
        return f"<context><head><b>Total count:</b></head> {data_list}</context>"

    def clean_date(dt):
        if not hasattr(dt, "strftime"):
            return dt
        try:
            # Convert aware UTC -> IST for display; if naive, assume UTC then convert
            if getattr(dt, "tzinfo", None) is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(IST).strftime("%d %b %H:%M")
        except Exception:
            return dt.strftime("%d %b %H:%M")

    t_style = 'width:100%; border-collapse: collapse; font-family: sans-serif; font-size: 13px; margin-top: 8px;'
    th_style = 'background-color: #f8f9fa; border-bottom: 2px solid #dee2e6; padding: 8px; text-align: left; color: #495057;'
    td_base = 'border-bottom: 1px solid #dee2e6; padding: 8px; vertical-align: top;'

    html_parts = []
    html_parts.append("<context>")

    html_parts.append("<head>")
    html_parts.append(f"<b>📊 {collection_name.upper()} REPORT</b><br>")
    if start_date and end_date:
        html_parts.append(f"<small style='color: #6c757d;'>Period: {start_date} to {end_date}</small><br>")
    elif start_date and not end_date:
        html_parts.append(f"<small style='color: #6c757d;'>Period: {start_date}</small><br>")
    html_parts.append(f"<small style='color: #6c757d;'>Total Records: {len(data_list)}</small>")
    html_parts.append("</head>")

    html_parts.append(f'<table style="{t_style}"><thead><tr>')
    rows_parts = []

    if collection_name == "position":
        html_parts.append(
            f'<th style="{th_style}">Symbol</th><th style="{th_style}">Qty</th><th style="{th_style}">P&L</th>'
            f'</tr></thead><tbody>'
        )
        for doc in data_list:
            pnl = doc.get("profitLoss", 0)
            pnl_color = "color: #28a745;" if pnl >= 0 else "color: #dc3545;"
            rows_parts.append(
                f'<tr>'
                f'<td style="{td_base}">{doc.get("symbolName")}</td>'
                f'<td style="{td_base}">{doc.get("totalQuantity")}</td>'
                f'<td style="{td_base} {pnl_color} font-weight: bold;">{pnl}</td>'
                f'</tr>'
            )

    elif collection_name == "trade":
        html_parts.append(
            f'<th style="{th_style}">Symbol</th><th style="{th_style}">Status</th><th style="{th_style}">Time</th>'
            f'</tr></thead><tbody>'
        )
        for doc in data_list:
            rows_parts.append(
                f'<tr>'
                f'<td style="{td_base}">{doc.get("symbolName")}</td>'
                f'<td style="{td_base}">{doc.get("status")}</td>'
                f'<td style="{td_base}">{clean_date(doc.get("createdAt"))}</td>'
                f'</tr>'
            )

    elif collection_name == "transaction":
        html_parts.append(
            f'<th style="{th_style}">Amt</th><th style="{th_style}">Type</th><th style="{th_style}">Time</th>'
            f'</tr></thead><tbody>'
        )
        for doc in data_list:
            is_credit = doc.get("type") == "credit"
            amt_color = "color: #28a745;" if is_credit else "color: #dc3545;"
            prefix = "+" if is_credit else "-"
            rows_parts.append(
                f'<tr>'
                f'<td style="{td_base} {amt_color}">{prefix}{doc.get("amount")}</td>'
                f'<td style="{td_base}">{doc.get("transactionType")}</td>'
                f'<td style="{td_base}">{clean_date(doc.get("createdAt"))}</td>'
                f'</tr>'
            )

    elif collection_name == "paymentRequest":
        status_map = {0: "🕒 Pending", 1: "✅ Approved", 2: "❌ Rejected"}
        html_parts.append(
            f'<th style="{th_style}">Method</th><th style="{th_style}">Amount</th><th style="{th_style}">Status</th>'
            f'</tr></thead><tbody>'
        )
        for doc in data_list:
            rows_parts.append(
                f'<tr>'
                f'<td style="{td_base}">{doc.get("paymentRequestType")}</td>'
                f'<td style="{td_base}">{doc.get("amount")}</td>'
                f'<td style="{td_base}">{status_map.get(doc.get("status"), "Unknown")}</td>'
                f'</tr>'
            )

    elif collection_name == "user":
        html_parts.append(
            f'<th style="{th_style}">User</th><th style="{th_style}">Balance</th><th style="{th_style}">P&L</th>'
            f'</tr></thead><tbody>'
        )
        for doc in data_list:
            rows_parts.append(
                f'<tr>'
                f'<td style="{td_base}">{doc.get("name")}</td>'
                f'<td style="{td_base}">{doc.get("balance")}</td>'
                f'<td style="{td_base}">{doc.get("profitLoss")}</td>'
                f'</tr>'
            )

    html_parts.extend(rows_parts)
    html_parts.append("</tbody></table>")
    html_parts.append("</context>")
    return "".join(html_parts)

def query_superadmin_db(user_msg: str):
    logger.info(f" [👑] SUPERADMIN_DB_CHECK: Processing request")
    msg = user_msg.lower()
    coll_name = None
    
    # --- 1. DATE RANGE DETECTION ---
    date_pattern = r'(\d{1,2}[/-]\d{1,2}[/-]\d{4})'
    found_dates = re.findall(date_pattern, user_msg)
    
    date_filter = None
    if len(found_dates) >= 2:
        try:
            start_dt = datetime.strptime(found_dates[0].replace('-', '/'), "%d/%m/%Y")
            end_base = datetime.strptime(found_dates[1].replace('-', '/'), "%d/%m/%Y")
            end_dt = datetime.combine(end_base, datetime_time.max) 
            date_filter = {"$gte": start_dt, "$lte": end_dt}
        except Exception as e:
            logger.error(f" [!] DATE_PARSE_ERROR: {e}")

    # --- 2. USER SEARCH DETECTION ---
    # If the Super Admin types a name, we search for that user
    name_match = re.search(r'user\s+([a-zA-Z]+)', msg)
    target_user_name = name_match.group(1) if name_match else None

    # --- 3. ROUTING PRIORITY ---
    # Same as user, but we default to broader categories
    if any(word in msg for word in ["p&l", "balance", "profit"]):
        coll_name = "user" # Super Admin sees all users' balances
    elif any(word in msg for word in ["payment", "deposit", "withdraw"]):
        coll_name = "paymentRequest"
    elif any(word in msg for word in ["position", "holding", "open"]):
        coll_name = "position"
    elif any(word in msg for word in ["trade", "order"]):
        coll_name = "trade"
    else:
        coll_name = "transaction"

    # --- 4. SECURE SUPERADMIN QUERY ---
    try:
        query_filter = {} # START WITH EMPTY FILTER (Show All Users)

        # If searching for a specific user name
        if target_user_name:
            query_filter["userName"] = {"$regex": target_user_name, "$options": "i"}
        
        if date_filter:
            query_filter["createdAt"] = date_filter

        # --- 5. DATA FETCHING ---
        # For Super Admin, we don't limit to 5. We fetch all relevant records (up to 500)
        # then let our formatting function handle the pagination (5 at a time).
        
        sort_field = "createdAt"
        # If it's a position report, prioritize CFM (Holding Margin) as requested
        if coll_name == "position":
            sort_field = "holdingMargin" 

        results = list(db[coll_name].find(query_filter).sort(sort_field, -1).limit(500))
        
        return {
            "data": results, 
            "collection": coll_name, 
            "total_count": len(results),
            "is_superadmin": True
        } if results else None

    except Exception as e:
        logger.error(f" [!] SUPERADMIN_DB_ERROR: {e}")
        return None
        
def format_superadmin_interactive(data_list, collection_name, page=1):
    per_page = 5
    total_count = len(data_list)
    start_idx = (page - 1) * per_page
    current_batch = data_list[start_idx : start_idx + per_page]
    
    # 1. Start Context & Head
    html = ["<context>", "<head>"]
    html.append(f"<div style='border-bottom: 2px solid #eee; padding-bottom: 10px; margin-bottom: 10px;'>")
    html.append(f"<span style='font-size: 12px; color: #666;'>SYSTEM OVERVIEW</span><br>")
    html.append(f"<b style='font-size: 18px; color: #1a73e8;'>{total_count} Total {collection_name.upper()}s</b>")
    html.append("</div>")
    html.append("</head>")

    # 2. Generate the 5 Interactive Buttons
    html.append("<div class='button-group'>")
    for doc in current_batch:
        # Determine P&L color logic
        pnl = doc.get("profitLoss", 0)
        pnl_color = "#28a745" if pnl >= 0 else "#dc3545"
        
        html.append(f"""
            <div class='superadmin-card' 
                 onclick='showItemDetail("{doc.get("_id")}")'
                 style='display: flex; justify-content: space-between; padding: 12px; margin-bottom: 8px; background: #fff; border: 1px solid #ddd; border-radius: 10px; cursor: pointer; transition: 0.3s;'>
                <div style='text-align: left;'>
                    <b style='font-size: 14px;'>{doc.get("userName", "User")}</b><br>
                    <small style='color: #555;'>{doc.get("symbolName")}</small>
                </div>
                <div style='text-align: right;'>
                    <b style='color: {pnl_color};'>{pnl}</b><br>
                    <small style='color: #999;'>Qty: {doc.get("totalQuantity")}</small>
                </div>
            </div>
        """)
    html.append("</div>")

    # 3. Pagination Footer
    if start_idx + per_page < total_count:
        html.append(f"""
            <button class='next-btn' 
                    onclick='loadNextPage({page + 1})'
                    style='width: 100%; padding: 12px; background: #007bff; color: white; border: none; border-radius: 8px; font-weight: bold; margin-top: 10px;'>
                View Next 5 Items ❯
            </button>
        """)
    
    html.append("</context>")
    return "".join(html)

def llm_fallback(user_msg, user_id: str) -> str:
    logger.info(f"--- Starting LLM Fallback Flow for User: {user_id} ---")

    # Normalize message once (handles dict / JSON string / raw string)
    text = _extract_text(user_msg)

    # 1) Greetings
    if _is_greeting(text):
        logger.info(" STEP: Greeting detected.")
        return "Hello! How can I assist with your trading account today?"

    # 2) Smart DB Check
    db_res = query_user_db(user_msg, user_id)  # query_user_db extracts text internally
    if db_res is not None:
        logger.info(f" STEP: DB check completed for collection: {db_res.get('collection')}")

        # If it's a count response, format_db_results already handles int
        period = db_res.get("period")
        clean_data = format_db_results(
            db_res.get("data"),
            db_res.get("collection"),
            start_date=period,
            end_date=None
        )

        # IMPORTANT: do NOT json.dumps(clean_data) (it causes \" and \\ud83d escapes)
        return f"Your {db_res.get('collection')} records are:\n\n{clean_data}"

    # 3) FAQ Check (Only runs if DB returned None, meaning "not a DB intent" or hard error)
    logger.info(" STEP: No DB route/data (None). Moving to FAQ check.")
    faq_answer = faq_reply(text)
    if faq_answer:
        return faq_answer

    # 4) Domain Guard
    logger.info(" STEP: No FAQ match. Checking Domain Guard.")
    ga = guard_action(text)

    # Allowlist: never refuse if clearly trading/account related.
    # This protects against false refusals on valid trading queries.
    trading_keywords = [
        "trade", "trades", "order", "orders", "position", "positions", "holding", "holdings",
        "pnl", "p&l", "balance", "deposit", "withdraw", "payment", "transaction", "transactions"
    ]
    if ga.get("action") == "refuse" and not any(k in text.lower() for k in trading_keywords):
        logger.info(" STEP: Message refused by Guard.")
        return ga.get("message")

    # 5) General LLM Answer
    logger.info(" STEP: Falling back to General LLM support.")
    SUPPORT_SYSTEM = (
        "You are a customer support agent for a trading app. "
        "Answer ONLY trading support questions (orders, PNL, KYC, deposits). "
        "Simple English, no AI mention, 80-130 words."
    )

    raw_support = _call_llm_internal(SUPPORT_SYSTEM, text)
    cleaned = _clean_llm_text(raw_support)
    return _enforce_medium_length(cleaned, 80, 130)

def superadmin_llm_fallback(user_msg: str, pro_id: str) -> str:
    logger.info(f"--- Starting SUPERADMIN LLM Flow for {pro_id} ---")

    # 1. Greetings
    if _is_greeting(user_msg):
        return "Hello 👑 Super Admin. What would you like to review today?"

    # 2. SUPERADMIN DB CHECK
    db_res = query_superadmin_db(user_msg)
    if db_res:
        logger.info(f" STEP: Superadmin data found in {db_res['collection']}")

        html = format_superadmin_interactive(
            db_res["data"],
            db_res["collection"],
            page=1
        )
        return html

    # 3. FAQ (optional – usually skip for SA)
    faq_answer = faq_reply(user_msg)
    if faq_answer:
        return faq_answer

    # 4. Guard
    ga = guard_action(user_msg)
    if ga["action"] == "refuse":
        return ga["message"]

    # 5. LLM (broader permissions than normal user)
    SYSTEM = (
        "You are an internal system assistant for a trading platform Super Admin. "
        "You may discuss users, balances, trades, payments, audits, and reports. "
        "Be concise, accurate, and structured. No emojis."
    )

    raw = _call_llm_internal(SYSTEM, user_msg)
    return _clean_llm_text(raw)
    
# ────────────────────── DB upserts ──────────────────────
def ensure_bot_user() -> SCUser:
    bot = SCUser.objects(is_bot=True).first()
    if bot:
        # keep updated_time fresh
        bot.update(set__updated_time=datetime.now(timezone.utc))
        bot.reload()
        return bot

    # Create a minimal bot doc (no role)
    bot = SCUser(
        user_id=None,
        role=None,
        parent_id=None,
        is_bot=True,
        created_time=datetime.now(timezone.utc),
        updated_time=datetime.now(timezone.utc),
    ).save()
    return bot


def resolve_owner_superadmin_id(user_or_master_id: ObjectId) -> Optional[ObjectId]:
    """
    Walk parentId → … until role == SUPERADMIN_ROLE_ID.
    Returns that user's _id (ObjectId) or None if the chain breaks.
    """
    current = _oid(user_or_master_id)
    if not current:
        return None
    # safety stop to avoid loops
    for _ in range(12):
        doc = PRO_USER_COLL.find_one(
            {"_id": current}, {"_id": 1, "role": 1, "parentId": 1}
        )
        if not doc:
            return None
        role = _oid(doc.get("role"))
        if role == SUPERADMIN_ROLE_ID:
            return _oid(doc["_id"])
        parent = _oid(doc.get("parentId"))
        if not parent:
            return None
        current = parent
    return None


# (duplicate imports remain, function names unchanged)
from datetime import datetime, timezone

def _to_oid(x):
    if isinstance(x, ObjectId):
        return x
    if not x:
        return None
    return ObjectId(str(x))


def upsert_support_user_from_jwt() -> SCUser:
    pro_oid = decode_jwt_id()  # already an ObjectId

    su = SCUser.objects(user_id=pro_oid).first()
    if su:
        su.update(set__updated_time=datetime.now(timezone.utc))
        su.reload()
        return su

    pro_doc = PRO_USER_COLL.find_one(
        {"_id": pro_oid},
        {
            "_id": 1,
            "role": 1,
            "parentId": 1,
            "name": 1,
            "userName": 1,
            "phone": 1,
        },
    )

    if not pro_doc:
        raise ValueError(f"pro.users not found for _id={pro_oid}")

    role_oid = _oid(pro_doc.get("role"))
    parent_raw = pro_doc.get("parent_id") or pro_doc.get("parentId")
    parent_oid = _oid(parent_raw)

    name = pro_doc.get("name") or ""
    user_name = pro_doc.get("userName") or ""
    phone = pro_doc.get("phone") or ""

    su = SCUser.objects(user_id=pro_oid).modify(
        upsert=True,
        new=True,
        set__role=role_oid,
        set__parent_id=parent_oid,
        set__name=name,
        set__user_name=user_name,
        set__phone=phone,
        set__updated_time=datetime.now(timezone.utc),
        set_on_insert__created_time=datetime.now(timezone.utc),
    )

    return su

# (duplicate imports remain, function names unchanged)
from datetime import datetime, timezone
from typing import Optional

# assumes these are defined in your db.py and imported here
# from src.db import PRO_DB, USER_ROLE_ID
# and your models: SCUser, ProUser, Chatroom


def _to_oid(x):
    if isinstance(x, ObjectId):
        return x
    if not x:
        return None
    return ObjectId(str(x))


def ensure_chatroom_for_pro(pro_id: ObjectId) -> Optional[Chatroom]:
    su = SCUser.objects(user_id=pro_id).first()
    if not su:
        pro_doc = PRO_USER_COLL.find_one(
            {"_id": _to_oid(pro_id)},
            {"_id": 1, "role": 1, "parentId": 1, "name": 1, "userName": 1, "phone": 1},
        )
        if not pro_doc:
            raise ValueError(f"pro.user not found for _id={pro_id}")

        role_oid = _to_oid(pro_doc.get("role"))
        parent_oid = _to_oid(pro_doc.get("parentId"))   # immediate parent (master/admin)

        su = SCUser.objects(user_id=pro_id).modify(
            upsert=True,
            new=True,
            set__role=role_oid,
            set__parent_id=parent_oid,
            set__name=pro_doc.get("name"),
            set__user_name=pro_doc.get("userName"),
            set__phone=pro_doc.get("phone"),
            set__updated_time=datetime.now(timezone.utc),
            set_on_insert__created_time=datetime.now(timezone.utc),
        )
    else:
        if not (su.name and su.phone):
            pro_doc = PRO_USER_COLL.find_one(
                {"_id": _to_oid(pro_id)}, {"name": 1, "userName": 1, "phone": 1}
            )
            if pro_doc:
                su.update(
                    set__name=pro_doc.get("name"),
                    set__user_name=pro_doc.get("userName"),
                    set__phone=pro_doc.get("phone"),
                )
        su.update(set__updated_time=datetime.now(timezone.utc))
        su.reload()

    user_oid = su.user_id
    parent_oid = su.parent_id  # immediate parent (master)

    # 🔹 NEW: resolve admin_id = parentId of parent (parent-of-parent)
    admin_oid: Optional[ObjectId] = None
    if parent_oid:
        parent_doc = PRO_USER_COLL.find_one(
            {"_id": _to_oid(parent_oid)},
            {"parentId": 1},
        )
        if parent_doc and parent_doc.get("parentId"):
            admin_oid = _to_oid(parent_doc["parentId"])

    # top superadmin; fallback as before
    owner_oid = resolve_owner_superadmin_id(user_oid) or parent_oid

    # ── 1) Try ideal key first (user + owner)
    if owner_oid:
        existing = Chatroom.objects(
            user_id=user_oid, owner_id=owner_oid, status="open"
        ).first()
        if existing:
            Chatroom.objects(id=existing.id).update_one(
                set__updated_time=datetime.now(timezone.utc)
            )
            return existing

    # ── 2) Legacy / mixed cases: any open room for this user → backfill in-place
    legacy = Chatroom.objects(user_id=user_oid, status="open").first()
    if legacy:
        updates: dict[str, Any] = {}

        # fill owner_id (top superadmin)
        if owner_oid and not getattr(legacy, "owner_id", None):
            updates["set__owner_id"] = owner_oid

        # keep immediate parent in super_admin_id (legacy meaning)
        if parent_oid and getattr(legacy, "super_admin_id", None) != parent_oid:
            updates["set__super_admin_id"] = parent_oid

        # 🔹 NEW: fill admin_id = parent-of-parent
        if admin_oid and getattr(legacy, "admin_id", None) != admin_oid:
            updates["set__admin_id"] = admin_oid

        # 🔹 NEW: ensure presence flags exist (default False)
        if getattr(legacy, "is_owner_active", None) is None:
            updates["set__is_owner_active"] = False
        if getattr(legacy, "is_admin_active", None) is None:
            updates["set__is_admin_active"] = False

        if updates:
            updates["set__updated_time"] = datetime.now(timezone.utc)
            Chatroom.objects(id=legacy.id).update_one(**updates)
            legacy.reload()
        return legacy

    # ── 3) Create a fresh room (only for client role)
    if su.role == USER_ROLE_ID:
        now = datetime.now(timezone.utc)
        return Chatroom(
            user_id=user_oid,
            owner_id=owner_oid,          # top superadmin
            super_admin_id=parent_oid,   # immediate parent (master) – legacy field
            admin_id=admin_oid,          # ✅ parent-of-parent = admin

            status="open",

            # presence flags – default all False
            is_user_active=False,
            is_superadmin_active=False,
            is_owner_active=False,
            is_admin_active=False,

            created_time=now,
            updated_time=now,
        ).save()

    return None

def ensure_staff_bot_room(pro_id: ObjectId) -> Chatroom:
    now = datetime.now(timezone.utc)

    # Resolve role/parent chain from SCUser / pro.users
    su = SCUser.objects(user_id=pro_id).first()
    if not su:
        pro_doc = PRO_USER_COLL.find_one(
            {"_id": _to_oid(pro_id)},
            {"_id": 1, "role": 1, "parentId": 1},
        )
        role_oid = _to_oid(pro_doc.get("role")) if pro_doc else None
        parent_oid = _to_oid((pro_doc or {}).get("parentId"))
    else:
        role_oid = su.role
        parent_oid = su.parent_id

    is_owner = role_oid == config.SUPERADMIN_ROLE_ID
    is_admin = role_oid == config.ADMIN_ROLE_ID
    is_master = role_oid == config.MASTER_ROLE_ID

    # Compute linkage ids
    owner_oid = None
    admin_oid = None
    master_oid = None

    if is_owner:
        owner_oid = pro_id
        admin_oid = None
        master_oid = None

    elif is_admin:
        # admin's parent is owner
        admin_oid = pro_id
        owner_oid = parent_oid
        master_oid = None

    elif is_master:
        # master's parent is admin; admin's parent is owner
        master_oid = pro_id
        admin_oid = parent_oid

        owner_from_admin = None
        if admin_oid:
            admin_doc = PRO_USER_COLL.find_one(
                {"_id": _to_oid(admin_oid)},
                {"parentId": 1},
            )
            if admin_doc and admin_doc.get("parentId"):
                owner_from_admin = _to_oid(admin_doc["parentId"])

        owner_oid = owner_from_admin or resolve_owner_superadmin_id(pro_id) or None

    # Create or touch staff_bot room, and write fields on insert
    room = Chatroom.objects(
        user_id=pro_id,
        room_type="staff_bot",
        status="open",
    ).modify(
        upsert=True,
        new=True,

        set__updated_time=now,

        set_on_insert__created_time=now,
        set_on_insert__user_id=pro_id,
        set_on_insert__room_type="staff_bot",
        set_on_insert__title="My Bot Chat",
        set_on_insert__status="open",

        set_on_insert__is_user_active=False,
        set_on_insert__is_superadmin_active=False,
        set_on_insert__is_owner_active=False,
        set_on_insert__is_admin_active=False,

        # ✅ NEW: write linkage fields ON INSERT
        set_on_insert__owner_id=owner_oid,
        set_on_insert__admin_id=admin_oid,
        set_on_insert__super_admin_id=master_oid,
    )

    # ✅ NEW: optional backfill if room already existed before this logic
    # (safe, does not overwrite existing non-null values)
    try:
        updates = {}
        if owner_oid and not getattr(room, "owner_id", None):
            updates["set__owner_id"] = owner_oid
        if admin_oid and not getattr(room, "admin_id", None):
            updates["set__admin_id"] = admin_oid
        if master_oid and not getattr(room, "super_admin_id", None):
            updates["set__super_admin_id"] = master_oid

        if updates:
            updates["set__updated_time"] = now
            Chatroom.objects(id=room.id).update_one(**updates)
            room.reload()
    except Exception:
        pass

    return room

# ────────────────────── Message utils ──────────────────────
def repeated_user_questions(
    chatroom_id: ObjectId, current_message: str, sender_id: ObjectId, threshold=3
) -> int:
    norm = _normalize((current_message or "").lower())
    q = (
        Message.objects(chatroom_id=chatroom_id, message_by=sender_id, is_file=False)
        .order_by("-created_time")
        .limit(50)
    )
    cnt = 0
    for m in q:
        if not m.message:
            continue
        if _similar(norm, _normalize(m.message.lower()), 0.85):
            cnt += 1
    return cnt


def msg_dict(m: Message) -> dict:
    return {
        "_id": str(m.id),
        "chatroom_id": str(m.chatroom_id),
        "message_by": str(m.message_by),
        "message": m.message,
        "is_file": m.is_file,
        "path": m.path,
        "is_bot": m.is_bot,
        "created_time": (m.created_time or datetime.now(timezone.utc)).isoformat(),
        "updated_time": (
            m.updated_time or m.created_time or datetime.now(timezone.utc)
        ).isoformat(),
    }


# ────────────────────── WebSocket rooms (broadcast) ──────────────────────
ROOMS: Dict[str, set] = {}
ROOMS_LOCK = Lock()


def room_add(chat_id: str, ws):
    with ROOMS_LOCK:
        ROOMS.setdefault(chat_id, set()).add(ws)


def room_remove(chat_id: str, ws):
    with ROOMS_LOCK:
        if chat_id in ROOMS and ws in ROOMS[chat_id]:
            ROOMS[chat_id].remove(ws)
            if not ROOMS[chat_id]:
                ROOMS.pop(chat_id, None)


def room_broadcast(chat_id: str, payload: dict):
    msg = json.dumps(payload, default=str)
    with ROOMS_LOCK:
        conns = list(ROOMS.get(chat_id, set()))
    dead = []
    for ws in conns:
        try:
            ws.send(msg)
        except Exception:
            dead.append(ws)
    if dead:
        with ROOMS_LOCK:
            for w in dead:
                for cid, s in list(ROOMS.items()):
                    if w in s:
                        s.remove(w)
                        if not s:
                            ROOMS.pop(cid, None)


# ────────────────────── Presence tracking per role ──────────────────────
log = logging.getLogger("presence")

PRESENCE: Dict[str, Dict[str, set]] = {}
PRES_LOCK = threading.RLock()


def _ensure_presence_bucket(chat_id: str):
    """Ensure per-room buckets exist."""
    with PRES_LOCK:
        if chat_id not in PRESENCE:
            PRESENCE[chat_id] = {
                "user": set(),
                "superadmin": set(),  # any non-user, non-bot role lands here
            }


def _role_bucket(role: str) -> Optional[str]:
    """
    Map an incoming role to the presence bucket:
      - 'user'      -> 'user'
      - 'bot'       -> None (do not track)
      - everything else (admin, master, superadmin, etc.) -> 'superadmin'

    We still maintain only 2 high-level buckets: 'user' and 'superadmin'.
    """
    r = (role or "").strip().lower()

    if r == "bot":
        return None
    if r == "user":
        return "user"

    return "superadmin"


def mark_role_join(chat: Chatroom, role: str, ws):
    chat_id = str(chat.id)
    bucket_name = _role_bucket(role)
    if bucket_name is None:
        return

    _ensure_presence_bucket(chat_id)

    role_key = (role or "").strip().lower()

    with PRES_LOCK:
        # ── 1) high-level bucket (user / staff) ────────────────────────
        bucket = PRESENCE[chat_id][bucket_name]
        was_empty = len(bucket) == 0
        bucket.add(ws)

        # ── 2) per-role tracking: master/admin/superadmin ─────────────
        roles_dict = PRESENCE[chat_id].setdefault(
            "_roles",
            {
                "master": set(),       # ➜ is_superadmin_active
                "admin": set(),        # ➜ is_admin_active
                "superadmin": set(),   # ➜ is_owner_active
            },
        )

        role_bucket = None
        role_was_empty = False
        if role_key in roles_dict:
            role_bucket = roles_dict[role_key]
            role_was_empty = len(role_bucket) == 0
            role_bucket.add(ws)

    now = datetime.now(timezone.utc)

    # ── aggregate user flag only ───────────────────────────────────────
    if was_empty and bucket_name == "user":
        Chatroom.objects(id=chat.id, is_user_active__ne=True).update_one(
            set__is_user_active=True,
            set__updated_time=now,
        )

    # ── per-role flags based on your mapping ──────────────────────────
    if role_bucket is not None and role_was_empty:
        # MASTER joined → super_admin_id present → is_superadmin_active = True
        if role_key == "master":
            Chatroom.objects(id=chat.id, is_superadmin_active__ne=True).update_one(
                set__is_superadmin_active=True,
                set__updated_time=now,
            )

        # ADMIN joined → admin_id present → is_admin_active = True
        elif role_key == "admin":
            Chatroom.objects(id=chat.id, is_admin_active__ne=True).update_one(
                set__is_admin_active=True,
                set__updated_time=now,
            )

        # SUPERADMIN joined → owner_id present → is_owner_active = True
        elif role_key == "superadmin":
            Chatroom.objects(id=chat.id, is_owner_active__ne=True).update_one(
                set__is_owner_active=True,
                set__updated_time=now,
            )


def mark_role_leave(chat: Chatroom, role: str, ws):
    chat_id = str(chat.id)
    bucket_name = _role_bucket(role)
    if bucket_name is None:
        return

    _ensure_presence_bucket(chat_id)
    role_key = (role or "").strip().lower()

    with PRES_LOCK:
        # ── 1) high-level bucket (user / staff) ────────────────────────
        bucket = PRESENCE[chat_id][bucket_name]
        if ws in bucket:
            bucket.remove(ws)
        became_empty = len(bucket) == 0

        # ── 2) per-role tracking ───────────────────────────────────────
        roles_dict = PRESENCE[chat_id].setdefault(
            "_roles",
            {
                "master": set(),      # super_admin_id
                "admin": set(),       # admin_id
                "superadmin": set(),  # owner_id
            },
        )

        role_bucket = None
        role_became_empty = False
        if role_key in roles_dict:
            role_bucket = roles_dict[role_key]
            if ws in role_bucket:
                role_bucket.remove(ws)
            role_became_empty = len(role_bucket) == 0

        # ✅ cleanup PRESENCE only if nobody is left (user bucket empty AND staff bucket empty AND no staff roles)
        user_empty = len(PRESENCE[chat_id].get("user", set())) == 0
        staff_bucket_empty = len(PRESENCE[chat_id].get("superadmin", set())) == 0  # your staff bucket name
        no_staff_roles = (
            len(roles_dict.get("master", set())) == 0
            and len(roles_dict.get("admin", set())) == 0
            and len(roles_dict.get("superadmin", set())) == 0
        )

        if user_empty and staff_bucket_empty and no_staff_roles:
            PRESENCE.pop(chat_id, None)

    now = datetime.now(timezone.utc)

    # ── aggregate user flag only ───────────────────────────────────────
    if became_empty and bucket_name == "user":
        Chatroom.objects(id=chat.id, is_user_active__ne=False).update_one(
            set__is_user_active=False,
            set__updated_time=now,
        )

    # ── per-role flags: flip off when last socket of that role leaves ─
    if role_bucket is not None and role_became_empty:
        # last MASTER left → is_superadmin_active = False
        if role_key == "master":
            Chatroom.objects(id=chat.id, is_superadmin_active__ne=False).update_one(
                set__is_superadmin_active=False,
                set__updated_time=now,
            )

        # last ADMIN left → is_admin_active = False
        elif role_key == "admin":
            Chatroom.objects(id=chat.id, is_admin_active__ne=False).update_one(
                set__is_admin_active=False,
                set__updated_time=now,
            )

        # last SUPERADMIN (owner) left → is_owner_active = False
        elif role_key == "superadmin":
            Chatroom.objects(id=chat.id, is_owner_active__ne=False).update_one(
                set__is_owner_active=False,
                set__updated_time=now,
            )

    # ✅ if no staff is present anymore, reset “staff engaged” and cancel pending bot reply timer
    if not is_any_staff_present(chat_id):
        STAFF_ENGAGED[chat_id] = False
        cancel_pending_bot_reply(chat_id)


# === NEW: quick presence check ===
def is_superadmin_present(chat_id: str) -> bool:
    """
    Fast check: if any superadmin sockets are present in-memory for this chat.
    If not known, fall back to DB flag.
    """
    with PRES_LOCK:
        if chat_id in PRESENCE and len(PRESENCE[chat_id].get("superadmin", set())) > 0:
            return True

    # Fallback to DB (covers fresh process or after restart)
    try:
        c = Chatroom.objects(id=_oid(chat_id)).only("is_superadmin_active").first()
        return bool(getattr(c, "is_superadmin_active", False)) if c else False
    except Exception:
        return False


# ────────────────────── NEW: Superadmin utilities ──────────────────────
def get_chatrooms_for_superadmin_from_jwt():
    """
    Read JWT → pro_id.
    If role ∉ {SUPERADMIN, ADMIN, MASTER} -> {"role":"not_superadmin", "chatroom_ids":[]}
    Else list chatrooms whose owner_id == <top superadmin of the caller>.
      - For SUPERADMIN: owner_id == caller's _id
      - For ADMIN/MASTER: owner_id == top superadmin above the caller
    Primary filter: owner_id
    Legacy fallback: super_admin_id
    """
    pro_id = decode_jwt_id()

    su = SCUser.objects(user_id=pro_id).first()
    if not su:
        # Backfill SCUser from pro.users
        pro_doc = PRO_USER_COLL.find_one(
            {"_id": _oid(pro_id)},
            {"_id": 1, "role": 1, "parentId": 1},
        )
        if not pro_doc:
            raise ValueError("pro_v2 user not found")
        su = SCUser.objects(user_id=pro_id).modify(
            upsert=True,
            new=True,
            set__role=_to_oid(pro_doc.get("role")),
            set__parent_id=_to_oid(pro_doc.get("parentId")),
            set__updated_time=datetime.now(timezone.utc),
            set_on_insert__created_time=datetime.now(timezone.utc),
        )

    # Only superadmin family can list rooms
    if su.role not in {SUPERADMIN_ROLE_ID, ADMIN_ROLE_ID, MASTER_ROLE_ID}:
        return {"role": "not_superadmin", "chatroom_ids": []}

    # Determine the true owner superadmin id
    if su.role == SUPERADMIN_ROLE_ID:
        owner_oid = su.user_id
    else:
        # ADMIN/MASTER → resolve their top superadmin
        owner_oid = resolve_owner_superadmin_id(su.user_id) or su.parent_id

    ids = set()

    # Primary: rooms owned by this superadmin
    for c in Chatroom.objects(owner_id=owner_oid).only("id"):
        ids.add(str(c.id))

    # Legacy fallback: older docs may not have owner_id
    if not ids:
        for c in Chatroom.objects(super_admin_id=owner_oid).only("id"):
            ids.add(str(c.id))

    # Keep "role":"superadmin" to avoid breaking existing clients
    return {"role": "superadmin", "chatroom_ids": sorted(ids)}

def get_chatrooms_for_admin_from_jwt():
    """
    Read JWT → pro_id.
    If role != ADMIN -> {"role":"not_admin", "chatroom_ids":[]}

    For ADMIN:
      - List chatrooms where chatroom.admin_id == admin's _id

    (You can extend later to let SUPERADMIN see by-admin, but this keeps
    it simple & symmetric with get_chatrooms_for_superadmin_from_jwt.)
    """
    pro_id = decode_jwt_id()

    # Ensure SCUser exists / is synced
    su = SCUser.objects(user_id=pro_id).first()
    if not su:
        pro_doc = PRO_USER_COLL.find_one(
            {"_id": _oid(pro_id)},
            {"_id": 1, "role": 1, "parentId": 1},
        )
        if not pro_doc:
            raise ValueError("pro_v2 user not found")

        su = SCUser.objects(user_id=pro_id).modify(
            upsert=True,
            new=True,
            set__role=_to_oid(pro_doc.get("role")),
            set__parent_id=_to_oid(pro_doc.get("parentId")),
            set__updated_time=datetime.now(timezone.utc),
            set_on_insert__created_time=datetime.now(timezone.utc),
        )

    # Only ADMIN can use this helper (you can relax this if you want)
    if su.role != ADMIN_ROLE_ID:
        return {"role": "not_admin", "chatroom_ids": []}

    admin_oid = su.user_id

    ids: set[str] = set()

    # Primary: rooms where this admin is the admin_id
    for c in Chatroom.objects(admin_id=admin_oid).only("id"):
        ids.add(str(c.id))

    # (Optional legacy fallback: if older docs didn't have admin_id set,
    #  you could try to approximate via super_admin_id / owner_id here.)

    return {
        "role": "admin",
        "chatroom_ids": sorted(ids),
    }


ROLE_MAP = {
    "64b63755c71461c502ea4717": "user",
    "64b63755c71461c502ea4715": "master",
    "64b63755c71461c502ea4714": "admin",
    "64b63755c71461c502ea4713": "superadmin",
    "68468774edced6e69c41d3f7": "bot",
}


def _role_name_from_role_field(role_field) -> str:
    # role_field may be ObjectId, string, or None
    if role_field is None:
        return "unknown"
    rid = str(role_field)  # ObjectId -> str
    return ROLE_MAP.get(rid, "unknown")


def chatroom_with_messages(chatroom_id: str) -> dict | None:
    """
    Return chatroom details + messages for a given chatroom_id.
    Resolves 'from' by looking up message_by in users (single users query).
    """
    oid = _oid(chatroom_id)
    chat = Chatroom.objects(id=oid).first()
    if not chat:
        return None

    # Pull messages first
    msgs = list(Message.objects(chatroom_id=oid).order_by("+created_time").limit(500))

    # Collect unique non-bot sender ids
    sender_ids = {
        m.message_by
        for m in msgs
        if getattr(m, "message_by", None) and not getattr(m, "is_bot", False)
    }
    # One query to users
    users = list(SCUser.objects(id__in=list(sender_ids)).only("id", "role"))

    # Build map: user_id(str) -> role_name
    user_role_map = {
        str(u.id): _role_name_from_role_field(getattr(u, "role", None)) for u in users
    }

    conversation = []
    for m in msgs:
        # Resolve "from"
        if getattr(m, "is_bot", False):
            from_role = "bot"
        else:
            uid = str(getattr(m, "message_by", "") or "")
            from_role = user_role_map.get(uid, "unknown")

        # Shape message
        if m.is_file and m.path:
            ext = (m.path.split(".")[-1] if "." in m.path else "").lower()
            if ext in {"mp3", "wav", "m4a", "webm"}:
                conversation.append(
                    {
                        "type": "audio",
                        "from": from_role,
                        "audio_url": m.path,
                        "created_at": m.created_time.isoformat(),
                    }
                )
            else:
                conversation.append(
                    {
                        "type": "file",
                        "from": from_role,
                        "file_url": m.path,
                        "created_at": m.created_time.isoformat(),
                    }
                )
        else:
            conversation.append(
                {
                    "from": from_role,
                    "text": m.message,
                    "created_at": m.created_time.isoformat(),
                }
            )

    return {
        "chatroom": {
            "id": str(chat.id),
            "user_id": str(chat.user_id) if chat.user_id else None,
            "super_admin_id": str(chat.super_admin_id) if chat.super_admin_id else None,
            "status": chat.status,
            "is_user_active": getattr(chat, "is_user_active", False),
            "is_superadmin_active": getattr(chat, "is_superadmin_active", False),
            "created_time": chat.created_time.isoformat(),
            "updated_time": (
                getattr(chat, "updated_time", None)
                or getattr(chat, "created_time", None)
            ).isoformat(),
        },
        "conversation": conversation,
    }


DEMO_CLIENT_ROLE_ID = USER_ROLE_ID  # fixed role: ObjectId("64b63755c71461c502ea4717")


def get_client_ip() -> str:
    """
    Resolve the real client IP (proxy aware). We store this as user_id for demo_users & demo_chatroom.
    """
    xff = request.headers.get("X-Forwarded-For", "") or request.headers.get(
        "x-forwarded-for", ""
    )
    if xff:
        return xff.split(",")[0].strip()
    return (request.remote_addr or "").strip() or "0.0.0.0"


def ensure_demo_user(client_ip: str, super_admin_id: str):
    # Check if the user already exists
    existing_user = demo_users_coll.find_one(
        {"user_id": client_ip, "super_admin_id": super_admin_id}
    )
    if existing_user:
        return existing_user
    # Insert a new user if doesn't exist
    new_user = {
        "user_id": client_ip,
        "super_admin_id": super_admin_id,
        "status": "open",
        "is_user_active": False,
        "is_superadmin_active": True,
        "created_time": datetime.now(),
        "updated_time": datetime.now(),
    }
    return demo_users_coll.insert_one(
        new_user
    ).inserted_id  # Insert and return the inserted document ID


def find_or_create_demo_chatroom(ip: str, super_admin_id: ObjectId) -> dict:
    """
    One open demo_chatroom per (super_admin_id, ip).
    """
    now = now_ist_iso()
    chat = demo_chatrooms_coll.find_one(
        {"user_id": ip, "super_admin_id": _oid(super_admin_id)}
    )
    if chat:
        demo_chatrooms_coll.update_one(
            {"_id": chat["_id"]}, {"$set": {"updated_time": now}}
        )
        return chat
    room = {
        "user_id": ip,  # string (IP)
        "super_admin_id": _oid(super_admin_id),
        "status": "open",
        "is_user_active": False,
        "is_superadmin_active": False,
        "created_time": now,
        "updated_time": now,
    }
    ins = demo_chatrooms_coll.insert_one(room)
    room["_id"] = ins.inserted_id
    return room


def save_demo_message(chatroom_id: ObjectId, sender: str, text: str) -> dict:
    """
    Persist a message in demo_messages. 'sender' ∈ {'user','admin','bot'}.
    """
    payload = {
        "chatroom_id": _oid(chatroom_id),
        "from": sender,
        "message": text,
        "is_file": False,
        "path": None,
        "created_time": datetime.now(timezone.utc),
    }
    ins = demo_messages_coll.insert_one(payload)
    payload["_id"] = ins.inserted_id
    return payload


# ─── Demo presence tracking (separate from normal Chatroom presence) ───
DEMO_PRESENCE: Dict[str, Dict[str, set]] = {}
DEMO_PRES_LOCK = threading.RLock()


def _ensure_demo_presence_bucket(chat_id: str):
    with DEMO_PRES_LOCK:
        if chat_id not in DEMO_PRESENCE:
            DEMO_PRESENCE[chat_id] = {
                "user": set(),
                "superadmin": set(),
            }


def _demo_role_bucket(role: str) -> Optional[str]:
    r = (role or "").strip().lower()
    if r == "bot":
        return None
    if r == "user":
        return "user"
    return "superadmin"


def demo_mark_role_join(chat_id: str, role: str, ws):
    """
    Mark presence in demo_chatroom flags (in DB) + memory.
    """
    bucket_name = _demo_role_bucket(role)
    if bucket_name is None:
        return
    _ensure_demo_presence_bucket(chat_id)
    with DEMO_PRES_LOCK:
        bucket = DEMO_PRESENCE[chat_id][bucket_name]
        was_empty = len(bucket) == 0
        bucket.add(ws)
    # Flip DB flags when first socket of a bucket arrives
    if was_empty:
        field = "is_user_active" if bucket_name == "user" else "is_superadmin_active"
        demo_chatrooms_coll.update_one(
            {"_id": _oid(chat_id)},
            {"$set": {field: True, "updated_time": now_ist_iso()}},
        )


def demo_mark_role_leave(chat_id: str, role: str, ws):
    bucket_name = _demo_role_bucket(role)
    if bucket_name is None:
        return
    remove_all = False
    with DEMO_PRES_LOCK:
        _ensure_demo_presence_bucket(chat_id)
        bucket = DEMO_PRESENCE[chat_id][bucket_name]
        if ws in bucket:
            bucket.remove(ws)
        became_empty = len(bucket) == 0
        if (
            len(DEMO_PRESENCE[chat_id]["user"]) == 0
            and len(DEMO_PRESENCE[chat_id]["superadmin"]) == 0
        ):
            remove_all = True
    if became_empty:
        field = "is_user_active" if bucket_name == "user" else "is_superadmin_active"
        demo_chatrooms_coll.update_one(
            {"_id": _oid(chat_id)},
            {"$set": {field: False, "updated_time": now_ist_iso()}},
        )
    if remove_all:
        with DEMO_PRES_LOCK:
            DEMO_PRESENCE.pop(chat_id, None)
    

def is_demo_superadmin_present(chat_id: str) -> bool:
    """
    Fast in-memory check; fallback to demo_chatroom DB flag.
    """
    with DEMO_PRES_LOCK:
        if (
            chat_id in DEMO_PRESENCE
            and len(DEMO_PRESENCE[chat_id].get("superadmin", set())) > 0
        ):
            return True
    try:
        doc = demo_chatrooms_coll.find_one(
            {"_id": _oid(chat_id)}, {"is_superadmin_active": 1}
        )
        return bool(doc and doc.get("is_superadmin_active"))
    except Exception:
        return False


def safe_send(ws, payload):
    """Safely send JSON data through a WebSocket without crashing on error."""
    try:
        ws.send(json.dumps(payload, default=str))
    except Exception as e:
        print("[WS] send error:", repr(e))


def safe_join(chat, role, ws):
    """Wrapper for mark_role_join with silent error handling."""
    try:
        mark_role_join(chat, role, ws)
    except Exception as e:
        print("[WS] join error:", repr(e))


def safe_leave(chat, role, ws):
    """Wrapper for mark_role_leave with silent error handling."""
    try:
        mark_role_leave(chat, role, ws)
    except Exception as e:
        print("[WS] leave error:", repr(e))


def safe_room_add(chat_id, ws):
    """Wrapper for room_add with silent error handling."""
    try:
        room_add(chat_id, ws)
    except Exception as e:
        print("[WS] room_add error:", repr(e))


def safe_room_remove(chat_id, ws):
    """Wrapper for room_remove with silent error handling."""
    try:
        room_remove(chat_id, ws)
    except Exception as e:
        print("[WS] room_remove error:", repr(e))


def safe_broadcast(chat_id, payload):
    """Wrapper for room_broadcast with silent error handling."""
    try:
        room_broadcast(chat_id, payload)
    except Exception as e:
        print("[WS] broadcast error:", repr(e))

def is_any_staff_present(chat_id: str) -> bool:
    _ensure_presence_bucket(chat_id)
    roles = PRESENCE.get(chat_id, {}).get("_roles", {})

    # staff roles in your system:
    # master      -> super_admin_id
    # admin       -> admin_id
    # superadmin  -> owner_id
    return (
        len(roles.get("master", set())) > 0
        or len(roles.get("admin", set())) > 0
        or len(roles.get("superadmin", set())) > 0
    )

def is_higher_staff_present(chat, sender_role: str, chat_id: str) -> bool:
    """
    Returns True if a *higher-level* staff than sender_role
    is present in this staff_bot room.
    """
    _ensure_presence_bucket(chat_id)
    roles = PRESENCE.get(chat_id, {}).get("_roles", {})

    has_owner = bool(getattr(chat, "owner_id", None))
    has_admin = bool(getattr(chat, "admin_id", None))
    has_master = bool(getattr(chat, "super_admin_id", None))

    # Master staff_bot room: admin or superadmin counts as higher
    if has_master and has_admin and has_owner:
        if sender_role == "master":
            return (
                len(roles.get("admin", set())) > 0
                or len(roles.get("superadmin", set())) > 0
            )

    # Admin staff_bot room: superadmin counts as higher
    if (not has_master) and has_admin and has_owner:
        if sender_role == "admin":
            return len(roles.get("superadmin", set())) > 0

    # Owner personal staff_bot room: no higher role
    return False

def cancel_pending_bot_reply(chat_id: str):
    with PENDING_LOCK:
        t = PENDING_BOT_TIMERS.pop(chat_id, None)
        PENDING_USER_TEXT.pop(chat_id, None)
        if t:
            try:
                t.cancel()
            except Exception:
                pass

def generate_bot_reply_lines(text: str, user_id: str = None) -> list[str]:
    """
    Main entry point for the bot. 
    Always returns a LIST of strings to prevent vertical character splitting.
    """
    if not text:
        return []

    # 1. (Optional) Insert your cache logic here if you use it

    # 2. Get the response from your AI model flow
    # Pass user_id so query_user_db can filter by the specific user
    ai_response = llm_fallback(text, user_id)

    # 3. THE CRITICAL FIX: Ensure the output is a LIST
    # If ai_response is "Hello World", split('\n') makes it ["Hello World"]
    # If ai_response is already a list, it returns as is.
    if isinstance(ai_response, str):
        # We split by newlines to keep paragraphs separate but as full sentences
        lines = [line.strip() for line in ai_response.split('\n') if line.strip()]
        return lines
    
    # If it's already a list (like from a specific FAQ return), return it
    if isinstance(ai_response, list):
        return ai_response

    return []

def schedule_bot_reply_after_2m(chat, chat_id: str, user_text: str, user_id_str: str = None):
    cancel_pending_bot_reply(chat_id)

    def _fire():
        with PENDING_LOCK:
            PENDING_BOT_TIMERS.pop(chat_id, None)

        # ─────────────────────────────────────────────
        # ✅ CRITICAL FIX: reset engagement AFTER fallback
        # This makes bot instant again until staff replies
        # ─────────────────────────────────────────────
        STAFF_ENGAGED[chat_id] = False

        try:
            if user_id_str:
                reply_lines = generate_bot_reply_lines(user_text, user_id_str)
            else:
                reply_lines = generate_bot_reply_lines(user_text)
        except Exception:
            reply_lines = None

        if not reply_lines:
            return

        bot = ensure_bot_user()
        m_bot = Message(
            chatroom_id=chat.id,
            message_by=bot.id,
            message="\n".join(reply_lines),
            is_file=False,
            path=None,
            is_bot=True,
        ).save()

        room_broadcast(
            chat_id,
            {
                "type": "message",
                "from": "bot",
                "message": "\n".join(reply_lines),
                "message_id": str(m_bot.id),
                "chat_id": chat_id,
                "created_time": m_bot.created_time.isoformat(),
            },
        )

    with PENDING_LOCK:
        PENDING_USER_TEXT[chat_id] = user_text
        t = Timer(120.0, _fire)
        PENDING_BOT_TIMERS[chat_id] = t
        t.daemon = True
        t.start()

def _utc_day_key() -> str:
    return datetime.now(timezone.utc).date().isoformat()

def _can_ask_and_inc(user_id_str: str) -> bool:
    """
    Count ONLY user messages (bucket_role == 'user').
    Returns True if allowed, False if limit reached.
    """
    day = _utc_day_key()
    bucket = _DAILY_QA_COUNTS.setdefault(day, {})

    cur = int(bucket.get(user_id_str, 0))
    if cur >= WS_DAILY_USER_LIMIT:
        return False

    bucket[user_id_str] = cur + 1

    # optional cleanup: keep only today
    for k in list(_DAILY_QA_COUNTS.keys()):
        if k != day:
            _DAILY_QA_COUNTS.pop(k, None)

    return True
#calling

def _sock_add(sock_map, user_id, ws):
    uid = str(user_id)
    if uid not in sock_map:
        sock_map[uid] = set()
    sock_map[uid].add(ws)  # Add WebSocket connection to the map
    logger.info(f"User {user_id} connected. Total active sockets: {len(sock_map[uid])}")

def _sock_remove(sock_map, user_id, ws):
    uid = str(user_id)
    sockets = sock_map.get(uid)
    if not sockets:
        return
    sockets.discard(ws)  # Remove WebSocket connection from the map
    if not sockets:  # No active sockets for the user, remove entry
        sock_map.pop(uid, None)
    logger.info(f"User {user_id} disconnected. Total active sockets: {len(sockets)}")

def _sock_send_any(sock_map, user_id, payload):
    """Send a message to any active socket for that user."""
    try:
        uid = str(user_id)
        sockets = sock_map.get(uid)
        if not sockets:
            logger.warning(f"No active sockets found for user {user_id}")
            return False
        
        msg = json.dumps(payload)
        for w in list(sockets):
            try:
                w.send(msg)
                logger.info(f"Message sent to user {user_id} via socket {w}")
                return True
            except Exception as e:
                logger.error(f"Error sending message to user {user_id} via socket {w}: {e}")
                sockets.discard(w)
        
        if not sockets:
            sock_map.pop(uid, None)
            logger.info(f"No active sockets left for user {user_id}. Removing from socket map.")
        
        return False
    except Exception as e:
        logger.error(f"Error sending message to user {user_id}: {e}")
        return False

def _sock_send_all(sock_map, user_id, payload):
    """Send a message to all active sockets for that user."""
    try:
        uid = str(user_id)
        sockets = sock_map.get(uid)
        if not sockets:
            logger.warning(f"No active sockets found for user {user_id}")
            return 0
        
        msg = json.dumps(payload)
        sent = 0
        for w in list(sockets):
            try:
                w.send(msg)
                sent += 1
                logger.info(f"Message sent to user {user_id} via socket {w}")
            except Exception as e:
                logger.error(f"Error sending message to user {user_id} via socket {w}: {e}")
                sockets.discard(w)
        
        if not sockets:
            sock_map.pop(uid, None)
            logger.info(f"No active sockets left for user {user_id}. Removing from socket map.")
        
        return sent
    except Exception as e:
        logger.error(f"Error sending message to user {user_id}: {e}")
        return 0
    
def _resolve_staff_links_from_clients(role, pro_id):
    """
    Returns (owner_id, admin_id) for the current staff user by deriving from client mappings.
    owner_id = superadmin/owner
    admin_id  = admin
    """
    try:
        if role == config.MASTER_ROLE_ID:
            doc = support_users_coll.find_one(
                {"super_admin_id": pro_id},
                {"owner_id": 1, "admin_id": 1},
            )
            if doc:
                return doc.get("owner_id"), doc.get("admin_id")
            return None, None

        if role == config.ADMIN_ROLE_ID:
            doc = support_users_coll.find_one(
                {"admin_id": pro_id},
                {"owner_id": 1},
            )
            if doc:
                return doc.get("owner_id"), pro_id
            return None, pro_id

        if role == config.SUPERADMIN_ROLE_ID:
            return pro_id, None

        return None, None
    except Exception:
        return None, None
    
def _is_staff_bot_room(chat):
    return (getattr(chat, "room_type", None) or "support") == "staff_bot"

def _staff_bot_peers_present(chat_id, sender_role, ws_role_map=None):
    """
    Decide if a higher-level staff is present in the same staff_bot room.
    ws_role_map is optional; if you don’t have per-socket role tracking,
    we fallback to 'any staff present' check.
    """
    # If you already have a reliable "is_any_staff_present(chat_id)" which checks websocket members,
    # keep using it. We’ll refine behavior using sender_role + chat doc membership.
    try:
        return is_any_staff_present(chat_id)
    except Exception:
        return False

def _staff_bot_should_bot_reply(chat, sender_role):
    """
    Role-based gating:
      - In Master room: bot replies for sender_role=='master' until admin/owner engages
      - In Admin room: bot replies for sender_role=='admin' until owner engages
      - In Owner room: bot replies for sender_role=='superadmin' (owner personal room)
    We determine room type by which id fields are set on the chatroom document.
    """
    try:
        has_owner = bool(getattr(chat, "owner_id", None))
        has_admin = bool(getattr(chat, "admin_id", None))
        has_master = bool(getattr(chat, "super_admin_id", None))

        # Master room pattern: owner+admin+super_admin_id set
        if has_master and has_admin and has_owner:
            return sender_role == "master"

        # Admin room pattern: owner+admin set, super_admin_id empty
        if (not has_master) and has_admin and has_owner:
            return sender_role == "admin"

        # Owner personal room: owner set, admin/master empty
        if has_owner and (not has_admin) and (not has_master):
            return sender_role in ("superadmin", "owner")  # depending on your naming

        # Fallback: allow bot reply only for superadmin
        return sender_role == "superadmin"
    except Exception:
        return False
    
# ────────────────────── Exported symbols ──────────────────────
__all__ = [
    "_oid",
    "decode_jwt_id",
    "now_ist_iso",
    "cache_get",
    "cache_set",
    "faq_reply",
    "llm_fallback",
    "ensure_bot_user",
    "upsert_support_user_from_jwt",
    "ensure_chatroom_for_pro",
    "repeated_user_questions",
    "msg_dict",
    "room_add",
    "room_remove",
    "room_broadcast",
    "mark_role_join",
    "mark_role_leave",
    "get_chatrooms_for_superadmin_from_jwt",
    "chatroom_with_messages",
    "is_superadmin_present",
    "get_client_ip",
    "ensure_demo_user",
    "find_or_create_demo_chatroom",
    "save_demo_message",
    "demo_mark_role_join",
    "demo_mark_role_leave",
    "is_demo_superadmin_present",
    "safe_send",
    "safe_join",
    "safe_leave",
    "safe_room_add",
    "safe_room_remove",
    "safe_broadcast",
    "is_any_staff_present",
    "cancel_pending_bot_reply",
    "generate_bot_reply_lines",
    "schedule_bot_reply_after_2m",
    "_utc_day_key",
    "_can_ask_and_inc",
    "ensure_staff_bot_room",
    "superadmin_llm_fallback",
    "_sock_add",
    "_sock_remove",
    "_sock_send_any",
    "_sock_send_all",
    "_resolve_staff_links_from_clients",
    "_staff_bot_should_bot_reply",
    "_staff_bot_peers_present",
    "_is_staff_bot_room",
    "is_higher_staff_present"
    
]
