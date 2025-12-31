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
import jwt
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

def query_user_db(user_msg: str, user_id: str):
    logger.info(f" [1] DB_CHECK: Starting for user: {user_id}")
    msg = user_msg.lower()
    coll_name = None
    
    # --- 1. DATE RANGE DETECTION ---
    date_pattern = r'(\d{1,2}[/-]\d{1,2}[/-]\d{4})'
    found_dates = re.findall(date_pattern, user_msg)
    
    date_filter = None
    if len(found_dates) >= 2:
        try:
            # Convert DD/MM/YYYY to start of day (00:00:00)
            start_dt = datetime.strptime(found_dates[0].replace('-', '/'), "%d/%m/%Y")
            
            # THE FIX: Use datetime_time.max to get end of day (23:59:59)
            # This ensures trades made on the afternoon of the final day are included
            end_base = datetime.strptime(found_dates[1].replace('-', '/'), "%d/%m/%Y")
            end_dt = datetime.combine(end_base, datetime_time.max) 
            
            date_filter = {"$gte": start_dt, "$lte": end_dt}
            logger.info(f" [1.1] DATE_FILTER_MATCH: {start_dt} to {end_dt}")
        except Exception as e:
            logger.error(f" [1.2] DATE_PARSE_ERROR: {e}")

    # --- 2. ID DETECTION ---
    id_pattern = r'[a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12}|[a-zA-Z0-9]{20,40}'
    id_match = re.search(id_pattern, user_msg)
    found_id = id_match.group() if id_match else None

    # --- 3. INTENT FLAGS ---
    is_last_query = any(word in msg for word in ["last", "latest", "recent"])
    is_count_query = any(word in msg for word in ["how many", "count", "total"])

    # --- 4. ROUTING PRIORITY ---
    if any(word in msg for word in ["p&l", "balance", "profit"]):
        coll_name = "user"
    elif found_id or any(word in msg for word in ["payment", "deposit", "withdraw", "request"]):
        coll_name = "paymentRequest"
    elif any(word in msg for word in ["position", "holding", "open"]):
        coll_name = "position"
    elif any(word in msg for word in ["trade", "order"]):
        coll_name = "trade"
    elif any(word in msg for word in ["alert", "notification"]):
        coll_name = "alerts"
    else:
        coll_name = "transaction"

    # --- 5. SECURE DATABASE QUERY ---
    try:
        if coll_name == "user":
            query_filter = {"_id": ObjectId(user_id)}
        else:
            query_filter = {"userId": ObjectId(user_id)}

        if found_id:
            query_filter["$or"] = [
                {"transactionId": found_id},
                {"_id": ObjectId(found_id) if len(found_id) == 24 else None}
            ]
        
        if date_filter:
            # Most of your collections use 'createdAt' for time-based filtering
            query_filter["createdAt"] = date_filter

        if is_count_query:
            count = db[coll_name].count_documents(query_filter)
            return {"data": count, "collection": coll_name, "type": "count"}

        # --- 6. SMART LIMITING ---
        # Increased limit to 100 for date ranges to show the full period requested
        if date_filter:
            limit_val = 100 
        else:
            limit_val = 1 if (is_last_query or found_id or coll_name == "user") else 5
        
        # We sort by createdAt descending to show newest data first
        results = list(db[coll_name].find(query_filter).sort("createdAt", -1).limit(limit_val))
        return {"data": results, "collection": coll_name} if results else None

    except Exception as e:
        logger.error(f" [1.7] DB_CHECK_ERROR: {e}")
        return None
    
def format_db_results(data_list, collection_name: str, start_date=None, end_date=None) -> str:
    # 1. Handle Empty or Count Results
    if not data_list:
        return "<p>No records found for the selected period.</p>"
    
    if isinstance(data_list, int):
        return f"<b>Total count:</b> {data_list}"

    # Define common styles for the table
    t_style = 'style="width:100%; border-collapse: collapse; font-family: sans-serif; font-size: 13px;"'
    th_style = 'style="background-color: #f8f9fa; border-bottom: 2px solid #dee2e6; padding: 8px; text-align: left;"'
    td_style = 'style="border-bottom: 1px solid #dee2e6; padding: 8px;"'

    def clean_date(dt):
        return dt.strftime("%d %b %H:%M") if hasattr(dt, 'strftime') else dt

    # 2. Build the Header/Report Info
    html_out = f"<b>📊 {collection_name.upper()} REPORT</b><br>"
    if start_date and end_date:
        html_out += f"<small>Period: {start_date} to {end_date}</small><br>"
    html_out += f"<small>Total Records: {len(data_list)}</small><br><br>"
    
    html_out += f'<table {t_style}><thead><tr>'

    # 3. Collection Specific Table Headers & Rows
    rows_html = ""
    
    if collection_name == "position":
        html_out += f'<th {th_style}>Symbol</th><th {th_style}>Type</th><th {th_style}>Qty</th><th {th_style}>P&L</th></tr></thead><tbody>'
        for doc in data_list:
            pnl = doc.get("profitLoss", 0)
            pnl_style = 'style="color: green; font-weight: bold;"' if pnl >= 0 else 'style="color: red; font-weight: bold;"'
            rows_html += f'''<tr>
                <td {td_style}>{doc.get("symbolName")}</td>
                <td {td_style}>{doc.get("tradeType")}</td>
                <td {td_style}>{doc.get("totalQuantity")}</td>
                <td {td_style} {pnl_style}>{pnl}</td>
            </tr>'''

    elif collection_name == "trade":
        html_out += f'<th {th_style}>Symbol</th><th {th_style}>Status</th><th {th_style}>Type</th><th {th_style}>Time</th></tr></thead><tbody>'
        for doc in data_list:
            rows_html += f'''<tr>
                <td {td_style}>{doc.get("symbolName")}</td>
                <td {td_style}>{doc.get("status")}</td>
                <td {td_style}>{doc.get("tradeType") or doc.get("orderType")}</td>
                <td {td_style}>{clean_date(doc.get("createdAt"))}</td>
            </tr>'''

    elif collection_name == "transaction":
        html_out += f'<th {th_style}>Amt</th><th {th_style}>Category</th><th {th_style}>Time</th></tr></thead><tbody>'
        for doc in data_list:
            amt_style = 'style="color: green;"' if doc.get("type") == "credit" else 'style="color: red;"'
            rows_html += f'''<tr>
                <td {td_style} {amt_style}>{doc.get("amount")}</td>
                <td {td_style}>{doc.get("transactionType")}</td>
                <td {td_style}>{clean_date(doc.get("createdAt"))}</td>
            </tr>'''

    elif collection_name == "paymentRequest":
        status_map = {0: "🕒 Pending", 1: "✅ Approved", 2: "❌ Rejected"}
        html_out += f'<th {th_style}>Method</th><th {th_style}>Amount</th><th {th_style}>Status</th></tr></thead><tbody>'
        for doc in data_list:
            rows_html += f'''<tr>
                <td {td_style}>{doc.get("paymentRequestType")}</td>
                <td {td_style}>{doc.get("amount")}</td>
                <td {td_style}>{status_map.get(doc.get("status"), "Unknown")}</td>
            </tr>'''

    elif collection_name == "user":
        html_out += f'<th {th_style}>User</th><th {th_style}>Balance</th><th {th_style}>P&L</th></tr></thead><tbody>'
        for doc in data_list:
            rows_html += f'''<tr>
                <td {td_style}>{doc.get("name")}</td>
                <td {td_style}>{doc.get("balance")}</td>
                <td {td_style}>{doc.get("profitLoss")}</td>
            </tr>'''

    html_out += rows_html + "</tbody></table>"
    return html_out

def llm_fallback(user_msg: str, user_id: str) -> str:
    logger.info(f"--- Starting LLM Fallback Flow for User: {user_id} ---")

    # 1. Greetings
    if _is_greeting(user_msg):
        logger.info(" STEP: Greeting detected.")
        return "Hello! How can I assist with your trading account today?"

    # 2. Smart DB Check
    db_res = query_user_db(user_msg, user_id)
    if db_res:
        logger.info(f" STEP: Data found in {db_res['collection']}. Formatting...")
        
        # APPLY THE FORMATTER HERE
        clean_data = format_db_results(db_res["data"], db_res["collection"])
        
        # Convert to pretty string
        raw_json = json.dumps(clean_data, default=str, indent=2)
        
        return f"Your {db_res['collection']} records are:\n\n{raw_json}"

    # 3. FAQ Check (Only runs if no DB data was found)
    logger.info(" STEP: No DB data found. Moving to FAQ check.")
    faq_answer = faq_reply(user_msg)
    if faq_answer:
        return faq_answer

    # 4. Domain Guard
    logger.info(" STEP: No FAQ match. Checking Domain Guard.")
    # No FAQ match. Checking Domain Guard.
    ga = guard_action(user_msg)
    if ga["action"] == "refuse":
        logger.info(" STEP: Message refused by Guard.")
        # Message refused by Guard.
        return ga["message"]

    # 5. General LLM Answer
    logger.info(" STEP: Falling back to General LLM support.")
    # Falling back to General LLM support.
    SUPPORT_SYSTEM = (
        "You are a customer support agent for a trading app. "
        "Answer ONLY trading support questions (orders, PNL, KYC, deposits). "
        "Simple English, no AI mention, 80-130 words."
    )

    raw_support = _call_llm_internal(SUPPORT_SYSTEM, user_msg)
    cleaned = _clean_llm_text(raw_support)
    return _enforce_medium_length(cleaned, 80, 130)
    
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

from bson import ObjectId


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

from bson import ObjectId

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

def schedule_bot_reply_after_2m(chat, chat_id: str, user_text: str):
    cancel_pending_bot_reply(chat_id)

    def _fire():
        # If staff replied or staff left, we decide what to do:
        # Requirement says: if staff present and no one answered -> bot replies.
        # If staff left, bot can reply immediately anyway, so we still reply.
        with PENDING_LOCK:
            PENDING_BOT_TIMERS.pop(chat_id, None)

        reply_lines = generate_bot_reply_lines(user_text)
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
    "_can_ask_and_inc"
    
]
