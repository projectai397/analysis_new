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
def _normalize(t: str) -> str:
    t = t or ""
    # unify quotes
    t = t.replace("’", "'").replace("‘", "'").replace("“", '"').replace("”", '"')
    # lower + strip
    t = t.lower().strip()

    # common contractions / variants
    repl = {
        r"\bwasn't\b": "was not",
        r"\bcan't\b": "cannot",
        r"\bdon't\b": "do not",
        r"\bforgot\b": "forget",  # align tense for matching
        r"\breseting\b": "resetting",
        r"\bpass word\b": "password",
        r"[\?\!\.\,]+": " ",  # drop punct
        r"\s+": " ",
    }
    for pat, rep in repl.items():
        t = re.sub(pat, rep, t)
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


def _keyword_boost(query: str, keyword: str) -> float:
    """
    Base similarity plus a small boost if key terms overlap.
    Helps cases like 'how to forgot password' vs 'how do i reset my password'.
    """
    base = _similar_ratio(query, keyword)  # 0..100
    qtok = _tokenize(query)
    ktok = _tokenize(keyword)
    overlap = len(qtok & ktok)
    boost = min(overlap * 2.5, 10.0)  # up to +10
    return min(base + boost, 100.0)


def _similar(a, b, thr=0.7):
    return SequenceMatcher(None, a, b).ratio() > thr


# ────────────────────── FAQ + LLM fallback ──────────────────────
def faq_reply(user_msg: str):
    """
    Best-match the user message against FAQ keywords.
    - Picks the single best FAQ across all keywords.
    - Uses robust similarity with optional RapidFuzz.
    - Returns the reply EXACTLY as stored (array or string).
    """
    if not user_msg:
        return None

    q = _normalize(user_msg)
    BEST = None  # (score, faq_doc)

    try:
        # Tune this threshold: 0..100. 70–75 works well; lower if your phrasing varies a lot.
        THRESH = 72.0

        cursor = faqs_coll.find({}, {"_id": 1, "keywords": 1, "reply": 1, "rating": 1})
        for faq in cursor:
            keywords = faq.get("keywords", []) or []
            # compute best keyword score for this FAQ
            best_kw_score = 0.0
            for kw in keywords:
                if not kw:
                    continue
                score = _keyword_boost(q, kw)
                if score > best_kw_score:
                    best_kw_score = score

            # keep the best overall FAQ
            if best_kw_score > THRESH and (BEST is None or best_kw_score > BEST[0]):
                BEST = (best_kw_score, faq)

        if BEST:
            _, faq = BEST
            faqs_coll.update_one({"_id": faq["_id"]}, {"$inc": {"rating": 1}})
            # return EXACT reply as stored (list or string)
            return faq.get("reply", [])

    except Exception as e:
        print("faq_reply error:", e)

    return None


def llm_fallback(user_msg: str):
    """
    1) Try local fuzzy FAQ (faq_reply) → exact stored response.
    2) Try your semantic router (answer_from_faq).
    3) Domain guard:
        - answer  → LLM concise reply (<=150 tokens)
        - clarify → short, single question to steer to in-app context
        - refuse  → OOD_MESSAGE (only for clear off-topic)
    """
    # 1) Local fuzzy FAQ
    faq = faq_reply(user_msg)
    if faq:
        return faq

    # 2) Semantic router
    try:
        sem = answer_from_faq(user_msg)
        if sem:
            return sem
    except Exception as e:
        print("answer_from_faq error:", e)

    # 3) Domain guard
    ga = guard_action(user_msg)
    if ga["action"] == "clarify":
        return ga["prompt"]
    if ga["action"] == "refuse":
        return ga["message"]
    # 'answer' → continue below

    # LLM answer (concise, on-topic)
    def _truncate_tokens(text: str, max_tokens: int = 150) -> str:
        parts = text.split()
        return " ".join(parts[:max_tokens]) if len(parts) > max_tokens else text

    try:
        # Fetch the URL from the environment
        llm_url = os.getenv("LLM_URL") 
        
        if not llm_url:
            print("Error: LLM_URL is not set in environment variables.")
            return "Sorry, there was an error with the support bot configuration."

        # IMPORTANT: If your URL ends in /api/generate, change it to /api/chat 
        # to match the "messages" format you are using.
        if llm_url.endswith("/api/generate"):
            llm_url = llm_url.replace("/api/generate", "/api/chat")

        # Send the request to the LLM API
        payload = {
            "model": "phi:2.7b",
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are the ProTrader5 Support Bot.\n\n"
                        "Rules (must follow exactly):\n"
                        "- Only answer questions about: buy/sell orders, order types (limit/market/SL/TP), "
                        "positions/PNL, margin/leverage, instruments/symbols, deposits/withdrawals/KYC, "
                        "account access/login/OTP/password reset, and app/site usage.\n"
                        "- If the user asks about password reset, give exact steps in the app/email flow.\n"
                        "- If the user asks how to buy/sell, give clear steps: search symbol → select order type → quantity → price → confirm.\n"
                        "- Answer in concise FAQ style (2–6 short steps or 2–4 sentences).\n"
                        "- Keep the entire answer under 150 tokens.\n"
                        "- No preface/disclaimers/stories/emoji."
                    ),
                },
                {"role": "user", "content": user_msg},
            ],
            "stream": False,
            "options": {"num_predict": 150, "temperature": 0.2},
        }

        r = requests.post(llm_url, json=payload, timeout=60)
        r.raise_for_status()
        d = r.json()

        # Handle the Chat API response format: d['message']['content']
        content = d.get("message", {}).get("content")
        
        # Fallback if content is missing from chat structure
        if not content:
            content = d.get("response")

        if not content:
            print(f"Error: The LLM response was empty for user message: {user_msg}")
            print(f"Full Response: {d}") # Debugging
            return "Sorry, the support bot could not generate a reply."

        return _truncate_tokens(content.strip(), 150)

    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return "Sorry, there was an error with our support bot. Please try again later."
    except Exception as e:
        print(f"LLM error: {e}")
        return "Sorry, there was an unexpected error. Please try again later."


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

def generate_bot_reply_lines(text: str) -> list[str]:
    reply = cache_get(text) or faq_reply(text)
    if reply:
        cache_set(text, reply)
        return reply

    ai = llm_fallback(text)
    lines = [ln.strip() for ln in (ai or "").split("\n") if ln.strip()]
    cache_set(text, lines)
    return lines

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
