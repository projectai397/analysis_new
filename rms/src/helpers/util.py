# src/helpers/util.py
from datetime import datetime, timedelta, timezone
import os
from typing import Any, Dict, List, Tuple, Optional
from bson import ObjectId
from pymongo import ASCENDING
from ..config import config, users,analysis,orders,trade
from functools import wraps
from flask import request, abort, g, jsonify, make_response
from bcrypt import checkpw
from ..extensions import cache
import time
import jwt

def _jwt_secret_and_alg():
    # Prefer explicit module constants if you have them, else env, else HS256
    secret = os.getenv("JWT_SECRET") 
    if not secret:
        raise RuntimeError("JWT secret is not configured (JWT_SECRET/SECRET_KEY).")
    alg = os.getenv("JWT_ALG")
    return secret, alg


def _classify_role(role_oid: ObjectId) -> str:
    if role_oid == config.SUPERADMIN_ROLE_ID:
        return "superadmin"
    if role_oid == config.ADMIN_ROLE_ID:
        return "admin"
    if role_oid == config.MASTER_ROLE_ID:
        return "master"
    if role_oid == config.USER_ROLE_ID:
        return "user"
    return "unknown"


class AuthService:
    def _json_error(self, status: int, msg: str):
        resp = jsonify({"ok": False, "error": msg})
        resp.status_code = status
        return abort(resp)

    # ----------
    # MINT TOKEN: produces SAME claims as your login flow
    # ----------
    def _mint_token(self, user: dict, role_val: ObjectId, days: int = 7) -> str:
        """
        Issue a Node-compatible HS256 JWT with the SAME payload shape used by login:
        {_id, name, phone, userName, role, role_id, preference, deviceToken, deviceId,
         deviceType, sequence, iat, exp}
        """
        now = int(time.time())
        exp = now + days * 24 * 3600
        role_name = _classify_role(role_val)

        payload = {
            "_id": str(user["_id"]),
            "name": user.get("name"),
            "phone": user.get("phone"),
            "userName": user.get("userName"),
            "role": role_name,               # human-readable role
            "role_id": str(role_val),        # ObjectId string
            "preference": user.get("preference"),
            "deviceToken": user.get("deviceToken"),
            "deviceId": user.get("deviceId"),
            "deviceType": user.get("deviceType"),
            "sequence": user.get("sequence"),
            "iat": now,
            "exp": exp,
        }

        secret, alg = _jwt_secret_and_alg()
        token = jwt.encode(payload, secret, algorithm=alg)
        return token.decode("utf-8") if isinstance(token, (bytes, bytearray)) else token

    def _verify_bcrypt(self, stored_hash: Optional[bytes | str], password: str) -> bool:
        if not stored_hash or not isinstance(stored_hash, (bytes, bytearray, str)):
            return False
        try:
            if isinstance(stored_hash, str):
                stored_hash = stored_hash.encode("utf-8")
            return checkpw(password.encode("utf-8"), stored_hash)
        except Exception:
            return False

    def resolve_identity_from_request_or_credentials(self) -> Tuple[str, Optional[str]]:
        """
        Returns (user_id_str, minted_token_or_None).
        - If Authorization Bearer token is present: validates JWT (PyJWT) and denies plain 'user'.
        - Else: accepts JSON credentials (exactly one of phone/username + password),
                verifies against bcrypt hash in DB, denies 'user', and mints a fresh token.
        """
        auth_hdr = request.headers.get("Authorization", "")
        has_auth_header = auth_hdr.lower().startswith("bearer ")

        # ---- Bearer JWT path (PyJWT) ----
        if has_auth_header:
            raw_token = auth_hdr.split(" ", 1)[1].strip()
            secret, alg = _jwt_secret_and_alg()
            try:
                claims = jwt.decode(
                    raw_token,
                    secret,
                    algorithms=[alg],
                    options={"require": ["exp", "iat"]},
                )
            except jwt.ExpiredSignatureError:
                self._json_error(401, "Token expired")
            except jwt.InvalidTokenError:
                self._json_error(401, "Invalid or malformed token")

            # Deny plain 'user' by role_id or role name
            rid = claims.get("role_id")
            rname = (claims.get("role") or "").strip().lower()
            if (rid and rid == str(config.USER_ROLE_ID)) or (rname == "user"):
                self._json_error(403, "Forbidden: role 'user' is not allowed")

            # Identity can be in _id (your login tokens) or sub (other issuers)
            uid = claims.get("_id") or claims.get("sub")
            if not uid:
                self._json_error(401, "Invalid token identity")
            try:
                _ = ObjectId(uid)
            except Exception:
                self._json_error(401, "Invalid token identity format")
            return uid, None

        # ---- Credentials fallback (JSON) ----
        data = request.get_json(silent=True) or {}
        phone = (data.get("phone") or "").strip()
        username = (data.get("username") or "").strip()
        password = data.get("password") or ""

        provided = [bool(phone), bool(username)]
        if sum(provided) == 0:
            self._json_error(401, "Missing token and no credentials provided")
        if sum(provided) > 1:
            self._json_error(400, "Provide only one: phone OR username")
        if not password:
            self._json_error(400, "Password required")

        # Your DB uses "userName" (camelCase), not "username"
        query = {"phone": phone} if phone else {"userName": username}

        # Support both 'password_hash' and legacy 'password' (bcrypt hash) fields
        # Also fetch the extra fields the token needs, so minting doesn't hit None
        user = users.find_one(
            query,
            {
                "_id": 1,
                "password_hash": 1,
                "password": 1,
                "role": 1,
                "phone": 1,
                "userName": 1,
                "name": 1,
                "preference": 1,
                "deviceToken": 1,
                "deviceId": 1,
                "deviceType": 1,
                "sequence": 1,
            },
        )
        if not user:
            self._json_error(401, "Invalid credentials")

        stored_hash = user.get("password_hash") or user.get("password")
        if not stored_hash or not self._verify_bcrypt(stored_hash, password):
            self._json_error(401, "Invalid credentials")

        role_val = user.get("role")
        if isinstance(role_val, str):
            try:
                role_val = ObjectId(role_val)
            except Exception:
                self._json_error(401, "Invalid role id on user")
        if not isinstance(role_val, ObjectId):
            self._json_error(401, "Invalid role id on user")

        # Deny plain 'user'
        if role_val == config.USER_ROLE_ID:
            self._json_error(403, "Access denied for role 'user'")

        # Mint fresh PyJWT with SAME payload as login
        token = self._mint_token(user, role_val)
        return str(user["_id"]), token


# instance
auth = AuthService()


def auth_superadmin(fn):
    """
    Guard that:
      - verifies identity via AuthService (JWT or credentials)
      - denies role == USER_ROLE_ID
      - allows superadmin/admin/master
      - sets g.current_user_id / g.current_user_oid
      - returns X-Access-Token header if a new token was minted (credentials path)
    """
    @wraps(fn)
    def wrapper(*args, **kwargs):
        uid_str, minted = auth.resolve_identity_from_request_or_credentials()
        g.current_user_id = uid_str
        try:
            g.current_user_oid = ObjectId(uid_str)
        except Exception:
            return jsonify({"ok": False, "error": "Invalid token identity format"}), 401

        resp = fn(*args, **kwargs)
        if minted:
            resp = make_response(resp)
            resp.headers["X-Access-Token"] = minted
        return resp
    return wrapper


def _boolish(v, default: bool = False) -> bool:
    """
    Convert common truthy/falsey values to bool.
    Accepts: True/False, 1/0, "true/false", "yes/no", "y/n", "on/off", "1/0".
    Whitespace and case are ignored. None returns `default`.
    """
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    if isinstance(v, bytes):
        try:
            v = v.decode("utf-8", "ignore")
        except Exception:
            return default
    if isinstance(v, str):
        s = v.strip().lower()
        if s in {"1", "true", "t", "yes", "y", "on"}:
            return True
        if s in {"0", "false", "f", "no", "n", "off"}:
            return False
    return default
# ----------------- existing helpers -----------------

def iso(v: Any) -> Any:
    if isinstance(v, datetime): return v.isoformat()
    if isinstance(v, ObjectId): return str(v)
    if isinstance(v, list): return [iso(x) for x in v]
    if isinstance(v, dict): return {k: iso(v) for k,v in v.items()}
    return v

def try_object_id(s: str):
    try: return ObjectId(s)
    except Exception: return None

def ist_week_window_now_for(scope: str, owner_id: ObjectId) -> tuple[datetime, datetime]:
    """
    Owner-scoped week window [start, now].

    Rules:
    - If an analysis doc exists for (scope, owner_id) and has start_date -> use it.
    - Else compute Monday 00:00 in IST, and if the doc exists, set start_date on THAT doc.
    - Never create a new analysis document here.
    - Returns timezone-aware datetimes per config.CREATED_AT_IS_UTC:
        * True  -> return UTC-aware times
        * False -> return IST-aware times (config.APP_TZ)
    """
    # 'now' in IST (we'll convert for return based on config)
    now_ist = datetime.now(config.APP_TZ)

    # Find THIS owner's analysis doc (newest if multiple exist from legacy runs)
    base_doc = analysis.find_one({"scope": scope, "owner_id": owner_id},
                                 sort=[("generated_at", -1)])

    # 1) Prefer stored start_date, if any
    start_dt: datetime | None = None
    if base_doc is not None:
        sd = base_doc.get("start_date")
        if isinstance(sd, datetime):
            start_dt = sd

    # 2) Otherwise compute Monday 00:00 IST and set it on the *same* doc (no upsert)
    if start_dt is None:
        week_start_ist = (now_ist - timedelta(days=now_ist.weekday())).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        to_store = week_start_ist.astimezone(timezone.utc) if config.CREATED_AT_IS_UTC else week_start_ist

        if base_doc is not None:
            analysis.update_one({"_id": base_doc["_id"]},
                                {"$set": {"start_date": to_store}},
                                upsert=False)
        start_dt = to_store

    # Normalize stored start_dt to be tz-aware for output
    if start_dt.tzinfo is None:
        assumed = timezone.utc if config.CREATED_AT_IS_UTC else config.APP_TZ
        start_dt = start_dt.replace(tzinfo=assumed)

    # Return window in the target TZ flavor
    if config.CREATED_AT_IS_UTC:
        return start_dt.astimezone(timezone.utc), now_ist.astimezone(timezone.utc)
    else:
        return start_dt.astimezone(config.APP_TZ), now_ist

def ist_week_window_weekly() -> Tuple[datetime, datetime]:
    """
    Return the current week window [monday 00:00 IST, now IST].
    Start is always Monday 00:00 IST of the current week,
    End is the current IST datetime.
    """
    now_ist = datetime.now(config.APP_TZ)

    # Compute Monday 00:00 IST
    start_ist = (now_ist - timedelta(days=now_ist.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    return start_ist, now_ist
def resolve_caps_by_balance(balance: float):
    return (150, 100_000_000) if balance >= 1_000_000 else (50, 10_000_000)

def _normalize(docs):
    out = []
    for d in docs:
        d = dict(d)

        for key in ["_id", "parentId", "addedBy", "role"]:
            if key in d and isinstance(d[key], ObjectId):
                d[key] = str(d[key])

        for key in ["createdAt", "updatedAt"]:
            if key in d and isinstance(d[key], datetime):
                d[key] = d[key].isoformat()

        out.append(d)
    return out

def get_child_admin_ids(logged_in_oid: ObjectId) -> Dict[str, List[str]]:
    _MIN_PROJ = {"_id": 1}
    me = users.find_one({"_id": logged_in_oid}, {"_id": 1, "role": 1, "parentId": 1})
    if not me:
        return []
    role = me.get("role")
    IDs = [ObjectId(logged_in_oid)]

    if config.SUPERADMIN_ROLE_ID is not None and role == config.SUPERADMIN_ROLE_ID:
        admin_cursor = users.find({"role": config.ADMIN_ROLE_ID, "parentId": logged_in_oid}, _MIN_PROJ)
        admin_oid_list = [doc["_id"] for doc in admin_cursor]
        for x in admin_oid_list:
            IDs.append(ObjectId(x))

        if admin_oid_list:
            master_cursor = users.find({"role": config.MASTER_ROLE_ID, "parentId": {"$in": admin_oid_list}}, _MIN_PROJ)
            for doc in master_cursor:
                IDs.append(ObjectId(doc["_id"]))

        return IDs

    if role == config.ADMIN_ROLE_ID:
        master_cursor = users.find({"role": config.MASTER_ROLE_ID, "parentId": logged_in_oid}, _MIN_PROJ)
        for doc in master_cursor:
            IDs.append(ObjectId(doc["_id"]))
        return IDs

    return IDs


def _ensure_trade_indexes():
    try:
        trade.create_index([("order_id", ASCENDING)], unique=True, background=True)
    except Exception:
        pass

def _now_utc():
    return datetime.now(timezone.utc)

def _build_change_metadata(existing: dict, new_doc: dict) -> dict | None:
    changes = {}
    if not existing:
        return None
    for k, new_val in new_doc.items():
        if k in ("_id", "metadata"):  # ignore internal fields
            continue
        old_val = existing.get(k)
        if old_val != new_val:
            changes[k] = {"old": old_val, "new": new_val}
    return {"timestamp": _now_utc(), "changes": changes} if changes else None

def sync_orders_to_trade():
    _ensure_trade_indexes()

    with orders.watch(full_document="updateLookup") as stream:
        for change in stream:
            op_type = change["operationType"]

            if op_type == "insert":
                src = change["fullDocument"]
                order_id = src["_id"]

                # prepare payload for trade (fresh _id; keep link in order_id)
                payload = dict(src)  # shallow copy ok for top-level
                payload["order_id"] = order_id
                payload["_id"] = ObjectId()      # new trade doc id
                payload.setdefault("metadata", [])

                # idempotent write: upsert by order_id so duplicates never happen
                trade.update_one(
                    {"order_id": order_id},
                    {"$setOnInsert": payload},
                    upsert=True,
                )
                print(f"[UPSERT][insert] orders {order_id} → trade (order_id unique)")

            elif op_type == "update":
                src = change["fullDocument"]
                order_id = src["_id"]

                # project existing trade doc (may be None if update arrives before insert)
                existing = trade.find_one({"order_id": order_id}) or {}

                # new state to write
                new_state = dict(src)
                new_state["order_id"] = order_id
                new_state.pop("_id", None)  # remove _id from $set (immutable field)

                meta_entry = _build_change_metadata(existing, src)

                update = {"$set": new_state}
                if not existing:
                    update.setdefault("$setOnInsert", {})["metadata"] = []
                    update["$setOnInsert"]["_id"] = ObjectId()  # set _id only on insert
                if meta_entry:
                    update.setdefault("$push", {})["metadata"] = meta_entry

                trade.update_one({"order_id": order_id}, update, upsert=True)
                print(f"[UPSERT][update] orders {order_id} → trade (metadata {'added' if meta_entry else 'no-change'})")