# src/app.py
from __future__ import annotations
from ast import Dict
from fileinput import filename
from waitress import serve
import jwt
import glob
import hashlib
import json
import logging
import os
import random
import shutil
import subprocess
import threading
import time
from flask import send_from_directory
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
import schedule
from bson import ObjectId
from flask import (Blueprint, Flask, jsonify, make_response, request,
                   send_from_directory)
from flask_cors import CORS
# NOTE: JWTManager is still used for REST; WS path uses our own decode from helpers
from flask_jwt_extended import JWTManager
from flask_sock import Sock
from src.api import api_blueprint
# ─────────────────────────────────────────────────────────────
# Local imports (merged structure: everything is under src/)
# ─────────────────────────────────────────────────────────────
from src.config import config
# chatbot dependencies (moved to src.*)
from src.db import (PRO_DB,  # NEW: demo chatroom listing/selection
                    PRO_USER_COLL, SUPPORT_DB, USER_ROLE_ID,
                    demo_chatrooms_coll, notifications_coll,
                    support_users_coll)
from src.domain_guard import OOD_MESSAGE, is_in_domain
from src.extensions import cache, ratelimit_guard
from src.helper import (_oid, cache_get, cache_set, chatroom_with_messages,
                        decode_jwt_id, demo_mark_role_join,
                        demo_mark_role_leave, ensure_bot_user,
                        ensure_chatroom_for_pro, ensure_demo_user, faq_reply,
                        find_or_create_demo_chatroom,
                        get_chatrooms_for_superadmin_from_jwt, get_client_ip,
                        is_demo_superadmin_present, is_superadmin_present,
                        llm_fallback, mark_role_join, mark_role_leave,
                        msg_dict, now_ist_iso, repeated_user_questions,
                        room_add, room_broadcast, room_remove,
                        save_demo_message, upsert_support_user_from_jwt,is_any_staff_present,cancel_pending_bot_reply,generate_bot_reply_lines,schedule_bot_reply_after_2m,
                        _can_ask_and_inc,ensure_staff_bot_room,superadmin_llm_fallback,_sock_add,_sock_remove,_sock_send_any,_sock_send_all,_resolve_staff_links_from_clients,
                        _staff_bot_should_bot_reply,_staff_bot_peers_present,_is_staff_bot_room,is_higher_staff_present,_ensure_presence_bucket)
# materializers (analytics)
from src.helpers.build_service import (materialize_admins_analysis,
                                       materialize_masters_analysis,
                                       materialize_superadmins_analysis,
                                       materialize_superadmins_users)
from src.helpers.s3 import backup_mongo_to_archive, upload_backup_to_s3
from src.helpers.util import sync_orders_to_trade
from src.helpers.hierarchy_service import (
    get_admins_for_superadmin,
    get_masters_for_superadmin,
    get_users_for_superadmin,
    get_masters_for_admin,
    get_users_for_admin,
    get_users_for_master,
)
from src.models import Chatroom, Message, ProUser, SCUser
from werkzeug.utils import secure_filename
from threading import Lock, Timer
from zoneinfo import ZoneInfo
# ─────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)
# ─────────────────────────────────────────────────────────────
# Globals for analytics job state
# ─────────────────────────────────────────────────────────────
_lock = threading.Lock()
_last_run_utc = None
_last_result = None

_lock_analysis = threading.Lock()
_lock_users = threading.Lock()
_lock_admins = threading.Lock()
_lock_masters = threading.Lock()

_last_run_utc_analysis = None
_last_run_utc_users = None
_last_run_utc_admins = None
_last_run_utc_masters = None

_last_result_analysis = None
_last_result_users = None
_last_result_admins = None
_last_result_masters = None

PENDING_LOCK = Lock()
PENDING_BOT_TIMERS: dict[str, Timer] = {}
PENDING_USER_TEXT: dict[str, str] = {}   # optional: keep last user question
STAFF_ENGAGED: dict[str, bool] = {}
WS_IDLE_TIMEOUT_SECONDS = int(os.getenv("WS_IDLE_TIMEOUT_SECONDS", "300"))   # 5 min default
WS_DAILY_USER_LIMIT     = int(os.getenv("WS_DAILY_USER_LIMIT", "40"))       # 20 default
_DAILY_QA_COUNTS = {}
UPLOAD_DIR = os.path.join(os.getcwd(), "call_recordings")
os.makedirs(UPLOAD_DIR, exist_ok=True)

MASTER_SOCKETS = defaultdict(set)  # master_user_id(str) -> set(ws)
USER_SOCKETS   = defaultdict(set)  # user_id(str) -> set(ws)
ADMIN_SOCKETS = defaultdict(set)
SUPERADMIN_SOCKETS = defaultdict(set)

ACTIVE_CALLS = {}  # call_id -> {"chat_id": str, "user_id": str, "master_id": str, "state": str}
PRESENCE: Dict[str, Dict[str, set]] = {}
# ─────────────────────────────────────────────────────────────
# Analytics job runners
# ─────────────────────────────────────────────────────────────
def _room_for_user(user_id: str) -> str:
    return f"user:{user_id}"

def _decode_token_from_query():
    token = request.args.get("token")
    if not token:
        raise RuntimeError("missing token")
    payload = jwt.decode(
        token,
        config.JWT_SECRET,
        algorithms=[getattr(config, "JWT_ALG", "HS256")],
    )
    return payload

def _run_job_analysis(trigger: str, limit: int | None = None):
    global _last_run_utc_analysis, _last_result_analysis
    limit = limit or getattr(config, "DEFAULT_LIMIT", 10)

    if _lock_analysis.locked():
        msg = "analysis job already running"
        logger.warning(msg)
        return {"ok": False, "message": msg, "trigger": trigger}

    _lock_analysis.acquire()
    try:
        _last_run_utc_analysis = datetime.now(timezone.utc)
        logger.info(
            f"[analysis] Starting (trigger={trigger}) @ {_last_run_utc_analysis.isoformat()}"
        )

        res_super = materialize_superadmins_analysis(limit=limit)
        res_admin = materialize_admins_analysis(limit=limit)
        res_master = materialize_masters_analysis(limit=limit)

        _last_result_analysis = {
            "ok": True,
            "trigger": trigger,
            "ran_at_utc": _last_run_utc_analysis.isoformat(),
            "result": {
                "superadmins": res_super,
                "admins": res_admin,
                "masters": res_master,
            },
        }
        logger.info("[analysis] Finished OK")
        return _last_result_analysis

    except Exception as e:
        _last_result_analysis = {
            "ok": False,
            "trigger": trigger,
            "error": str(e),
            "ran_at_utc": (
                _last_run_utc_analysis.isoformat() if _last_run_utc_analysis else None
            ),
        }
        logger.exception(f"[analysis] FAILED: {e}")
        return _last_result_analysis
    finally:
        _lock_analysis.release()


def _run_job_users(trigger: str, limit: int | None = None):
    global _last_run_utc_users, _last_result_users
    limit = limit or getattr(config, "DEFAULT_LIMIT", 10)

    if _lock_users.locked():
        msg = "analysis_users job already running"
        logger.warning(msg)
        return {"ok": False, "message": msg, "trigger": trigger}

    _lock_users.acquire()
    try:
        _last_run_utc_users = datetime.now(timezone.utc)
        logger.info(
            f"[analysis_users] Starting (trigger={trigger}) @ {_last_run_utc_users.isoformat()}"
        )
        out = materialize_superadmins_users(limit=limit)
        _last_result_users = {
            "ok": True,
            "trigger": trigger,
            "result": out,
            "ran_at_utc": _last_run_utc_users.isoformat(),
        }
        logger.info("[analysis_users] Finished OK")
        return _last_result_users
    except Exception as e:
        _last_result_users = {
            "ok": False,
            "trigger": trigger,
            "error": str(e),
            "ran_at_utc": (
                _last_run_utc_users.isoformat() if _last_run_utc_users else None
            ),
        }
        logger.exception(f"[analysis_users] FAILED: {e}")
        return _last_result_users
    finally:
        _lock_users.release()


def _run_job_admins(trigger: str, limit: int | None = None):
    global _last_run_utc_admins, _last_result_admins
    limit = limit or getattr(config, "DEFAULT_LIMIT", 10)

    if _lock_admins.locked():
        msg = "admins job already running"
        logger.warning(msg)
        return {"ok": False, "message": msg, "trigger": trigger}

    _lock_admins.acquire()
    try:
        _last_run_utc_admins = datetime.now(timezone.utc)
        logger.info(
            f"[admins] Starting (trigger={trigger}) @ {_last_run_utc_admins.isoformat()}"
        )
        out = materialize_admins_analysis(limit=limit)
        _last_result_admins = {
            "ok": True,
            "trigger": trigger,
            "result": out,
            "ran_at_utc": _last_run_utc_admins.isoformat(),
        }
        logger.info("[admins] Finished OK")
        return _last_result_admins
    except Exception as e:
        _last_result_admins = {
            "ok": False,
            "trigger": trigger,
            "error": str(e),
            "ran_at_utc": (
                _last_run_utc_admins.isoformat() if _last_run_utc_admins else None
            ),
        }
        logger.exception(f"[admins] FAILED: {e}")
        return _last_result_admins
    finally:
        _lock_admins.release()


def _run_job_masters(trigger: str, limit: int | None = None):
    global _last_run_utc_masters, _last_result_masters
    limit = limit or getattr(config, "DEFAULT_LIMIT", 10)

    if _lock_masters.locked():
        msg = "masters job already running"
        logger.warning(msg)
        return {"ok": False, "message": msg, "trigger": trigger}

    _lock_masters.acquire()
    try:
        _last_run_utc_masters = datetime.now(timezone.utc)
        logger.info(
            f"[masters] Starting (trigger={trigger}) @ {_last_run_utc_masters.isoformat()}"
        )
        out = materialize_masters_analysis(limit=limit)
        _last_result_masters = {
            "ok": True,
            "trigger": trigger,
            "result": out,
            "ran_at_utc": _last_run_utc_masters.isoformat(),
        }
        logger.info("[masters] Finished OK")
        return _last_result_masters
    except Exception as e:
        _last_result_masters = {
            "ok": False,
            "trigger": trigger,
            "error": str(e),
            "ran_at_utc": (
                _last_run_utc_masters.isoformat() if _last_run_utc_masters else None
            ),
        }
        logger.exception(f"[masters] FAILED: {e}")
        return _last_result_masters
    finally:
        _lock_masters.release()


def _run_job(trigger: str = "manual"):
    """Combined: analysis trio + per-user docs"""
    global _last_run_utc, _last_result
    if _lock.locked():
        msg = f"Job already running (trigger={trigger})"
        logger.warning(msg)
        return {"ok": False, "message": msg}

    with _lock:
        _last_run_utc = datetime.now(timezone.utc)
        logger.info(
            f"Starting combined job (trigger={trigger}) at {_last_run_utc.isoformat()}"
        )
        try:
            res_analysis = _run_job_analysis(trigger=f"{trigger}:combined")
            res_users = _run_job_users(trigger=f"{trigger}:combined")
            ok = bool(res_analysis.get("ok") and res_users.get("ok"))
            _last_result = {
                "ok": ok,
                "ran_at_utc": _last_run_utc.isoformat(),
                "analysis": res_analysis,
                "analysis_users": res_users,
            }
            if ok:
                logger.info("Combined job finished OK")
            else:
                logger.warning("Combined job completed with failures")
            return _last_result
        except Exception as e:
            _last_result = {
                "ok": False,
                "error": str(e),
                "ran_at_utc": _last_run_utc.isoformat(),
            }
            logger.exception(f"Combined job FAILED (trigger={trigger}): {e}")
            return _last_result


def _run_async(fn, label: str = "job"):
    """Run a callable in a daemon thread so schedule() doesn't block the server."""

    def _wrapped():
        try:
            logger.info(f"[sched:{label}] start")
            fn()
            logger.info(f"[sched:{label}] done")
        except Exception as e:
            logger.exception(f"[sched:{label}] crashed: {e}")

    threading.Thread(target=_wrapped, daemon=True).start()

def _screen_auth_from_jwt():
    """
    Reuse your existing JWT auth helper for Socket.IO events.
    Raises if token invalid.
    """
    return upsert_support_user_from_jwt()


def _is_user_role(su):
    # Real end-user / client
    return su.role == USER_ROLE_ID


def _is_staff_role(su):
    # Anyone who is not plain user → admin / superadmin / master, etc.
    return su.role != USER_ROLE_ID

# ─────────────────────────────────────────────────────────────
# Blueprint for analytics status + control
# ─────────────────────────────────────────────────────────────
analysis_bp = Blueprint("analysis", __name__)


@analysis_bp.get("/analysis/status")
def status():
    return (
        jsonify(
            {
                "ok": True,
                "running_combined": _lock.locked(),
                "last_run_utc_combined": (
                    _last_run_utc.isoformat() if _last_run_utc else None
                ),
                "last_result_combined": _last_result,
                "running_analysis": _lock_analysis.locked(),
                "last_run_utc_analysis": (
                    _last_run_utc_analysis.isoformat()
                    if _last_run_utc_analysis
                    else None
                ),
                "last_result_analysis": _last_result_analysis,
                "running_users": _lock_users.locked(),
                "last_run_utc_users": (
                    _last_run_utc_users.isoformat() if _last_run_utc_users else None
                ),
                "last_result_users": _last_result_users,
                "running_admins": _lock_admins.locked(),
                "last_run_utc_admins": (
                    _last_run_utc_admins.isoformat() if _last_run_utc_admins else None
                ),
                "last_result_admins": _last_result_admins,
                "running_masters": _lock_masters.locked(),
                "last_run_utc_masters": (
                    _last_run_utc_masters.isoformat() if _last_run_utc_masters else None
                ),
                "last_result_masters": _last_result_masters,
            }
        ),
        200,
    )


@analysis_bp.post("/analysis/run-analysis")
def run_analysis_only():
    run_async = request.args.get("async") in ("1", "true", "yes")
    if run_async:
        if _lock_analysis.locked():
            return (
                jsonify({"ok": False, "message": "analysis job already running"}),
                409,
            )
        threading.Thread(target=_run_job_analysis, args=("async",), daemon=True).start()
        return (
            jsonify({"ok": True, "started": True, "running": True, "job": "analysis"}),
            202,
        )
    out = _run_job_analysis("api")
    return jsonify(out), (200 if out.get("ok") else 500)


@analysis_bp.post("/analysis/run-users")
def run_users_only():
    run_async = request.args.get("async") in ("1", "true", "yes")
    if run_async:
        if _lock_users.locked():
            return (
                jsonify({"ok": False, "message": "analysis_users job already running"}),
                409,
            )
        threading.Thread(target=_run_job_users, args=("async",), daemon=True).start()
        return (
            jsonify(
                {"ok": True, "started": True, "running": True, "job": "analysis_users"}
            ),
            202,
        )
    out = _run_job_users("api")
    return jsonify(out), (200 if out.get("ok") else 500)


@analysis_bp.post("/analysis/run-admins")
def run_admins_only():
    run_async = request.args.get("async") in ("1", "true", "yes")
    if run_async:
        if _lock_admins.locked():
            return jsonify({"ok": False, "message": "admins job already running"}), 409
        threading.Thread(target=_run_job_admins, args=("async",), daemon=True).start()
        return (
            jsonify({"ok": True, "started": True, "running": True, "job": "admins"}),
            202,
        )
    out = _run_job_admins("api")
    return jsonify(out), (200 if out.get("ok") else 500)


@analysis_bp.post("/analysis/run-masters")
def run_masters_only():
    run_async = request.args.get("async") in ("1", "true", "yes")
    if run_async:
        if _lock_masters.locked():
            return jsonify({"ok": False, "message": "masters job already running"}), 409
        threading.Thread(target=_run_job_masters, args=("async",), daemon=True).start()
        return (
            jsonify({"ok": True, "started": True, "running": True, "job": "masters"}),
            202,
        )
    out = _run_job_masters("api")
    return jsonify(out), (200 if out.get("ok") else 500)


@analysis_bp.post("/analysis/run")
def run_now():
    run_async = request.args.get("async") in ("1", "true", "yes")
    if run_async:
        if _lock.locked():
            return jsonify({"ok": False, "message": "Job already running"}), 409
        threading.Thread(target=_run_job, args=("async",), daemon=True).start()
        return jsonify({"ok": True, "started": True, "running": True}), 202
    out = _run_job("api")
    return jsonify(out), (200 if out.get("ok") else 500)


# ─────────────────────────────────────────────────────────────
# Chatbot helpers and endpoints (same behavior, unified app)
# ─────────────────────────────────────────────────────────────
def allowed_file(filename: str) -> bool:
    ALLOWED = {
        "png",
        "jpg",
        "jpeg",
        "gif",
        "pdf",
        "doc",
        "docx",
        "txt",
        "csv",
        "xlsx",
        "webm",
        "mp3",
        "wav",
        "m4a",
    }
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED


# ─────────────────────────────────────────────────────────────
# App factory (single Flask app for everything)
# ─────────────────────────────────────────────────────────────
def create_app() -> Flask:
    app = Flask(__name__, static_folder="static")

    # Core config
    app.config["JWT_SECRET_KEY"] = config.JWT_SECRET
    app.config["JWT_ALG"] = getattr(config, "JWT_ALG", "HS256")
    app.config.setdefault("CACHE_TYPE", "SimpleCache")
    app.config.setdefault("CACHE_DEFAULT_TIMEOUT", 10800)  # 3h
    
    # Uploads
    app.config["UPLOAD_FOLDER"] = getattr(config, "UPLOAD_FOLDER", "uploads")
    app.config["MAX_CONTENT_LENGTH"] = (
        int(getattr(config, "MAX_CONTENT_LENGTH_MB", 2)) * 1024 * 1024
    )
    os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)

    # Init extensions
    cache.init_app(app)
    JWTManager(app)

    # CORS
    CORS(
        app,
        resources={
            r"/*": {
                "origins": "*",
                "methods": ["GET", "POST", "OPTIONS"],
                "allow_headers": ["Authorization", "Content-Type"],
                "max_age": 7200,
            }
        },
        automatic_options=True,
    )

    # Handle preflight & rate limiting early
    def _preflight():
        if request.method == "OPTIONS":
            return make_response(("", 204))
        return None

    app.before_request(_preflight)
    app.before_request(ratelimit_guard)

    # Register analytics blueprints
    app.register_blueprint(api_blueprint)
    app.register_blueprint(analysis_bp)

    # Health
    @app.get("/health")
    def _health():
        return {"ok": True}

    # ── Chatbot WebSocket
    sock = Sock(app)

    @sock.route("/ws")
    def ws_chat(ws):
        chat = None
        chat_id = None
        conn_role = "user"

        # ✅ track last activity (use dict so watchdog can read updated value)
        last_activity = {"ts": time.time()}
        stop_watchdog = threading.Event()

        # ✅ WATCHDOG: disconnect if no activity for N seconds (because ws.receive() blocks)
        def _idle_watchdog():
            while not stop_watchdog.is_set():
                try:
                    if (time.time() - last_activity["ts"]) > WS_IDLE_TIMEOUT_SECONDS:
                        try:
                            ws.send(json.dumps({"type": "error", "error": "idle_timeout"}))
                        except Exception:
                            pass
                        try:
                            ws.close()
                        except Exception:
                            pass
                        break
                except Exception:
                    break
                time.sleep(2)  # check every 2s

        threading.Thread(target=_idle_watchdog, daemon=True).start()

        # ──────────────────────────────────────────────────────────────
        # ✅ NEW: helper for staff_bot access (ADD-ONLY)
        # Allow access if current staff is in any of: user_id / owner_id / admin_id / super_admin_id
        # ──────────────────────────────────────────────────────────────
        def _staff_bot_allowed(room, pro_id):
            try:
                pid = str(pro_id)
                allowed_ids = {
                    str(getattr(room, "user_id", "") or ""),
                    str(getattr(room, "owner_id", "") or ""),
                    str(getattr(room, "admin_id", "") or ""),
                    str(getattr(room, "super_admin_id", "") or ""),
                }
                return pid in allowed_ids
            except Exception:
                return False

        # ──────────────────────────────────────────────────────────────
        # ✅ NEW: staff_bot room kind resolver (ADD-ONLY)
        # Robust even if owner_id/admin_id are missing:
        # - master_room if super_admin_id exists
        # - admin_room if admin_id exists and super_admin_id missing
        # - else owner_room
        # ──────────────────────────────────────────────────────────────
        def _staff_bot_room_kind(room):
            try:
                if (getattr(room, "room_type", None) or "support") != "staff_bot":
                    return None
                if getattr(room, "super_admin_id", None):
                    return "master_room"
                if getattr(room, "admin_id", None) and not getattr(room, "super_admin_id", None):
                    return "admin_room"
                return "owner_room"
            except Exception:
                return "owner_room"

        # ──────────────────────────────────────────────────────────────
        # ✅ NEW: role-aware presence for staff_bot (ADD-ONLY)
        # - master_room: higher staff = admin/superadmin
        # - admin_room: higher staff = superadmin
        # - owner_room: no higher staff
        # ──────────────────────────────────────────────────────────────
        def is_higher_staff_present(room, sender_role: str, chat_id: str) -> bool:
            try:
                _ensure_presence_bucket(chat_id)
                roles = PRESENCE.get(chat_id, {}).get("_roles", {})

                kind = _staff_bot_room_kind(room)

                if kind == "master_room":
                    # higher than master = admin or superadmin
                    if sender_role == "master":
                        return (len(roles.get("admin", set())) > 0) or (len(roles.get("superadmin", set())) > 0)
                    return False

                if kind == "admin_room":
                    # higher than admin = superadmin
                    if sender_role == "admin":
                        return len(roles.get("superadmin", set())) > 0
                    return False

                # owner_room: no higher role
                return False
            except Exception:
                return False

        # ──────────────────────────────────────────────────────────────
        # ✅ NEW: who gets bot replies in staff_bot rooms (ADD-ONLY)
        # - master_room: only master gets immediate bot replies
        # - admin_room: only admin gets immediate bot replies
        # - owner_room: only superadmin gets immediate bot replies
        # ──────────────────────────────────────────────────────────────
        def _staff_bot_sender_gets_bot(room, sender_role: str) -> bool:
            try:
                kind = _staff_bot_room_kind(room)
                if kind == "master_room":
                    return sender_role == "master"
                if kind == "admin_room":
                    return sender_role == "admin"
                # owner_room
                return sender_role == "superadmin"
            except Exception:
                return False

        # ──────────────────────────────────────────────────────────────
        # ✅ NEW: engagement rules for staff_bot (ADD-ONLY)
        # - master_room: if admin or superadmin sends a message -> engaged True
        # - admin_room: if superadmin sends a message -> engaged True
        # owner_room: no engage rules
        # ──────────────────────────────────────────────────────────────
        def _staff_bot_apply_engagement(room, chat_id: str, sender_role: str):
            try:
                kind = _staff_bot_room_kind(room)
                if kind == "master_room":
                    if sender_role in ("admin", "superadmin"):
                        STAFF_ENGAGED[chat_id] = True
                elif kind == "admin_room":
                    if sender_role == "superadmin":
                        STAFF_ENGAGED[chat_id] = True
            except Exception:
                pass

        # ──────────────────────────────────────────────────────────────
        # ✅ NEW: derive staff links stub if missing (ADD-ONLY, safe)
        # ──────────────────────────────────────────────────────────────
        try:
            _resolve_staff_links_from_clients  # noqa: F401
        except Exception:
            def _resolve_staff_links_from_clients(role, pro_id):
                return None, None

        # ──────────────────────────────────────────────────────────────
        # ✅ NEW: 2m buffer for STAFF_BOT rooms using staff LLM (ADD-ONLY)
        # IMPORTANT: your existing schedule_bot_reply_after_2m uses generate_bot_reply_lines(),
        # which can return nothing for staff messages. This new scheduler uses superadmin_llm_fallback().
        # ──────────────────────────────────────────────────────────────
        def schedule_staff_bot_reply_after_2m(chat, chat_id: str, user_text: str, pro_id_str: str):
            cancel_pending_bot_reply(chat_id)

            def _fire():
                try:
                    with PENDING_LOCK:
                        PENDING_BOT_TIMERS.pop(chat_id, None)
                except Exception:
                    pass

                # ✅ IMPORTANT: after fallback -> bot becomes instant again until higher staff speaks
                try:
                    STAFF_ENGAGED[chat_id] = False
                except Exception:
                    pass

                try:
                    bot_reply = superadmin_llm_fallback(user_text, pro_id_str)
                except Exception as e:
                    try:
                        logger.error(f"[STAFF BOT 2M ERROR] {e}")
                    except Exception:
                        pass
                    bot_reply = "An internal error occurred while processing this request."

                if not bot_reply:
                    return

                bot_local = ensure_bot_user()
                m_bot = Message(
                    chatroom_id=chat.id,
                    message_by=bot_local.id,
                    message=bot_reply,
                    is_file=False,
                    path=None,
                    is_bot=True,
                ).save()

                room_broadcast(
                    chat_id,
                    {
                        "type": "message",
                        "from": "bot",
                        "message": bot_reply,
                        "message_id": str(m_bot.id),
                        "chat_id": chat_id,
                        "created_time": m_bot.created_time.isoformat(),
                    },
                )

            try:
                with PENDING_LOCK:
                    PENDING_USER_TEXT[chat_id] = user_text
                    t = Timer(120.0, _fire)
                    PENDING_BOT_TIMERS[chat_id] = t
                    t.daemon = True
                    t.start()
            except Exception:
                pass

        # ──────────────────────────────────────────────────────────────
        # ✅ NEW: SUPPORT-ROOM 2m buffer reset logic (ADD-ONLY)
        # This is the missing part for client->superadmin flow.
        # Your timer must reset STAFF_ENGAGED so bot becomes instant again.
        # ──────────────────────────────────────────────────────────────
        # NOTE: If you already replaced schedule_bot_reply_after_2m globally, keep it.
        # This local definition is add-only safety, and will override only inside this ws function.
        def schedule_bot_reply_after_2m(chat, chat_id: str, user_text: str, user_id_str: str = None):
            cancel_pending_bot_reply(chat_id)

            def _fire():
                try:
                    with PENDING_LOCK:
                        PENDING_BOT_TIMERS.pop(chat_id, None)
                except Exception:
                    pass

                # ✅ CRITICAL: after fallback -> bot becomes instant again until staff speaks
                try:
                    STAFF_ENGAGED[chat_id] = False
                except Exception:
                    pass

                try:
                    if user_id_str:
                        reply_lines = generate_bot_reply_lines(user_text, user_id_str)
                    else:
                        reply_lines = generate_bot_reply_lines(user_text)
                except Exception:
                    reply_lines = None

                if not reply_lines:
                    return

                bot_local = ensure_bot_user()
                m_bot = Message(
                    chatroom_id=chat.id,
                    message_by=bot_local.id,
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

            try:
                with PENDING_LOCK:
                    PENDING_USER_TEXT[chat_id] = user_text
                    t = Timer(120.0, _fire)
                    PENDING_BOT_TIMERS[chat_id] = t
                    t.daemon = True
                    t.start()
            except Exception:
                pass

        # ──────────────────────────────────────────────────────────────
        # ✅ NEW: CALL ESCALATION ROUTING HELPERS (ADD-ONLY)
        # Flow:
        # client(user) -> chat.super_admin_id
        # master       -> chat.admin_id
        # admin        -> chat.owner_id
        # ──────────────────────────────────────────────────────────────
        def _resolve_call_target(chat, caller_role: str):
            try:
                r = (caller_role or "").lower()
                if r == "user":
                    return "master", str(getattr(chat, "super_admin_id", "") or "")
                if r == "master":
                    return "admin", str(getattr(chat, "admin_id", "") or "")
                if r == "admin":
                    return "superadmin", str(getattr(chat, "owner_id", "") or "")
                return None, ""
            except Exception:
                return None, ""

        def _sock_send_role(role: str, target_id: str, payload: dict) -> bool:
            try:
                rr = (role or "").lower()
                if rr == "user":
                    return bool(_sock_send_any(USER_SOCKETS, target_id, payload))
                if rr == "admin":
                    return bool(_sock_send_any(ADMIN_SOCKETS, target_id, payload))
                if rr == "superadmin":
                    return bool(_sock_send_any(SUPERADMIN_SOCKETS, target_id, payload))
                # master default
                return bool(_sock_send_any(MASTER_SOCKETS, target_id, payload))
            except Exception:
                return False

        try:
            su = upsert_support_user_from_jwt()
            pro_id = su.user_id
            bot = ensure_bot_user()

            is_user = su.role == USER_ROLE_ID
            is_superadmin = su.role == config.SUPERADMIN_ROLE_ID
            is_admin = su.role == config.ADMIN_ROLE_ID
            is_master = su.role == config.MASTER_ROLE_ID

            # ✅ real role to send to frontend / mark presence
            if is_user:
                conn_role = "user"
            elif is_superadmin:
                conn_role = "superadmin"
            elif is_admin:
                conn_role = "admin"
            elif is_master:
                conn_role = "master"
            else:
                conn_role = "staff"

            # 🧱 keep old 2-bucket logic for "user vs staff" behaviour
            bucket_role = "user" if is_user else "superadmin"

            def _as_oid(v):
                try:
                    return v if isinstance(v, ObjectId) else ObjectId(str(v))
                except Exception:
                    return None

            def _iso(dt):
                try:
                    return dt.isoformat()
                except Exception:
                    return None

            def _get_chatrooms_for_admin(admin_id_oid, page=1, limit=50):
                admin_staff_bot = Chatroom.objects(user_id=admin_id_oid, room_type="staff_bot").first()
                masters = get_masters_for_admin(admin_id_oid)

                chatrooms = []
                if admin_staff_bot:
                    chatrooms.append({
                        "chat_id": str(admin_staff_bot.id),
                        "user_id": str(admin_staff_bot.user_id) if admin_staff_bot.user_id else None,
                        "is_user_active": bool(getattr(admin_staff_bot, "is_user_active", False)),
                        "is_superadmin_active": bool(getattr(admin_staff_bot, "is_superadmin_active", False)),
                        "is_owner_active": bool(getattr(admin_staff_bot, "is_owner_active", False)),
                        "is_admin_active": bool(getattr(admin_staff_bot, "is_admin_active", False)),
                        "updated_time": _iso(getattr(admin_staff_bot, "updated_time", None) or getattr(admin_staff_bot, "created_time", None)),
                        "user": {"name": "", "userName": "", "phone": ""},
                        "room_type": "staff_bot",
                    })

                masters_list = [
                    {
                        "id": m.get("_id"),
                        "name": m.get("name") or "",
                        "userName": m.get("username") or "",
                        "phone": m.get("phone") or "",
                    }
                    for m in masters
                ]

                return {
                    "chatrooms": chatrooms,
                    "masters": masters_list,
                    "total_count": len(chatrooms),
                    "total_pages": 1,
                    "current_page": 1,
                    "limit": limit,
                }

            def _get_chatrooms_for_master(master_id_oid, page=1, limit=50):
                base_fields = (
                    "id",
                    "user_id",
                    "is_user_active",
                    "is_superadmin_active",
                    "is_owner_active",
                    "is_admin_active",
                    "updated_time",
                    "created_time",
                    "room_type",
                    "owner_id",
                    "admin_id",
                    "super_admin_id",
                )

                master_staff_bot = Chatroom.objects(user_id=master_id_oid, room_type="staff_bot").first()
                master_chatrooms = Chatroom.objects(super_admin_id=master_id_oid).only(*base_fields).order_by("-updated_time")

                users = get_users_for_master(master_id_oid)
                user_ids = [_as_oid(u.get("_id")) for u in users if u.get("_id")]
                user_chatrooms = Chatroom.objects(user_id__in=user_ids).only(*base_fields).order_by("-updated_time") if user_ids else Chatroom.objects(id=None)

                # Deduplicate by chat_id
                seen_ids = set()
                all_rooms = []
                for r in list(master_chatrooms) + list(user_chatrooms):
                    rid = str(r.id)
                    if rid not in seen_ids:
                        seen_ids.add(rid)
                        all_rooms.append(r)
                
                if master_staff_bot:
                    bot_id = str(master_staff_bot.id)
                    if bot_id not in seen_ids:
                        all_rooms.insert(0, master_staff_bot)
                        seen_ids.add(bot_id)

                all_rooms = sorted(all_rooms, key=lambda r: getattr(r, "updated_time", getattr(r, "created_time", datetime.min.replace(tzinfo=timezone.utc))), reverse=True)

                total_count = len(all_rooms)
                total_pages = (total_count + limit - 1) // limit if limit > 0 else 1
                page = max(1, min(page, total_pages)) if total_pages > 0 else 1
                skip = (page - 1) * limit
                rooms_list = all_rooms[skip:skip + limit]

                user_oid_set = {
                    _as_oid(getattr(r, "user_id", None))
                    for r in rooms_list
                    if getattr(r, "user_id", None)
                }
                user_oid_list = [oid for oid in user_oid_set if oid is not None]

                meta_by_userid = {}
                if user_oid_list:
                    cursor = support_users_coll.find(
                        {"user_id": {"$in": user_oid_list}},
                        {"_id": 0, "user_id": 1, "name": 1, "userName": 1, "user_name": 1, "phone": 1},
                    )
                    for doc in cursor:
                        key = str(doc.get("user_id"))
                        meta_by_userid[key] = {
                            "name": doc.get("name") or "",
                            "userName": (doc.get("userName") or doc.get("user_name") or ""),
                            "phone": doc.get("phone") or "",
                        }

                chatrooms = [
                    {
                        "chat_id": str(r.id),
                        "user_id": str(r.user_id) if r.user_id else None,
                        "is_user_active": bool(getattr(r, "is_user_active", False)),
                        "is_superadmin_active": bool(getattr(r, "is_superadmin_active", False)),
                        "is_owner_active": bool(getattr(r, "is_owner_active", False)),
                        "is_admin_active": bool(getattr(r, "is_admin_active", False)),
                        "updated_time": _iso(getattr(r, "updated_time", None) or getattr(r, "created_time", None)),
                        "user": meta_by_userid.get(str(getattr(r, "user_id", "")), {"name": "", "userName": "", "phone": ""}),
                        "room_type": (getattr(r, "room_type", None) or "support"),
                    }
                    for r in rooms_list
                ]

                return {
                    "chatrooms": chatrooms,
                    "total_count": total_count,
                    "total_pages": total_pages,
                    "current_page": page,
                    "limit": limit,
                }

            def _search_hierarchy(search_query, caller_role, caller_id):
                from src.config import users as main_users_coll
                import re as regex_module
                search_term = search_query.strip()
                rx = regex_module.compile(regex_module.escape(search_term), regex_module.IGNORECASE)
                search_filter = {
                    "$or": [
                        {"name": rx},
                        {"userName": rx},
                        {"phone": rx},
                    ],
                    "isDemoAccount": {"$ne": True},
                }
                projection = {
                    "_id": 1, "name": 1, "userName": 1, "phone": 1, "role": 1, "parentId": 1,
                }

                def _user_info(doc):
                    uid = doc.get("_id")
                    room = Chatroom.objects(user_id=uid).first()
                    return {
                        "id": str(uid) if uid else None,
                        "name": doc.get("name") or doc.get("userName") or "",
                        "userName": doc.get("userName") or "",
                        "phone": doc.get("phone") or "",
                        "role": str(doc.get("role")) if doc.get("role") else None,
                        "chat_id": str(room.id) if room else None,
                    }

                def _get_user_by_id(uid):
                    doc = main_users_coll.find_one({"_id": uid}, projection)
                    return doc

                if caller_role == "superadmin":
                    admins_raw = list(main_users_coll.find({"role": config.ADMIN_ROLE_ID, "parentId": caller_id, "isDemoAccount": {"$ne": True}}, {"_id": 1}))
                    admin_ids = [a["_id"] for a in admins_raw]
                    if not admin_ids:
                        return {"hierarchy": [], "search_type": "hierarchical", "total_count": 0}

                    masters_raw = list(main_users_coll.find({"role": config.MASTER_ROLE_ID, "parentId": {"$in": admin_ids}, "isDemoAccount": {"$ne": True}}, {"_id": 1, "parentId": 1}))
                    master_ids = [m["_id"] for m in masters_raw]
                    master_to_admin = {m["_id"]: m["parentId"] for m in masters_raw}

                    matched_users = list(main_users_coll.find({**search_filter, "role": config.USER_ROLE_ID, "parentId": {"$in": master_ids}}, projection))
                    matched_masters = list(main_users_coll.find({**search_filter, "role": config.MASTER_ROLE_ID, "parentId": {"$in": admin_ids}}, projection))

                    hierarchy = {}

                    for u in matched_users:
                        master_id = u.get("parentId")
                        if not master_id:
                            continue
                        admin_id = master_to_admin.get(master_id)
                        if not admin_id:
                            continue
                        admin_id_str = str(admin_id)
                        master_id_str = str(master_id)
                        if admin_id_str not in hierarchy:
                            admin_doc = _get_user_by_id(admin_id)
                            hierarchy[admin_id_str] = {
                                "admin": _user_info(admin_doc) if admin_doc else {"id": admin_id_str, "name": "", "userName": "", "phone": ""},
                                "masters": {},
                            }
                        if master_id_str not in hierarchy[admin_id_str]["masters"]:
                            master_doc = _get_user_by_id(master_id)
                            hierarchy[admin_id_str]["masters"][master_id_str] = {
                                "master": _user_info(master_doc) if master_doc else {"id": master_id_str, "name": "", "userName": "", "phone": ""},
                                "clients": [],
                            }
                        hierarchy[admin_id_str]["masters"][master_id_str]["clients"].append(_user_info(u))

                    for m in matched_masters:
                        admin_id = m.get("parentId")
                        if not admin_id:
                            continue
                        admin_id_str = str(admin_id)
                        master_id_str = str(m["_id"])
                        if admin_id_str not in hierarchy:
                            admin_doc = _get_user_by_id(admin_id)
                            hierarchy[admin_id_str] = {
                                "admin": _user_info(admin_doc) if admin_doc else {"id": admin_id_str, "name": "", "userName": "", "phone": ""},
                                "masters": {},
                            }
                        if master_id_str not in hierarchy[admin_id_str]["masters"]:
                            hierarchy[admin_id_str]["masters"][master_id_str] = {
                                "master": _user_info(m),
                                "clients": [],
                            }

                    result = []
                    for admin_id_str, admin_data in hierarchy.items():
                        masters_list = []
                        for master_id_str, master_data in admin_data["masters"].items():
                            masters_list.append({
                                "master": master_data["master"],
                                "clients": master_data["clients"],
                            })
                        result.append({
                            "admin": admin_data["admin"],
                            "masters": masters_list,
                        })

                    total = sum(len(a["masters"]) + sum(len(m["clients"]) for m in a["masters"]) for a in result)
                    return {"hierarchy": result, "search_type": "hierarchical", "total_count": total}

                elif caller_role == "admin":
                    masters_raw = list(main_users_coll.find({"role": config.MASTER_ROLE_ID, "parentId": caller_id, "isDemoAccount": {"$ne": True}}, {"_id": 1}))
                    master_ids = [m["_id"] for m in masters_raw]
                    if not master_ids:
                        return {"hierarchy": [], "search_type": "hierarchical", "total_count": 0}

                    matched_users = list(main_users_coll.find({**search_filter, "role": config.USER_ROLE_ID, "parentId": {"$in": master_ids}}, projection))
                    matched_masters = list(main_users_coll.find({**search_filter, "role": config.MASTER_ROLE_ID, "parentId": caller_id}, projection))

                    hierarchy = {}

                    for u in matched_users:
                        master_id = u.get("parentId")
                        if not master_id:
                            continue
                        master_id_str = str(master_id)
                        if master_id_str not in hierarchy:
                            master_doc = _get_user_by_id(master_id)
                            hierarchy[master_id_str] = {
                                "master": _user_info(master_doc) if master_doc else {"id": master_id_str, "name": "", "userName": "", "phone": ""},
                                "clients": [],
                            }
                        hierarchy[master_id_str]["clients"].append(_user_info(u))

                    for m in matched_masters:
                        master_id_str = str(m["_id"])
                        if master_id_str not in hierarchy:
                            hierarchy[master_id_str] = {
                                "master": _user_info(m),
                                "clients": [],
                            }

                    result = [{"master": v["master"], "clients": v["clients"]} for v in hierarchy.values()]
                    total = len(result) + sum(len(m["clients"]) for m in result)
                    return {"hierarchy": result, "search_type": "hierarchical", "total_count": total}

                elif caller_role == "master":
                    matched_users = list(main_users_coll.find({**search_filter, "role": config.USER_ROLE_ID, "parentId": caller_id}, projection))
                    result = [_user_info(u) for u in matched_users]
                    return {"hierarchy": result, "search_type": "hierarchical", "total_count": len(result)}

                return {"hierarchy": [], "search_type": "hierarchical", "total_count": 0}

            def _get_chatrooms_paginated(page=1, search_query=None, limit=50, selected_admin_id=None, selected_master_id=None):
                base_fields = (
                    "id",
                    "user_id",
                    "is_user_active",
                    "is_superadmin_active",
                    "is_owner_active",
                    "is_admin_active",
                    "updated_time",
                    "created_time",
                    "room_type",
                    "owner_id",
                    "admin_id",
                    "super_admin_id",
                )

                from mongoengine.queryset.visitor import Q

                if selected_admin_id:
                    return _get_chatrooms_for_admin(_as_oid(selected_admin_id), page, limit)

                if selected_master_id:
                    return _get_chatrooms_for_master(_as_oid(selected_master_id), page, limit)

                if search_query and search_query.strip():
                    caller_role = "superadmin" if is_superadmin else ("admin" if is_admin else ("master" if is_master else "user"))
                    hierarchy_result = _search_hierarchy(search_query, caller_role, pro_id)
                    return {
                        "chatrooms": [],
                        "hierarchy": hierarchy_result.get("hierarchy", []),
                        "search_type": "hierarchical",
                        "total_count": hierarchy_result.get("total_count", 0),
                        "total_pages": 1,
                        "current_page": 1,
                        "limit": limit,
                    }

                if is_superadmin:
                    try:
                        rooms_query = Chatroom.objects(
                            Q(owner_id=pro_id)
                            | Q(super_admin_id=pro_id)
                            | Q(user_id=pro_id, room_type="staff_bot")
                        )
                    except Exception:
                        try:
                            rooms_query = Chatroom.objects(owner_id=pro_id)
                            if rooms_query.count() == 0:
                                rooms_query = Chatroom.objects(super_admin_id=pro_id)
                        except Exception:
                            rooms_query = Chatroom.objects(id=None)
                elif is_admin:
                    rooms_query = Chatroom.objects(admin_id=pro_id)
                elif is_master:
                    rooms_query = Chatroom.objects(super_admin_id=pro_id)
                else:
                    rooms_query = Chatroom.objects(id=None)

                rooms_query = rooms_query.only(*base_fields).order_by("-updated_time")

                total_count = rooms_query.count()
                total_pages = (total_count + limit - 1) // limit if limit > 0 else 1
                page = max(1, min(page, total_pages)) if total_pages > 0 else 1

                skip = (page - 1) * limit
                rooms_list = list(rooms_query.skip(skip).limit(limit))

                user_oid_set = {
                    _as_oid(getattr(r, "user_id", None))
                    for r in rooms_list
                    if getattr(r, "user_id", None)
                }
                user_oid_list = [oid for oid in user_oid_set if oid is not None]

                meta_by_userid = {}
                if user_oid_list:
                    cursor = support_users_coll.find(
                        {"user_id": {"$in": user_oid_list}},
                        {"_id": 0, "user_id": 1, "name": 1, "userName": 1, "user_name": 1, "phone": 1},
                    )
                    for doc in cursor:
                        key = str(doc.get("user_id"))
                        meta_by_userid[key] = {
                            "name": doc.get("name") or "",
                            "userName": (doc.get("userName") or doc.get("user_name") or ""),
                            "phone": doc.get("phone") or "",
                        }

                chatrooms = [
                    {
                        "chat_id": str(r.id),
                        "user_id": str(r.user_id) if r.user_id else None,
                        "is_user_active": bool(getattr(r, "is_user_active", False)),
                        "is_superadmin_active": bool(getattr(r, "is_superadmin_active", False)),
                        "is_owner_active": bool(getattr(r, "is_owner_active", False)),
                        "is_admin_active": bool(getattr(r, "is_admin_active", False)),
                        "updated_time": _iso(getattr(r, "updated_time", None) or getattr(r, "created_time", None)),
                        "user": meta_by_userid.get(str(getattr(r, "user_id", "")), {"name": "", "userName": "", "phone": ""}),
                        "room_type": (getattr(r, "room_type", None) or "support"),
                    }
                    for r in rooms_list
                ]

                return {
                    "chatrooms": chatrooms,
                    "total_count": total_count,
                    "total_pages": total_pages,
                    "current_page": page,
                    "limit": limit,
                }

            # ──────────────────────────────────────────────────────────────
            # ✅ NEW: Global socket registration for user<->master call popup
            # ──────────────────────────────────────────────────────────────
            try:
                if is_user:
                    _sock_add(USER_SOCKETS, su.user_id, ws)
                if is_superadmin or is_master:
                    _sock_add(MASTER_SOCKETS, pro_id, ws)
            except Exception:
                pass

            # ──────────────────────────────────────────────────────────────
            # ✅ NEW: register admin/superadmin buckets for call escalation (ADD-ONLY)
            # ──────────────────────────────────────────────────────────────
            try:
                if is_admin:
                    try:
                        _sock_add(ADMIN_SOCKETS, pro_id, ws)
                    except Exception:
                        pass
                if is_superadmin:
                    try:
                        _sock_add(SUPERADMIN_SOCKETS, pro_id, ws)
                    except Exception:
                        pass
            except Exception:
                pass

            qs_chatroom_id = request.args.get("chatroom_id")
            qs_child_user_id = request.args.get("child_user_id")

            # ✅ NEW: ensure superadmin personal bot room exists (created once)
            try:
                if is_superadmin or is_admin or is_master:
                    ensured = ensure_staff_bot_room(pro_id)
                    try:
                        if ensured:
                            staff_owner_id = getattr(su, "owner_id", None)
                            staff_admin_id = getattr(su, "admin_id", None)

                            if not staff_owner_id or (is_master and not staff_admin_id):
                                derived_owner_id, derived_admin_id = _resolve_staff_links_from_clients(su.role, pro_id)
                                if not staff_owner_id:
                                    staff_owner_id = derived_owner_id
                                if is_master and not staff_admin_id:
                                    staff_admin_id = derived_admin_id

                            try:
                                if (getattr(ensured, "room_type", None) or "") != "staff_bot":
                                    ensured.room_type = "staff_bot"
                            except Exception:
                                pass

                            if is_superadmin:
                                try:
                                    if not getattr(ensured, "owner_id", None):
                                        ensured.owner_id = pro_id
                                except Exception:
                                    pass

                            if is_admin:
                                try:
                                    ensured.admin_id = pro_id
                                except Exception:
                                    pass
                                try:
                                    if staff_owner_id:
                                        ensured.owner_id = staff_owner_id
                                except Exception:
                                    pass

                            if is_master:
                                try:
                                    ensured.super_admin_id = pro_id
                                except Exception:
                                    pass
                                try:
                                    if staff_owner_id:
                                        ensured.owner_id = staff_owner_id
                                except Exception:
                                    pass
                                try:
                                    if staff_admin_id:
                                        ensured.admin_id = staff_admin_id
                                except Exception:
                                    pass

                            try:
                                ensured.save()
                            except Exception:
                                pass
                    except Exception:
                        pass
            except Exception as e:
                print("ensure_staff_bot_room failed:", repr(e))

            if is_user:
                chat = ensure_chatroom_for_pro(pro_id)
                chat_id = str(chat.id)

                # ✅ Ensure engaged state exists; default False
                STAFF_ENGAGED.setdefault(chat_id, False)

                conn_role = "user"
                room_add(chat_id, ws)
                mark_role_join(chat, conn_role, ws)
                ws.send(json.dumps({"type": "joined", "chat_id": chat_id, "role": conn_role}))
                last_activity["ts"] = time.time()
            else:
                # --- open a specific chatroom by id ---
                if qs_chatroom_id:
                    picked = Chatroom.objects(id=_oid(qs_chatroom_id)).first()
                    if not picked:
                        ws.send(json.dumps({"type": "error", "error": "chatroom_not_found"}))
                        last_activity["ts"] = time.time()
                        return

                    picked_room_type = (getattr(picked, "room_type", None) or "support")
                    is_staff_bot_room = (picked_room_type == "staff_bot")

                    if is_staff_bot_room:
                        if not _staff_bot_allowed(picked, pro_id):
                            ws.send(json.dumps({"type": "error", "error": "forbidden_chatroom"}))
                            last_activity["ts"] = time.time()
                            return
                    else:
                        if is_superadmin:
                            if picked.owner_id and picked.owner_id != pro_id:
                                ws.send(json.dumps({"type": "error", "error": "forbidden_chatroom"}))
                                last_activity["ts"] = time.time()
                                return
                            if (not picked.owner_id and picked.super_admin_id and picked.super_admin_id != pro_id):
                                ws.send(json.dumps({"type": "error", "error": "forbidden_chatroom"}))
                                last_activity["ts"] = time.time()
                                return
                        else:
                            if is_admin:
                                if picked.admin_id and picked.admin_id != pro_id:
                                    ws.send(json.dumps({"type": "error", "error": "forbidden_chatroom"}))
                                    last_activity["ts"] = time.time()
                                    return
                            elif is_master:
                                if picked.super_admin_id and picked.super_admin_id != pro_id:
                                    ws.send(json.dumps({"type": "error", "error": "forbidden_chatroom"}))
                                    last_activity["ts"] = time.time()
                                    return
                            else:
                                ws.send(json.dumps({"type": "error", "error": "forbidden_chatroom"}))
                                last_activity["ts"] = time.time()
                                return

                    chat = picked
                    chat_id = str(chat.id)

                    STAFF_ENGAGED.setdefault(chat_id, False)

                    room_add(chat_id, ws)
                    mark_role_join(chat, conn_role, ws)
                    ws.send(json.dumps({"type": "joined", "chat_id": chat_id, "role": conn_role}))
                    last_activity["ts"] = time.time()

                # --- open (or create) by child_user_id ---
                elif qs_child_user_id:
                    chat = ensure_chatroom_for_pro(_oid(qs_child_user_id))

                    if is_superadmin:
                        if chat.owner_id and chat.owner_id != pro_id:
                            ws.send(json.dumps({"type": "error", "error": "forbidden_child_room"}))
                            last_activity["ts"] = time.time()
                            return
                        if (not chat.owner_id and chat.super_admin_id and chat.super_admin_id != pro_id):
                            ws.send(json.dumps({"type": "error", "error": "forbidden_child_room"}))
                            last_activity["ts"] = time.time()
                            return
                    else:
                        if is_admin:
                            if chat.admin_id and chat.admin_id != pro_id:
                                ws.send(json.dumps({"type": "error", "error": "forbidden_child_room"}))
                                last_activity["ts"] = time.time()
                                return
                        elif is_master:
                            if chat.super_admin_id and chat.super_admin_id != pro_id:
                                ws.send(json.dumps({"type": "error", "error": "forbidden_child_room"}))
                                last_activity["ts"] = time.time()
                                return
                        else:
                            ws.send(json.dumps({"type": "error", "error": "forbidden_child_room"}))
                            last_activity["ts"] = time.time()
                            return

                    chat_id = str(chat.id)

                    STAFF_ENGAGED.setdefault(chat_id, False)

                    room_add(chat_id, ws)
                    mark_role_join(chat, conn_role, ws)
                    ws.send(json.dumps({"type": "joined", "chat_id": chat_id, "role": conn_role}))
                    last_activity["ts"] = time.time()

                # --- list rooms for selection (no params) ---
                else:
                    if is_superadmin:
                        admins = get_admins_for_superadmin(pro_id)
                        superadmin_staff_bot = Chatroom.objects(user_id=pro_id, room_type="staff_bot").first()
                        
                        admins_list = [
                            {
                                "id": a.get("_id"),
                                "name": a.get("name") or "",
                                "userName": a.get("username") or "",
                                "phone": a.get("phone") or "",
                            }
                            for a in admins
                        ]

                        chatrooms_list = []
                        if superadmin_staff_bot:
                            chatrooms_list.append({
                                "chat_id": str(superadmin_staff_bot.id),
                                "user_id": str(superadmin_staff_bot.user_id) if superadmin_staff_bot.user_id else None,
                                "is_user_active": bool(getattr(superadmin_staff_bot, "is_user_active", False)),
                                "is_superadmin_active": bool(getattr(superadmin_staff_bot, "is_superadmin_active", False)),
                                "is_owner_active": bool(getattr(superadmin_staff_bot, "is_owner_active", False)),
                                "is_admin_active": bool(getattr(superadmin_staff_bot, "is_admin_active", False)),
                                "updated_time": _iso(getattr(superadmin_staff_bot, "updated_time", None) or getattr(superadmin_staff_bot, "created_time", None)),
                                "user": {"name": "", "userName": "", "phone": ""},
                                "room_type": "staff_bot",
                            })

                        payload = {
                            "type": "joined",
                            "role": conn_role,
                            "needs_selection": True,
                            "hierarchy": {
                                "type": "superadmin",
                                "admins": admins_list,
                            },
                            "chatrooms": chatrooms_list,
                        }
                    elif is_admin:
                        masters = get_masters_for_admin(pro_id)
                        admin_staff_bot = Chatroom.objects(user_id=pro_id, room_type="staff_bot").first()

                        masters_list = [
                            {
                                "id": m.get("_id"),
                                "name": m.get("name") or "",
                                "userName": m.get("username") or "",
                                "phone": m.get("phone") or "",
                            }
                            for m in masters
                        ]

                        chatrooms_list = []
                        if admin_staff_bot:
                            chatrooms_list.append({
                                "chat_id": str(admin_staff_bot.id),
                                "user_id": str(admin_staff_bot.user_id) if admin_staff_bot.user_id else None,
                                "is_user_active": bool(getattr(admin_staff_bot, "is_user_active", False)),
                                "is_superadmin_active": bool(getattr(admin_staff_bot, "is_superadmin_active", False)),
                                "is_owner_active": bool(getattr(admin_staff_bot, "is_owner_active", False)),
                                "is_admin_active": bool(getattr(admin_staff_bot, "is_admin_active", False)),
                                "updated_time": _iso(getattr(admin_staff_bot, "updated_time", None) or getattr(admin_staff_bot, "created_time", None)),
                                "user": {"name": "", "userName": "", "phone": ""},
                                "room_type": "staff_bot",
                            })

                        payload = {
                            "type": "joined",
                            "role": conn_role,
                            "needs_selection": True,
                            "hierarchy": {
                                "type": "admin",
                                "masters": masters_list,
                            },
                            "chatrooms": chatrooms_list,
                        }
                    elif is_master:
                        result = _get_chatrooms_for_master(pro_id, page=1, limit=50)
                        payload = {
                            "type": "joined",
                            "role": conn_role,
                            "needs_selection": True,
                            "chatrooms": result["chatrooms"],
                            "pagination": {
                                "total_count": result["total_count"],
                                "total_pages": result["total_pages"],
                                "current_page": result["current_page"],
                                "limit": result["limit"],
                            },
                        }
                    else:
                        result = _get_chatrooms_paginated(page=1, search_query=None, limit=50)
                        payload = {
                            "type": "joined",
                            "role": conn_role,
                            "needs_selection": True,
                            "chatrooms": result["chatrooms"],
                            "pagination": {
                                "total_count": result["total_count"],
                                "total_pages": result["total_pages"],
                                "current_page": result["current_page"],
                                "limit": result["limit"],
                            },
                        }
                    ws.send(json.dumps(payload))
                    last_activity["ts"] = time.time()

            # ── main WS loop ───────────────────────────────────────────────
            while True:
                raw = ws.receive()  # (blocking)
                if raw is None:
                    break

                last_activity["ts"] = time.time()  # ✅ touch on any inbound

                try:
                    data = json.loads(raw)
                except Exception:
                    ws.send(json.dumps({"type": "error", "error": "invalid_json"}))
                    last_activity["ts"] = time.time()
                    continue

                t = (data.get("type") or "").lower()

                if t == "ping":
                    ws.send(json.dumps({"type": "pong"}))
                    last_activity["ts"] = time.time()
                    continue

                if t == "list_chatrooms" and bucket_role != "user":
                    try:
                        page = int(data.get("page", 1))
                        search_query = data.get("search", "").strip() or None
                        limit = int(data.get("limit", 50))

                        if page < 1:
                            page = 1
                        if limit < 1 or limit > 100:
                            limit = 50

                        result = _get_chatrooms_paginated(page=page, search_query=search_query, limit=limit)

                        if result.get("search_type") == "hierarchical":
                            payload = {
                                "type": "chatrooms_list",
                                "chatrooms": result.get("chatrooms", []),
                                "hierarchy": result.get("hierarchy", []),
                                "search_type": "hierarchical",
                                "pagination": {
                                    "total_count": result["total_count"],
                                    "total_pages": result["total_pages"],
                                    "current_page": result["current_page"],
                                    "limit": result["limit"],
                                    "search": search_query if search_query else None,
                                },
                            }
                        else:
                            payload = {
                                "type": "chatrooms_list",
                                "chatrooms": result["chatrooms"],
                                "pagination": {
                                    "total_count": result["total_count"],
                                    "total_pages": result["total_pages"],
                                    "current_page": result["current_page"],
                                    "limit": result["limit"],
                                    "search": search_query if search_query else None,
                                },
                            }
                        ws.send(json.dumps(payload))
                        last_activity["ts"] = time.time()
                        continue
                    except Exception as e:
                        ws.send(json.dumps({"type": "error", "error": f"list_chatrooms_failed: {str(e)}"}))
                        last_activity["ts"] = time.time()
                        continue

                if t == "select_chatroom" and bucket_role != "user":
                    target_id = data.get("chat_id")
                    if not target_id:
                        ws.send(json.dumps({"type": "error", "error": "chat_id_required"}))
                        last_activity["ts"] = time.time()
                        continue
                    picked = Chatroom.objects(id=_oid(target_id)).first()
                    if not picked:
                        ws.send(json.dumps({"type": "error", "error": "chatroom_not_found"}))
                        last_activity["ts"] = time.time()
                        continue

                    picked_room_type = (getattr(picked, "room_type", None) or "support")
                    is_staff_bot_room = (picked_room_type == "staff_bot")

                    if is_staff_bot_room:
                        if not _staff_bot_allowed(picked, pro_id):
                            ws.send(json.dumps({"type": "error", "error": "forbidden_chatroom"}))
                            last_activity["ts"] = time.time()
                            return
                    else:
                        if is_superadmin:
                            if picked.owner_id and picked.owner_id != pro_id:
                                ws.send(json.dumps({"type": "error", "error": "forbidden_chatroom"}))
                                last_activity["ts"] = time.time()
                                return
                            if (not picked.owner_id and picked.super_admin_id and picked.super_admin_id != pro_id):
                                ws.send(json.dumps({"type": "error", "error": "forbidden_chatroom"}))
                                last_activity["ts"] = time.time()
                                return
                        else:
                            if is_admin:
                                if picked.admin_id and picked.admin_id != pro_id:
                                    ws.send(json.dumps({"type": "error", "error": "forbidden_chatroom"}))
                                    last_activity["ts"] = time.time()
                                    return
                            elif is_master:
                                if picked.super_admin_id and picked.super_admin_id != pro_id:
                                    ws.send(json.dumps({"type": "error", "error": "forbidden_chatroom"}))
                                    last_activity["ts"] = time.time()
                                    return
                            else:
                                ws.send(json.dumps({"type": "error", "error": "forbidden_chatroom"}))
                                last_activity["ts"] = time.time()
                                return

                    if chat and chat_id:
                        try:
                            mark_role_leave(chat, conn_role, ws)
                        finally:
                            room_remove(chat_id, ws)

                    chat = picked
                    chat_id = str(chat.id)

                    STAFF_ENGAGED.setdefault(chat_id, False)

                    room_add(chat_id, ws)
                    mark_role_join(chat, conn_role, ws)
                    ws.send(json.dumps({"type": "selected", "chat_id": chat_id, "role": conn_role}))
                    last_activity["ts"] = time.time()
                    continue

                if t == "select_admin" and is_superadmin:
                    admin_id = data.get("admin_id")
                    if not admin_id:
                        ws.send(json.dumps({"type": "error", "error": "admin_id_required"}))
                        last_activity["ts"] = time.time()
                        continue

                    admin_oid = _as_oid(admin_id)
                    if not admin_oid:
                        ws.send(json.dumps({"type": "error", "error": "invalid_admin_id"}))
                        last_activity["ts"] = time.time()
                        continue

                    admins = get_admins_for_superadmin(pro_id)
                    if not any(_as_oid(a.get("_id")) == admin_oid for a in admins):
                        ws.send(json.dumps({"type": "error", "error": "forbidden_admin"}))
                        last_activity["ts"] = time.time()
                        continue

                    result = _get_chatrooms_for_admin(admin_oid, page=1, limit=50)
                    payload = {
                        "type": "admin_selected",
                        "admin_id": admin_id,
                        "chatrooms": result["chatrooms"],
                        "masters": result.get("masters", []),
                        "pagination": {
                            "total_count": result["total_count"],
                            "total_pages": result["total_pages"],
                            "current_page": result["current_page"],
                            "limit": result["limit"],
                        },
                    }
                    ws.send(json.dumps(payload))
                    last_activity["ts"] = time.time()
                    continue

                if t == "select_master" and (is_superadmin or is_admin):
                    master_id = data.get("master_id")
                    if not master_id:
                        ws.send(json.dumps({"type": "error", "error": "master_id_required"}))
                        last_activity["ts"] = time.time()
                        continue

                    master_oid = _as_oid(master_id)
                    if not master_oid:
                        ws.send(json.dumps({"type": "error", "error": "invalid_master_id"}))
                        last_activity["ts"] = time.time()
                        continue

                    if is_superadmin:
                        admin_id = data.get("admin_id")
                        if admin_id:
                            admin_oid = _as_oid(admin_id)
                            if admin_oid:
                                masters = get_masters_for_admin(admin_oid)
                                if not any(_as_oid(m.get("_id")) == master_oid for m in masters):
                                    ws.send(json.dumps({"type": "error", "error": "forbidden_master"}))
                                    last_activity["ts"] = time.time()
                                    continue
                    elif is_admin:
                        masters = get_masters_for_admin(pro_id)
                        if not any(_as_oid(m.get("_id")) == master_oid for m in masters):
                            ws.send(json.dumps({"type": "error", "error": "forbidden_master"}))
                            last_activity["ts"] = time.time()
                            continue

                    result = _get_chatrooms_for_master(master_oid, page=1, limit=50)
                    payload = {
                        "type": "master_selected",
                        "master_id": master_id,
                        "chatrooms": result["chatrooms"],
                        "pagination": {
                            "total_count": result["total_count"],
                            "total_pages": result["total_pages"],
                            "current_page": result["current_page"],
                            "limit": result["limit"],
                        },
                    }
                    ws.send(json.dumps(payload))
                    last_activity["ts"] = time.time()
                    continue

                # ──────────────────────────────────────────────────────────────
                # ✅ UPDATED: CALL SIGNALING LOGIC (ADD-ONLY / UPDATED BLOCK)
                # Flow:
                # - user   -> chat.super_admin_id (master)
                # - master -> chat.admin_id (admin)
                # - admin  -> chat.owner_id (superadmin)
                # 
                # IMPORTANT: Call signaling is sent to ANY connected socket for the target,
                # regardless of which chatroom they're currently viewing. This allows masters
                # to receive calls even when not in that specific chatroom.
                # 
                # NOTE: Only call.start requires a selected chatroom. Other call operations
                # (accept, reject, end, offer, answer, ice) can work without a selected chatroom
                # because they use call_id to look up the call info from ACTIVE_CALLS.
                #
                # LIVE vs LOCAL: If call.accepted reaches user locally but not in live, the usual
                # cause is multiple app workers: USER_SOCKETS/ACTIVE_CALLS are per-process. The
                # user may be connected to worker A and the master to worker B; when master
                # accepts on B, B has no socket for the user. Fix: run a single process for /ws,
                # or use sticky sessions so both clients hit the same worker, or use Redis pub/sub
                # to forward call.accepted to the worker that holds the user's socket.
                # ──────────────────────────────────────────────────────────────
                if t.startswith("call."):
                    print("CALL BRANCH HIT:", t, "chat_id:", chat_id, "conn_role:", conn_role)

                    if t == "call.start":
                        # ✅ call.start requires a selected chatroom
                        if not chat or not chat_id:
                            ws.send(json.dumps({"type": "call.error", "error": "no_chat_selected"}))
                            last_activity["ts"] = time.time()
                            continue

                        # keep legacy name (do not remove)
                        master_id = str(getattr(chat, "super_admin_id", "") or "")
                        # ✅ allow user/master/admin to initiate call
                        if conn_role not in ("user", "master", "admin"):
                            ws.send(json.dumps({"type": "call.error", "error": "forbidden_role_for_call"}))
                            last_activity["ts"] = time.time()
                            continue

                        target_role, target_id = _resolve_call_target(chat, conn_role)
                        if not target_id:
                            ws.send(json.dumps({"type": "call.error", "error": "no_target_assigned"}))
                            last_activity["ts"] = time.time()
                            continue

                        call_id = uuid.uuid4().hex
                        ACTIVE_CALLS[call_id] = {
                            "chat_id": chat_id,
                            "user_id": str(getattr(chat, "user_id", "") or str(pro_id)),
                            # keep legacy key, now means "target id"
                            "master_id": target_id,
                            "state": "ringing",
                            # ✅ new metadata
                            "target_role": target_role,
                            "target_id": target_id,
                            "caller_role": conn_role,
                            "caller_id": str(pro_id),
                        }

                        # ✅ Send call.incoming to target - works even if target is not in this chatroom
                        # _sock_send_role uses _sock_send_any which sends to ANY connected socket
                        # for that user_id, regardless of which chatroom they're viewing
                        ok = _sock_send_role(
                            target_role,
                            target_id,
                            {
                                "type": "call.incoming",
                                "call_id": call_id,
                                "chat_id": chat_id,
                                "from_user_id": str(getattr(chat, "user_id", "") or ""),
                                "from_role": conn_role,
                                "to_role": target_role,
                            },
                        )

                        if not ok:
                            ACTIVE_CALLS.pop(call_id, None)
                            ws.send(json.dumps({"type": "call.error", "error": "target_offline"}))
                            last_activity["ts"] = time.time()
                            continue

                        ws.send(json.dumps({"type": "call.ringing", "call_id": call_id, "chat_id": chat_id}))
                        last_activity["ts"] = time.time()
                        continue

                    if t == "call.accept":
                        call_id = (data.get("call_id") or "").strip()
                        c = ACTIVE_CALLS.get(call_id)
                        if not c:
                            ws.send(json.dumps({"type": "call.error", "error": "call_not_found"}))
                            last_activity["ts"] = time.time()
                            continue

                        if str(pro_id) != str(c.get("target_id") or c.get("master_id") or ""):
                            ws.send(json.dumps({"type": "call.error", "error": "forbidden"}))
                            last_activity["ts"] = time.time()
                            continue

                        c["state"] = "accepted"

                        caller_role = (c.get("caller_role") or "user").strip().lower()
                        caller_id = str(c.get("caller_id") or c.get("user_id") or "").strip()
                        if not caller_id:
                            logger.warning(f"[CALL.ACCEPT] caller_id empty for call_id={call_id}, c={c}")
                            ws.send(json.dumps({"type": "call.error", "error": "caller_id_missing"}))
                            last_activity["ts"] = time.time()
                            continue

                        payload_accepted = {"type": "call.accepted", "call_id": call_id, "chat_id": c["chat_id"]}
                        logger.info(f"[CALL.ACCEPT] Sending call.accepted to caller_id={caller_id!r}, caller_role={caller_role!r}")
                        # Send to ALL sockets for this user so every client (Postman, browser, etc.) receives it
                        if caller_role == "user":
                            sent_count = _sock_send_all(USER_SOCKETS, caller_id, payload_accepted)
                            ok_sent = sent_count > 0
                            if ok_sent:
                                logger.info(f"[CALL.ACCEPT] call.accepted delivered to {sent_count} socket(s) for caller_id={caller_id}")
                        else:
                            ok_sent = _sock_send_role(caller_role, caller_id, payload_accepted)
                            if ok_sent:
                                logger.info(f"[CALL.ACCEPT] call.accepted delivered to caller_id={caller_id}")
                        if not ok_sent:
                            logger.warning(
                                f"[CALL.ACCEPT] call.accepted NOT delivered to user (caller_id={caller_id}). "
                                "User may be on another server instance (multi-worker). Ensure sticky sessions or single WS process."
                            )

                        ws.send(json.dumps({"type": "call.accepted_ack", "call_id": call_id}))
                        last_activity["ts"] = time.time()
                        continue

                    if t == "call.reject":
                        call_id = (data.get("call_id") or "").strip()
                        c = ACTIVE_CALLS.pop(call_id, None)
                        if not c:
                            ws.send(json.dumps({"type": "call.error", "error": "call_not_found"}))
                            last_activity["ts"] = time.time()
                            continue

                        if str(pro_id) != str(c.get("target_id") or c.get("master_id") or ""):
                            ws.send(json.dumps({"type": "call.error", "error": "forbidden"}))
                            last_activity["ts"] = time.time()
                            continue

                        caller_role = c.get("caller_role", "user")
                        caller_id = c.get("caller_id", c.get("user_id", ""))
                        _sock_send_role(
                            caller_role,
                            caller_id,
                            {"type": "call.rejected", "call_id": call_id, "chat_id": c["chat_id"]},
                        )

                        ws.send(json.dumps({"type": "call.rejected_ack", "call_id": call_id}))
                        last_activity["ts"] = time.time()
                        continue

                    if t in ("call.offer", "call.answer", "call.ice"):
                        call_id = (data.get("call_id") or "").strip()
                        c = ACTIVE_CALLS.get(call_id)
                        if not c:
                            ws.send(json.dumps({"type": "call.error", "error": "call_not_found"}))
                            last_activity["ts"] = time.time()
                            continue

                        payload = {
                            "type": t,
                            "call_id": call_id,
                            "chat_id": c["chat_id"],
                            "from_role": conn_role,
                        }

                        if t in ("call.offer", "call.answer"):
                            payload["sdp"] = data.get("sdp")
                            if not payload["sdp"]:
                                ws.send(json.dumps({"type": "call.error", "error": "sdp_required"}))
                                last_activity["ts"] = time.time()
                                continue
                        else:
                            payload["candidate"] = data.get("candidate")
                            if not payload["candidate"]:
                                ws.send(json.dumps({"type": "call.error", "error": "candidate_required"}))
                                last_activity["ts"] = time.time()
                                continue

                        # ✅ routing: determine peer based on who is sending
                        caller_id = str(c.get("caller_id", "") or c.get("user_id", ""))
                        target_id = str(c.get("target_id", "") or c.get("master_id", ""))
                        
                        if str(pro_id) == caller_id:
                            # Current sender is the caller, send to target
                            ok = _sock_send_role(
                                str(c.get("target_role") or "master"),
                                target_id,
                                payload,
                            )
                        else:
                            # Current sender is the target, send to caller
                            caller_role = c.get("caller_role", "user")
                            ok = _sock_send_role(
                                caller_role,
                                caller_id,
                                payload,
                            )

                        if not ok:
                            ws.send(json.dumps({"type": "call.error", "error": "peer_offline"}))
                        last_activity["ts"] = time.time()
                        continue

                    if t == "call.end":
                        call_id = (data.get("call_id") or "").strip()
                        c = ACTIVE_CALLS.pop(call_id, None)
                        if c:
                            caller_role = c.get("caller_role", "user")
                            caller_id = c.get("caller_id", c.get("user_id", ""))
                            _sock_send_role(
                                caller_role,
                                caller_id,
                                {"type": "call.ended", "call_id": call_id, "chat_id": c["chat_id"]},
                            )
                            _sock_send_role(
                                str(c.get("target_role") or "master"),
                                str(c.get("target_id") or c.get("master_id") or ""),
                                {"type": "call.ended", "call_id": call_id, "chat_id": c["chat_id"]},
                            )
                        last_activity["ts"] = time.time()
                        continue

                    ws.send(json.dumps({"type": "call.error", "error": "unknown_call_type"}))
                    last_activity["ts"] = time.time()
                    continue

                # ──────────────────────────────────────────────────────────────
                # ✅ SIMPLE CALL SIGNALING (for direct WebRTC via chatroom)
                # Handles: call_offer, call_answer, call_ice_candidate, call_end, call_accept, call_reject
                # These are broadcasted to all sockets in the same chatroom
                # ──────────────────────────────────────────────────────────────
                if t == "call_offer":
                    if not chat or not chat_id:
                        ws.send(json.dumps({"type": "error", "error": "no_chat_selected"}))
                        last_activity["ts"] = time.time()
                        continue
                    room_broadcast(chat_id, {
                        "type": "call_offer",
                        "from": conn_role,
                        "chat_id": chat_id,
                        "offer": data.get("offer"),
                    })
                    room_broadcast(chat_id, {
                        "type": "call_ringing",
                        "from": conn_role,
                        "chat_id": chat_id,
                    })
                    last_activity["ts"] = time.time()
                    continue

                if t == "call_answer":
                    if not chat or not chat_id:
                        ws.send(json.dumps({"type": "error", "error": "no_chat_selected"}))
                        last_activity["ts"] = time.time()
                        continue
                    room_broadcast(chat_id, {
                        "type": "call_answer",
                        "from": conn_role,
                        "chat_id": chat_id,
                        "answer": data.get("answer"),
                    })
                    last_activity["ts"] = time.time()
                    continue

                if t == "call_ice_candidate":
                    if not chat or not chat_id:
                        ws.send(json.dumps({"type": "error", "error": "no_chat_selected"}))
                        last_activity["ts"] = time.time()
                        continue
                    room_broadcast(chat_id, {
                        "type": "call_ice_candidate",
                        "from": conn_role,
                        "chat_id": chat_id,
                        "candidate": data.get("candidate"),
                    })
                    last_activity["ts"] = time.time()
                    continue

                if t == "call_end":
                    if not chat or not chat_id:
                        ws.send(json.dumps({"type": "error", "error": "no_chat_selected"}))
                        last_activity["ts"] = time.time()
                        continue
                    room_broadcast(chat_id, {
                        "type": "call_end",
                        "from": conn_role,
                        "chat_id": chat_id,
                    })
                    last_activity["ts"] = time.time()
                    continue

                if t == "call_accept":
                    if not chat or not chat_id:
                        ws.send(json.dumps({"type": "error", "error": "no_chat_selected"}))
                        last_activity["ts"] = time.time()
                        continue
                    room_broadcast(chat_id, {
                        "type": "call_accepted",
                        "from": conn_role,
                        "chat_id": chat_id,
                    })
                    last_activity["ts"] = time.time()
                    continue

                if t == "message":
                    if not chat or not chat_id:
                        ws.send(json.dumps({"type": "error", "error": "no_chat_selected"}))
                        last_activity["ts"] = time.time()
                        continue

                    text = (data.get("text") or "").strip()
                    if not text:
                        ws.send(json.dumps({"type": "error", "error": "empty_message"}))
                        last_activity["ts"] = time.time()
                        continue

                    room_type = (getattr(chat, "room_type", None) or "support")
                    is_staff_bot_room = (room_type == "staff_bot")

                    if bucket_role == "user":
                        user_id_str = str(getattr(su, "user_id", "") or getattr(su, "id", "") or "")
                        if not _can_ask_and_inc(user_id_str):
                            ws.send(
                                json.dumps(
                                    {
                                        "type": "error",
                                        "error": "limit_reached",
                                        "message": f"Daily limit reached ({WS_DAILY_USER_LIMIT}/day). Please try tomorrow.",
                                    }
                                )
                            )
                            last_activity["ts"] = time.time()
                            continue

                    is_first_msg = Message.objects(chatroom_id=chat.id).first() is None

                    m_user = Message(
                        chatroom_id=chat.id,
                        message_by=su.id,
                        message=text,
                        is_file=False,
                        path=None,
                        is_bot=False,
                    ).save()

                    if is_first_msg and bucket_role == "user":
                        notifications_coll.insert_one(
                            {
                                "type": "FIRST_MESSAGE",
                                "chatroom_id": str(chat.id),
                                "message_by": str(su.id),
                                "message": text,
                                "created_time": now_ist_iso(),
                            }
                        )

                    room_broadcast(
                        chat_id,
                        {
                            "type": "message",
                            "from": "admin" if bucket_role != "user" else "user",
                            "message": text,
                            "message_id": str(m_user.id),
                            "chat_id": chat_id,
                            "created_time": m_user.created_time.isoformat(),
                        },
                    )

                    # ──────────────────────────────────────────────────────────────
                    # ✅ NEW STAFF_BOT LOGIC (ADD-ONLY)
                    # ──────────────────────────────────────────────────────────────
                    if is_staff_bot_room:
                        try:
                            _staff_bot_apply_engagement(chat, chat_id, conn_role)

                            engaged = bool(STAFF_ENGAGED.get(chat_id, False))
                            staff_present = is_higher_staff_present(chat, conn_role, chat_id)
                            sender_gets_bot = _staff_bot_sender_gets_bot(chat, conn_role)

                            kind = _staff_bot_room_kind(chat)
                            lower_sender = (
                                (kind == "master_room" and conn_role == "master")
                                or (kind == "admin_room" and conn_role == "admin")
                            )

                            # ✅ higher-staff message cancels pending timer (ADD-ONLY)
                            try:
                                if kind == "master_room" and conn_role in ("admin", "superadmin"):
                                    cancel_pending_bot_reply(chat_id)
                                elif kind == "admin_room" and conn_role == "superadmin":
                                    cancel_pending_bot_reply(chat_id)
                            except Exception:
                                pass

                            if sender_gets_bot and not engaged:
                                try:
                                    bot_reply = superadmin_llm_fallback(text, str(pro_id))
                                except Exception as e:
                                    logger.error(f"[STAFF BOT ERROR] {e}")
                                    bot_reply = "An internal error occurred while processing this request."

                                if bot_reply:
                                    bot_local = ensure_bot_user()
                                    m_bot = Message(
                                        chatroom_id=chat.id,
                                        message_by=bot_local.id,
                                        message=bot_reply,
                                        is_file=False,
                                        is_bot=True,
                                    ).save()

                                    room_broadcast(
                                        chat_id,
                                        {
                                            "type": "message",
                                            "from": "bot",
                                            "message": bot_reply,
                                            "message_id": str(m_bot.id),
                                            "chat_id": chat_id,
                                            "created_time": m_bot.created_time.isoformat(),
                                        },
                                    )
                                continue

                            if engaged and lower_sender:
                                schedule_staff_bot_reply_after_2m(chat, chat_id, text, str(pro_id))
                                continue

                            if engaged and staff_present:
                                schedule_staff_bot_reply_after_2m(chat, chat_id, text, str(pro_id))
                                continue

                            continue
                        except Exception:
                            pass

                    # ✅ YOUR BOT/STAFF LOGIC (unchanged) for normal support rooms
                    if bucket_role != "user":
                        STAFF_ENGAGED[chat_id] = True
                        cancel_pending_bot_reply(chat_id)
                        continue

                    engaged = bool(STAFF_ENGAGED.get(chat_id, False))
                    staff_present = is_any_staff_present(chat_id)

                    # ✅ SUPPORT ROOM RULE:
                    # If not engaged -> bot answers immediately (even if staff is connected).
                    if not engaged:
                        user_id_str = str(su.user_id)
                        reply_lines = generate_bot_reply_lines(text, user_id_str)

                        if reply_lines:
                            bot_text = "\n".join(reply_lines)
                            bot_local = ensure_bot_user()
                            m_bot = Message(
                                chatroom_id=chat.id,
                                message_by=bot_local.id,
                                message=bot_text,
                                is_file=False,
                                is_bot=True,
                            ).save()

                            room_broadcast(
                                chat_id,
                                {
                                    "type": "message",
                                    "from": "bot",
                                    "message": bot_text,
                                    "message_id": str(m_bot.id),
                                    "chat_id": chat_id,
                                    "created_time": m_bot.created_time.isoformat(),
                                },
                            )
                        continue

                    # ✅ SUPPORT ROOM FIX:
                    # If engaged and staff is present -> schedule 2m.
                    # IMPORTANT: pass user_id_str AND timer resets STAFF_ENGAGED -> bot becomes instant again.
                    if engaged and staff_present:
                        user_id_str = str(su.user_id)
                        schedule_bot_reply_after_2m(chat, chat_id, text, user_id_str)
                        continue

                    # Existing fallback path (kept)
                    reply_lines = generate_bot_reply_lines(text)
                    if reply_lines:
                        bot_local = ensure_bot_user()
                        m_bot = Message(
                            chatroom_id=chat.id,
                            message_by=bot_local.id,
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
                    continue

                ws.send(json.dumps({"type": "error", "error": "unknown"}))
                last_activity["ts"] = time.time()

        except Exception as e:
            try:
                ws.send(json.dumps({"type": "error", "error": f"unauthorized: {e}"}))
            except Exception:
                pass
        finally:
            stop_watchdog.set()  # ✅ stop watchdog thread

            # ──────────────────────────────────────────────────────────────
            # ✅ NEW: unregister presence sockets (ADD-ONLY)
            # ──────────────────────────────────────────────────────────────
            try:
                if "su" in locals() and su:
                    if su.role == USER_ROLE_ID:
                        _sock_remove(USER_SOCKETS, su.user_id, ws)
                    if su.role in (config.SUPERADMIN_ROLE_ID, config.MASTER_ROLE_ID):
                        _sock_remove(MASTER_SOCKETS, su.user_id, ws)
            except Exception:
                pass

            # ──────────────────────────────────────────────────────────────
            # ✅ NEW: unregister admin/superadmin buckets for call escalation (ADD-ONLY)
            # ──────────────────────────────────────────────────────────────
            try:
                if "su" in locals() and su:
                    if su.role == config.ADMIN_ROLE_ID:
                        try:
                            _sock_remove(ADMIN_SOCKETS, su.user_id, ws)
                        except Exception:
                            pass
                    if su.role == config.SUPERADMIN_ROLE_ID:
                        try:
                            _sock_remove(SUPERADMIN_SOCKETS, su.user_id, ws)
                        except Exception:
                            pass
            except Exception:
                pass

            if chat and chat_id:
                try:
                    mark_role_leave(chat, conn_role, ws)
                finally:
                    room_remove(chat_id, ws)
                    cancel_pending_bot_reply(chat_id)

    # ─────────────────────────────────────────────────────────
    # NEW: Demo WebSocket (IP-keyed visitor rooms)
    # ─────────────────────────────────────────────────────────
    @sock.route("/ws-demo")
    def ws_demo(ws):
        chat_id = None
        conn_role = "user"

        try:
            # Step 1: Handle JWT of parent (admin/superadmin/master) to link to demo user
            su = upsert_support_user_from_jwt()
            is_real_user = su.role == USER_ROLE_ID
            conn_role = "superadmin" if not is_real_user else "user"
            super_admin_id = su.user_id

            # Step 2: Get client IP address (demo user identity)
            client_ip = (
                get_client_ip()
            )  # You can use the `request.remote_addr` to get IP

            # Case 1: Visitor / Demo User
            if conn_role == "superadmin":
                _demo_user = ensure_demo_user(
                    client_ip, super_admin_id
                )  # Insert new user if they don't exist
                print(_demo_user)
                room = find_or_create_demo_chatroom(client_ip, super_admin_id)
                chat_id = str(room["_id"])
                room_add(chat_id, ws)
                demo_mark_role_join(chat_id, "user", ws)

                # Send message to confirm joining
                ws.send(
                    json.dumps(
                        {
                            "type": "joined",
                            "chat_id": chat_id,
                            "role": "user",
                            "ip": client_ip,
                        }
                    )
                )

            # Case 2: Parent (superadmin/master/admin) - Show the chatrooms for admin/superadmin
            else:
                if client_ip:
                    # Get or create the demo chatroom for admin
                    room = find_or_create_demo_chatroom(client_ip, super_admin_id)
                    chat_id = str(room["_id"])
                    room_add(chat_id, ws)
                    demo_mark_role_join(chat_id, "admin", ws)

                    # Send message to confirm joining
                    ws.send(
                        json.dumps(
                            {
                                "type": "joined",
                                "chat_id": chat_id,
                                "role": "admin",
                                "ip": client_ip,
                            }
                        )
                    )
                else:
                    # List all demo chatrooms owned by this super_admin_id
                    rooms_cur = demo_chatrooms_coll.find(
                        {"super_admin_id": super_admin_id},
                        {
                            "user_id": 1,
                            "is_user_active": 1,
                            "is_superadmin_active": 1,
                            "updated_time": 1,
                            "created_time": 1,
                        },
                    ).sort("updated_time", -1)
                    rooms_list = list(rooms_cur)

                    # Prepare chatrooms list for admin selection
                    payload = {
                        "type": "joined",
                        "role": "admin",
                        "needs_selection": True,
                        "chatrooms": [
                            {
                                "chat_id": str(r["_id"]),
                                "user_id": r.get("user_id"),
                                "is_user_active": bool(r.get("is_user_active", False)),
                                "is_superadmin_active": bool(
                                    r.get("is_superadmin_active", False)
                                ),
                                "updated_time": r.get("updated_time")
                                or r.get("created_time"),
                            }
                            for r in rooms_list
                        ],
                    }
                    ws.send(json.dumps(payload))

            # Main loop for handling incoming messages
            while True:
                raw = ws.receive()
                if raw is None:
                    break

                try:
                    data = json.loads(raw)
                except Exception:
                    ws.send(json.dumps({"type": "error", "error": "invalid_json"}))
                    continue

                # Process message types
                t = (data.get("type") or "").lower()
                print("WS IN:", t)
                if t == "ping":
                    # Handle ping-pong messages to check the connection
                    ws.send(json.dumps({"type": "pong"}))
                    continue

                if t == "message":
                    if not chat_id:
                        ws.send(json.dumps({"type": "error", "error": "no_chat_selected"}))
                        continue

                    text = (data.get("text") or "").strip()
                    if not text:
                        ws.send(json.dumps({"type": "error", "error": "empty_message"}))
                        continue

                    target_account_id = str(su.user_id)
                    sender = "user" if conn_role == "user" else "admin"

                    # Save and Broadcast the original user message
                    msg = save_demo_message(ObjectId(chat_id), sender, text)
                    room_broadcast(chat_id, {
                        "type": "message",
                        "from": sender,
                        "message": text,
                        "message_id": str(msg["_id"]),
                        "chat_id": chat_id,
                        "created_time": msg["created_time"].isoformat(),
                    })

                    # BOT RESPONSE
                    if sender == "user" and not is_demo_superadmin_present(chat_id):
                        # Get the raw response from your brain function
                        raw_reply = llm_fallback(text, target_account_id)

                        # ✅ 1. Check if raw_reply is a list. If it is, join it with SPACES, not newlines.
                        if isinstance(raw_reply, list):
                            bot_text = " ".join(raw_reply)
                        else:
                            bot_text = str(raw_reply)

                        # ✅ 2. Clean up any weird double-newlines or trailing spaces
                        bot_text = bot_text.strip()

                        if bot_text:
                            bot_msg = save_demo_message(ObjectId(chat_id), "bot", bot_text)
                            
                            room_broadcast(chat_id, {
                                "type": "message",
                                "from": "bot",
                                "message": bot_text, # Send the clean string
                                "message_id": str(bot_msg["_id"]),
                                "chat_id": chat_id,
                                "created_time": bot_msg["created_time"].isoformat(),
                            })

                # Catch unknown message types
                ws.send(json.dumps({"type": "error", "error": "unknown"}))

        except Exception as e:
            # Handle unexpected errors
            ws.send(json.dumps({"type": "error", "error": str(e)}))
            ws.close()

    @sock.route("/ws-demo-admin")
    def ws_demo_admin(ws):
        chat_id = None
        conn_role = "admin"
        try:
            su = upsert_support_user_from_jwt()  # Get parent/admin/superadmin JWT
            pro_id = su.user_id  # superadmin/master/admin ID
            chat_id = None

            # Handle parent/admin connection - Get list of demo chatrooms they manage
            qs_chatroom_id = request.args.get("chatroom_id")

            if qs_chatroom_id:
                picked = demo_chatrooms_coll.find_one(
                    {"_id": _oid(qs_chatroom_id), "super_admin_id": pro_id}
                )
                if not picked:
                    ws.send(
                        json.dumps({"type": "error", "error": "chatroom_not_found"})
                    )
                    return
                chat_id = str(picked["_id"])
                room_add(chat_id, ws)
                demo_mark_role_join(chat_id, "admin", ws)
                ws.send(
                    json.dumps({"type": "joined", "chat_id": chat_id, "role": "admin"})
                )

            else:
                # List all demo rooms owned by this super_admin_id
                rooms_cur = demo_chatrooms_coll.find({"super_admin_id": pro_id}).sort(
                    "updated_time", -1
                )
                rooms_list = list(rooms_cur)
                payload = {
                    "type": "joined",
                    "role": "admin",
                    "needs_selection": True,
                    "chatrooms": [
                        {
                            "chat_id": str(r["_id"]),
                            "user_id": r.get("user_id"),
                            "is_user_active": bool(r.get("is_user_active", False)),
                            "is_superadmin_active": bool(
                                r.get("is_superadmin_active", False)
                            ),
                            "updated_time": r.get("updated_time")
                            or r.get("created_time"),
                        }
                        for r in rooms_list
                    ],
                }
                ws.send(json.dumps(payload))

            # main loop for incoming messages from parent/admin
            while True:
                raw = ws.receive()
                if raw is None:
                    break
                try:
                    data = json.loads(raw)
                except Exception:
                    ws.send(json.dumps({"type": "error", "error": "invalid_json"}))
                    continue

                t = (data.get("type") or "").lower()

                if t == "ping":
                    ws.send(json.dumps({"type": "pong"}))
                    continue

                if t == "message":
                    if not chat_id:
                        ws.send(
                            json.dumps({"type": "error", "error": "no_chat_selected"})
                        )
                        continue
                    text = (data.get("text") or "").strip()
                    if not text:
                        ws.send(json.dumps({"type": "error", "error": "empty_message"}))
                        continue

                    sender = "admin" if conn_role == "admin" else "user"
                    msg = save_demo_message(ObjectId(chat_id), sender, text)

                    room_broadcast(
                        chat_id,
                        {
                            "type": "message",
                            "from": sender,
                            "message": text,
                            "message_id": str(msg["_id"]),
                            "chat_id": chat_id,
                            "created_time": msg["created_time"].isoformat(),
                        },
                    )

                ws.send(json.dumps({"type": "error", "error": "unknown"}))

        except Exception as e:
            try:
                ws.send(json.dumps({"type": "error", "error": f"unauthorized: {e}"}))
            except Exception:
                pass
        finally:
            if chat_id:
                try:
                    demo_mark_role_leave(chat_id, "admin", ws)
                finally:
                    room_remove(chat_id, ws)
    

    # ── Chatbot REST
    @app.post("/api/history")
    def api_history():
        data = request.get_json(silent=True) or {}
        chatroom_id = data.get("chat_id") or data.get("chatroom_id")
        if not chatroom_id:
            return jsonify({"ok": True, "conversation": [], "chat_id": None})
        try:
            oid = _oid(chatroom_id)
        except Exception:
            return jsonify({"ok": True, "conversation": [], "chat_id": None})

        from datetime import datetime, timedelta
        today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday_start = today - timedelta(days=1)
        msgs = Message.objects(chatroom_id=oid, created_time__gte=yesterday_start).order_by("+created_time").limit(500)
        conv = []
        for m in msgs:
            if m.is_file and m.path:
                ext = os.path.splitext(m.path)[1].lower().strip(".")
                is_audio = ext in {"mp3", "wav", "m4a", "webm"}
                if is_audio:
                    conv.append(
                        {
                            "type": "audio",
                            "from": "bot" if m.is_bot else "user",
                            "audio_url": m.path,
                            "audio_name": os.path.basename(m.path),
                            "audio_type": ext,
                            "created_at": m.created_time.isoformat(),
                        }
                    )
                else:
                    conv.append(
                        {
                            "type": "file",
                            "from": "bot" if m.is_bot else "user",
                            "file_url": m.path,
                            "file_name": os.path.basename(m.path),
                            "file_type": ext,
                            "created_at": m.created_time.isoformat(),
                        }
                    )
            else:
                conv.append(
                    {
                        "from": "bot" if m.is_bot else "user",
                        "text": m.message,
                        "created_at": m.created_time.isoformat(),
                    }
                )
        return jsonify({"ok": True, "conversation": conv, "chat_id": chatroom_id})

    @app.post("/api/chat")
    def api_chat():
        try:
            su = upsert_support_user_from_jwt()
            is_user = su.role == USER_ROLE_ID
            role = "user" if is_user else "superadmin"
        except Exception as e:
            return jsonify({"reply": ["Unauthorized"], "error": str(e)}), 401

        data = request.get_json(silent=True) or {}
        text = (data.get("message") or "").strip()
        if not text:
            return jsonify({"reply": ["Please enter your message."]}), 400

        if role != "user":
            chat_id = data.get("chat_id")
            if not chat_id:
                return jsonify({"error": "chat_id required for non-user roles"}), 400
            chat = Chatroom.objects(id=_oid(chat_id)).first()
            if not chat:
                return jsonify({"error": "chatroom not found"}), 404
            if chat.super_admin_id and chat.super_admin_id != su.user_id:
                return jsonify({"error": "forbidden chatroom"}), 403
            Message(
                chatroom_id=chat.id,
                message_by=su.id,
                message=text,
                is_file=False,
                path=None,
                is_bot=False,
            ).save()
            return (
                jsonify(
                    {"reply": [text], "chat_id": str(chat.id), "admin_takeover": True}
                ),
                200,
            )

        chat = ensure_chatroom_for_pro(su.user_id)
        if not chat:
            return jsonify({"error": "failed to resolve user chatroom"}), 500

        is_first_msg = Message.objects(chatroom_id=chat.id).first() is None
        m_saved = Message(
            chatroom_id=chat.id,
            message_by=su.id,
            message=text,
            is_file=False,
            path=None,
            is_bot=False,
        ).save()

        if is_first_msg:
            notifications_coll.insert_one(
                {
                    "type": "FIRST_MESSAGE",
                    "chatroom_id": str(chat.id),
                    "message_by": str(su.id),
                    "message": text,
                    "created_time": now_ist_iso(),
                }
            )

        if repeated_user_questions(chat.id, text, su.id, threshold=3) >= 3:
            base = request.host_url.rstrip("/")
            pwd = "{:06d}".format(random.randint(0, 999999))
            pwd_hash = hashlib.sha256(pwd.encode("utf-8")).hexdigest()
            alert_url = f"{base}/{chat.id}/{str(chat.super_admin_id)}/{str(chat.user_id)}?hash={pwd_hash}"
            notifications_coll.insert_one(
                {
                    "type": "REPEAT_QUESTION",
                    "chat_id": str(chat.id),
                    "chatroom_id": str(chat.id),
                    "user_support_id": str(su.id),
                    "alert_time": now_ist_iso(),
                    "reason": "User repeated similar question >= 3",
                    "url": alert_url,
                    "password": pwd,
                    "password_hash": pwd_hash,
                }
            )

        if not is_superadmin_present(str(chat.id)):
            reply = cache_get(text) or faq_reply(text)
            if reply:
                cache_set(text, reply)
                bot = ensure_bot_user()
                Message(
                    chatroom_id=chat.id,
                    message_by=bot.id,
                    message="\n".join(reply),
                    is_file=False,
                    path=None,
                    is_bot=True,
                ).save()
                return jsonify({"reply": reply, "chat_id": str(chat.id)})

            ai = llm_fallback(text)
            reply = [ln.strip() for ln in ai.split("\n") if ln.strip()]
            cache_set(text, reply)
            bot = ensure_bot_user()
            Message(
                chatroom_id=chat.id,
                message_by=bot.id,
                message="\n".join(reply),
                is_file=False,
                path=None,
                is_bot=True,
            ).save()
            return jsonify({"reply": reply, "chat_id": str(chat.id)})

        return jsonify({"reply": [], "chat_id": str(chat.id), "admin_active": True})

    @app.post("/api/upload")
    def upload_file():
        try:
            su = upsert_support_user_from_jwt()
            is_user = su.role == USER_ROLE_ID
            role = "user" if is_user else "superadmin"
        except Exception as e:
            return jsonify({"ok": False, "error": f"Unauthorized: {e}"}), 401

        if "file" not in request.files:
            return jsonify({"ok": False, "error": "No file part"}), 400
        file = request.files["file"]
        if file.filename == "":
            return jsonify({"ok": False, "error": "No selected file"}), 400
        if not allowed_file(file.filename):
            return jsonify({"ok": False, "error": "File type not allowed"}), 400

        if role != "user":
            chat_id = request.form.get("chat_id")
            if not chat_id:
                return (
                    jsonify(
                        {"ok": False, "error": "chat_id required for non-user roles"}
                    ),
                    400,
                )
            chat = Chatroom.objects(id=_oid(chat_id)).first()
            if not chat:
                return jsonify({"ok": False, "error": "chatroom not found"}), 404
            if chat.super_admin_id and chat.super_admin_id != su.user_id:
                return jsonify({"ok": False, "error": "forbidden chatroom"}), 403
        else:
            chat = ensure_chatroom_for_pro(su.user_id)
            if not chat:
                return (
                    jsonify({"ok": False, "error": "failed to resolve user chatroom"}),
                    500,
                )

        fname = secure_filename(f"{uuid.uuid4().hex}_{file.filename}")
        fpath = os.path.join(app.config["UPLOAD_FOLDER"], fname)
        file.save(fpath)
        url = f"/uploads/{fname}"

        m = Message(
            chatroom_id=chat.id,
            message_by=su.id,
            message=None,
            is_file=True,
            path=url,
            is_bot=False,
        ).save()

        sender = "admin" if role != "user" else "user"
        room_broadcast(
            str(chat.id),
            {
                "type": "message",
                "from": sender,
                "is_file": True,
                "kind": "file",
                "file_url": url,
                "file_name": file.filename,
                "file_type": file.mimetype,
                "message_id": str(m.id),
                "chat_id": str(chat.id),
                "created_time": m.created_time.isoformat(),
            },
        )

        return jsonify(
            {
                "ok": True,
                "file_url": url,
                "file_name": file.filename,
                "file_type": file.mimetype,
                "message": msg_dict(m),
            }
        )

    @app.post("/api/upload_audio")
    def upload_audio():
        try:
            su = upsert_support_user_from_jwt()
            is_user = su.role == USER_ROLE_ID
            role = "user" if is_user else "superadmin"
        except Exception as e:
            return jsonify({"ok": False, "error": f"Unauthorized: {e}"}), 401

        if "audio" not in request.files:
            return jsonify({"ok": False, "error": "No audio part"}), 400
        file = request.files["audio"]
        if file.filename == "":
            return jsonify({"ok": False, "error": "No selected audio"}), 400
        if not allowed_file(file.filename):
            return jsonify({"ok": False, "error": "Audio type not allowed"}), 400

        if role != "user":
            chat_id = request.form.get("chat_id")
            if not chat_id:
                return (
                    jsonify(
                        {"ok": False, "error": "chat_id required for non-user roles"}
                    ),
                    400,
                )
            chat = Chatroom.objects(id=_oid(chat_id)).first()
            if not chat:
                return jsonify({"ok": False, "error": "chatroom not found"}), 404
            if chat.super_admin_id and chat.super_admin_id != su.user_id:
                return jsonify({"ok": False, "error": "forbidden chatroom"}), 403
        else:
            chat = ensure_chatroom_for_pro(su.user_id)
            if not chat:
                return (
                    jsonify({"ok": False, "error": "failed to resolve user chatroom"}),
                    500,
                )

        fname = secure_filename(f"{uuid.uuid4().hex}_{file.filename}")
        fpath = os.path.join(app.config["UPLOAD_FOLDER"], fname)
        file.save(fpath)
        url = f"/uploads/{fname}"

        m = Message(
            chatroom_id=chat.id,
            message_by=su.id,
            message=None,
            is_file=True,
            path=url,
            is_bot=False,
        ).save()

        sender = "admin" if role != "user" else "user"
        room_broadcast(
            str(chat.id),
            {
                "type": "message",
                "from": sender,
                "is_file": True,
                "kind": "audio",
                "audio_url": url,
                "audio_name": file.filename,
                "audio_type": file.mimetype,
                "file_url": url,
                "file_name": file.filename,
                "file_type": file.mimetype,
                "message_id": str(m.id),
                "chat_id": str(chat.id),
                "created_time": m.created_time.isoformat(),
            },
        )

        return jsonify(
            {
                "ok": True,
                "audio_url": url,
                "audio_name": file.filename,
                "audio_type": file.mimetype,
                "message": msg_dict(m),
            }
        )

    @app.get("/api/superadmin/chatrooms")
    def api_superadmin_chatrooms():
        try:
            result = get_chatrooms_for_superadmin_from_jwt()
            return jsonify(result), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 400

    @app.get("/api/chatroom/<chatroom_id>")
    def api_chatroom_detail(chatroom_id):
        try:
            data = chatroom_with_messages(chatroom_id)
            if not data:
                return jsonify({"error": "chatroom not found"}), 404
            return jsonify(data), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 400

    @app.get("/uploads/<filename>")
    def uploaded_file(filename):
        return send_from_directory(app.config["UPLOAD_FOLDER"], filename)

    @app.get("/")
    def index():
        # optional landing for chatbot UI
        return send_from_directory("static", "chatbot.html")

    @app.get("/api/my/chatrooms")
    def api_my_chatrooms():
        try:
            pro_id = decode_jwt_id()
            su = SCUser.objects(user_id=pro_id).first()
            if not su:
                pro_doc = PRO_USER_COLL.find_one(
                    {"_id": pro_id}, {"_id": 1, "role": 1, "parentId": 1}
                )
                if not pro_doc:
                    return jsonify({"error": "pro_v2 user not found"}), 404
                su = SCUser.objects(user_id=pro_id).modify(
                    upsert=True,
                    new=True,
                    set__role=_oid(pro_doc.get("role")),
                    set__parent_id=_oid(pro_doc.get("parentId")),
                    set__updated_time=datetime.now(timezone.utc),
                    set_on_insert__created_time=datetime.now(timezone.utc),
                )
            is_user = su.role == USER_ROLE_ID
            role_label = "user" if is_user else "superadmin"
            if is_user:
                return jsonify({"role": role_label, "chatroom_ids": []}), 200
            ids = [
                str(c.id) for c in Chatroom.objects(super_admin_id=pro_id).only("id")
            ]
            return jsonify({"role": role_label, "chatroom_ids": ids}), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 400
    
    @app.route("/call/upload", methods=["POST"])
    def upload_call_recording():
        try:
            f = request.files.get("file")
            if not f:
                return jsonify({"ok": False, "error": "file_required"}), 400

            chat_id = (request.form.get("chat_id") or "nochat").strip()
            call_id = (request.form.get("call_id") or "nocall").strip()
            role = (request.form.get("role") or "unknown").strip()

            # Ensure folder exists
            os.makedirs(UPLOAD_DIR, exist_ok=True)

            # Make base name unique (prevents overwrite if you record multiple times)
            # If you prefer overwrite behavior, remove rand_suffix.
            rand_suffix = uuid.uuid4().hex[:10]
            base = f"{chat_id}_{call_id}_{rand_suffix}_{role}"

            # Detect extension from upload name (fallback .webm)
            orig_name = f.filename or ""
            ext = os.path.splitext(orig_name)[1].lower()
            if ext not in [".webm", ".ogg", ".wav"]:
                ext = ".webm"

            # Save upload temporarily (WILL BE DELETED if not wav)
            tmp_path = os.path.join(UPLOAD_DIR, base + ext)

            # Final output: WAV only
            wav_path = os.path.join(UPLOAD_DIR, base + ".wav")

            f.save(tmp_path)

            # If already WAV, just rename/move to the final wav_path
            if ext == ".wav":
                # Ensure final path is exactly .wav
                os.replace(tmp_path, wav_path)
                return jsonify({
                    "ok": True,
                    "wav": os.path.basename(wav_path)
                })

            # Convert to WAV (PCM 16-bit, mono, 48kHz)
            cmd = [
                "ffmpeg",
                "-y",
                "-i", tmp_path,
                "-ac", "1",
                "-ar", "48000",
                "-c:a", "pcm_s16le",
                wav_path
            ]

            p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if p.returncode != 0:
                # If conversion fails, keep tmp file for debugging
                return jsonify({
                    "ok": False,
                    "error": "ffmpeg_failed",
                    "stderr": p.stderr[-2000:]
                }), 500

            # ✅ IMPORTANT: delete original upload so folder contains ONLY WAV
            try:
                os.remove(tmp_path)
            except Exception:
                pass

            return jsonify({
                "ok": True,
                "wav": os.path.basename(wav_path)
            })

        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500


    @app.route("/call/recordings/<path:filename>")
    def get_recording(filename):
        # Optional: protect so only .wav can be served
        if not filename.lower().endswith(".wav"):
            return jsonify({"ok": False, "error": "only_wav_allowed"}), 400

        return send_from_directory(UPLOAD_DIR, filename, as_attachment=False)

    return app


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app = create_app()

    if getattr(config, "RUN_STARTUP_MATERIALIZE", False):
        logger.info("Running initial materialization job (startup)")
        _run_job(trigger="startup")
    else:
        logger.info(
            "Startup materialization is disabled (RUN_STARTUP_MATERIALIZE=False)."
        )

    # ── Schedule analytics materialization ──
    schedule.every(6).hours.do(_run_job, trigger="schedule")

    def _daily_backup_job():
        try:
            res = backup_mongo_to_archive([PRO_DB], out_root="backups")
            if res.get("ok"):
                logger.info(
                    f"✔ Daily backup done → {res['archive_path']} ({res['used_format']})"
                )
            else:
                logger.error(f"✖ Daily backup failed → {res.get('error')}")
        except Exception as e:
            logger.exception(f"✖ Daily backup crashed: {e}")

    schedule.every().day.at("11:55").do(
        lambda: _run_async(_daily_backup_job, "backup-03")
    )

    def _daily_upload_job():
        try:
            # Upload the backup to S3
            res_up = upload_backup_to_s3(
                date_str=None,
                out_root="backups",
                bucket=None,
                s3_prefix="mongo_backup",
            )

            if res_up.get("ok"):
                logger.info(
                    f"✔ Daily upload done → s3://{res_up['bucket']}/{res_up['key']}"
                )
            else:
                logger.error(f"✖ Daily upload failed → {res_up.get('error')}")

        except Exception as e:
            logger.exception(f"✖ Daily upload crashed: {e}")

    schedule.every().day.at("12:05").do(
        lambda: _run_async(_daily_upload_job, "upload-04")
    )

    def _daily_cleanup_job():
        try:
            backup_dir = "backups"
            deleted = 0
            for f in glob.glob(f"{backup_dir}/*"):
                try:
                    if os.path.isdir(f):
                        shutil.rmtree(f)
                    else:
                        os.remove(f)
                    deleted += 1
                except Exception as e:
                    logger.warning(f"Could not delete {f}: {e}")
            logger.info(
                f"🧹 Daily cleanup done → deleted {deleted} files/folders from {backup_dir}"
            )
        except Exception as e:
            logger.exception(f"✖ Daily cleanup crashed: {e}")

    schedule.every().day.at("12:10").do(
        lambda: _run_async(_daily_cleanup_job, "cleanup-05")
    )

    def _sched_loop():
        while True:
            schedule.run_pending()
            time.sleep(60)

    threading.Thread(target=_sched_loop, daemon=True).start()
    threading.Thread(target=sync_orders_to_trade, daemon=True).start()

    # Optional DB pings/logs
    try:
        _support_client = notifications_coll.database.client
        _support_client.admin.command("ping")
        logger.info(f"✔ MongoDB connected (SUPPORT_DB='{SUPPORT_DB}')")
    except Exception as e:
        logger.error(f"✖ SUPPORT_DB connection FAILED → {e}")

    try:
        _pro_db = ProUser._get_db()
        _pro_db.client.admin.command("ping")
        logger.info(f"✔ MongoDB connected (PRO_DB='{_pro_db.name}')")
    except Exception as e:
        logger.error(f"✖ PRO_DB connection FAILED → {e}")

    # Option A: WebSocket call signaling (call.accepted, call.incoming, etc.) requires a
    # single process so USER_SOCKETS / MASTER_SOCKETS / ACTIVE_CALLS are shared.
    # Waitress is single-process by default, so /ws works. If you use Gunicorn in live,
    # run with 1 worker only: gunicorn -w 1 -k ... app:create_app()
    if getattr(config, "DEVELOPMENT", False):
        app.run(host="0.0.0.0", port=config.PORT, debug=True, use_reloader=False)
    else:
        logger.info(
            f"Serving API + Scheduler on 0.0.0.0:{config.PORT} (single process – WebSocket call signaling enabled)"
        )
        serve(app, host="0.0.0.0", port=config.PORT)
