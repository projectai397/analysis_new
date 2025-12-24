from __future__ import annotations
import eventlet
eventlet.monkey_patch()

import logging
from flask import Flask, request
from flask_socketio import SocketIO, emit, join_room, leave_room, disconnect

from bson import ObjectId

# Import your config + helpers
from src.config import config
from src.db import USER_ROLE_ID, demo_chatrooms_coll
from src.helper import (
    upsert_support_user_from_jwt,
    room_add,
    room_remove,
    demo_mark_role_join,
    demo_mark_role_leave,
    save_demo_message,
    cache_get,
    cache_set,
    faq_reply,
    llm_fallback,
    is_demo_superadmin_present,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# Initialize Socket.IO server
socketio = SocketIO(cors_allowed_origins="*", async_mode="eventlet")

LIVE_NS = "/live"
LIVE_ROOM = "live_global"
SCREEN_NS = "/screen"


def _auth_ws():
    """Extract JWT and return support user document."""
    token = request.args.get("token") or request.headers.get("Authorization")

    if token and token.startswith("Bearer "):
        token = token.split(" ", 1)[1]

    if not token:
        raise RuntimeError("JWT token missing")

    request.environ["HTTP_AUTHORIZATION"] = f"Bearer {token}"
    return upsert_support_user_from_jwt()


def _is_user_role(su):
    return su.role == USER_ROLE_ID


def _is_staff_role(su):
    return su.role != USER_ROLE_ID


# -------------------------------------------------------------------
# LIVE namespace  (rrweb global)
# -------------------------------------------------------------------
@socketio.on("connect", namespace=LIVE_NS)
def live_connect(auth):
    try:
        su = _auth_ws()
        join_room(LIVE_ROOM)

        logger.info(
            f"[LIVE] connected → user={su.user_id}, role={su.role}, sid={request.sid}, ip={request.remote_addr}"
        )

        emit(
            "live-status",
            {"status": "connected", "room": LIVE_ROOM},
            room=request.sid,
            namespace=LIVE_NS,
        )

    except Exception as e:
        logger.error(f"[LIVE] connect failed: {e}")
        return False


@socketio.on("disconnect", namespace=LIVE_NS)
def live_disconnect():
    # We don't have su here easily, but log SID + IP
    logger.info(f"[LIVE] disconnect sid={request.sid}, ip={request.remote_addr}")


@socketio.on("rrweb_event", namespace=LIVE_NS)
def live_rrweb_event(payload):
    try:
        event = payload.get("event")
        if not event:
            return
        emit("rrweb_event", event, room=LIVE_ROOM, include_self=False)
    except Exception as e:
        logger.error(f"[LIVE] rrweb relay failed: {e}")


# -------------------------------------------------------------------
# SCREEN namespace (per chatroom screen share)
# -------------------------------------------------------------------

# 🔹 Log when anyone connects to /screen
@socketio.on("connect", namespace=SCREEN_NS)
def screen_connect():
    try:
        su = _auth_ws()
        logger.info(
            f"[SCREEN] connected → user={su.user_id}, role={su.role}, sid={request.sid}, ip={request.remote_addr}"
        )
    except Exception as e:
        logger.error(f"[SCREEN] connect failed: {e}")
        return False


# 🔹 Log when anyone disconnects from /screen
@socketio.on("disconnect", namespace=SCREEN_NS)
def screen_disconnect():
    logger.info(f"[SCREEN] disconnect sid={request.sid}, ip={request.remote_addr}")


@socketio.on("screen:start", namespace=SCREEN_NS)
def screen_start(data):
    try:
        su = _auth_ws()
    except Exception as e:
        logger.error(f"[SCREEN] screen:start auth failed: {e}")
        emit("screen:error", {"error": f"auth_failed: {e}"})
        disconnect()
        return

    if not _is_user_role(su):
        logger.warning(
            f"[SCREEN] non-user tried to start screen share → user={su.user_id}, role={su.role}, sid={request.sid}"
        )
        emit("screen:error", {"error": "only_user_can_start"})
        return

    chat_id = data.get("chat_id")
    if not chat_id:
        logger.warning(f"[SCREEN] screen:start missing chat_id, sid={request.sid}")
        emit("screen:error", {"error": "chat_id_required"})
        return

    logger.info(
        f"[SCREEN] screen:start → chat_id={chat_id}, user={su.user_id}, sid={request.sid}"
    )

    join_room(chat_id)

    emit("screen:ack", {"status": "started", "chat_id": chat_id})
    emit(
        "screen:started",
        {"chat_id": chat_id, "by": "user", "user_id": str(su.user_id)},
        room=chat_id,
        include_self=False,
    )


@socketio.on("screen:event", namespace=SCREEN_NS)
def screen_event(data):
    try:
        su = _auth_ws()
    except Exception as e:
        logger.error(f"[SCREEN] screen:event auth failed: {e}")
        disconnect()
        return

    if not _is_user_role(su):
        logger.warning(
            f"[SCREEN] non-user tried to send screen:event → user={su.user_id}, role={su.role}, sid={request.sid}"
        )
        return

    chat_id = data.get("chat_id")
    event = data.get("event")
    if not chat_id or event is None:
        logger.debug(
            f"[SCREEN] screen:event missing chat_id or event, sid={request.sid}, data={data}"
        )
        return

    # Optional: very verbose, uncomment if you want to see count
    # logger.debug(f"[SCREEN] screen:event → chat_id={chat_id}, user={su.user_id}")

    emit(
        "screen:event",
        {"chat_id": chat_id, "event": event},
        room=chat_id,
        include_self=False,
    )


@socketio.on("screen:stop", namespace=SCREEN_NS)
def screen_stop(data):
    try:
        su = _auth_ws()
    except Exception as e:
        logger.error(f"[SCREEN] screen:stop auth failed: {e}")
        disconnect()
        return

    chat_id = data.get("chat_id")
    if not chat_id:
        logger.warning(f"[SCREEN] screen:stop missing chat_id, sid={request.sid}")
        emit("screen:error", {"error": "chat_id_required"})
        return

    logger.info(
        f"[SCREEN] screen:stop → chat_id={chat_id}, user={su.user_id}, sid={request.sid}"
    )

    leave_room(chat_id)
    emit("screen:ack", {"status": "stopped", "chat_id": chat_id})
    emit("screen:stopped", {"chat_id": chat_id}, room=chat_id, include_self=False)


@socketio.on("screen:join", namespace=SCREEN_NS)
def screen_join(data):
    try:
        su = _auth_ws()
    except Exception as e:
        logger.error(f"[SCREEN] screen:join auth failed: {e}")
        disconnect()
        return

    if not _is_staff_role(su):
        logger.warning(
            f"[SCREEN] non-staff tried to join screen → user={su.user_id}, role={su.role}, sid={request.sid}"
        )
        emit("screen:error", {"error": "only_staff_can_watch"})
        return

    chat_id = data.get("chat_id")
    if not chat_id:
        logger.warning(f"[SCREEN] screen:join missing chat_id, sid={request.sid}")
        emit("screen:error", {"error": "chat_id_required"})
        return

    logger.info(
        f"[SCREEN] screen:join → chat_id={chat_id}, staff_user={su.user_id}, sid={request.sid}"
    )

    join_room(chat_id)
    emit("screen:joined", {"chat_id": chat_id})


# -------------------------------------------------------------------
# Create Flask app only for Socket.IO
# -------------------------------------------------------------------
def create_socketio_app():
    app = Flask(__name__)
    app.config["JWT_SECRET_KEY"] = config.JWT_SECRET
    app.config["JWT_ALG"] = getattr(config, "JWT_ALG", "HS256")
    socketio.init_app(app, cors_allowed_origins="*")
    return app


# -------------------------------------------------------------------
# Run server
# -------------------------------------------------------------------
if __name__ == "__main__":
    app = create_socketio_app()
    logger.info(f"Socket.IO running on 127.0.0.1:{config.SOCKET_PORT}")

    socketio.run(
        app,
        host="127.0.0.1",
        port=config.SOCKET_PORT,
        debug=False,
        use_reloader=False,
    )
