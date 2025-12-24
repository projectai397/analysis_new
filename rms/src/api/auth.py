# src/routes/auth_routes.py
from datetime import timedelta
from typing import Union, Optional
import os
import time
import jwt
from flask import request
from flask_restx import Namespace, Resource
from bcrypt import checkpw
from bson import ObjectId
from dotenv import load_dotenv
from ..config import users

load_dotenv()
SECRET_KEY = os.getenv("JWT_SECRET")
ALGORITHM = os.getenv("JWT_ALG")


ns = Namespace("auth", path="/auth")


def _err(msg: str, code: int):
    return {"ok": False, "error": msg}, code


def _get_oid(key: str) -> ObjectId:
    val = (os.getenv(key) or "").strip()
    if not val:
        raise RuntimeError(f"{key} missing in environment.")
    try:
        return ObjectId(val)
    except Exception:
        raise RuntimeError(f"{key} in environment is not a valid ObjectId.")


USER_ROLE_ID = _get_oid("USER_ROLE_ID")
SUPERADMIN_ROLE_ID = _get_oid("SUPERADMIN_ROLE_ID")
ADMIN_ROLE_ID = _get_oid("ADMIN_ROLE_ID")
MASTER_ROLE_ID = _get_oid("MASTER_ROLE_ID")


def _role_name(role_oid: Optional[ObjectId]) -> str:
    if not isinstance(role_oid, ObjectId):
        return "unknown"
    if role_oid == SUPERADMIN_ROLE_ID:
        return "superadmin"
    if role_oid == ADMIN_ROLE_ID:
        return "admin"
    if role_oid == MASTER_ROLE_ID:
        return "master"
    if role_oid == USER_ROLE_ID:
        return "user"
    return "unknown"


def _verify_password_bcrypt(stored_hash: Union[str, bytes, bytearray], password: str) -> bool:
    if not stored_hash or not isinstance(stored_hash, (str, bytes, bytearray)):
        return False
    try:
        if isinstance(stored_hash, str):
            stored_hash = stored_hash.encode("utf-8")
        return checkpw(password.encode("utf-8"), stored_hash)
    except Exception:
        return False


@ns.route("/login")
class Login(Resource):
    def post(self):
        data = request.get_json(silent=True) or {}

        phone = (data.get("phone") or "").strip()
        username = (data.get("username") or "").strip()
        password = data.get("password") or ""

        provided = [bool(phone), bool(username)]
        if sum(provided) == 0:
            return _err("Provide either phone or username, plus password.", 400)
        if sum(provided) > 1:
            return _err("Provide only one field: phone OR username (not both).", 400)
        if not password:
            return _err("Password required.", 400)

        query = {"phone": phone} if phone else {"userName": username}

        match_ids = list(users.find(query, {"_id": 1}).limit(2))
        if not match_ids:
            return _err("Invalid credentials.", 401)
        if len(match_ids) > 1:
            return _err("Invalid user: duplicate phone/username.", 401)

        uid = match_ids[0]["_id"]

        user = users.find_one(
            {"_id": uid},
            {
                "_id": 1,
                "password": 1,
                "password_hash": 1,
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
            return _err("Invalid credentials.", 401)

        stored_hash = user.get("password_hash") or user.get("password")
        if not stored_hash or not _verify_password_bcrypt(stored_hash, password):
            return _err("Invalid credentials.", 401)

        role_val = user.get("role")
        if isinstance(role_val, str):
            try:
                role_val = ObjectId(role_val)
            except Exception:
                return _err("Invalid role on user.", 401)
        if not isinstance(role_val, ObjectId):
            return _err("Invalid role on user.", 401)
        if role_val == USER_ROLE_ID:
            return _err("Access denied for role 'user'.", 403)

        role_name = _role_name(role_val)

        # --- Build Node-style JWT payload manually ---
        now = int(time.time())
        exp = now + 7 * 24 * 3600  # 7 days

        payload = {
            "_id": str(user["_id"]),
            "name": user.get("name"),
            "phone": user.get("phone"),
            "userName": user.get("userName"),
            "role": role_name,  # human-readable role
            "role_id": str(role_val),  # ObjectId string
            "preference": user.get("preference"),
            "deviceToken": user.get("deviceToken"),
            "deviceId": user.get("deviceId"),
            "deviceType": user.get("deviceType"),
            "sequence": user.get("sequence"),
            "iat": now,
            "exp": exp,
        }

        token = jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)
        if not isinstance(token, str):  # PyJWT v1 returns bytes
            token = token.decode("utf-8")

        return {
            "ok": True,
            "access_token": token,
            "user": {
                "id": str(user["_id"]),
                "phone": user.get("phone"),
                "username": user.get("userName") or username or phone,
                "role": str(role_val),
                "role_name": role_name,
            },
        }, 200
