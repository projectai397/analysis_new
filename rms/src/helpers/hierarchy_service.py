# src/helpers/hierarchy_service.py
from typing import Any, Dict, List
from bson import ObjectId
from ..config import users, config

# ---- helpers (ONLY parentId) ----
def _parent_eq(parent_oid: ObjectId) -> Dict[str, Any]:
    return {"parentId": parent_oid}

def _parent_in(parent_oids: List[ObjectId]) -> Dict[str, Any]:
    return {"parentId": {"$in": parent_oids}}

# Output includes phone + username (no email, no status, no fullName)
_PROJECTION = {
    "_id": 1,
    "role": 1,
    "parentId": 1,
    "name": 1,
    "userName": 1,
    "phone": 1,
    "username": 1,
}

def get_user_full_by_id(uid: ObjectId) -> Dict[str, Any]:
    """
    Return full user document (no projection) for details in Telegram bot.
    """
    doc = users.find_one({"_id": uid})
    if not doc:
        return {}
    return doc

def _name(d: Dict[str, Any]) -> str | None:
    return d.get("name") or d.get("userName")

def _norm(docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for d in docs:
        out.append({
            "_id": str(d.get("_id")) if d.get("_id") is not None else None,
            "role": str(d.get("role")) if d.get("role") is not None else None,
            "parentId": str(d.get("parentId")) if d.get("parentId") is not None else None,
            "name": _name(d),
            "phone": d.get("phone"),
            "username": d.get("username"),
        })
    return out

# =========================
# Superadmin scope
# =========================
def get_admins_for_superadmin(sid_oid: ObjectId) -> List[Dict[str, Any]]:
    cur = users.find({"role": config.ADMIN_ROLE_ID, **_parent_eq(sid_oid)}, _PROJECTION)
    return _norm(list(cur))

def get_masters_for_superadmin(sid_oid: ObjectId) -> List[Dict[str, Any]]:
    cur = users.find({"role": config.MASTER_ROLE_ID}, _PROJECTION)
    return _norm(list(cur))

def get_users_for_superadmin(sid_oid: ObjectId) -> List[Dict[str, Any]]:
    cur = users.find({"role": config.USER_ROLE_ID}, _PROJECTION)
    return _norm(list(cur))

# =========================
# Admin scope
# =========================
def get_masters_for_admin(admin_oid: ObjectId) -> List[Dict[str, Any]]:
    cur = users.find({"role": config.MASTER_ROLE_ID, **_parent_eq(admin_oid)}, _PROJECTION)
    return _norm(list(cur))

def get_users_for_admin(admin_oid: ObjectId) -> List[Dict[str, Any]]:
    master_ids = [m["_id"] for m in users.find({"role": config.MASTER_ROLE_ID, **_parent_eq(admin_oid)}, {"_id": 1})]
    if not master_ids:
        return []
    cur = users.find({"role": config.USER_ROLE_ID, **_parent_in(master_ids)}, _PROJECTION)
    return _norm(list(cur))

# =========================
# Master scope
# =========================
def get_users_for_master(master_oid: ObjectId) -> List[Dict[str, Any]]:
    cur = users.find({"role": config.USER_ROLE_ID, **_parent_eq(master_oid)}, _PROJECTION)
    return _norm(list(cur))

__all__ = [
    "get_admins_for_superadmin",
    "get_masters_for_superadmin",
    "get_users_for_superadmin",
    "get_masters_for_admin",
    "get_users_for_admin",
    "get_users_for_master",
]
