from typing import Any, Dict, List, Tuple, Optional
from bson import ObjectId
from src.helpers.util import get_child_admin_ids
from ..config import users, config,trade,exchange
from datetime import datetime
import re
# ----------------------------------------------------------------------
# Basic hierarchy helpers
# ----------------------------------------------------------------------

def find_superadmins() -> List[Dict]:
    return list(users.find({"role": config.SUPERADMIN_ROLE_ID}, {"_id": 1}))

def find_children_of(parent_oid: ObjectId) -> List[Dict]:
    # Note: this uses snake_case parent_id. Keep if you have that in some docs.
    return list(users.find({"parent_id": parent_oid}, {"_id": 1}))

def collect_descendants(root_oid: ObjectId, include_root: bool = True) -> List[ObjectId]:
    seen = {root_oid} if include_root else set()
    queue: List[ObjectId] = [root_oid]
    while queue:
        cur = queue.pop(0)
        for child in find_children_of(cur):
            cid = child["_id"]
            if cid in seen:
                continue
            seen.add(cid)
            queue.append(cid)
    return list(seen)

def _normalize(docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def conv(v: Any) -> Any:
        if isinstance(v, ObjectId):
            return str(v)
        if isinstance(v, datetime):
            # If you prefer UTC with 'Z': v.astimezone(timezone.utc).isoformat().replace('+00:00','Z')
            return v.isoformat()
        if isinstance(v, list):
            return [conv(x) for x in v]
        if isinstance(v, dict):
            return {k: conv(x) for k, x in v.items()}
        return v
    return [conv(d) for d in docs]
# ----------------------------------------------------------------------
# Flat user sets
# ----------------------------------------------------------------------

def get_flat_users_under_superadmin(super_oid: ObjectId) -> List[Dict[str, Any]]:
    """
    All end-users visible to a superadmin, but only those with status == 1.
    NOTE: As written, this returns *all* users with role USER_ROLE_ID and status=1.
          If you need to restrict by superadmin's subtree, swap this out
          to walk admins/masters as done below for admins/masters.
    """
    def _nm(d: Dict) -> str:
        return d.get("name") or d.get("userName") or d.get("fullName") or ""

    end_users = list(
        users.find(
            {"role": config.USER_ROLE_ID, "status": 1},   # ✅ filter for active users
            {"_id": 1, "email": 1, "name": 1, "userName": 1, "fullName": 1, "status": 1},
        )
    )
    return [
        {
            "_id": u["_id"],
            "email": u.get("email"),
            "name": _nm(u),
            "status": u.get("status"),
        }
        for u in end_users
    ]

def get_flat_users_under_admin(admin_oid: ObjectId) -> List[Dict[str, Any]]:
    """All end-users (status==1) that belong under a given admin (via that admin's masters)."""
    def _nm(d: Dict) -> str:
        return d.get("name") or d.get("userName") or d.get("fullName") or ""

    # 1) Find masters under this admin (support both parentId and parent_id)
    master_ids = list(
        users.distinct(
            "_id",
            {
                "role": config.MASTER_ROLE_ID,
                "$or": [{"parentId": admin_oid}, {"parent_id": admin_oid}],
            },
        )
    )
    if not master_ids:
        return []

    # 2) Find end users under those masters (support both parentId and parent_id), only active (status == 1)
    end_users = list(
        users.find(
            {
                "role": config.USER_ROLE_ID,
                "status": 1,
                "$or": [
                    {"parentId": {"$in": master_ids}},
                    {"parent_id": {"$in": master_ids}},
                ],
            },
            {
                "_id": 1,
                "email": 1,
                "name": 1,
                "userName": 1,
                "fullName": 1,
                "status": 1,
            },
        )
    )

    return [
        {"_id": u["_id"], "email": u.get("email"), "name": _nm(u), "status": u.get("status")}
        for u in end_users
    ]

    # 2) Find users whose parent is one of those masters (support both parentId and parent_id)
    end_users = list(
        users.find(
            {
                "role": config.USER_ROLE_ID,
                "$or": [{"parentId": {"$in": master_ids}}, {"parent_id": {"$in": master_ids}}],
            },
            {"_id": 1, "email": 1, "name": 1, "userName": 1, "fullName": 1, "status": 1},
        )
    )
    return [
        {"_id": u["_id"], "email": u.get("email"), "name": _nm(u), "status": u.get("status")}
        for u in end_users
    ]

def get_flat_users_under_master(master_oid: ObjectId) -> List[Dict[str, Any]]:
    """All end-users (status==1) directly under a given master."""
    def _nm(d: Dict) -> str:
        return d.get("name") or d.get("userName") or d.get("fullName") or ""

    end_users = list(
        users.find(
            {
                "role": config.USER_ROLE_ID,
                "status": 1,  
                "$or": [{"parentId": master_oid}, {"parent_id": master_oid}],
            },
            {"_id": 1, "email": 1, "name": 1, "userName": 1, "fullName": 1, "status": 1},
        )
    )
    return [
        {"_id": u["_id"], "email": u.get("email"), "name": _nm(u), "status": u.get("status")}
        for u in end_users
    ]

# ----------------------------------------------------------------------
# Admin & Master discovery
# ----------------------------------------------------------------------

def get_flat_admins_under_superadmin(super_oid: ObjectId) -> List[Dict[str, Any]]:
    """
    Return all admins directly under the given superadmin as a flat list
    with {_id, email, name, status}.
    Supports both parentId and parent_id.
    """
    def _nm(d: Dict) -> str:
        return d.get("name") or d.get("userName") or d.get("fullName") or ""

    admins = list(
        users.find(
            {"role": config.ADMIN_ROLE_ID, "$or": [{"parentId": super_oid}, {"parent_id": super_oid}]},
            {"_id": 1, "email": 1, "name": 1, "userName": 1, "fullName": 1, "status": 1},
        )
    )
    return [
        {"_id": a["_id"], "email": a.get("email"), "name": _nm(a), "status": a.get("status")}
        for a in admins
    ]

def get_flat_masters_under_admin(admin_oid: ObjectId) -> List[Dict[str, Any]]:
    """
    Return all masters directly under the given admin as a flat list
    with {_id, email, name, status}.
    Supports both parentId and parent_id.
    """
    def _nm(d: Dict) -> str:
        return d.get("name") or d.get("userName") or d.get("fullName") or ""

    masters = list(
        users.find(
            {"role": config.MASTER_ROLE_ID, "$or": [{"parentId": admin_oid}, {"parent_id": admin_oid}]},
            {"_id": 1, "email": 1, "name": 1, "userName": 1, "fullName": 1, "status": 1},
        )
    )
    return [
        {"_id": m["_id"], "email": m.get("email"), "name": _nm(m), "status": m.get("status")}
        for m in masters
    ]

def get_flat_masters_under_superadmin(super_oid: ObjectId) -> List[Dict[str, Any]]:
    """
    All masters under a superadmin, discovered via that superadmin's admins.
    De-duplicates masters in case of any data oddities.
    """
    seen: set[ObjectId] = set()
    out: List[Dict[str, Any]] = []

    admins = get_flat_admins_under_superadmin(super_oid)
    for adm in admins:
        for m in get_flat_masters_under_admin(adm["_id"]):
            mid = m["_id"]
            if mid in seen:
                continue
            seen.add(mid)
            out.append(m)
    return out

# ----------------------------------------------------------------------
# Search under superadmin (kept as-is, with your prints)
# ----------------------------------------------------------------------

def search_users_for_superadmin(
    sid_oid: ObjectId,
    q: str,
    page: int,
    limit: int,
    sort: str,
    order: str,
    status: int | None = None,
    demo: bool | None = None,
) -> Tuple[List[Dict[str, Any]], int]:
    import re
    from ..config import users as users_coll
    from .util import _normalize

    mv = get_child_admin_ids(sid_oid)
    print(mv)

    flt: Dict[str, Any] = {"parentId": {"$in": mv}}
    if status is not None:
        flt["status"] = status
    if demo is not None:
        flt["isDemoAccount"] = bool(demo)
    if q:
        rx = re.compile(re.escape(q), re.IGNORECASE)
        flt["$or"] = [{"name": rx}, {"userName": rx}, {"phone": rx}]

    sdir = 1 if order == "asc" else -1
    if sort not in {"userName", "createdAt", "status"}:
        sort = "userName"

    print(flt)
    cur = (
        users_coll.find(
            flt,
            {
                "_id": 1,
                "status": 1,
                "name": 1,
                "userName": 1,
                "phone": 1,
                "credit": 1,
                "balance": 1,
                "profitLoss": 1,
                "role": 1,
                "parentId": 1,
                "addedBy": 1,
                "createdAt": 1,
                "deviceId": 1,
                "deviceType": 1,
                "ipAddress": 1,
                "isDemoAccount": 1,
            },
        )
        .sort([(sort, sdir), ("_id", 1)])
        .skip((page - 1) * limit)
        .limit(limit)
    )
    items = _normalize(list(cur))
    total = users_coll.count_documents(flt)
    return items, total



def _parse_dt(v: Optional[str]) -> Optional[datetime]:
    if not v: return None
    try:
        return datetime.fromisoformat(v)
    except Exception:
        return None

def search_trades_by_exchange_name(
    *,
    exchange_name: str,
    page: int,
    limit: int,
    q: str = "",
    product_type: Optional[str] = None,
    trade_type: Optional[str] = None,
    status: Optional[str] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    sort: str = "createdAt",
    order: str = "desc",
) -> Tuple[List[Dict[str, Any]], int]:

    # 1) resolve exchange _id by exact name (simple)
    ex_doc = exchange.find_one({"name": exchange_name}, {"_id": 1}) \
         or exchange.find_one({"masterName": exchange_name}, {"_id": 1})
    if not ex_doc:
        return [], 0

    ex_id: ObjectId = ex_doc["_id"]
    ex_id_str = str(ex_id)

    # 2) build trade filter
    flt: Dict[str, Any] = {
        "$or": [
            {"exchangeId": ex_id},      # ObjectId form
            {"exchangeId": ex_id_str},  # string form (if stored as string)
        ]
    }

    if q:
        flt["symbolName"] = re.compile(re.escape(q), re.IGNORECASE)
    if product_type:
        flt["productType"] = product_type
    if trade_type:
        flt["tradeType"] = trade_type
    if status:
        flt["status"] = status
    if date_from or date_to:
        dt: Dict[str, Any] = {}
        if date_from: dt["$gte"] = date_from
        if date_to:   dt["$lt"]  = date_to
        flt["createdAt"] = dt

    sdir = 1 if order == "asc" else -1
    allowed_sorts = {"symbolName", "createdAt", "status", "productType", "tradeType", "price", "quantity", "profitLoss"}
    if sort not in allowed_sorts:
        sort = "createdAt"

    projection = {
        "_id": 1,
        "userId": 1,
        "userName": 1,
        "symbolName": 1,
        "productType": 1,
        "exchangeId": 1,
        "tradeType": 1,
        "status": 1,
        "price": 1,
        "quantity": 1,
        "profitLoss": 1,
        "createdAt": 1,
    }

    cur = (
        trade.find(flt, projection)
        .sort([(sort, sdir), ("_id", 1)])
        .skip(max(0, (page - 1) * limit))
        .limit(limit)
    )
    items = _normalize(list(cur))
    total = trade.count_documents(flt)

    # attach exchangeName (since we already know it)
    for d in items:
        d["exchangeName"] = exchange_name
    return items, total