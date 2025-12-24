# src/routes/hierarchy_routes.py
from flask import g, jsonify, request
from flask_restx import Namespace, Resource
from bson import ObjectId
from src.helpers.users_flat import search_users_for_superadmin, search_trades_by_exchange_name
from src.helpers.util import auth_superadmin, _boolish
from ..config import users, config
from src.extensions import cache
from src.helpers import hierarchy_service as hs
from datetime import datetime, timedelta, timezone
from typing import Iterable, Dict, Set, List, Tuple
from itertools import combinations
from pymongo.collection import Collection

ns = Namespace("hierarchy")


def _classify_role(role_val) -> str:
    """Return 'superadmin' | 'admin' | 'master' | 'user' | 'unknown'."""
    if not role_val:
        return "unknown"
    r = str(role_val)
    if config.USER_ROLE_ID and r == str(config.USER_ROLE_ID):
        return "user"
    if config.SUPERADMIN_ROLE_ID and r == str(config.SUPERADMIN_ROLE_ID):
        return "superadmin"
    if config.ADMIN_ROLE_ID and r == str(config.ADMIN_ROLE_ID):
        return "admin"
    if config.MASTER_ROLE_ID and r == str(config.MASTER_ROLE_ID):
        return "master"
    return "unknown"


def _actor_role() -> str:
    doc = users.find_one({"_id": g.current_user_oid}, {"role": 1})
    if not doc:
        return "unknown"
    return _classify_role(doc.get("role"))


def _get_fn(name: str):
    fn = getattr(hs, name, None)
    return fn if callable(fn) else None


def _ensure(name: str):
    fn = _get_fn(name)
    if not fn:
        return None, jsonify({"ok": False, "error": f"Missing hierarchy fn '{name}'."}), 501
    return fn, None, None


@ns.route("/hierarchy/users")
class Users(Resource):
    method_decorators = [auth_superadmin]

    def get(self):
        sid = g.current_user_oid
        role = _actor_role()
        if role == "user":
            return {"ok": False, "error": "Access denied for role 'user'."}, 403
        if role == "unknown":
            return {"ok": False, "error": "Unknown role."}, 403

        if role == "superadmin":
            fn, err, code = _ensure("get_users_for_superadmin")
            if err:
                return err, code
            items = fn(sid)
        elif role == "admin":
            fn, err, code = _ensure("get_users_for_admin")
            if err:
                return err, code
            items = fn(sid)
        elif role == "master":
            fn, err, code = _ensure("get_users_for_master")
            if err:
                return err, code
            items = fn(sid)
        else:
            return {"ok": False, "error": "Unsupported role."}, 403

        return {
            "ok": True,
            "actor_id": str(sid),
            "actor_role": role,
            "count": len(items),
            "items": items,
        }, 200


@ns.route("/hierarchy/all")
class All(Resource):
    method_decorators = [auth_superadmin]

    def get(self):
        sid = g.current_user_oid
        role = _actor_role()
        if role == "user":
            return {"ok": False, "error": "Access denied for role 'user'."}, 403
        if role == "unknown":
            return {"ok": False, "error": "Unknown role."}, 403

        admins_items, masters_items, users_items = [], [], []

        if role == "superadmin":
            # admins
            fnA, eA, cA = _ensure("get_admins_for_superadmin")
            if eA:
                return eA, cA
            admins_items = fnA(sid)

            # masters
            fnM, eM, cM = _ensure("get_masters_for_superadmin")
            if eM:
                return eM, cM
            masters_items = fnM(sid)

            # users
            fnU, eU, cU = _ensure("get_users_for_superadmin")
            if eU:
                return eU, cU
            users_items = fnU(sid)

        elif role == "admin":
            # masters under this admin
            fnM, eM, cM = _ensure("get_masters_for_admin")
            if eM:
                return eM, cM
            masters_items = fnM(sid)

            # users under this admin
            fnU, eU, cU = _ensure("get_users_for_admin")
            if eU:
                return eU, cU
            users_items = fnU(sid)

        elif role == "master":
            # only users under this master
            fnU, eU, cU = _ensure("get_users_for_master")
            if eU:
                return eU, cU
            users_items = fnU(sid)

        else:
            return {"ok": False, "error": "Unsupported role."}, 403

        return {
            "ok": True,
            "actor_id": str(sid),
            "actor_role": role,
            "admins": {"count": len(admins_items), "items": admins_items},
            "masters": {"count": len(masters_items), "items": masters_items},
            "users": {"count": len(users_items), "items": users_items},
        }, 200


@ns.route("/hierarchy/users/search")
class UsersSearch(Resource):
    method_decorators = [auth_superadmin]

    def get(self):
        sid = g.current_user_oid
        q = (request.args.get("q") or "").strip()
        page_s = request.args.get("page", "1")
        limit_s = request.args.get("limit", "30")
        sort = (request.args.get("sort") or "name").strip()
        order = (request.args.get("order") or "asc").strip().lower()
        status_s = request.args.get("status")
        demo = _boolish(request.args.get("demo"))
        try:
            page = max(1, int(page_s))
            limit = max(1, min(200, int(limit_s)))
        except:
            return {"success": False, "error": "page and limit must be integers"}, 400
        status = None
        if status_s is not None and status_s != "":
            try:
                status = int(status_s)
            except:
                return {"success": False, "error": "status must be int"}, 400
        ckey = f"user_search:{sid}:{q}:{page}:{limit}:{sort}:{order}:{status}:{1 if demo else 0}"
        cached = cache.get(ckey)
        if cached:
            return cached, 200
        items, total = search_users_for_superadmin(
            ObjectId(sid), q, page, limit, sort, order, status, demo
        )
        resp = {"success": True, "page": page, "limit": limit, "total": total, "results": items}
        cache.set(ckey, resp, timeout=30)
        return resp, 200


@ns.route("/trades/search")
class TradesSearch(Resource):
    method_decorators = [auth_superadmin]  # or any auth you prefer

    def get(self):
        exchange_name = (request.args.get("exchangeName") or "").strip()
        if not exchange_name:
            return {"success": False, "error": "exchangeName is required"}, 400

        page = int(request.args.get("page", "1"))
        limit = int(request.args.get("limit", "30"))
        q = (request.args.get("q") or "").strip()
        product_type = (request.args.get("productType") or "").strip() or None
        trade_type = (request.args.get("tradeType") or "").strip() or None
        status = (request.args.get("status") or "").strip() or None
        sort = (request.args.get("sort") or "createdAt").strip()
        order = (request.args.get("order") or "desc").strip().lower()
        date_from = (
            datetime.fromisoformat(request.args.get("date_from"))
            if request.args.get("date_from")
            else None
        )
        date_to = (
            datetime.fromisoformat(request.args.get("date_to"))
            if request.args.get("date_to")
            else None
        )

        items, total = search_trades_by_exchange_name(
            exchange_name=exchange_name,
            page=page,
            limit=limit,
            q=q,
            product_type=product_type,
            trade_type=trade_type,
            status=status,
            date_from=date_from,
            date_to=date_to,
            sort=sort,
            order=order,
        )

        return {
            "success": True,
            "page": page,
            "limit": limit,
            "total": total,
            "results": items,
        }, 200


def _yyyy_mm_dd_ist(dt_utc: datetime) -> str:
    """
    Convert UTC -> IST calendar date string "YYYY-MM-DD" without time.
    We want day-streaks in India time.
    """
    # IST is UTC+5:30
    ist_offset = timedelta(hours=5, minutes=30)
    ist = dt_utc.replace(tzinfo=timezone.utc) + ist_offset
    return ist.date().isoformat()


def _max_consecutive_days(stamped_days: List[str]) -> int:
    """
    stamped_days are like ["2025-09-27","2025-09-28","2025-09-29", ...] (unique, sorted).
    Return the longest streak length of consecutive calendar days.
    """
    if not stamped_days:
        return 0
    # Convert to ordinal for easy consecutive checks
    ords = sorted({datetime.strptime(d, "%Y-%m-%d").date().toordinal() for d in stamped_days})
    best = 1
    cur = 1
    for i in range(1, len(ords)):
        if ords[i] == ords[i - 1] + 1:
            cur += 1
            best = max(best, cur)
        else:
            cur = 1
    return best


# --- core detector ---


def detect_wash_trading_user_ids_for_master(
    orders: Collection,
    get_flat_users_under_master,
    master_id: ObjectId | str,
    *,
    start_utc: datetime | None = None,
    end_utc: datetime | None = None,
    threshold_days: int = 3,
) -> Set[ObjectId]:
    """
    Return the set of userIds under the given master who participated in
    a wash-trading streak:
      - For any symbolId, any pair of siblings (uA,uB) under the same master
        both placed BUYs on that symbol on each day of a run of >= threshold_days
        consecutive IST calendar days.

    Parameters:
      orders: Mongo collection with trades/orders (must contain fields:
              userId, symbolId, tradeType, executionDateTime, status or equivalent)
      get_flat_users_under_master: callable(master_id) -> Iterable[Dict or ids]
      master_id: the master to inspect
      start_utc / end_utc: optional UTC window to limit scanning
      threshold_days: number of consecutive days to trigger flag

    Returns:
      Set of user ObjectIds to be marked wash_trade=True
    """

    # Resolve users under master
    flat_users: Iterable = get_flat_users_under_master(master_id)
    # Accept both docs and raw ids
    all_user_ids: List[ObjectId] = []
    for u in flat_users:
        if isinstance(u, dict):
            uid = u.get("_id") or u.get("id") or u.get("userId")
        else:
            uid = u
        if uid:
            all_user_ids.append(ObjectId(uid))

    if not all_user_ids:
        return set()

    # Build match filter
    match: Dict = {
        "userId": {"$in": all_user_ids},
        "tradeType": {"$in": ["buy", "sell"]},  # scope to BUY only (your rule)
        # Optionally restrict to executed/filled if you store status:
        # "status": "executed",
    }
    if start_utc or end_utc:
        dt_filter: Dict[str, Dict] = {}
        if start_utc:
            dt_filter["$gte"] = start_utc
        if end_utc:
            dt_filter["$lt"] = end_utc
        match["executionDateTime"] = dt_filter

    # We’ll fetch lean data and post-process streaks in Python.
    # Step A: For each (day, symbolId), gather the set of users who bought.
    cursor = orders.find(
        match,
        {
            "_id": 0,
            "userId": 1,
            "symbolId": 1,
            "executionDateTime": 1,
        },
        no_cursor_timeout=True,
    )

    # day_symbol -> set(userId)
    day_symbol_users: Dict[Tuple[str, ObjectId], Set[ObjectId]] = {}

    for doc in cursor:
        uid = doc["userId"]
        sid = doc["symbolId"]
        dt: datetime = doc["executionDateTime"]
        day_key = _yyyy_mm_dd_ist(dt)
        key = (day_key, sid)
        s = day_symbol_users.get(key)
        if s is None:
            s = set()
            day_symbol_users[key] = s
        s.add(uid)

    cursor.close()

    if not day_symbol_users:
        return set()

    # Step B: For each symbol, build pair->list_of_days where both users bought on same day.
    # pair is normalized tuple (min_id, max_id) to avoid duplicates
    pair_days_by_symbol: Dict[ObjectId, Dict[Tuple[ObjectId, ObjectId], List[str]]] = {}

    # Iterate each (day, symbol) bucket
    for (day, symbol_id), users in day_symbol_users.items():
        if len(users) < 2:
            continue
        for u1, u2 in combinations(sorted(users), 2):
            per_symbol = pair_days_by_symbol.setdefault(symbol_id, {})
            pair = (u1, u2)
            per_symbol.setdefault(pair, []).append(day)

    # Step C: For each (symbol, pair), check longest consecutive-day streak.
    flagged: Set[ObjectId] = set()
    for symbol_id, pair_map in pair_days_by_symbol.items():
        for (u1, u2), days in pair_map.items():
            if _max_consecutive_days(days) >= threshold_days:
                flagged.add(u1)
                flagged.add(u2)

    return flagged
