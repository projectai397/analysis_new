# src/helpers/build_service.py
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import pytz
from bson import ObjectId
from pymongo import ASCENDING
from pymongo import errors as pymongo_errors
from src.api.finance import _resolve_user_balance
from src.api.hierarchy import detect_wash_trading_user_ids_for_master
from src.helpers.hierarchy_service import (get_users_for_admin,
                                           get_users_for_master,
                                           get_users_for_superadmin)
from src.helpers.metrics_service import (least_traded_scripts,
                                         most_traded_scripts, overall_kpis,
                                         top_biggest_buy_trades,
                                         top_biggest_sell_trades,
                                         top_biggest_trades, top_loser,
                                         top_profitable)
from src.helpers.pipelines import _get_live_user_ids, kpis_from_orders_pipeline
from src.helpers.tx_service import (overall_transactions_for_users,
                                    top_biggest_deposits,
                                    top_biggest_withdrawals)
from src.helpers.users_flat import (find_superadmins,
                                    get_flat_admins_under_superadmin,
                                    get_flat_users_under_admin,
                                    get_flat_users_under_master,
                                    get_flat_users_under_superadmin)
from src.helpers.util import ist_week_window_now_for, ist_week_window_weekly

from ..config import analysis, analysis_users, config, orders, users

IST_TZ = pytz.timezone("Asia/Kolkata")
# -------------------------- filters & helpers --------------------------


def _ensure_tz(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(
            tzinfo=(
                timezone.utc
                if getattr(config, "CREATED_AT_IS_UTC", False)
                else config.APP_TZ
            )
        )
    return dt


def _time_exec_between(start: datetime, end: datetime) -> Dict[str, Any]:
    return {"executionDateTime": {"$gte": start, "$lt": end}}


def _user_match_or(user_ids: List[ObjectId]) -> Dict[str, Any]:
    if not user_ids:
        return {"user_id": {"$in": [ObjectId("0" * 24)]}}
    user_ids_str = [str(u) for u in user_ids]
    return {
        "$or": [
            {"user_id": {"$in": user_ids}},
            {"user_id": {"$in": user_ids_str}},
            {"userId": {"$in": user_ids}},
            {"userId": {"$in": user_ids_str}},
        ]
    }


def _user_match_single(u_id: ObjectId) -> Dict[str, Any]:
    s = str(u_id)
    return {"$or": [{"user_id": u_id}, {"user_id": s}, {"userId": u_id}, {"userId": s}]}


# -------------------------- indexes --------------------------


def _ensure_indexes() -> None:
    # ✅ Strict: one doc per (scope, owner_id)
    try:
        # drop the "per-window" unique index if it exists
        try:
            analysis.drop_index("uniq_scope_owner_window")
        except Exception:
            pass

        analysis.create_index(
            [("scope", ASCENDING), ("owner_id", ASCENDING)],
            name="uniq_scope_owner",
            unique=True,
            background=True,
            partialFilterExpression={
                "scope": {"$exists": True},
                "owner_id": {"$exists": True},
            },
        )
    except pymongo_errors.OperationFailure:
        pass

    # helpful lookups stay
    for name, key in [
        ("by_superadmin", [("superadmin_id", ASCENDING)]),
        ("by_admin", [("admin_id", ASCENDING)]),
        ("by_master", [("master_id", ASCENDING)]),
        ("by_generated_at", [("generated_at", ASCENDING)]),
    ]:
        try:
            analysis.create_index(key, name=name, background=True)
        except pymongo_errors.OperationFailure:
            pass
    for name, key in [
        ("by_superadmin", [("superadmin_id", ASCENDING)]),
        ("by_admin", [("admin_id", ASCENDING)]),
        ("by_master", [("master_id", ASCENDING)]),
    ]:
        try:
            analysis.create_index(key, name=name, background=True)
        except pymongo_errors.OperationFailure:
            pass

    try:
        analysis_users.create_index(
            [("superadmin_id", ASCENDING), ("user_id", ASCENDING)],
            name="uniq_sa_user",
            unique=True,
            background=True,
        )
        analysis_users.create_index(
            [("superadmin_id", ASCENDING)], name="by_superadmin", background=True
        )
        analysis_users.create_index(
            [("user_id", ASCENDING)], name="by_user", background=True
        )
    except pymongo_errors.OperationFailure:
        pass


# -------------------------- builders --------------------------


def _build_group_doc(
    *,
    scope: str,
    owner_field: str,
    owner_id: ObjectId,
    user_docs: List[Dict[str, Any]],
    total_user_docs: Optional[List[Dict[str, Any]]] = None,  # NEW
    limit: int = 10,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
) -> Dict[str, Any]:
    # ── Weekly window (existing behaviour)
    if start is None or end is None:
        start, end = ist_week_window_weekly()

    start = _ensure_tz(start)
    end = _ensure_tz(end)

    # KPI base (keep using user_docs as before)
    user_ids = [u["_id"] for u in user_docs]
    weekly_match = {**_time_exec_between(start, end), **_user_match_or(user_ids)}

    weekly_kpis = overall_kpis(weekly_match, limit=limit, start=start, end=end)
    weekly_tx = overall_transactions_for_users(user_ids, start, end)

    weekly_analysis = {
        "top_10_profitable_trades": top_profitable(limit, weekly_match),
        "top_10_loser_trades": top_loser(limit, weekly_match),
        "top_10_biggest_trades": top_biggest_trades(limit, weekly_match),
        "top_10_most_traded_scripts": most_traded_scripts(limit, weekly_match),
        "top_10_least_traded_scripts": least_traded_scripts(limit, weekly_match),
        "top_10_biggest_deposits": top_biggest_deposits(limit, user_ids, start, end),
        "top_10_biggest_withdrawals": top_biggest_withdrawals(
            limit, user_ids, start, end
        ),
        "top_10_biggest_buy_trades": weekly_kpis.get("top_10_biggest_buy_trades", []),
        "top_10_biggest_sell_trades": weekly_kpis.get("top_10_biggest_sell_trades", []),
    }

    # ─────────────────────────────────────────────────────
    # DAILY BLOCK (new) – full same features but for 1 day
    # ─────────────────────────────────────────────────────

    # Take the "end" timestamp, convert to IST, and get that calendar day
    end_ist = end.astimezone(IST_TZ)
    day_start_ist = end_ist.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end_ist = day_start_ist + timedelta(days=1)

    # Convert back to same tz / ensure tz-awareness
    day_start = _ensure_tz(day_start_ist.astimezone(end.tzinfo))
    day_end = _ensure_tz(day_end_ist.astimezone(end.tzinfo))

    daily_match = {**_time_exec_between(day_start, day_end), **_user_match_or(user_ids)}

    daily_kpis = overall_kpis(daily_match, limit=limit, start=day_start, end=day_end)
    daily_tx = overall_transactions_for_users(user_ids, day_start, day_end)

    daily_analysis = {
        "top_10_profitable_trades": top_profitable(limit, daily_match),
        "top_10_loser_trades": top_loser(limit, daily_match),
        "top_10_biggest_trades": top_biggest_trades(limit, daily_match),
        "top_10_most_traded_scripts": most_traded_scripts(limit, daily_match),
        "top_10_least_traded_scripts": least_traded_scripts(limit, daily_match),
        "top_10_biggest_deposits": top_biggest_deposits(
            limit, user_ids, day_start, day_end
        ),
        "top_10_biggest_withdrawals": top_biggest_withdrawals(
            limit, user_ids, day_start, day_end
        ),
        "top_10_biggest_buy_trades": daily_kpis.get("top_10_biggest_buy_trades", []),
        "top_10_biggest_sell_trades": daily_kpis.get("top_10_biggest_sell_trades", []),
    }

    # ── Use total_user_docs for counts (fallback to user_docs if not provided)
    base_for_counts = total_user_docs if total_user_docs is not None else user_docs

    total_users = len(base_for_counts)
    active_users = sum(1 for u in user_docs if u.get("status") == 1)
    live_user_ids = _get_live_user_ids([u["_id"] for u in user_docs])
    live_users = len(live_user_ids)

    return {
        "scope": scope,
        "owner_id": owner_id,
        owner_field: owner_id,
        "start_date": None,
        "total_trades": weekly_kpis.get("total_trades", 0),
        "win_trades": weekly_kpis.get("win_trades", 0),
        "win_percent": weekly_kpis.get("win_percent", 0.0),
        "total_volume": weekly_kpis.get("total_volume", 0.0),
        "trading_volume": weekly_kpis.get("total_volume", 0.0),
        "total_deposits": weekly_tx.get("total_deposits", 0.0),
        "total_withdrawals": weekly_tx.get("total_withdrawals", 0.0),
        "net_balance": weekly_tx.get("net_balance", 0.0),
        "tx_count": weekly_tx.get("tx_count", 0),
        "avg_risk_score": weekly_kpis.get("avg_risk_score", 0.0),
        "avg_risk_status": weekly_kpis.get("avg_risk_status", "Low Risk"),
        "total_users": total_users,
        "active_users": active_users,
        "live_users": live_users,
        "analysis": weekly_analysis,
        "analysis_daily": daily_analysis,  # <── NEW
        "generated_at": datetime.utcnow(),
        "window": {"start": start, "end": end, "tz": "Asia/Kolkata"},
        "window_daily": {  # <── NEW
            "start": day_start,
            "end": day_end,
            "tz": "Asia/Kolkata",
        },
    }


def build_superadmin_doc(
    super_oid: ObjectId, limit: int = 10, *, start=None, end=None
) -> Dict[str, Any]:
    user_docs = get_flat_users_under_superadmin(super_oid)
    total_users = get_users_for_superadmin(super_oid)
    return _build_group_doc(
        scope="superadmin",
        owner_field="superadmin_id",
        owner_id=super_oid,
        user_docs=user_docs,
        total_user_docs=total_users,  # ← use here
        limit=limit,
        start=start,
        end=end,
    )


def build_admin_doc(
    admin_oid: ObjectId, limit: int = 10, *, start=None, end=None
) -> Dict[str, Any]:
    user_docs = get_flat_users_under_admin(admin_oid)
    total_users = get_users_for_admin(admin_oid)
    return _build_group_doc(
        scope="admin",
        owner_field="admin_id",
        owner_id=admin_oid,
        user_docs=user_docs,
        total_user_docs=total_users,  # ← use here
        limit=limit,
        start=start,
        end=end,
    )


def build_master_doc(
    master_oid: ObjectId, limit: int = 10, *, start=None, end=None
) -> Dict[str, Any]:
    user_docs = get_flat_users_under_master(master_oid)
    total_users = get_users_for_master(master_oid)
    return _build_group_doc(
        scope="master",
        owner_field="master_id",
        owner_id=master_oid,
        user_docs=user_docs,
        total_user_docs=total_users,  # ← use here
        limit=limit,
        start=start,
        end=end,
    )


def build_user_stats_doc(
    u: Dict[str, Any],
    super_oid: ObjectId,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    limit: int = 10,
    *,
    # new optional params to avoid recomputing per user
    wash_flagged_ids: Optional[Set[ObjectId]] = None,
    threshold_days: int = 3,
    # if your schema needs a custom way to resolve "master id" from a user,
    # you can pass a resolver callable; otherwise we try common keys.
    master_resolver: Optional[Callable[[Dict[str, Any]], Optional[ObjectId]]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Builds a per-user stats snapshot over the given window.

    - Skips if user.status != 1
    - Adds holding time fields (overall + per_user)
    - Includes user-specific risk score
    - Adds wash_trade boolean based on '3 consecutive IST days same-symbol BUY with a sibling under same master'
    """
    # Only process active users
    if u.get("status") != 1:
        return None

    if start is None or end is None:
        start, end = ist_week_window_weekly()
    start, end = _ensure_tz(start), _ensure_tz(end)

    u_id: ObjectId = u["_id"]

    # -------------------------
    # Wash trading computation
    # -------------------------
    wash_trade = False

    if wash_flagged_ids is not None:
        # Fast path: caller already computed flagged IDs for this master
        wash_trade = u_id in wash_flagged_ids
    else:
        # Resolve master id from the user doc (customize if needed)
        if master_resolver is not None:
            master_id = master_resolver(u)
        else:
            # Try common fields in your hierarchy; tweak if your schema differs
            master_id = u.get("masterId") or u.get("parentId") or u.get("brokerId")

        if master_id:
            try:
                flagged = detect_wash_trading_user_ids_for_master(
                    orders=orders,
                    get_flat_users_under_master=get_flat_users_under_master,
                    master_id=master_id,
                    start_utc=start,  # evaluate within the same stats window
                    end_utc=end,
                    threshold_days=threshold_days,  # default 3
                )
                wash_trade = u_id in flagged
            except Exception:
                # Fail-open: don't block stats if detector has an issue
                wash_trade = False

    # -------------------------
    # KPIs aggregation (your existing logic)
    # -------------------------
    match_one: Dict[str, Any] = {
        **_time_exec_between(start, end),
        **_user_match_single(u_id),
    }

    pipeline = kpis_from_orders_pipeline(match_one, start=start, end=end)
    agg_result = list(orders.aggregate(pipeline))
    print(agg_result)
    kpis = agg_result[0] if agg_result else {}

    overall = (
        kpis.get("overall", {}) if isinstance(kpis.get("overall", {}), dict) else {}
    )
    per_users = (
        kpis.get("per_user", []) if isinstance(kpis.get("per_user", []), list) else []
    )
    user_stats = next((p for p in per_users if p.get("_id") == u_id), {}) or {}
    balance_val = _resolve_user_balance(u_id, u)
    return {
        "superadmin_id": super_oid,
        "user_id": u_id,
        "email": u.get("email"),
        "name": u.get("name"),
        "status": u.get("status"),
        # Overall period KPIs
        "total_trades": overall.get("total_trades", 0),
        "win_trades": overall.get("win_trades", 0),
        "win_percent": overall.get("win_percent", 0.0),
        "total_volume": overall.get("total_volume", 0.0),
        # User-specific stats
        # ✅ pull balance from the user document, not the collection
        "balance": balance_val,
        # ✅ make sure the key matches what the pipeline outputs
        "avg_holding_minutes_user": user_stats.get("avg_holding_minutes_user", 0.0),
        "risk_score_user": user_stats.get("risk_score", 0.0),
        # Aggregated averages across users
        "avg_risk_score": kpis.get("avg_risk_score", 0.0),
        "avg_risk_status": kpis.get("avg_risk_status", "Low Risk"),
        "wash_trade": wash_trade,
        "generated_at": datetime.utcnow(),
        "window": {"start": start, "end": end, "tz": "Asia/Kolkata"},
    }


# -------------------------- upserts --------------------------


def _upsert_entity(doc: Dict[str, Any]) -> None:
    """
    Strictly one document per (scope, owner_id).
    Any run will UPDATE the same record; never creates a second one.
    """
    filt = {"scope": doc["scope"], "owner_id": doc["owner_id"]}
    analysis.update_one(filt, {"$set": doc}, upsert=True)


def upsert_superadmin(doc: Dict[str, Any]) -> None:
    _upsert_entity(doc)


def upsert_admin(doc: Dict[str, Any]) -> None:
    _upsert_entity(doc)


def upsert_master(doc: Dict[str, Any]) -> None:
    _upsert_entity(doc)


def upsert_user(doc: Dict[str, Any]) -> None:
    analysis_users.replace_one(
        {"superadmin_id": doc["superadmin_id"], "user_id": doc["user_id"]},
        doc,
        upsert=True,
    )


# -------------------------- materializers --------------------------


def materialize_superadmins_analysis(limit: int = 10) -> Dict[str, Any]:
    _ensure_indexes()
    supers = find_superadmins()
    updated: List[str] = []

    for sa in supers:
        super_oid: ObjectId = sa["_id"]
        # ✅ window that respects this superadmin's start_date
        start, end = ist_week_window_now_for("superadmin", super_oid)
        doc = build_superadmin_doc(super_oid, limit=limit, start=start, end=end)
        upsert_superadmin(doc)
        updated.append(str(super_oid))

    return {
        "updated_count": len(updated),
        "superadmins": updated,
        "collection": analysis.name,
    }


def materialize_admins_analysis(limit: int = 10) -> Dict[str, Any]:
    _ensure_indexes()
    supers = find_superadmins()
    updated: List[str] = []

    for sa in supers:
        for adm in get_flat_admins_under_superadmin(sa["_id"]):
            admin_oid: ObjectId = adm["_id"]
            # ✅ admin-specific window (uses that admin's start_date if set)
            start, end = ist_week_window_now_for("admin", admin_oid)
            doc = build_admin_doc(admin_oid, limit=limit, start=start, end=end)
            upsert_admin(doc)
            updated.append(str(admin_oid))

    return {
        "updated_count": len(updated),
        "admins": updated,
        "collection": analysis.name,
    }


def materialize_masters_analysis(limit: int = 10) -> Dict[str, Any]:
    _ensure_indexes()
    updated: List[str] = []

    masters = list(users.find({"role": config.MASTER_ROLE_ID}, {"_id": 1}))
    for m in masters:
        mid: ObjectId = m["_id"]
        # ✅ master-specific window (uses that master's start_date if set)
        start, end = ist_week_window_now_for("master", mid)
        doc = build_master_doc(mid, limit=limit, start=start, end=end)
        upsert_master(doc)
        updated.append(str(mid))

    return {
        "updated_count": len(updated),
        "masters": updated,
        "collection": analysis.name,
    }


def materialize_superadmins_users(limit: int = 10) -> Dict[str, Any]:
    _ensure_indexes()
    supers = find_superadmins()
    user_counts: List[Tuple[str, int]] = []

    start, end = ist_week_window_weekly()
    start, end = _ensure_tz(start), _ensure_tz(end)

    for sa in supers:
        super_oid: ObjectId = sa["_id"]
        under = get_flat_users_under_superadmin(super_oid)
        written = 0

        for u in under:
            # build_user_stats_doc returns None for inactive users (status != 1)
            doc: Optional[Dict[str, Any]] = build_user_stats_doc(
                u, super_oid, limit=limit, start=start, end=end
            )
            if not doc:
                # Skip inactive user
                continue

            upsert_user(doc)
            written += 1

        user_counts.append((str(super_oid), written))

    return {
        "superadmins": [sid for sid, _ in user_counts],
        "user_docs_written": [
            {"superadmin_id": sid, "count": cnt} for sid, cnt in user_counts
        ],
        "collection": analysis_users.name,
        "window": {"start": start, "end": end, "tz": "Asia/Kolkata"},
    }
