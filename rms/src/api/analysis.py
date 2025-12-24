from datetime import datetime as _dt

from bson import ObjectId
from flask import g, request
from flask_restx import Namespace, Resource
from src.helpers.pipelines import (_get_live_user_ids,
                                   build_top_risk_users_pipeline,
                                   kpi_pipeline_for_positions)
from src.helpers.pipelines import \
    pipelines as _pipelines  # kept to avoid changing imports
from src.helpers.pipelines import weekly_kpi_pipeline
from src.helpers.util import auth_superadmin
from src.helpers.util import ist_week_window_now_for as week_window_now
from src.helpers.util import resolve_caps_by_balance, try_object_id

from ..config import analysis as analysis_col
from ..config import analysis_users as analysis_users_col
from ..config import config
from ..config import data as data_col
from ..config import wallets
from ..extensions import cache

ns = Namespace("analysis")


def _ck(sid: str) -> str:
    return f"analysis:{sid}"


def _bypass() -> bool:
    q = (request.args.get("cache") or "").strip().lower()
    if q in {"0", "no", "false", "bypass", "refresh"}:
        return True
    hb = (request.headers.get("X-Bypass-Cache") or "").strip().lower()
    if hb in {"1", "true", "yes"}:
        return True
    cc = (request.headers.get("Cache-Control") or "").lower()
    return "no-cache" in cc or "max-age=0" in cc


def _safe(v):
    """Recursively convert ObjectId → str and datetime → ISO8601 strings."""
    if isinstance(v, ObjectId):
        return str(v)
    if isinstance(v, (_dt,)):
        return v.isoformat() + "Z"
    if isinstance(v, list):
        return [_safe(x) for x in v]
    if isinstance(v, dict):
        return {k: _safe(x) for k, x in v.items()}
    return v


def _get_doc_for_superadmin(sid: str, bypass_cache: bool = False):
    key = _ck(sid)
    if cache and not bypass_cache:
        c = cache.get(key)
        if c is not None:
            return c
    doc = analysis_col.find_one({"superadmin_id": sid})
    if not doc:
        try:
            oid = ObjectId(sid)
        except Exception:
            oid = None
        if oid:
            doc = analysis_col.find_one({"superadmin_id": oid})
    if doc and cache:
        cache.set(key, doc, timeout=config.TTL)
    return doc


# ---- KPI + Top10 keys ----
KPI_KEYS = [
    "owner_id",
    "total_trades",
    "win_trades",
    "win_percent",
    "total_volume",
    "trading_volume",
    "total_deposits",
    "total_withdrawals",
    "net_balance",
    "tx_count",
    "avg_risk_score",
    "avg_risk_status",
    "total_users",
    "active_users",
    "live_users",
]

TOP10_KEYS = [
    "top_10_profitable_trades",
    "top_10_loser_trades",
    "top_10_biggest_trades",
    "top_10_most_traded_scripts",
    "top_10_least_traded_scripts",
    "top_10_biggest_deposits",
    "top_10_biggest_withdrawals",
    "top_10_biggest_buy_trades",
    "top_10_biggest_sell_trades",
]


def _doc_or_404(doc):
    return ({"ok": False, "error": "Analysis doc not found"}, 404) if not doc else None


# -------------------- Me --------------------
@ns.route("/me")
class Me(Resource):
    method_decorators = [auth_superadmin]

    def get(self):
        sid = g.current_user_id
        doc = _get_doc_for_superadmin(sid, bypass_cache=_bypass())
        err = _doc_or_404(doc)
        return err or _safe(doc)


def _to_oid(v):
    try:
        return v if isinstance(v, ObjectId) else ObjectId(str(v))
    except Exception:
        return None


@ns.route("/me/kpis")
class MeKpis(Resource):
    method_decorators = [auth_superadmin]  # any authenticated account

    def get(self):
        uid = getattr(g, "current_user_id", None)
        if not uid:
            return {"error": "No authenticated user id"}, 401

        oid = _to_oid(uid)
        if oid is None:
            return {"error": "Invalid user id"}, 400

        # 1) Primary: match by the canonical owner_id (new schema)
        doc = analysis_col.find_one(
            {"owner_id": oid},
            sort=[("generated_at", -1)],
        )

        # 2) Fallbacks: support legacy fields if owner_id wasn't set in older docs
        if not doc:
            doc = analysis_col.find_one(
                {
                    "$or": [
                        {"superadmin_id": oid},
                        {"admin_id": oid},
                        {"master_id": oid},
                    ]
                },
                sort=[("generated_at", -1)],
            )

        err = _doc_or_404(doc)
        if err:
            return err

        blk = doc.get("analysis") or {}
        resp = {k: _safe(doc.get(k)) for k in KPI_KEYS}
        for k in TOP10_KEYS:
            resp[k] = _safe(blk.get(k, []))
        return resp


@ns.route("/me/top-profitable")
class MeTopProfitable(Resource):
    method_decorators = [auth_superadmin]

    def get(self):
        doc = _get_doc_for_superadmin(g.current_user_id, bypass_cache=_bypass())
        err = _doc_or_404(doc)
        return err or _safe(doc.get("analysis", {}).get("top_10_profitable_trades", []))


@ns.route("/me/top-loser")
class MeTopLoser(Resource):
    method_decorators = [auth_superadmin]

    def get(self):
        doc = _get_doc_for_superadmin(g.current_user_id, bypass_cache=_bypass())
        err = _doc_or_404(doc)
        return err or _safe(doc.get("analysis", {}).get("top_10_loser_trades", []))


@ns.route("/me/biggest-trades")
class MeBiggestTrades(Resource):
    method_decorators = [auth_superadmin]

    def get(self):
        doc = _get_doc_for_superadmin(g.current_user_id, bypass_cache=_bypass())
        err = _doc_or_404(doc)
        return err or _safe(doc.get("analysis", {}).get("top_10_biggest_trades", []))


@ns.route("/me/most-traded-scripts")
class MeMostTraded(Resource):
    method_decorators = [auth_superadmin]

    def get(self):
        doc = _get_doc_for_superadmin(g.current_user_id, bypass_cache=_bypass())
        err = _doc_or_404(doc)
        return err or _safe(
            doc.get("analysis", {}).get("top_10_most_traded_scripts", [])
        )


@ns.route("/me/least-traded-scripts")
class MeLeastTraded(Resource):
    method_decorators = [auth_superadmin]

    def get(self):
        doc = _get_doc_for_superadmin(g.current_user_id, bypass_cache=_bypass())
        err = _doc_or_404(doc)
        return err or _safe(
            doc.get("analysis", {}).get("top_10_least_traded_scripts", [])
        )


@ns.route("/me/biggest-deposits")
class MeBiggestDeposits(Resource):
    method_decorators = [auth_superadmin]

    def get(self):
        doc = _get_doc_for_superadmin(g.current_user_id, bypass_cache=_bypass())
        err = _doc_or_404(doc)
        return err or _safe(doc.get("analysis", {}).get("top_10_biggest_deposits", []))


@ns.route("/me/biggest-withdrawals")
class MeBiggestWithdrawals(Resource):
    method_decorators = [auth_superadmin]

    def get(self):
        doc = _get_doc_for_superadmin(g.current_user_id, bypass_cache=_bypass())
        err = _doc_or_404(doc)
        return err or _safe(
            doc.get("analysis", {}).get("top_10_biggest_withdrawals", [])
        )


# -------------------- Live KPIs --------------------
@ns.route("/live-kpis")
class LiveKpis(Resource):
    method_decorators = [auth_superadmin]

    def post(self):
        data = request.get_json(silent=True) or {}
        user_id_str = data.get("user_id")
        if not user_id_str:
            return {"ok": False, "error": "Missing 'user_id'"}, 400

        or_clauses, oid = [], try_object_id(user_id_str)
        if oid:
            or_clauses.append({"user_id": oid})
        or_clauses.append({"user_id": user_id_str})
        match = {"$or": or_clauses}

        from ..config import positions

        out = list(
            positions.aggregate(kpi_pipeline_for_positions(match), allowDiskUse=True)
        )

        if not out:
            return {
                "user_id": user_id_str,
                "total_trades": 0,
                "win_trades": 0,
                "win_percent": 0.0,
                "total_volume": 0.0,
                "generated_at": _dt.utcnow().isoformat() + "Z",
            }
        kpis = _safe(out[0])
        kpis.update(
            {"user_id": user_id_str, "generated_at": _dt.utcnow().isoformat() + "Z"}
        )
        return kpis


@ns.route("/live-weekly-kpis")
class LiveWeeklyKpis(Resource):
    method_decorators = [auth_superadmin]

    def post(self):
        data = request.get_json(silent=True) or {}
        user_id_str = data.get("user_id")
        if not user_id_str:
            return {"ok": False, "error": "Missing 'user_id'"}, 400

        or_clauses, oid = [], try_object_id(user_id_str)
        if oid:
            or_clauses.append({"user_id": oid})
        or_clauses.append({"user_id": user_id_str})
        match = {"$or": or_clauses}

        wallet_doc = wallets.find_one({"user_id": oid}, {"balance": 1}) if oid else None
        if not wallet_doc:
            wallet_doc = wallets.find_one({"user_id": user_id_str}, {"balance": 1})
        balance = wallet_doc.get("balance", 0.0) if wallet_doc else 0.0

        start, end = week_window_now()
        from ..config import positions

        out = list(
            positions.aggregate(
                weekly_kpi_pipeline(match, start, end), allowDiskUse=True
            )
        )

        if not out:
            return {
                "user_id": user_id_str,
                "balance": balance,
                "total_trades": 0,
                "win_trades": 0,
                "win_percent": 0.0,
                "total_volume": 0.0,
                "risk_score": 0.0,
                "risk_status": "Low Risk",
                "window": {"start": start.isoformat(), "end": end.isoformat()},
            }

        kpis = _safe(out[0])
        max_trades, max_volume = resolve_caps_by_balance(balance)
        trades_norm = min(kpis["total_trades"] / max_trades, 1) if max_trades else 0
        volume_norm = min(kpis["total_volume"] / max_volume, 1) if max_volume else 0
        win_percent = kpis["win_percent"]
        win_risk = 1.0 if win_percent >= 70 else win_percent / 100.0
        score = round(10 * (0.4 * win_risk + 0.3 * trades_norm + 0.3 * volume_norm), 1)
        status = (
            "Low Risk" if score < 4 else ("Medium Risk" if score < 7 else "High Risk")
        )

        kpis.update(
            {
                "user_id": user_id_str,
                "balance": balance,
                "risk_score": score,
                "risk_status": status,
                "window": {"start": start.isoformat(), "end": end.isoformat()},
            }
        )
        return kpis


# -------------------- Date Update --------------------
def _to_naive_utc(dt: _dt) -> _dt:
    # store naive UTC in start_date to match your prior behavior
    if dt.tzinfo is not None:
        return dt.astimezone(tz=None).replace(tzinfo=None)
    return dt


def _parse_incoming_date(ds: str) -> _dt:
    s = (ds or "").strip()
    if not s:
        raise ValueError("empty")
    if len(s) == 10:  # YYYY-MM-DD
        return _dt.strptime(s, "%Y-%m-%d")
    s = s.replace("Z", "+00:00")
    dt = _dt.fromisoformat(s)
    return _to_naive_utc(dt)


@ns.route("/date_update")
class DateUpdate(Resource):
    # allow any authenticated owner (superadmin/admin/master)
    method_decorators = [auth_superadmin]

    def post(self):
        """
        Body:
          {
            "date": "YYYY-MM-DD"   // or ISO-8601 string
            // or "start_date": "ISO-8601"
          }
        Updates start_date on the *latest* analysis doc for the caller.
        Uses (scope, owner_id, window.start, window.end) to avoid duplicates.
        """
        data = request.get_json(silent=True) or {}
        raw = (data.get("date") or data.get("start_date") or "").strip()
        if not raw:
            return {
                "ok": False,
                "error": "Missing 'date' (or 'start_date') in body",
            }, 400

        try:
            dt = _parse_incoming_date(raw)  # your helper (keeps naive UTC)
        except Exception:
            return {
                "ok": False,
                "error": f"Invalid date format: {raw}. Use 'YYYY-MM-DD' or ISO-8601.",
            }, 400

        uid = getattr(g, "current_user_id", None)
        if not uid:
            return {"ok": False, "error": "No authenticated user id"}, 401

        oid = try_object_id(uid)  # returns ObjectId or None
        owner_value = oid or uid
        now = _dt.utcnow()

        # ---- 1) Find the latest doc for this owner (prefer canonical owner_id)
        doc = analysis_col.find_one({"owner_id": owner_value})

        # Fallback to legacy owner fields if needed
        if not doc:
            doc = analysis_col.find_one(
                {
                    "$or": [
                        {"superadmin_id": owner_value},
                        {"admin_id": owner_value},
                        {"master_id": owner_value},
                    ]
                }
            )

        if not doc:
            return {
                "ok": False,
                "error": "No analysis document found for this account",
            }, 404

        # ---- 2) Update EXACT document via unique-key filter (no upsert)
        filt = {
            "scope": doc.get("scope"),
            "owner_id": doc.get("owner_id"),
            "window.start": doc["window"]["start"],
            "window.end": doc["window"]["end"],
        }

        res = analysis_col.update_one(
            filt,
            {"$set": {"start_date": dt, "updated_at": now}},
            upsert=False,  # ⛔ never create a new doc here
        )

        # Optional cache bust
        if cache:
            try:
                cache.delete(_ck(uid))
            except Exception:
                pass

        return {
            "ok": True,
            "matched": res.matched_count,
            "modified": res.modified_count,
            "updated_doc_id": str(doc["_id"]),
            "owner_id": (
                str(owner_value) if isinstance(owner_value, ObjectId) else owner_value
            ),
            "start_date": dt.isoformat() + "Z",
            "updated_at": now.isoformat() + "Z",
        }, 200


@ns.route("/metrics_update")
class MetricsUpdate(Resource):
    method_decorators = [auth_superadmin]

    def post(self):
        payload = request.get_json(silent=True) or {}
        sid = g.current_user_id
        oid = try_object_id(sid)

        # Use superadmin_id as the unique key for upsert
        match = {"superadmin_id": oid or sid}

        update_fields = {}
        for field in [
            "negative_balance",
            "max_trades",
            "average_trading_volume",
            "win_rate_percentage",
        ]:
            if field in payload:
                update_fields[field] = payload[field]

        if not update_fields:
            return {"ok": False, "error": "No valid fields provided"}, 400

        res = data_col.update_one(
            match,
            {
                "$set": {
                    "superadmin_id": oid or sid,
                    **update_fields,
                    "updated_at": _dt.utcnow(),
                }
            },
            upsert=True,
        )

        return {
            "ok": True,
            "matched": res.matched_count,
            "modified": res.modified_count,
            "upserted_id": str(res.upserted_id) if res.upserted_id else None,
            "superadmin_id": sid,
            "updated_fields": list(update_fields.keys()),
        }, 200


# ==================== NEW: Top-Risk Users ====================


def _parse_iso(s: str | None) -> _dt | None:
    if not s:
        return None
    s2 = s.strip().replace("Z", "+00:00")
    try:
        return _dt.fromisoformat(s2)
    except Exception:
        # try simple date "YYYY-MM-DD"
        try:
            return _dt.strptime(s.strip(), "%Y-%m-%d")
        except Exception:
            return None


@ns.route("/users/top-risk")
class UsersTopRisk(Resource):
    method_decorators = [auth_superadmin]

    def get(self):
        try:
            superadmin_id = request.args.get("superadmin_id")
            start = _parse_iso(request.args.get("start"))
            end = _parse_iso(request.args.get("end"))
            min_score = float((request.args.get("min_score") or "0").strip() or "0")

            # Build pipeline with very high limit once
            pipeline = build_top_risk_users_pipeline(
                limit=1000,  # fetch enough to slice
                superadmin_id=superadmin_id,
                start=start,
                end=end,
                min_score=min_score,
            )

            all_rows = list(analysis_users_col.aggregate(pipeline, allowDiskUse=True))

            # Slice into 3 categories
            rows_10 = all_rows[:10]
            rows_50 = all_rows[:50]
            rows_100 = all_rows[:100]

            return {
                "ok": True,
                "count_total": len(all_rows),
                "top_10": _safe(rows_10),
                "top_50": _safe(rows_50),
                "top_100": _safe(rows_100),
            }, 200

        except Exception as e:
            return {"ok": False, "error": str(e)}, 500


@ns.route("/user-list")
class UserList(Resource):
    """
    GET /analysis/user-list?limit=10
    Returns N random active (status=1) users from analysis_users.
    """

    method_decorators = [auth_superadmin]

    def get(self):
        try:
            try:
                limit = int(request.args.get("limit", 10))
            except (TypeError, ValueError):
                limit = 10
            limit = max(1, min(limit, 50))

            pipeline = [
                {"$match": {"status": 1}},
                {"$sample": {"size": limit}},
                {
                    "$project": {
                        "_id": 1,
                        "superadmin_id": 1,
                        "user_id": 1,
                        "email": 1,
                        "name": 1,
                        "status": 1,
                        "total_trades": 1,
                        "win_trades": 1,
                        "win_percent": 1,
                        "total_volume": 1,
                        "balance": 1,
                        "avg_risk_score": 1,
                        "avg_risk_status": 1,
                        "generated_at": 1,
                        "window": 1,
                    }
                },
            ]

            docs = list(analysis_users_col.aggregate(pipeline, allowDiskUse=True))
            return {
                "ok": True,
                "count": len(docs),
                "limit": limit,
                "items": _safe(docs),
            }, 200

        except Exception as e:
            return {"ok": False, "error": str(e)}, 500
