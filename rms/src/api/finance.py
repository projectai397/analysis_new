# src/routes/finance_routes.py
from ast import Dict
from datetime import datetime, timedelta
from typing import Any
from flask import jsonify, request, g
from flask_restx import Namespace, Resource
from bson import ObjectId
from src.helpers.util import auth_superadmin
from src.helpers.finance_service import finance_summary_for_actor
from collections import Counter
from src.config import orders, exchange, wallets, users
from src.helpers.util import ist_week_window_weekly
from src.helpers.metrics_service import group_connected_trades
from src.config import config

ns = Namespace("finance")


def _parse_dt(s):
    if not s:
        return None
    s = s.strip().replace("Z", "+00:00") if s.endswith("Z") else s
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


@ns.route("/finance/summary")
class FinanceSummary(Resource):
    method_decorators = [auth_superadmin]

    def get(self):
        # accept either query params or JSON body
        data = request.get_json(silent=True) or {}
        start = _parse_dt(data.get("start") or request.args.get("start"))
        end = _parse_dt(data.get("end") or request.args.get("end"))

        # current authenticated actor (superadmin/admin/master)
        actor_oid = ObjectId(g.current_user_id)

        summary = finance_summary_for_actor(actor_oid, start, end)
        return {"ok": True, "summary": summary}, 200


@ns.route("/exchange-trade-counts")
class ExchangeTradeCounts(Resource):
    method_decorators = [auth_superadmin]

    def get(self):
        """
        Exchange-wise trade counts for the current IST week window,
        computed via grouped trades (BUY parent + SELL children).
        """
        start, end = ist_week_window_weekly()  # UTC datetimes

        # Pull only fields the grouper needs (and exchangeId)
        cursor = orders.find(
            {"executionDateTime": {"$gte": start, "$lt": end}},
            {
                "_id": 1,
                "userId": 1,
                "symbolId": 1,
                "productType": 1,
                "executionDateTime": 1,
                "tradeType": 1,
                "quantity": 1,
                "price": 1,
                "tradeParentId": 1,
                "exchangeId": 1,
            },
        )

        docs = list(cursor)

        # Group using your FIFO/parent-aware function
        groups = group_connected_trades(
            docs,
            key_fields=("userId", "symbolId", "productType"),
            time_field="executionDateTime",
        )

        # Count per exchangeId based on groups.
        # For each group (one BUY parent + SELL children), we count:
        #   1 (the buy) + len(sells)  == total order docs in that group
        counter = Counter()
        for g in groups:
            buy_doc = g.get("buy") or {}
            exid = buy_doc.get("exchangeId")
            if isinstance(exid, ObjectId):
                counter[str(exid)] += 1 + len(g.get("sells", []))

        # Resolve exchange names
        name_map = {}
        if counter:
            ex_docs = exchange.find({"_id": {"$in": [ObjectId(e) for e in counter.keys()]}})
            for ex in ex_docs:
                name_map[str(ex["_id"])] = ex.get("name", "<unknown>")

        total_count = sum(counter.values())
        denom = total_count or 1  # avoid div-by-zero

        rows = []
        for eid, cnt in counter.most_common():
            rows.append(
                {
                    "exchangeId": eid,
                    "name": name_map.get(eid, "<unknown>"),
                    "count": cnt,
                    "pct": round((cnt * 100.0) / denom, 2),
                }
            )

        return jsonify(
            {
                "ok": True,
                "from": start.isoformat(),
                "to": end.isoformat(),
                "total": total_count if rows else 0,
                "exchanges": rows,
            }
        )


def _as_dt(val):
    """Convert MongoDB datetime/string to Python datetime (UTC-aware)."""
    if isinstance(val, datetime):
        return val
    if isinstance(val, str):
        try:
            return datetime.fromisoformat(val)
        except Exception:
            return None
    return None


def _f(val, default=0.0):
    """Convert to float safely."""
    try:
        return float(val)
    except Exception:
        return default


def _lower(val):
    """Lowercase string safely."""
    return str(val).lower() if val is not None else ""


def _sid(val):
    """Convert ObjectId or anything to string id."""
    if isinstance(val, ObjectId):
        return str(val)
    return str(val) if val is not None else None


def _resolve_user_balance(u_id: ObjectId, u_doc: dict) -> float:
    # 1) If already present on the user doc that was passed in, use it.
    if u_doc.get("balance") is not None:
        try:
            return float(u_doc["balance"])
        except Exception:
            pass

    # 2) Re-read a minimal user doc (in case the caller used a projection that omitted balance).
    doc = users.find_one({"_id": u_id}, {"balance": 1})
    if doc and doc.get("balance") is not None:
        try:
            return float(doc["balance"])
        except Exception:
            pass

    # 3) Optional fallback to wallets collection (handles schemas that store it there).
    w = wallets.find_one(
        {
            "$or": [
                {"userId": u_id},
                {"user_id": u_id},
                {"userId": {"$eq": str(u_id)}},
                {"user_id": {"$eq": str(u_id)}},
            ]
        },
        {"balance": 1},
    )
    if w and w.get("balance") is not None:
        try:
            return float(w["balance"])
        except Exception:
            pass

    return 0.0


# =====================
# Main API endpoint
# =====================


@ns.route("/weekly-trade-volume")
class WeeklyTradeVolume(Resource):
    method_decorators = [auth_superadmin]

    def get(self):
        """
        Daily trade volume for the current IST week window (Mon 00:00 IST → now).
        Uses grouped trades (BUY parent + SELL children).

        Query params:
          - include_sells=1 | true | yes (optional) → add SELL notional too
        """
        include_sells = str(request.args.get("include_sells", "0")).lower() in ("1", "true", "yes")

        # --- Window ---
        start_ist, end_ist = ist_week_window_weekly()
        tz = config.APP_TZ

        # --- Fetch raw trades ---
        cursor = orders.find(
            {
                "status": "executed",
                "executionDateTime": {"$gte": start_ist, "$lt": end_ist},
            },
            {
                "_id": 1,
                "userId": 1,
                "symbolId": 1,
                "productType": 1,
                "executionDateTime": 1,
                "tradeType": 1,
                "quantity": 1,
                "price": 1,
                "lotSize": 1,
                "tradeParentId": 1,
            },
        )
        docs = list(cursor)

        # --- Group into BUY+SELLs ---
        groups = group_connected_trades(
            docs,
            key_fields=("userId", "symbolId", "productType"),
            time_field="executionDateTime",
        )

        # --- Build Mon..Sun buckets ---
        week_days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        monday_date = start_ist.astimezone(tz).date()
        buckets = {
            d: {
                "day": d,
                "date": (monday_date + timedelta(days=i)).isoformat(),
                "volume": 0.0,
                "groups": 0,
            }
            for i, d in enumerate(week_days)
        }

        # --- Aggregate per group ---
        for g in groups:
            buy_doc = g.get("buy") or {}
            dt = _as_dt(buy_doc.get("executionDateTime"))
            if not dt:
                continue

            day_key = dt.astimezone(tz).strftime("%a")  # Mon..Sun

            lot_size = _f(buy_doc.get("lotSize") or 1.0)
            buy_qty = _f(buy_doc.get("quantity"))
            buy_px = _f(buy_doc.get("price"))
            vol = buy_qty * buy_px * lot_size

            if include_sells:
                for s in g.get("sells", []):
                    vol += (
                        _f(s.get("quantity"))
                        * _f(s.get("price"))
                        * _f(s.get("lotSize") or lot_size)
                    )

            if day_key in buckets:
                buckets[day_key]["volume"] += vol
                buckets[day_key]["groups"] += 1

        # --- Flatten ---
        days = [
            {
                "day": d,
                "date": buckets[d]["date"],
                "volume": round(buckets[d]["volume"], 2),
                "groups": buckets[d]["groups"],
            }
            for d in week_days
        ]

        total_vol = round(sum(x["volume"] for x in days), 2)
        total_groups = sum(x["groups"] for x in days)

        return jsonify(
            {
                "ok": True,
                "from": start_ist.isoformat(),
                "to": end_ist.isoformat(),
                "totalVolume": total_vol,
                "totalGroups": total_groups,
                "currency": "INR",
                "days": days,
            }
        )


def _month_start_ist(dt, tz):
    """Return IST month start (1st 00:00) for the given datetime."""
    dt_ist = dt.astimezone(tz)
    return dt_ist.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _add_months(dt_ist, n):
    """Add n months to an IST date/datetime safely (keeps at day=1)."""
    y = dt_ist.year + (dt_ist.month - 1 + n) // 12
    m = (dt_ist.month - 1 + n) % 12 + 1
    return dt_ist.replace(year=y, month=m, day=1)


@ns.route("/monthly-trade-volume")
class MonthlyTradeVolume(Resource):
    method_decorators = [auth_superadmin]

    def get(self):
        """
        Month-wise trade volume for the *last N months including current month-to-date*.
        Default N=5. Window is computed in IST. Volume is:
            buy_qty * buy_price * lotSize  (per BUY parent group)
        Optional: include_sells=1 to also add SELL legs' notional.

        Response:
        {
          ok, from, to, totalVolume,
          months: [
            { ym: "2025-06", label: "Jun 2025", start: "...", end: "...",
              volume: 123.45, groups: 27 }
          ]
        }
        """
        tz = config.APP_TZ
        include_sells = str(request.args.get("include_sells", "0")).lower() in ("1", "true", "yes")
        months_back = int(request.args.get("months", 5))
        if months_back < 1:
            months_back = 1
        if months_back > 24:
            months_back = 24  # hard cap

        now_ist = datetime.now(tz)
        # Start at the 1st of the month N-1 months ago
        start_month_ist = _add_months(_month_start_ist(now_ist, tz), -(months_back - 1))
        # Query end is "now"
        end_ist = now_ist

        # Query only executed orders in this overall range
        cursor = orders.find(
            {
                "status": "executed",
                "executionDateTime": {"$gte": start_month_ist, "$lt": end_ist},
            },
            {
                "_id": 1,
                "userId": 1,
                "symbolId": 1,
                "productType": 1,
                "executionDateTime": 1,
                "tradeType": 1,
                "quantity": 1,
                "price": 1,
                "lotSize": 1,
                "tradeParentId": 1,
            },
        )
        docs = list(cursor)

        # Group into BUY parents with SELL children
        groups = group_connected_trades(
            docs,
            key_fields=("userId", "symbolId", "productType"),
            time_field="executionDateTime",
        )

        # Prepare month buckets in order
        month_keys = []
        buckets = {}
        for i in range(months_back):
            mstart = _add_months(start_month_ist, i)
            mend = _add_months(start_month_ist, i + 1)
            ym = f"{mstart.year:04d}-{mstart.month:02d}"
            label = mstart.strftime("%b %Y")  # e.g., "Oct 2025"
            month_keys.append(ym)
            buckets[ym] = {
                "ym": ym,
                "label": label,
                "start": mstart.isoformat(),
                "end": (mend if mend < end_ist else end_ist).isoformat(),
                "volume": 0.0,
                "groups": 0,
                "_start_dt": mstart,
                "_end_dt": mend if mend < end_ist else end_ist,
            }

        # Aggregate each BUY group into the month of its BUY time
        for g in groups:
            buy_doc = g.get("buy") or {}
            dt = _as_dt(buy_doc.get("executionDateTime"))
            if not dt:
                continue
            dt_ist = dt.astimezone(tz)
            ym = f"{dt_ist.year:04d}-{dt_ist.month:02d}"
            if ym not in buckets:
                continue  # out of the 5-month window

            lot_size = _f(buy_doc.get("lotSize") or 1.0)
            buy_qty = _f(buy_doc.get("quantity"))
            buy_px = _f(buy_doc.get("price"))
            vol = buy_qty * buy_px * lot_size

            if include_sells:
                for s in g.get("sells", []):
                    vol += (
                        _f(s.get("quantity"))
                        * _f(s.get("price"))
                        * _f(s.get("lotSize") or lot_size)
                    )

            buckets[ym]["volume"] += vol
            buckets[ym]["groups"] += 1

        # Flatten in chronological order
        months = [
            {
                "ym": ym,
                "label": buckets[ym]["label"],
                "start": buckets[ym]["start"],
                "end": buckets[ym]["end"],
                "volume": round(buckets[ym]["volume"], 2),
                "groups": buckets[ym]["groups"],
            }
            for ym in month_keys
        ]

        total_vol = round(sum(m["volume"] for m in months), 2)

        return jsonify(
            {
                "ok": True,
                "from": (
                    buckets[month_keys[0]]["start"] if month_keys else start_month_ist.isoformat()
                ),
                "to": end_ist.isoformat(),
                "monthsBack": months_back,
                "totalVolume": total_vol,
                "currency": "INR",
                "months": months,  # feed to a LineChart
            }
        )
