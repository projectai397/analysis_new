# src/helpers/trade_kpis.py  (REPLACEMENT: read from `orders`, compute from grouped trades)
from __future__ import annotations
from typing import Dict, Any, List, Optional, Iterable, Tuple
from bson import ObjectId
from math import isfinite
from collections import defaultdict
from datetime import datetime

# Handles exposed by your app
from ..config import orders, config, users  # positions not needed
# ^ we will read exclusively from `orders`

# ======================== Small helpers ========================

def _lower(v: Optional[str]) -> str:
    return (v or "").strip().lower()

def _f(v) -> float:
    try:
        x = float(v or 0)
        return x if isfinite(x) else 0.0
    except Exception:
        return 0.0

def _absf(v) -> float:
    x = _f(v)
    return -x if x < 0 else x

def _sid(x: Any) -> str:
    return str(x)

def _as_dt(x: Any) -> Optional[datetime]:
    if isinstance(x, datetime): return x
    if x is None: return None
    s = str(x)
    try: return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception: return None

def _get_balance(user_id: ObjectId) -> float:
    """Best-effort wallet balance lookup; returns 0 if wallets not wired up."""
    if users is None:
        return 0.0
    doc = users.find_one({"_id": user_id}, {"balance": 1})
    return _f(doc.get("balance")) if doc else 0.0

# ======================== Normalization (orders -> trade-like rows) ========================

def _norm_user_id(d: Dict[str, Any]):
    return d.get("userId")

def _norm_symbol_id(d: Dict[str, Any]):
    return d.get("symbolId")

def _norm_symbol_label(d: Dict[str, Any]):
    return d.get("symbolName") 

def _norm_side(d: Dict[str, Any]) -> str:
    return _lower(d.get("tradeType"))

def _norm_time(d: Dict[str, Any]):
    return d.get("executionDateTime")

def _norm_price(d: Dict[str, Any]) -> float:
    if d.get("price") is not None:
        return _f(d.get("price"))
    if _lower(d.get("tradeType")) == "buy" and d.get("open_price") is not None:
        return _f(d.get("open_price"))
    if _lower(d.get("tradeType")) == "sell" and d.get("close_price") is not None:
        return _f(d.get("close_price"))
    # generic fallback
    return _f(d.get("open_price")) or _f(d.get("close_price"))

def _norm_qty(d: Dict[str, Any]) -> float:
    return _f(d.get("quantity"))

def _norm_lot(d: Dict[str, Any]) -> float:
    return _f(d.get("lotSize") or 1)

def _norm_prod(d: Dict[str, Any]):
    return d.get("productType")  # may be None for orders; grouping works without it

def _norm_parent(d: Dict[str, Any]):
    return d.get("tradeParentId") or d.get("parentId") or d.get("order_parent_id")

def _normalize_order(d: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert an `orders` row into the unified trade-like shape used by grouping.
    Fields used downstream:
      _id, userId, symbolId, symbolName/symbolTitle, productType, tradeType,
      price, quantity, lotSize, executionDateTime, tradeParentId
    """
    return {
        "_id": d.get("_id"),
        "userId": _norm_user_id(d),
        "symbolId": _norm_symbol_id(d),
        "symbolName": d.get("symbolName"),
        "symbolTitle": d.get("symbolTitle") or d.get("script"),
        "productType": _norm_prod(d),
        "tradeType": _norm_side(d),  # "buy" | "sell"
        "price": _norm_price(d),
        "quantity": _norm_qty(d),
        "lotSize": _norm_lot(d),
        "executionDateTime": _norm_time(d),
        "tradeParentId": _norm_parent(d),
        # pass through for convenience
        "status": d.get("status", "executed"),
    }
Doc = Dict[str, Any]

def group_connected_trades(
    docs: Iterable[Doc],
    *,
    key_fields: Tuple[str, str, str] = ("userId", "symbolId", "productType"),
    time_field: str = "executionDateTime",
) -> List[Dict[str, Any]]:
    """
    Groups trades that are connected (one BUY parent + SELL children).
    Respects tradeParentId when present, otherwise FIFO within (userId, symbolId, productType).
    """
    buckets: Dict[Tuple[str, str, str], List[Doc]] = defaultdict(list)
    for d in docs:
        buckets[tuple(_sid(d.get(k)) for k in key_fields)].append(d)

    groups_by_parent: List[Dict[str, Any]] = []

    def _ensure_group(buy_doc: Doc, key) -> Dict[str, Any]:
        q = _f(buy_doc.get("quantity"))
        return {
            "parentId": buy_doc.get("_id"),
            "key": key,
            "buy": buy_doc,
            "sells": [],
            "matches": [],
            "total_buy_qty": q,
            "total_sell_qty": 0.0,
            "remaining_qty": q,
        }

    for key, rows in buckets.items():
        rows.sort(key=lambda d: (_as_dt(d.get(time_field)) or datetime.min, _sid(d.get("_id"))))

        buy_docs_by_id: Dict[str, Doc] = {}
        for d in rows:
            if _lower(d.get("tradeType")) == "buy":
                buy_docs_by_id[_sid(d.get("_id"))] = d

        open_buys: List[Dict[str, Any]] = []  # [{parent_id,buy_price,remaining,group}]
        group_for_buy: Dict[str, Dict[str, Any]] = {}

        def attach_to_parent(ob, sell_doc, match_qty, sell_price):
            grp = ob["group"]
            grp["total_sell_qty"] += match_qty
            grp["remaining_qty"]  -= match_qty
            if sell_doc not in grp["sells"]:
                grp["sells"].append(sell_doc)
            grp["matches"].append({
                "sellId": sell_doc.get("_id"),
                "matched_qty": match_qty,
                "buy_price": ob["buy_price"],
                "sell_price": sell_price,
            })

        for d in rows:
            side = _lower(d.get("tradeType"))
            qty  = _f(d.get("quantity"))
            px   = _f(d.get("price"))

            if side == "buy":
                pid = _sid(d.get("_id"))
                grp = group_for_buy.get(pid)
                if grp is None:
                    grp = _ensure_group(d, key)
                    group_for_buy[pid] = grp
                    groups_by_parent.append(grp)
                open_buys.append({"parent_id": pid, "buy_price": px, "remaining": qty, "group": grp})
                continue

            if side != "sell" or qty <= 0:
                continue

            remaining = qty
            directed  = d.get("tradeParentId")

            # try explicit parent first
            if directed:
                target_pid = _sid(directed)
                if target_pid in buy_docs_by_id and target_pid not in group_for_buy:
                    grp = _ensure_group(buy_docs_by_id[target_pid], key)
                    group_for_buy[target_pid] = grp
                    groups_by_parent.append(grp)
                for ob in open_buys:
                    if ob["parent_id"] == target_pid and ob["remaining"] > 0:
                        m = min(remaining, ob["remaining"])
                        if m > 0:
                            ob["remaining"] -= m
                            attach_to_parent(ob, d, m, px)
                            remaining -= m
                        break

            # spill remainder via FIFO
            while remaining > 1e-12 and open_buys:
                ob = open_buys[0]
                m = min(remaining, ob["remaining"])
                if m <= 0:
                    if ob["remaining"] <= 1e-12:
                        open_buys.pop(0)
                    else:
                        break
                else:
                    ob["remaining"] -= m
                    attach_to_parent(ob, d, m, px)
                    remaining -= m
                    if ob["remaining"] <= 1e-12:
                        open_buys.pop(0)

    return groups_by_parent

def compute_group_pnl(group: Dict[str, Any]) -> float:
    """
    PnL for one round-trip (BUY group) using:
        (sell_price − buy_price) × matched_qty × lotSize
    """
    buy_doc  = group["buy"]
    lot_size = _f(buy_doc.get("lotSize") or 1.0)
    pnl = 0.0
    for m in group.get("matches", []):
        pnl += (_f(m["sell_price"]) - _f(m["buy_price"])) * _f(m["matched_qty"]) * lot_size
    return round(pnl, 2)

def _group_entry_notional(g: Dict[str, Any]) -> float:
    b = g["buy"]
    return _f(b.get("quantity")) * _f(b.get("price")) * _f(b.get("lotSize") or 1.0)

# ======================== Source fetch (orders) ========================

def _closed_groups(match: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Read from `orders`, normalize to trade-like rows, group, and return CLOSED groups (with any sells matched).
    """
    q = {"status": "executed"}
    if match:
        q.update(match)

    # Project the fields we may normalize from
    proj = {
        "_id": 1,
        # ids / keys
        "userId": 1, "user_id": 1,
        "symbolId": 1, "symbolName": 1, "symbolTitle": 1, "script": 1,
        "productType": 1,
        # side / prices / qty
        "tradeType": 1, "side": 1, "price": 1, "open_price": 1, "close_price": 1, "quantity": 1,
        # misc
        "lotSize": 1, "executionDateTime": 1, "created_at": 1,
        "tradeParentId": 1, "parentId": 1, "order_parent_id": 1,
        "status": 1, "leverage": 1,
    }
    raw = list(orders.find(q, proj))

    # normalize into trade-shaped docs
    docs = []
    for d in raw:
        nd = _normalize_order(d)
        if _lower(nd.get("status")) == "executed":
            docs.append(nd)

    groups = group_connected_trades(docs)
    return [g for g in groups if _f(g.get("total_sell_qty")) > 0]  # CLOSED groups only

# ======================== KPIs (from grouped trades) ========================

def overall_kpis(match: Dict[str, Any], limit: int = 10, *, start: datetime | None = None, end: datetime | None = None) -> Dict[str, Any]:
    """
    KPIs from grouped trades (built from `orders`),
    plus top-10 biggest single BUY/SELL orders by notional.
    """
    groups = _closed_groups({**match, **_time_between_q(start, end, "executionDateTime")})

    total_trades = 0
    win_trades = 0
    total_volume = 0.0
    per_user: Dict[Any, Dict[str, float]] = {}

    for g in groups:
        buy_doc   = g["buy"]
        buy_qty   = _f(buy_doc.get("quantity"))
        buy_price = _f(buy_doc.get("price"))
        lot_size  = _f(buy_doc.get("lotSize") or 1.0)

        pnl = compute_group_pnl(g)

        total_trades += 1
        if pnl > 0:
            win_trades += 1

        vol = buy_qty * buy_price * lot_size
        uid = buy_doc.get("userId")
        s = per_user.setdefault(uid, {"total_trades": 0.0, "win_trades": 0.0, "total_volume": 0.0})
        s["total_trades"] += 1
        if pnl > 0:
            s["win_trades"] += 1
        s["total_volume"] += vol
        total_volume += vol

    win_percent = round((win_trades / total_trades) * 100.0, 2) if total_trades else 0.0

    # risk score block (same math; uses per-user totals computed above)
    risk_scores: List[float] = []
    for uid, s in per_user.items():
        u_total = s["total_trades"]
        u_win   = s["win_trades"]
        u_vol   = s["total_volume"]

        u_win_percent = (u_win / u_total * 100.0) if u_total > 0 else 0.0
        balance = _get_balance(uid)

        max_trades_cap = 150 if balance >= 1_000_000 else 50
        max_volume_cap = 100_000_000 if balance >= 1_000_000 else 10_000_000

        trades_norm = min((u_total / max_trades_cap) if max_trades_cap else 0.0, 1.0)
        volume_norm = min((u_vol   / max_volume_cap) if max_volume_cap else 0.0, 1.0)

        win_risk = 1.0 if u_win_percent >= 70 else max(min(u_win_percent / 100.0, 1.0), 0.0)
        risk0_1 = (win_risk * 0.4) + (trades_norm * 0.3) + (volume_norm * 0.3)
        risk_scores.append(round(risk0_1 * 10.0, 1))

    avg_risk_score = round(sum(risk_scores) / len(risk_scores), 1) if risk_scores else 0.0
    if avg_risk_score < 4:
        avg_risk_status = "Low Risk"
    elif avg_risk_score < 7:
        avg_risk_status = "Medium Risk"
    else:
        avg_risk_status = "High Risk"

    biggest_buys  = top_biggest_buy_trades(limit, match, start=start, end=end)
    biggest_sells = top_biggest_sell_trades(limit, match, start=start, end=end)

    return {
        "total_trades": total_trades,
        "win_trades": win_trades,
        "total_volume": round(total_volume, 2),
        "win_percent": win_percent,
        "avg_risk_score": avg_risk_score,
        "avg_risk_status": avg_risk_status,
        # extra lists requested:
        "top_10_biggest_buy_trades": biggest_buys,
        "top_10_biggest_sell_trades": biggest_sells,
    }

# ======================== Rankings (from grouped trades) ========================

def top_profitable(limit: int, match: Dict[str, Any]):
    """Top N closed trades by PnL descending."""
    groups = _closed_groups(match)
    rows = []
    for g in groups:
        pnl = compute_group_pnl(g)
        b = g["buy"]
        rows.append({
            "parentId": g["parentId"],
            "userId": b.get("userId"),
            "symbolId": b.get("symbolId"),
            "symbolName": b.get("symbolTitle") or b.get("symbolName"),
            "productType": b.get("productType"),
            "pnl": pnl,
            "tradeValue": _group_entry_notional(g),
        })
    rows.sort(key=lambda r: r["pnl"], reverse=True)
    return rows[: max(0, int(limit))]

def top_loser(limit: int, match: Dict[str, Any]):
    """Top N closed trades by PnL ascending (most negative first)."""
    groups = _closed_groups(match)
    rows = []
    for g in groups:
        pnl = compute_group_pnl(g)
        b = g["buy"]
        rows.append({
            "parentId": g["parentId"],
            "userId": b.get("userId"),
            "symbolId": b.get("symbolId"),
            "symbolName": b.get("symbolTitle") or b.get("symbolName"),
            "productType": b.get("productType"),
            "pnl": pnl,
            "tradeValue": _group_entry_notional(g),
        })
    rows.sort(key=lambda r: r["pnl"])  # ascending
    return rows[: max(0, int(limit))]

def top_biggest_trades(limit: int, match: Dict[str, Any]):
    """
    Top N biggest CLOSED trades by entry notional (buy_qty * buy_price * lotSize).
    """
    groups = _closed_groups(match)
    rows = []
    for g in groups:
        b = g["buy"]
        rows.append({
            "parentId": g["parentId"],
            "userId": b.get("userId"),
            "symbolId": b.get("symbolId"),
            "symbolName": b.get("symbolTitle") or b.get("symbolName"),
            "productType": b.get("productType"),
            "tradeValue": _group_entry_notional(g),
            "pnl": compute_group_pnl(g),
        })
    rows.sort(key=lambda r: r["tradeValue"], reverse=True)
    return rows[: max(0, int(limit))]

def most_traded_scripts(limit: int, match: Dict[str, Any]):
    """Highest number of CLOSED trades per script (counting grouped trades)."""
    groups = _closed_groups(match)
    counts: Dict[str, int] = {}
    labels: Dict[str, str] = {}
    for g in groups:
        b = g["buy"]
        sid = _sid(b.get("symbolId"))
        label = b.get("symbolTitle") or b.get("symbolName") or sid
        counts[sid] = counts.get(sid, 0) + 1
        labels[sid] = label
    pairs = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[: max(0, int(limit))]
    return [{"symbolId": sid, "script": labels[sid], "totalTrades": c} for sid, c in pairs]

def least_traded_scripts(limit: int, match: Dict[str, Any]):
    """Lowest number of CLOSED trades per script (ties by script label)."""
    groups = _closed_groups(match)
    counts: Dict[str, int] = {}
    labels: Dict[str, str] = {}
    for g in groups:
        b = g["buy"]
        sid = _sid(b.get("symbolId"))
        label = b.get("symbolTitle") or b.get("symbolName") or sid
        counts[sid] = counts.get(sid, 0) + 1
        labels[sid] = label
    pairs = sorted(counts.items(), key=lambda kv: (kv[1], labels[kv[0]]))[: max(0, int(limit))]
    return [{"symbolId": sid, "script": labels[sid], "totalTrades": c} for sid, c in pairs]

# ======================== Single-order rankings (NEW) ========================

def _time_between_q(start: datetime | None, end: datetime | None, field: str = "executionDateTime"):
    if not start or not end:
        return {}
    return {field: {"$gte": start, "$lt": end}}

def _top_biggest_single_side(
    limit: int,
    match: Dict[str, Any],
    side: str,
    *,
    start: datetime | None = None,
    end: datetime | None = None,
):
    """
    Top-N individual orders by entry notional for a given side ('buy' or 'sell').
    Filters:
      - status='executed'
      - executionDateTime in [start, end) if provided
      - plus anything in `match` (user/symbol filters etc.)
    """
    q = {"status": "executed"}
    if match:
        q.update(match)
    # add explicit time window if given
    q.update(_time_between_q(start, end, field="executionDateTime"))

    proj = {
        "_id": 1,
        "userId": 1,
        "symbolId": 1, "symbolName": 1, "symbolTitle": 1, "script": 1,
        "productType": 1,
        "tradeType": 1, "side": 1,
        "price": 1, "open_price": 1, "close_price": 1, "quantity": 1,
        "lotSize": 1,
        "executionDateTime": 1,
    }

    rows: List[Dict[str, Any]] = []
    for d in orders.find(q, proj):
        nd = _normalize_order(d)
        s  = _lower(nd.get("tradeType"))
        if s != side:
            continue
        qty  = _f(nd.get("quantity"))
        px   = _f(nd.get("price"))
        lot  = _f(nd.get("lotSize") or 1.0)
        notional = qty * px * lot
        if notional <= 0:
            continue
        rows.append({
            "orderId": nd.get("_id"),
            "userId": nd.get("userId"),
            "symbolId": nd.get("symbolId"),
            "symbolName": nd.get("symbolTitle") or nd.get("symbolName"),
            "productType": nd.get("productType"),
            "side": s,  # 'buy' or 'sell'
            "tradeValue": round(notional, 2),
            "price": px,
            "quantity": qty,
            "lotSize": lot,
            "executionDateTime": nd.get("executionDateTime"),
        })

    rows.sort(key=lambda r: r["tradeValue"], reverse=True)
    return rows[: max(0, int(limit))]

def top_biggest_buy_trades(
    limit: int,
    match: Dict[str, Any],
    *,
    start: datetime | None = None,
    end: datetime | None = None,
):
    """Top-N individual BUY orders by entry notional in [start, end)."""
    return _top_biggest_single_side(limit, match, "buy", start=start, end=end)

def top_biggest_sell_trades(
    limit: int,
    match: Dict[str, Any],
    *,
    start: datetime | None = None,
    end: datetime | None = None,
):
    """Top-N individual SELL orders by entry notional in [start, end)."""
    return _top_biggest_single_side(limit, match, "sell", start=start, end=end)
