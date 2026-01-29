from typing import Dict, Any, List, Iterable, Tuple
from datetime import datetime
from bson import ObjectId
import heapq
from ..config import transactions, users

# ----------------- helpers -----------------

def _user_id_match_or(user_ids: List[ObjectId]) -> Dict[str, Any]:
    if not user_ids:
        return {"userId": {"$in": [ObjectId("000000000000000000000000")]}}
    return {
        "$or": [
            {"userId": {"$in": user_ids}},
            {"userId": {"$in": [str(u) for u in user_ids]}},
        ]
    }

def _base_query(user_ids: List[ObjectId], start: datetime, end: datetime) -> Dict[str, Any]:
    return {
        **_user_id_match_or(user_ids),
        "status": 1,  # completed
        "createdAt": {"$gte": start, "$lt": end},
    }

def _project_fields() -> Dict[str, int]:
    return {
        "_id": 1,
        "userId": 1,
        "transactionType": 1,  # credit|debit
        "amount": 1,
        "to": 1,
        "from": 1,
        "createdAt": 1,
    }

def _to_float(v: Any) -> float:
    try:
        return float(v or 0)
    except Exception:
        return 0.0

def _lower(s: Any) -> str:
    return (s or "").lower()

def _uid_str(v: Any) -> str:
    return str(v) if v is not None else ""

def _iter_transactions(user_ids: List[ObjectId], start: datetime, end: datetime) -> Iterable[Dict[str, Any]]:
    q = _base_query(user_ids, start, end)
    return transactions.find(q, _project_fields()).batch_size(500)

def _format_tx_doc(d: Dict[str, Any]) -> Dict[str, Any]:
    created = d.get("createdAt")
    return {
        "tx_id": _uid_str(d.get("_id")),
        "user_id": _uid_str(d.get("userId")),
        "amount": round(_to_float(d.get("amount")), 2),
        "transactionType": d.get("transactionType"),
        "to": _uid_str(d.get("to")) if d.get("to") else None,
        "from": _uid_str(d.get("from")) if d.get("from") else None,
        "created_at": created.isoformat() + "Z" if isinstance(created, datetime) else None,
    }

def _created_ts(d: Dict[str, Any]) -> float:
    c = d.get("createdAt")
    return float(c.timestamp()) if isinstance(c, datetime) else 0.0

def _id_str_safe(d: Dict[str, Any]) -> str:
    _id = d.get("_id")
    return str(_id) if _id is not None else ""

def _user_name_map(user_ids: List[Any]) -> Dict[Any, str]:
    if users is None or not user_ids:
        return {}
    oids = []
    for uid in user_ids:
        if uid is None:
            continue
        try:
            oids.append(uid if isinstance(uid, ObjectId) else ObjectId(uid))
        except Exception:
            pass
    if not oids:
        return {}
    out: Dict[Any, str] = {}
    for doc in users.find({"_id": {"$in": oids}}, {"_id": 1, "name": 1, "userName": 1, "username": 1}):
        oid = doc.get("_id")
        name = (doc.get("userName") or doc.get("name") or doc.get("username") or "")
        out[oid] = name
        out[str(oid)] = name
    return out

# ----------------- overall summary -----------------

def overall_transactions_for_users(user_ids: List[ObjectId], start: datetime, end: datetime) -> Dict[str, Any]:
    """
    Summarize deposits (credits), withdrawals (debits), net_balance = deposits - withdrawals.
    """
    if not user_ids:
        return {"total_deposits": 0.0, "total_withdrawals": 0.0, "net_balance": 0.0, "tx_count": 0}

    total_credit = 0.0
    total_debit = 0.0
    tx_count = 0

    for d in _iter_transactions(user_ids, start, end):
        tx_count += 1
        ttype = _lower(d.get("transactionType"))
        amt = _to_float(d.get("amount"))
        if amt <= 0:
            continue
        if ttype == "credit":
            total_credit += amt
        elif ttype == "debit":
            total_debit += amt

    net_balance = total_credit - total_debit

    return {
        "total_deposits": round(total_credit, 2),
        "total_withdrawals": round(total_debit, 2),
        "net_balance": round(net_balance, 2),
        "tx_count": tx_count,
    }

# ----------------- top-N deposits -----------------

def top_biggest_deposits(limit: int, user_ids: List[ObjectId], start: datetime, end: datetime):
    if not user_ids or limit <= 0:
        return []

    top: List[Tuple[float, float, str, Dict[str, Any]]] = []
    for d in _iter_transactions(user_ids, start, end):
        if _lower(d.get("transactionType")) != "credit":
            continue
        amt = _to_float(d.get("amount"))
        if amt <= 0:
            continue
        heapq.heappush(top, (amt, _created_ts(d), _id_str_safe(d), d))
        if len(top) > limit:
            heapq.heappop(top)

    top_sorted = sorted(top, key=lambda x: (x[0], x[1], x[2]), reverse=True)
    rows = [_format_tx_doc(d) for _, _, _, d in top_sorted]
    name_map = _user_name_map([r["user_id"] for r in rows])
    for r in rows:
        r["userName"] = name_map.get(r["user_id"]) or ""
    return rows

# ----------------- top-N withdrawals -----------------

def top_biggest_withdrawals(limit: int, user_ids: List[ObjectId], start: datetime, end: datetime):
    if not user_ids or limit <= 0:
        return []

    top: List[Tuple[float, float, str, Dict[str, Any]]] = []
    for d in _iter_transactions(user_ids, start, end):
        if _lower(d.get("transactionType")) != "debit":
            continue
        amt = _to_float(d.get("amount"))
        if amt <= 0:
            continue
        heapq.heappush(top, (amt, _created_ts(d), _id_str_safe(d), d))
        if len(top) > limit:
            heapq.heappop(top)

    top_sorted = sorted(top, key=lambda x: (x[0], x[1], x[2]), reverse=True)
    rows = [_format_tx_doc(d) for _, _, _, d in top_sorted]
    name_map = _user_name_map([r["user_id"] for r in rows])
    for r in rows:
        r["userName"] = name_map.get(r["user_id"]) or ""
    return rows
