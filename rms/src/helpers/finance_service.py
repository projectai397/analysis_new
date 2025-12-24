# src/helpers/finance_service.py
from typing import Dict, Any, List, Optional
from datetime import datetime
from bson import ObjectId

from ..config import transactions, positions, users, config
from src.helpers import hierarchy_service as hs


# ===== Helpers =====
def _classify_role(role_val) -> str:
    """Return 'superadmin' | 'admin' | 'master' | 'user' | 'unknown'."""
    if not role_val:
        return "unknown"
    r = str(role_val)
    if getattr(config, "USER_ROLE_ID", None) and r == str(config.USER_ROLE_ID):
        return "user"
    if getattr(config, "SUPERADMIN_ROLE_ID", None) and r == str(config.SUPERADMIN_ROLE_ID):
        return "superadmin"
    if getattr(config, "ADMIN_ROLE_ID", None) and r == str(config.ADMIN_ROLE_ID):
        return "admin"
    if getattr(config, "MASTER_ROLE_ID", None) and r == str(config.MASTER_ROLE_ID):
        return "master"
    return "unknown"


def _actor_role(actor_oid: ObjectId) -> str:
    doc = users.find_one({"_id": actor_oid}, {"role": 1})
    if not doc:
        return "unknown"
    return _classify_role(doc.get("role"))


def _user_docs(actor_oid: ObjectId) -> List[Dict[str, Any]]:
    role = _actor_role(actor_oid)
    if role == "superadmin":
        return hs.get_users_for_superadmin(actor_oid)
    if role == "admin":
        return hs.get_users_for_admin(actor_oid)
    if role == "master":
        return hs.get_users_for_master(actor_oid)
    return []


def _user_ids(actor_oid: ObjectId) -> List[ObjectId]:
    docs = _user_docs(actor_oid)
    out: List[ObjectId] = []
    for d in docs:
        uid = d.get("_id")
        if not uid:
            continue
        try:
            out.append(ObjectId(uid))
        except Exception:
            
            continue
    return out


# ===== Summary (all math done in Python) =====
def finance_summary_for_actor(
    actor_oid: ObjectId,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None
) -> Dict[str, Any]:
    """
    Role-aware finance summary:
      - superadmin -> users under that superadmin
      - admin      -> users under that admin
      - master     -> users under that master

    New transactions schema:
      - userId (ObjectId)
      - createdAt (datetime)
      - transactionType: "credit" (deposit) | "debit" (withdrawal)

    Returns:
      total_deposits, total_withdrawals, net_balance, tx_count,
      trading_volume, total_trades, window
    """
    uids = _user_ids(actor_oid)
    if not uids:
        return {
            "total_deposits": 0.0,
            "total_withdrawals": 0.0,
            "net_balance": 0.0,
            "tx_count": 0,
            "trading_volume": 0.0,
            "total_trades": 0,
            "window": {
                "start": start.isoformat() if start else None,
                "end":   end.isoformat()   if end   else None,
            },
        }

    # ---- Transactions (NEW SCHEMA) ----
    tx_q: Dict[str, Any] = {"userId": {"$in": uids}}
    if start or end:
        rng: Dict[str, Any] = {}
        if start: rng["$gte"] = start
        if end:   rng["$lt"]  = end
        tx_q["createdAt"] = rng

    total_dep = 0.0
    total_wd  = 0.0
    tx_count  = 0

    for tx in transactions.find(tx_q, {"transactionType": 1, "amount": 1}):
        tx_count += 1
        amt = abs(float(tx.get("amount") or 0))
        ttype = str(tx.get("transactionType") or "").lower()

        if ttype == "credit":
            total_dep += amt
        elif ttype == "debit":
            total_wd += amt

    net_balance = total_dep - total_wd

    # ---- Trading Volume (adjust field names if your positions schema is camelCase) ----
    pos_q: Dict[str, Any] = {"userId": {"$in": uids}}
    if start or end:
        rng: Dict[str, Any] = {}
        if start: rng["$gte"] = start
        if end:   rng["$lt"]  = end
        pos_q["createdAt"] = rng

    total_trades = 0
    total_volume = 0.0

    for pos in positions.find(pos_q, {"price": 1, "totalQuantity": 1}):
        qty = abs(float(pos.get("totalQuantity") or 0))
        price = pos.get("price")
        if price is None:
            continue
        try:
            total_volume += qty * float(price)
        except Exception:
            pass
        total_trades += 1

    return {
        "total_deposits": round(total_dep, 2),
        "total_withdrawals": round(total_wd, 2),
        "net_balance": round(net_balance, 2),
        "tx_count": tx_count,
        "trading_volume": round(total_volume, 2),
        "total_trades": total_trades,
        "window": {
            "start": start.isoformat() if start else None,
            "end":   end.isoformat()   if end   else None,
        },
    }
