# src/helpers/pipelines.py
from __future__ import annotations
from datetime import datetime
from typing import Dict, List, Iterable, Any, TypedDict, Optional,Tuple
from ..config import data,login_history
from bson import ObjectId


# ---------------------------
# Python-side KPI calculations
# ---------------------------

class KpiResult(TypedDict, total=False):
    total_trades: int
    win_trades: int
    total_volume: float
    total_profit: float
    highest_profit: float
    win_percent: float


def _to_lower(s: Optional[str]) -> str:
    return (s or "").lower()


def _abs_num(x: Any) -> float:
    try:
        return abs(float(x))
    except Exception:
        return 0.0


def _num(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _round2(x: float) -> float:
    return round(float(x), 2)


def _profit_per_trade(doc: Dict[str, Any]) -> float:
    """
    Mirror the old pipeline logic:
      profit_per_trade = (eff_close - eff_open) * qty  (independent of side)
      only counted when both open/close present, else 0
    """
    eff_open = _num(doc.get("open_price"))
    eff_close = _num(doc.get("close_price"))
    qty = _abs_num(doc.get("quantity"))

    if eff_open is None or eff_close is None:
        return 0.0

    return (eff_close - eff_open) * qty


def _trade_value(doc: Dict[str, Any]) -> float:
    """
    trade_value = qty * eff_open (when open exists), else 0
    """
    eff_open = _num(doc.get("open_price"))
    qty = _abs_num(doc.get("quantity"))
    if eff_open is None:
        return 0.0
    return qty * eff_open


def _is_win(doc: Dict[str, Any]) -> int:
    """
    Old logic (note: did NOT account for side when judging win),
    win when: is_closed AND has open & close AND eff_close > eff_open
    """
    status_lc = _to_lower(doc.get("status"))
    eff_open = _num(doc.get("open_price"))
    eff_close = _num(doc.get("close_price"))

    is_closed = status_lc in ("closed", "closed_position")
    if not is_closed:
        return 0

    if eff_open is None or eff_close is None:
        return 0

    return 1 if eff_close > eff_open else 0


def compute_kpis(docs: Iterable[Dict[str, Any]]) -> KpiResult:
    """
    Compute KPIs in Python for the given iterable of position documents.
    Matches the previous Mongo pipeline outcomes:
      - total_trades: count of docs
      - win_trades:   sum of _is_win
      - total_volume: sum of trade_value, rounded(2)
      - total_profit: sum of positive profit_per_trade only, rounded(2)
      - highest_profit: max positive profit_per_trade, rounded(2)
      - win_percent: (win_trades / total_trades) * 100, rounded(2)
    """
    total_trades = 0
    win_trades = 0
    total_volume = 0.0
    total_profit = 0.0
    highest_profit = 0.0

    for doc in docs:
        total_trades += 1

        win_trades += _is_win(doc)

        tv = _trade_value(doc)
        total_volume += tv

        ppt = _profit_per_trade(doc)
        if ppt > 0:
            total_profit += ppt
            if ppt > highest_profit:
                highest_profit = ppt

    if total_trades > 0:
        win_percent = round((win_trades / total_trades) * 100.0, 2)
    else:
        win_percent = 0.0

    return KpiResult(
        total_trades=total_trades,
        win_trades=win_trades,
        total_volume=_round2(total_volume),
        total_profit=_round2(total_profit),
        highest_profit=_round2(highest_profit),
        win_percent=win_percent,
    )


def compute_weekly_kpis(docs: Iterable[Dict[str, Any]]) -> KpiResult:
    """
    Weekly KPIs (old pipeline only exposed total_trades, win_trades, total_volume, win_percent).
    We include the same superset as compute_kpis for convenience; the caller can ignore extra fields.
    """
    return compute_kpis(docs)


# ---------------------------
# Mongo "fetch-only" pipelines
# ---------------------------

class PipelineBuilder:
    """
    These pipelines are intentionally "thin": they do NOT perform any math/aggregation.
    They only filter (match) and project the minimal fields needed by the Python calculators above.
    """

    _COMMON_PROJECT = {
        # Keep only the fields Python needs; you can add more if required by callers.
        "_id": 1,
        "status": 1,
        "side": 1,
        "quantity": 1,
        "open_price": 1,
        "close_price": 1,
        "created_at": 1,
    }

    def kpi_pipeline_for_positions(self, match: Dict) -> List[Dict]:
        """
        Fetch documents based on 'match' with the minimal fields.
        No math here; do calculations in Python via compute_kpis(docs).
        """
        return [
            {"$match": match or {}},
            {"$project": self._COMMON_PROJECT},
        ]

    def weekly_kpi_pipeline(self, match: Dict, start: datetime, end: datetime) -> List[Dict]:
        """
        Fetch documents between [start, end) with the minimal fields.
        No math here; do calculations in Python via compute_weekly_kpis(docs).
        """
        full_match = dict(match or {})
        full_match["created_at"] = {"$gte": start, "$lt": end}

        return [
            {"$match": full_match},
            {"$project": self._COMMON_PROJECT},
        ]


pipelines = PipelineBuilder()

def kpi_pipeline_for_positions(match: Dict) -> List[Dict]:
    """
    Returns a fetch-only pipeline (no math). Use compute_kpis(...) on the result set.
    """
    return pipelines.kpi_pipeline_for_positions(match)

def weekly_kpi_pipeline(match: Dict, start: datetime, end: datetime) -> List[Dict]:
    """
    Returns a fetch-only pipeline (no math). Use compute_weekly_kpis(...) on the result set.
    """
    return pipelines.weekly_kpi_pipeline(match, start, end)

def orders_closed_groups_pipeline(
    match: Dict[str, Any],
    *,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    time_field: str = "executionDateTime",
) -> List[Dict[str, Any]]:
    """
    Raw orders -> normalize -> sort -> group by (userId,symbolId,productType)
    -> JS $function (FIFO + directed matching) -> unwind groups
    -> compute totals + PnL + holding_ms -> filter CLOSED groups (total_sell_qty>0).
    """
    time_filter: Dict[str, Any] = {}
    if start is not None or end is not None:
        rng: Dict[str, Any] = {}
        if start is not None: rng["$gte"] = start
        if end is not None:   rng["$lte"] = end
        time_filter = {time_field: rng}

    # IMPORTANT: include updatedAt/createdAt so sells/buys carry fallback timestamps
    normalize_proj = {
        "_id": 1,
        "userId": 1, "user_id": 1,
        "symbolId": 1, "symbolName": 1, "symbolTitle": 1, "script": 1,
        "productType": 1,
        "tradeType": 1, "side": 1,
        "price": 1, "open_price": 1, "close_price": 1, "quantity": 1,
        "lotSize": 1,
        "executionDateTime": 1,
        "updatedAt": 1,
        "createdAt": 1,
        "tradeParentId": 1, "parentId": 1, "order_parent_id": 1,
        "status": 1,
    }

    return [
        {"$match": {"status": "executed", **match, **time_filter}},
        {"$project": normalize_proj},
        {
            "$project": {
                "_id": 1,
                "userId": {"$ifNull": ["$userId", "$user_id"]},
                "symbolId": "$symbolId",
                "symbolName": {"$ifNull": ["$symbolName", {"$ifNull": ["$symbolTitle", "$script"]}]},
                "productType": "$productType",
                "side": {"$toLower": {"$ifNull": ["$tradeType", "$side"]}},
                "price": {
                    "$let": {
                        "vars": {"s": {"$toLower": {"$ifNull": ["$tradeType", ""]}}},
                        "in": {
                            "$cond": [
                                {"$ne": ["$price", None]},
                                {"$toDouble": "$price"},
                                {
                                    "$cond": [
                                        {"$eq": ["$$s", "buy"]},
                                        {"$toDouble": {"$ifNull": ["$open_price", 0]}},
                                        {
                                            "$cond": [
                                                {"$eq": ["$$s", "sell"]},
                                                {"$toDouble": {"$ifNull": ["$close_price", 0]}},
                                                {"$toDouble": {"$ifNull": ["$open_price", "$close_price"]}},
                                            ]
                                        },
                                    ]
                                },
                            ]
                        },
                    }
                },
                "quantity": {"$toDouble": {"$ifNull": ["$quantity", 0]}},
                "lotSize": {"$toDouble": {"$ifNull": ["$lotSize", 1]}},
                "executionDateTime": f"${time_field}",
                "updatedAt": "$updatedAt",
                "createdAt": "$createdAt",
                "tradeParentId": {"$ifNull": ["$tradeParentId", {"$ifNull": ["$parentId", "$order_parent_id"]}]},
                "status": {"$toLower": {"$ifNull": ["$status", "executed"]}},
            }
        },
        {"$match": {"status": "executed"}},
        {"$set": {
            "groupKey": [
                {"$toString": "$userId"},
                {"$toString": "$symbolId"},
                {"$ifNull": ["$productType", ""]},
            ]
        }},
        {"$sort": {"groupKey": 1, "executionDateTime": 1, "_id": 1}},
        {"$group": {"_id": "$groupKey", "rows": {"$push": "$$ROOT"}}},
        {
            "$set": {
                "groups": {
                    "$function": {
                        "lang": "js",
                        "args": ["$rows"],
                        "body": """
function(rows){
  function sid(x){return (x===null||x===undefined)?"":String(x);}
  function f(x){var n=Number(x||0);return isFinite(n)?n:0;}
  function lower(s){return (s||"").toString().trim().toLowerCase();}
  function ts(d){ return d.executionDateTime || d.updatedAt || d.createdAt || null; }

  var groups=[], openBuys=[];
  function ensureGroup(buyDoc, keyArr){
    var q=f(buyDoc.quantity);
    var g={
      parentId: buyDoc._id,
      key: keyArr,
      buy: buyDoc,
      sells: [],
      matches: [],
      total_buy_qty: q,
      total_sell_qty: 0.0,
      remaining_qty: q,
      buy_ts: ts(buyDoc),     // <-- compute here
      sell_max_ts: null       // <-- maintain max sell ts
    };
    groups.push(g); return g;
  }

  for (var i=0;i<rows.length;i++){
    var d=rows[i], side=lower(d.side), qty=f(d.quantity), px=f(d.price);

    if (side==="buy"){
      var grp=ensureGroup(d, rows[0].groupKey);
      openBuys.push({parent_id:sid(d._id), buy_price:px, remaining:qty, lotSize:f(d.lotSize), group:grp});
      continue;
    }
    if (side!=="sell" || qty<=0) continue;

    var remaining=qty, directed=d.tradeParentId, sell_ts=ts(d);

    // directed parent match first
    if (directed){
      var target=sid(directed);
      for (var j=0;j<openBuys.length && remaining>0; j++){
        var ob=openBuys[j];
        if (ob.parent_id!==target || ob.remaining<=0) continue;
        var m=Math.min(remaining, ob.remaining);
        if (m>0){
          ob.remaining-=m;
          var g2=ob.group;
          g2.total_sell_qty+=m; g2.remaining_qty-=m;
          if (g2.sells.indexOf(d)===-1) g2.sells.push(d);
          if (!g2.sell_max_ts || (sell_ts && sell_ts>g2.sell_max_ts)) g2.sell_max_ts = sell_ts;
          g2.matches.push({sellId:d._id, matched_qty:m, buy_price:ob.buy_price, sell_price:px});
          remaining-=m;
          if (ob.remaining<=1e-12){ openBuys.splice(j,1); j--; }
        }
      }
    }

    // spill remainder FIFO
    var k=0;
    while (remaining>1e-12 && k<openBuys.length){
      var ob2=openBuys[k];
      var m2=Math.min(remaining, ob2.remaining);
      if (m2>0){
        ob2.remaining-=m2;
        var g3=ob2.group;
        g3.total_sell_qty+=m2; g3.remaining_qty-=m2;
        if (g3.sells.indexOf(d)===-1) g3.sells.push(d);
        if (!g3.sell_max_ts || (sell_ts && sell_ts>g3.sell_max_ts)) g3.sell_max_ts = sell_ts;
        g3.matches.push({sellId:d._id, matched_qty:m2, buy_price:ob2.buy_price, sell_price:px});
        remaining-=m2;
        if (ob2.remaining<=1e-12){ openBuys.splice(k,1); continue; }
      }
      k++;
    }
  }

  // finalize holding_ms on each group
  for (var g=0; g<groups.length; g++){
    var G = groups[g];
    var h = 0;
    if (G.buy_ts && G.sell_max_ts && (G.sell_max_ts > G.buy_ts)){
      h = Number(new Date(G.sell_max_ts)) - Number(new Date(G.buy_ts));
      if (!isFinite(h) || h<0) h = 0;
    }
    G.holding_ms = h;
  }

  return groups;
}
                        """
                    }
                }
            }
        },
        {"$unwind": "$groups"},
        {"$replaceRoot": {"newRoot": "$groups"}},
        {"$addFields": {
            "pnl": {
                "$let": {
                    "vars": {"lot": {"$toDouble": {"$ifNull": ["$buy.lotSize", 1]}} },
                    "in": {"$round": [
                        {"$sum": {
                            "$map": {
                                "input": {"$ifNull": ["$matches", []]},
                                "as": "m",
                                "in": {
                                    "$multiply": [
                                        {"$subtract": [
                                            {"$toDouble": {"$ifNull": ["$$m.sell_price", 0]}},
                                            {"$toDouble": {"$ifNull": ["$$m.buy_price", 0]}}
                                        ]},
                                        {"$toDouble": {"$ifNull": ["$$m.matched_qty", 0]}},
                                        "$$lot"
                                    ]
                                }
                            }
                        }}, 2]}
                }
            },
            "volume": {
                "$multiply": [
                    {"$toDouble": {"$ifNull": ["$buy.quantity", 0]}},
                    {"$toDouble": {"$ifNull": ["$buy.price", 0]}},
                    {"$toDouble": {"$ifNull": ["$buy.lotSize", 1]}},
                ]
            }
        }},
        {"$match": {"total_sell_qty": {"$gt": 0}}},  # closed groups only
    ]

def kpis_from_orders_pipeline(
    match: Dict[str, Any],
    *,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    time_field: str = "executionDateTime",
    wallets_coll: str = "wallets",
    wallet_user_field: str = "userId",
    wallet_balance_field: str = "balance",
    limits_coll: str = "analysis",
    limits_filter: Optional[Dict[str, Any]] = None,
    neg_balance_full_factor: float = 10.0,
) -> List[Dict[str, Any]]:
    """
    Orders -> closed groups (grp) [holding_ms already computed there] -> KPI facet.
    NOTE: We do NOT recompute holding time here; we only aggregate what's already on grp.
    """
    grp = orders_closed_groups_pipeline(match, start=start, end=end, time_field=time_field)
    limits_match = limits_filter or {}

    kpi = [
        {
            "$facet": {
                "overall": [
                    {
                        "$group": {
                            "_id": None,
                            "total_trades": {"$sum": 1},
                            "win_trades": {"$sum": {"$cond": [{"$gt": ["$pnl", 0]}, 1, 0]}},
                            "total_volume": {"$sum": "$volume"},
                        }
                    },
                    {
                        "$addFields": {
                            "win_percent": {
                                "$cond": [
                                    {"$gt": ["$total_trades", 0]},
                                    {"$round": [
                                        {"$multiply": [
                                            {"$divide": ["$win_trades", "$total_trades"]}, 100
                                        ]},
                                        2
                                    ]},
                                    0.0
                                ]
                            }
                        }
                    }
                ],

                "per_user": [
                    # aggregate per user using holding_ms already on the group documents
                    {
                        "$group": {
                            "_id": "$buy.userId",
                            "total_trades": {"$sum": 1},
                            "win_trades": {"$sum": {"$cond": [{"$gt": ["$pnl", 0]}, 1, 0]}},
                            "total_volume": {"$sum": "$volume"},
                            "sum_holding_ms": {
                                "$sum": {"$cond": [{"$gt": ["$holding_ms", 0]}, "$holding_ms", 0]}
                            },
                            "closed_trades": {
                                "$sum": {"$cond": [{"$gt": ["$holding_ms", 0]}, 1, 0]}
                            }
                        }
                    },

                    # 💡 No users lookup/filter here (prevents dropping rows)

                    # Wallet lookup (tolerant to different user id field names/types)
                    {
                        "$lookup": {
                            "from": wallets_coll,
                            "let": {"uid": "$_id", "uidStr": {"$toString": "$_id"}},
                            "pipeline": [
                                {
                                    "$match": {
                                        "$expr": {
                                            "$or": [
                                                {"$eq": [f"${wallet_user_field}", "$$uid"]},
                                                {"$eq": [{"$toString": f"${wallet_user_field}"}, "$$uidStr"]},
                                                {"$eq": ["$user_id", "$$uid"]},
                                                {"$eq": [{"$toString": "$user_id"}, "$$uidStr"]},
                                                {"$eq": ["$userId", "$$uid"]},
                                                {"$eq": [{"$toString": "$userId"}, "$$uidStr"]},
                                            ]
                                        }
                                    }
                                },
                                {"$project": {"_id": 0, "balance": f"${wallet_balance_field}"}},
                                {"$limit": 1}
                            ],
                            "as": "wallet"
                        }
                    },
                    {"$addFields": {"balance": {"$ifNull": [{"$first": "$wallet.balance"}, 0]}}},

                    # Limits lookup
                    {
                        "$lookup": {
                            "from": limits_coll,
                            "pipeline": [
                                {"$match": limits_match},
                                {"$sort": {"updated_at": -1}},
                                {"$project": {
                                    "_id": 0,
                                    "max_trades": 1,
                                    "average_trading_volume": 1,
                                    "win_rate_percentage": 1,
                                    "negative_balance": 1
                                }},
                                {"$limit": 1}
                            ],
                            "as": "limits"
                        }
                    },
                    {
                        "$addFields": {
                            "max_trades_cap": {"$first": "$limits.max_trades"},
                            "max_volume_cap": {"$first": "$limits.average_trading_volume"},
                            "limit_win_pct": {"$first": "$limits.win_rate_percentage"},
                            "limit_neg_balance": {"$first": "$limits.negative_balance"}
                        }
                    },

                    # Derived: win %, avg holding (minutes) from holding_ms
                    {
                        "$addFields": {
                            "u_win_percent": {
                                "$cond": [
                                    {"$gt": ["$total_trades", 0]},
                                    {"$multiply": [{"$divide": ["$win_trades", "$total_trades"]}, 100]},
                                    0
                                ]
                            },
                            "avg_holding_minutes_user": {
                                "$cond": [
                                    {"$gt": ["$closed_trades", 0]},
                                    {"$round": [
                                        {"$divide": [
                                            {"$divide": ["$sum_holding_ms", "$closed_trades"]},
                                            1000 * 60
                                        ]},
                                        2
                                    ]},
                                    0.0
                                ]
                            }
                        }
                    },

                    # Risk scoring (unchanged)
                    {
                        "$addFields": {
                            "trades_norm": {
                                "$min": [
                                    {
                                        "$cond": [
                                            {"$and": [
                                                {"$ne": ["$max_trades_cap", None]},
                                                {"$gt": ["$max_trades_cap", 0]}
                                            ]},
                                            {"$divide": ["$total_trades", "$max_trades_cap"]},
                                            0
                                        ]
                                    },
                                    1
                                ]
                            },
                            "volume_norm": {
                                "$min": [
                                    {
                                        "$cond": [
                                            {"$and": [
                                                {"$ne": ["$max_volume_cap", None]},
                                                {"$gt": ["$max_volume_cap", 0]}
                                            ]},
                                            {"$divide": ["$total_volume", "$max_volume_cap"]},
                                            0
                                        ]
                                    },
                                    1
                                ]
                            }
                        }
                    },
                    {
                        "$addFields": {
                            "win_risk": {
                                "$cond": [
                                    {"$and": [
                                        {"$ne": ["$limit_win_pct", None]},
                                        {"$gte": ["$u_win_percent", "$limit_win_pct"]}
                                    ]},
                                    1,
                                    {"$min": [{"$max": [{"$divide": ["$u_win_percent", 100]}, 0]}, 1]}
                                ]
                            }
                        }
                    },
                    {
                        "$addFields": {
                            "has_neg_limit": {"$ne": ["$limit_neg_balance", None]},
                            "neg_limit_abs": {
                                "$cond": ["$has_neg_limit", {"$abs": "$limit_neg_balance"}, None]
                            },
                            "neg_full_abs": {
                                "$cond": [
                                    "$has_neg_limit",
                                    {"$multiply": [{"$abs": "$limit_neg_balance"}, neg_balance_full_factor]},
                                    None
                                ]
                            },
                            "neg_span": {
                                "$cond": [
                                    "$has_neg_limit",
                                    {"$max": [{"$subtract": ["$neg_full_abs", "$neg_limit_abs"]}, 1]},
                                    1
                                ]
                            },
                            "neg_balance_deficit": {
                                "$cond": [
                                    {"$and": ["$has_neg_limit", {"$lt": ["$balance", 0]}]},
                                    {"$max": [0, {"$subtract": [{"$abs": "$balance"}, "$neg_limit_abs"]}]},
                                    0
                                ]
                            },
                            "neg_balance_norm": {
                                "$cond": [
                                    "$has_neg_limit",
                                    {"$min": [{"$divide": ["$neg_balance_deficit", "$neg_span"]}, 1]},
                                    0
                                ]
                            }
                        }
                    },
                    {
                        "$addFields": {
                            "risk_score": {"$round": [
                                {"$multiply": [
                                    {"$add": [
                                        {"$multiply": ["$win_risk",         0.25]},
                                        {"$multiply": ["$trades_norm",      0.25]},
                                        {"$multiply": ["$volume_norm",      0.25]},
                                        {"$multiply": ["$neg_balance_norm", 0.25]},
                                    ]},
                                    10
                                ]},
                                1
                            ]}
                        }
                    },

                    {"$project": {"wallet": 0, "limits": 0}}
                ]
            }
        },
        {
            "$project": {
                "overall": {"$ifNull": [{"$arrayElemAt": ["$overall", 0]}, {}]},
                "per_user": 1
            }
        },
        {
            "$addFields": {
                "avg_risk_score": {
                    "$cond": [
                        {"$gt": [{"$size": "$per_user"}, 0]},
                        {"$round": [{"$avg": "$per_user.risk_score"}, 1]},
                        0.0
                    ]
                }
            }
        },
        {
            "$addFields": {
                "avg_risk_status": {
                    "$switch": {
                        "branches": [
                            {"case": {"$lt": ["$avg_risk_score", 4]}, "then": "Low Risk"},
                            {"case": {"$lt": ["$avg_risk_score", 7]}, "then": "Medium Risk"},
                        ],
                        "default": "High Risk"
                    }
                }
            }
        }
    ]

    # Important: 'grp' already has holding_ms; we do NOT recompute it here.
    return grp + kpi

def build_top_risk_users_pipeline(
    *,
    limit: int = 10,
    superadmin_id: Optional[ObjectId | str] = None,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    min_score: float = 0.0,
) -> List[Dict[str, Any]]:
    match: Dict[str, Any] = {
        "status": 1,                         # ← fixed
        "avg_risk_score": {"$gte": float(min_score)},
    }

    if superadmin_id is not None:
        match["superadmin_id"] = (
            ObjectId(superadmin_id) if isinstance(superadmin_id, str) else superadmin_id
        )

    if start is not None:
        match.setdefault("window.start", {})["$gte"] = start
    if end is not None:
        match.setdefault("window.end", {})["$lte"] = end

    return [
        {"$match": match},
        {"$addFields": {"_ars": {"$toDouble": "$avg_risk_score"}}},
        {"$sort": {"_ars": -1, "total_volume": -1, "win_percent": -1, "generated_at": -1}},
        {"$limit": int(max(1, limit))},
        {"$project": {"_ars": 0}},
    ]
login_history = login_history  # your Mongo collection

def _get_live_user_ids(user_ids: List[ObjectId]) -> set[ObjectId]:
    """
    Return set of userIds whose latest loginHistory doc (by createdAt) has isLogin=True.
    """
    pipeline = [
        {"$match": {"userId": {"$in": user_ids}}},
        {"$sort": {"userId": 1, "createdAt": -1}},  # newest first for each user
        {"$group": {
            "_id": "$userId",
            "lastIsLogin": {"$first": "$isLogin"}
        }},
        {"$match": {"lastIsLogin": True}},
        {"$project": {"_id": 0, "userId": "$_id"}}
    ]
    return {doc["userId"] for doc in login_history.aggregate(pipeline)}