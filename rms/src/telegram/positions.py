# src/telegram/positions.py

from typing import Dict, Any, List, Tuple, Optional
import html
from datetime import datetime, timedelta, timezone
from bson import ObjectId
import logging
from collections import defaultdict

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from src.config import trade_market, exchange, open_positions, symbols
from src.helpers.hierarchy_service import (
    get_users_for_superadmin,
    get_users_for_admin,
    get_users_for_master,
)
from .main import (
    get_logged_in,
    require_login,
    require_login_from_query,
    today_utc_range,
    role_name_from_user,
)
from .session_store import remember_bot_message_from_message

logger = logging.getLogger(__name__)

POSITION_FILTERS_CACHE: Dict[int, Dict[str, Any]] = {}
POSITION_DATA_CACHE: Dict[int, List[Dict[str, Any]]] = {}
EXCHANGE_LIST_CACHE: Dict[int, List[Dict[str, Any]]] = {}
ALL_EXCHANGE_PAGINATION_CACHE: Dict[int, Dict[str, Any]] = {}


def get_all_user_ids_under_user(user: dict) -> List[ObjectId]:
    """
    Get all user IDs (clients) under the logged-in user based on their role.
    - Superadmin: uses get_users_for_superadmin
    - Admin: uses get_users_for_admin
    - Master: uses get_users_for_master
    """
    role = role_name_from_user(user)
    user_ids: List[ObjectId] = []
    
    try:
        user_oid = ObjectId(user.get("id") or user.get("_id"))
    except Exception:
        logger.error(f"get_all_user_ids_under_user: Invalid user ID in user dict")
        return []
    
    try:
        if role == "superadmin":
            all_users = get_users_for_superadmin(user_oid)
        elif role == "admin":
            all_users = get_users_for_admin(user_oid)
        elif role == "master":
            all_users = get_users_for_master(user_oid)
        else:
            logger.warning(f"get_all_user_ids_under_user: Unknown role '{role}', returning empty list")
            return []
        
        for u in all_users:
            uid = u.get("id") or u.get("_id")
            if not uid:
                continue
            try:
                user_ids.append(ObjectId(uid))
            except Exception:
                continue
        
        logger.info(f"get_all_user_ids_under_user: Found {len(user_ids)} users for role '{role}'")
        return user_ids
    except Exception as e:
        logger.error(f"get_all_user_ids_under_user: Error fetching users for role '{role}': {e}", exc_info=True)
        return []


_EXCHANGE_CACHE: Dict[Any, str] = {}

def resolve_exchange_name(exchange_id: Any) -> str:
    """Resolve exchange ID to exchange name (with caching)."""
    if not exchange_id:
        return "—"
    
    if exchange_id in _EXCHANGE_CACHE:
        return _EXCHANGE_CACHE[exchange_id]
    
    try:
        ex_doc = None
        if isinstance(exchange_id, ObjectId):
            ex_doc = exchange.find_one({"_id": exchange_id}, {"name": 1, "masterName": 1})
        elif isinstance(exchange_id, str):
            try:
                ex_doc = exchange.find_one({"_id": ObjectId(exchange_id)}, {"name": 1, "masterName": 1})
            except Exception:
                ex_doc = exchange.find_one({"name": exchange_id}, {"name": 1, "masterName": 1}) or \
                         exchange.find_one({"masterName": exchange_id}, {"name": 1, "masterName": 1})
        else:
            result = str(exchange_id)
            _EXCHANGE_CACHE[exchange_id] = result
            return result
        
        if ex_doc:
            result = ex_doc.get("name") or ex_doc.get("masterName") or str(exchange_id)
        else:
            result = str(exchange_id)
        
        _EXCHANGE_CACHE[exchange_id] = result
        return result
    except Exception as e:
        result = str(exchange_id)
        _EXCHANGE_CACHE[exchange_id] = result
        return result


def get_current_week_range() -> Tuple[datetime, datetime]:
    """Get Monday to Sunday of the current week in UTC."""
    now = datetime.now(timezone.utc)
    days_since_monday = now.weekday()
    monday = now - timedelta(days=days_since_monday)
    monday_start = datetime(monday.year, monday.month, monday.day, 0, 0, 0, tzinfo=timezone.utc)
    sunday_end = monday_start + timedelta(days=7)
    return monday_start, sunday_end


def aggregate_positions(
    user_ids: List[ObjectId],
    exchange_filter: Optional[str] = None,
    symbol_filter: Optional[str] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    only_open: bool = True,
) -> List[Dict[str, Any]]:
    """
    Aggregate positions by exchange and symbol.
    Excludes trades from open positions and shows only this week's closed trades.
    
    Args:
        user_ids: List of user IDs to query
        exchange_filter: Optional exchange name filter
        symbol_filter: Optional symbol name filter (regex)
        date_from: Optional start date (default: None = no limit)
        date_to: Optional end date (default: None = no limit)
        only_open: If True, only show open positions (squareOff: False)
    """
    if not user_ids:
        return []
    
    try:
        logger.info(f"aggregate_positions: Starting query for {len(user_ids)} users...")
        
        positions_data = []
        
        exchange_id_for_filter = None
        if exchange_filter:
            ex_doc = exchange.find_one(
                {"$or": [{"name": exchange_filter}, {"masterName": exchange_filter}]},
                {"_id": 1}
            )
            if ex_doc:
                exchange_id_for_filter = ex_doc["_id"]
                logger.info(f"aggregate_positions: Exchange filter '{exchange_filter}' resolved to ID: {exchange_id_for_filter}")
        
        open_positions_query = {"userId": {"$in": user_ids}}
        if exchange_id_for_filter:
            open_positions_query["exchangeId"] = {"$in": [exchange_id_for_filter, str(exchange_id_for_filter)]}
        
        open_positions_list = list(open_positions.find(open_positions_query))
        open_positions_count = len(open_positions_list)
        logger.info(f"📊 [1] Fetched from open_positions: {open_positions_count} documents (filter: {exchange_filter})")
        
        open_position_ids = [pos.get("_id") for pos in open_positions_list if pos.get("_id")]
        
        if open_positions_list:
            positions_data.extend(open_positions_list)
            logger.info(f"📊 [2] Added to array from open_positions: {len(open_positions_list)} documents")
        
        excluded_trade_market_ids = []
        if open_position_ids:
            trade_market_docs = list(trade_market.find(
                {"positionId": {"$in": open_position_ids}},
                {"_id": 1}
            ))
            excluded_trade_market_ids = [doc.get("_id") for doc in trade_market_docs if doc.get("_id")]
            logger.info(f"📊 [3] Found {len(excluded_trade_market_ids)} trade_market documents linked to open positions (to exclude)")
        
        week_start, week_end = get_current_week_range()
        logger.info(f"aggregate_positions: Current week range: {week_start} to {week_end}")
        
        query_without_exclude: Dict[str, Any] = {
            "userId": {"$in": user_ids},
            "createdAt": {
                "$gte": week_start,
                "$lt": week_end
            }
        }
        
        if exchange_id_for_filter:
            query_without_exclude["exchangeId"] = {"$in": [exchange_id_for_filter, str(exchange_id_for_filter)]}
        
        if symbol_filter:
            query_without_exclude["symbolName"] = {"$regex": symbol_filter, "$options": "i"}
        
        trade_market_total_before_exclude = trade_market.count_documents(query_without_exclude)
        logger.info(f"📊 [4] Total fetched from trade_market (without exclude): {trade_market_total_before_exclude} documents")
        
        query: Dict[str, Any] = query_without_exclude.copy()
        
        if excluded_trade_market_ids:
            query["_id"] = {"$nin": excluded_trade_market_ids}
        
        trade_market_data = list(trade_market.find(query).limit(10000))
        trade_market_after_exclude = len(trade_market_data)
        logger.info(f"📊 [5] Remaining trade_market after exclude: {trade_market_after_exclude} documents")
        
        if trade_market_data:
            positions_data.extend(trade_market_data)
        
        total_combined = len(positions_data)
        logger.info(f"📊 [6] TOTAL: open_positions ({open_positions_count}) + trade_market ({trade_market_after_exclude}) = {total_combined} documents")
        
        if not positions_data:
            logger.warning(f"aggregate_positions: No positions found")
            return []
        
        sample = positions_data[0]
        logger.info(f"aggregate_positions: Sample position keys: {list(sample.keys())}")
        logger.info(f"aggregate_positions: Sample - tradeType: {sample.get('tradeType')}, squareOff: {sample.get('squareOff')}")
        
        unique_exchange_ids = set()
        for pos in positions_data:
            ex_id = pos.get("exchangeId")
            if ex_id:
                unique_exchange_ids.add(ex_id)
        
        logger.info(f"aggregate_positions: Pre-loading {len(unique_exchange_ids)} unique exchange IDs into cache...")
        for ex_id in unique_exchange_ids:
            resolve_exchange_name(ex_id)
        logger.info(f"aggregate_positions: Exchange cache pre-loaded, starting aggregation...")
    except Exception as e:
        logger.error(f"Error fetching positions: {e}", exc_info=True)
        return []
    
    grouped: Dict[Tuple[str, str], Dict[str, Any]] = defaultdict(lambda: {
        "exchange": "",
        "symbol": "",
        "buy_qty": 0.0,
        "buy_total_value": 0.0,
        "sell_qty": 0.0,
        "sell_total_value": 0.0,
        "ltp": 0.0,
        "ltp_timestamp": None,
    })
    
    processed_count = 0
    buy_count = 0
    sell_count = 0
    unknown_type_count = 0
    error_count = 0
    
    for pos in positions_data:
        try:
            ex_id = pos.get("exchangeId")
            ex_name = resolve_exchange_name(ex_id)
            symbol = pos.get("symbolName") or pos.get("symbolTitle") or pos.get("symbol") or "—"
            
            key = (ex_name, symbol)
            group = grouped[key]
            group["exchange"] = ex_name
            group["symbol"] = symbol
            
            trade_type = str(pos.get("tradeType") or pos.get("orderType") or "").lower()
            quantity = float(pos.get("quantity") or pos.get("totalQuantity") or 0)
            price = float(pos.get("price") or 0)
            lot_size = float(pos.get("lotSize") or 1)
            total_qty = quantity * lot_size
            value = total_qty * price
            
            created_at = pos.get("createdAt") or pos.get("updatedAt")
            
            if trade_type in ["buy", "b"]:
                group["buy_qty"] += total_qty
                group["buy_total_value"] += value
                buy_count += 1
            elif trade_type in ["sell", "s"]:
                group["sell_qty"] += total_qty
                group["sell_total_value"] += value
                sell_count += 1
            else:
                unknown_type_count += 1
                if processed_count < 5:
                    logger.warning(f"Unknown trade type: '{trade_type}' for position {pos.get('_id')}, symbol: {symbol}")
            
            if created_at and (not group["ltp_timestamp"] or created_at > group["ltp_timestamp"]):
                group["ltp"] = price
                group["ltp_timestamp"] = created_at
            
            processed_count += 1
            
            if processed_count % 1000 == 0:
                logger.info(f"aggregate_positions: Processed {processed_count}/{len(positions_data)} positions...")
        except Exception as e:
            error_count += 1
            if error_count < 5:
                logger.error(f"Error processing position {pos.get('_id')}: {e}", exc_info=True)
            continue
    
    logger.info(f"aggregate_positions: Processed {processed_count} positions - Buy: {buy_count}, Sell: {sell_count}, Unknown: {unknown_type_count}, Errors: {error_count}")
    logger.info(f"aggregate_positions: Created {len(grouped)} groups")
    
    results: List[Dict[str, Any]] = []
    
    for (ex_name, symbol), group in grouped.items():
        buy_qty = group["buy_qty"]
        sell_qty = group["sell_qty"]
        net_qty = buy_qty - sell_qty
        
        buy_avg = group["buy_total_value"] / buy_qty if buy_qty > 0 else 0.0
        sell_avg = group["sell_total_value"] / sell_qty if sell_qty > 0 else 0.0
        
        net_avg_price = 0.0
        if abs(net_qty) > 0.01:
            if net_qty > 0:
                net_avg_price = (group["buy_total_value"] - group["sell_total_value"]) / net_qty
            else:
                net_avg_price = (group["sell_total_value"] - group["buy_total_value"]) / abs(net_qty)
        elif buy_qty > 0:
            net_avg_price = buy_avg
        elif sell_qty > 0:
            net_avg_price = sell_avg
        
        ltp = group["ltp"] if group["ltp"] > 0 else (buy_avg if buy_avg > 0 else (sell_avg if sell_avg > 0 else 0.0))
        
        pnl = 0.0
        if abs(net_qty) > 0.01 and ltp > 0:
            if net_avg_price > 0:
                if net_qty > 0:
                    pnl = (ltp - net_avg_price) * net_qty
                else:
                    pnl = (net_avg_price - ltp) * abs(net_qty)
            elif buy_qty > 0 and sell_qty > 0:
                pnl = (sell_avg - buy_avg) * min(buy_qty, sell_qty)
        
        results.append({
            "exchange": ex_name or "—",
            "symbol": symbol or "—",
            "buy_qty": round(buy_qty, 2),
            "buy_avg": round(buy_avg, 2),
            "sell_qty": round(sell_qty, 2),
            "sell_avg": round(sell_avg, 2),
            "net_qty": round(net_qty, 2),
            "net_avg_price": round(net_avg_price, 2),
            "ltp": round(ltp, 2),
            "pnl": round(pnl, 2),
        })
    
    logger.info(f"aggregate_positions: Returning {len(results)} aggregated results")
    if results and len(results) > 0:
        sample_result = results[0]
        logger.info(f"aggregate_positions: Sample result - Exchange: {sample_result['exchange']}, Symbol: {sample_result['symbol']}, Buy: {sample_result['buy_qty']}, Sell: {sample_result['sell_qty']}, Net: {sample_result['net_qty']}")
    else:
        logger.warning(f"aggregate_positions: No results created from {len(grouped)} groups!")
    
    exchange_sequence_map = {}
    try:
        all_exchanges = list(exchange.find({}, {"name": 1, "masterName": 1, "sequence": 1}))
        for ex in all_exchanges:
            name = ex.get("name") or ex.get("masterName")
            if name:
                exchange_sequence_map[name] = ex.get("sequence", 999)
                if ex.get("masterName") and ex.get("masterName") != name:
                    exchange_sequence_map[ex.get("masterName")] = ex.get("sequence", 999)
    except Exception as e:
        logger.warning(f"Error building exchange sequence map: {e}")
    
    sorted_results = sorted(results, key=lambda x: (
        exchange_sequence_map.get(x["exchange"], 999),
        x["exchange"],
        x["symbol"]
    ))
    logger.info(f"aggregate_positions: Final sorted results count: {len(sorted_results)}")
    return sorted_results


def format_positions_table(data: List[Dict[str, Any]], page: int = 0, page_size: int = 10) -> Tuple[str, List[List[InlineKeyboardButton]]]:
    """Format positions data as a table."""
    if not data:
        return "📊 No positions found.\n\nTry:\n• Check if you have any open positions\n• Use filters to narrow your search", []
    
    total = len(data)
    max_page = (total - 1) // page_size
    if page < 0:
        page = 0
    if page > max_page:
        page = max_page
    
    start = page * page_size
    end = start + page_size
    chunk = data[start:end]
    
    header = (
        "📊 <b>Positions Summary</b>\n"
        f"Page {page + 1} / {max_page + 1} | Total: {total}\n"
    )
    
    table_rows = []
    header_line = (
        f"{'EX':<8} | {'SYMBOL':<12} | {'BUY QTY':>10} | {'BUY AVG':>9} | "
        f"{'SELL QTY':>10} | {'SELL AVG':>9} | {'NET QTY':>10} | {'NET AVG':>10} | {'LTP':>9} | {'PNL':>14}"
    )
    table_rows.append(f"<pre>{header_line}</pre>")
    
    for row in chunk:
        net_qty = row['net_qty']
        net_qty_str = f"{net_qty:,.2f}"
        pnl = row['pnl']
        
        if pnl >= 0:
            pnl_str = f"+{pnl:,.2f}"
        else:
            pnl_str = f"{pnl:,.2f}"
        
        ex_name = (row['exchange'][:7] if row['exchange'] else "—")[:7]
        sym_name = (row['symbol'][:11] if row['symbol'] else "—")[:11]
        
        buy_qty_str = f"{row['buy_qty']:,.2f}"
        sell_qty_str = f"{row['sell_qty']:,.2f}"
        buy_avg_str = f"{row['buy_avg']:,.2f}"
        sell_avg_str = f"{row['sell_avg']:,.2f}"
        net_avg_str = f"{row['net_avg_price']:,.2f}"
        ltp_str = f"{row['ltp']:,.2f}"
        
        data_line = (
            f"{ex_name:<8} | {sym_name:<12} | {buy_qty_str:>10} | {buy_avg_str:>9} | "
            f"{sell_qty_str:>10} | {sell_avg_str:>9} | {net_qty_str:>10} | {net_avg_str:>10} | {ltp_str:>9} | {pnl_str:>14}"
        )
        table_rows.append(f"<pre>{data_line}</pre>")
    
    text = header + "\n".join(table_rows)
    
    keyboard: List[List[InlineKeyboardButton]] = []
    
    nav_row: List[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅ Prev", callback_data="positions_page:prev"))
    if end < total:
        nav_row.append(InlineKeyboardButton("Next ➡", callback_data="positions_page:next"))
    if nav_row:
        keyboard.append(nav_row)
    
    return text, keyboard


async def positions_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /positions command - show exchange selection buttons."""
    token, user = get_logged_in(update, context)
    if not token or not user:
        return await require_login(update, context)
    
    user_ids = get_all_user_ids_under_user(user)
    logger.info(f"positions_cmd: Found {len(user_ids)} user IDs under logged-in user")
    
    if not user_ids:
        msg = await update.message.reply_text("📊 No users found under you.")
        remember_bot_message_from_message(update, msg)
        return
    
    tg_id = update.effective_user.id
    
    try:
        all_exchanges = list(exchange.find({}, {"name": 1, "masterName": 1, "sequence": 1}).sort("sequence", 1))
        exchange_list = []
        for ex in all_exchanges:
            name = ex.get("name") or ex.get("masterName") or "Unknown"
            if name.upper() == "CDS":
                continue
            exchange_list.append({
                "name": name,
                "_id": ex.get("_id"),
                "sequence": ex.get("sequence", 999)
            })
        
        exchange_list.sort(key=lambda x: x.get("sequence", 999))
        EXCHANGE_LIST_CACHE[tg_id] = exchange_list
        logger.info(f"positions_cmd: Found {len(exchange_list)} exchanges (CDS excluded)")
        seq_info = [f"{ex['name']}(seq:{ex.get('sequence', 999)})" for ex in exchange_list[:10]]
        logger.info(f"positions_cmd: Exchange sequences: {seq_info}")
        
        if not exchange_list:
            msg = await update.message.reply_text("📊 No exchanges found.")
            remember_bot_message_from_message(update, msg)
            return
        
        keyboard: List[List[InlineKeyboardButton]] = []
        
        for i in range(0, len(exchange_list), 2):
            row: List[InlineKeyboardButton] = []
            row.append(InlineKeyboardButton(
                exchange_list[i]["name"],
                callback_data=f"positions_exchange:{exchange_list[i]['name']}"
            ))
            if i + 1 < len(exchange_list):
                row.append(InlineKeyboardButton(
                    exchange_list[i + 1]["name"],
                    callback_data=f"positions_exchange:{exchange_list[i + 1]['name']}"
                ))
            keyboard.append(row)
        
        keyboard.append([InlineKeyboardButton("📊 All Exchange", callback_data="positions_exchange:all")])
        
        msg = await update.message.reply_text(
            "📊 <b>Select Exchange</b>\n\nChoose an exchange to view positions:",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        remember_bot_message_from_message(update, msg)
        
    except Exception as e:
        logger.error(f"positions_cmd: Error loading exchanges: {e}", exc_info=True)
        msg = await update.message.reply_text(f"⚠ Error while loading exchanges: {str(e)}")
        remember_bot_message_from_message(update, msg)


async def positions_exchange_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle exchange selection callbacks."""
    query = update.callback_query
    await query.answer()
    
    token, user = get_logged_in(update, context)
    if not token or not user:
        return await require_login_from_query(query, context)
    
    tg_id = query.from_user.id
    _, exchange_name = query.data.split(":", 1)
    
    if exchange_name == "back":
        ALL_EXCHANGE_PAGINATION_CACHE.pop(tg_id, None)
        exchange_list = EXCHANGE_LIST_CACHE.get(tg_id, [])
        if not exchange_list:
            await query.edit_message_text("📊 No exchanges available. Please use /positions again.")
            return
        
        keyboard: List[List[InlineKeyboardButton]] = []
        for i in range(0, len(exchange_list), 2):
            row: List[InlineKeyboardButton] = []
            row.append(InlineKeyboardButton(
                exchange_list[i]["name"],
                callback_data=f"positions_exchange:{exchange_list[i]['name']}"
            ))
            if i + 1 < len(exchange_list):
                row.append(InlineKeyboardButton(
                    exchange_list[i + 1]["name"],
                    callback_data=f"positions_exchange:{exchange_list[i + 1]['name']}"
                ))
            keyboard.append(row)
        
        keyboard.append([InlineKeyboardButton("📊 All Exchange", callback_data="positions_exchange:all")])
        
        await query.edit_message_text(
            "📊 <b>Select Exchange</b>\n\nChoose an exchange to view positions:",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return
    
    user_ids = get_all_user_ids_under_user(user)
    if not user_ids:
        await query.edit_message_text("📊 No users found under you.")
        return
    
    if exchange_name == "all":
        await handle_all_exchange_view(query, user_ids, tg_id)
    elif exchange_name == "load_more":
        await handle_all_exchange_view(query, user_ids, tg_id)
    elif exchange_name == "all_loaded":
        await query.answer("All exchanges are already loaded!", show_alert=True)
    else:
        await handle_single_exchange_view(query, user_ids, exchange_name, tg_id)


async def handle_single_exchange_view(query, user_ids: List[ObjectId], exchange_name: str, tg_id: int):
    """Handle single exchange view."""
    try:
        aggregated_data = aggregate_positions(
            user_ids,
            exchange_filter=exchange_name
        )
        
        if not aggregated_data:
            await query.edit_message_text(
                f"📊 No positions found for exchange: <b>{exchange_name}</b>",
                parse_mode="HTML"
            )
            return
        
        POSITION_DATA_CACHE[tg_id] = aggregated_data
        POSITION_FILTERS_CACHE[tg_id] = {"page": 0, "exchange": exchange_name, "symbol": None}
        
        text, keyboard = format_positions_table(aggregated_data, page=0)
        
        back_button = [InlineKeyboardButton("⬅ Back to Exchanges", callback_data="positions_exchange:back")]
        keyboard.append(back_button)
        
        if len(text) > 4096:
            text = text[:4000] + "\n\n... (truncated)"
        
        await query.edit_message_text(
            text,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
        )
    except Exception as e:
        logger.error(f"Error showing exchange {exchange_name}: {e}", exc_info=True)
        await query.edit_message_text(f"⚠ Error loading exchange: {str(e)}")


async def handle_all_exchange_view(query, user_ids: List[ObjectId], tg_id: int):
    """Handle all exchange view with pagination (2 exchanges at a time, skipping empty ones)."""
    try:
        exchange_list = EXCHANGE_LIST_CACHE.get(tg_id, [])
        if not exchange_list:
            await query.edit_message_text("📊 No exchanges available. Please use /positions again.")
            return
        
        pagination_data = ALL_EXCHANGE_PAGINATION_CACHE.get(tg_id, {"last_index": 0, "data": {}, "loaded_exchanges": []})
        last_index = pagination_data.get("last_index", 0)
        cached_data = pagination_data.get("data", {})
        loaded_exchanges = pagination_data.get("loaded_exchanges", [])
        
        logger.info(f"handle_all_exchange_view: Starting from index {last_index}, already loaded: {loaded_exchanges}")
        
        if last_index >= len(exchange_list):
            await query.answer("All exchanges already loaded!", show_alert=True)
            return
        
        exchanges_loaded_this_round = 0
        current_index = last_index
        
        while exchanges_loaded_this_round < 2 and current_index < len(exchange_list):
            ex = exchange_list[current_index]
            ex_name = ex["name"]
            ex_sequence = ex.get("sequence", 999)
            
            logger.info(f"Checking exchange: {ex_name} (sequence: {ex_sequence}, index: {current_index})")
            
            if ex_name not in cached_data:
                ex_data = aggregate_positions(user_ids, exchange_filter=ex_name)
                cached_data[ex_name] = ex_data
                logger.info(f"Loaded {len(ex_data)} aggregated results for {ex_name} (sequence: {ex_sequence})")
            
            aggregated_count = len(cached_data[ex_name])
            if aggregated_count > 0:
                if ex_name not in loaded_exchanges:
                    loaded_exchanges.append(ex_name)
                    exchanges_loaded_this_round += 1
                    logger.info(f"✅ Added {ex_name} (sequence: {ex_sequence}) to loaded exchanges (has {aggregated_count} aggregated positions)")
                else:
                    logger.info(f"⚠ {ex_name} (sequence: {ex_sequence}) already in loaded_exchanges, skipping")
            else:
                logger.info(f"⏭ Skipped {ex_name} (sequence: {ex_sequence}) - no aggregated positions (all netted out or empty)")
            
            current_index += 1
        
        logger.info(f"handle_all_exchange_view: Loaded {exchanges_loaded_this_round} exchanges this round. Total loaded: {len(loaded_exchanges)}. Checked up to index: {current_index}")
        
        pagination_data["last_index"] = current_index
        pagination_data["data"] = cached_data
        pagination_data["loaded_exchanges"] = loaded_exchanges
        ALL_EXCHANGE_PAGINATION_CACHE[tg_id] = pagination_data
        
        loaded_count = len(loaded_exchanges)
        
        logger.info(f"handle_all_exchange_view: Building combined_data from {loaded_count} loaded exchanges: {loaded_exchanges}")
        
        combined_data = []
        for ex_name in loaded_exchanges:
            ex_data = cached_data.get(ex_name, [])
            combined_data.extend(ex_data)
            logger.info(f"  - {ex_name}: {len(ex_data)} positions added to combined_data")
        
        logger.info(f"handle_all_exchange_view: Total combined_data: {len(combined_data)} positions")
        
        if not combined_data:
            await query.edit_message_text("📊 No positions found for loaded exchanges.")
            return
        
        POSITION_DATA_CACHE[tg_id] = combined_data
        POSITION_FILTERS_CACHE[tg_id] = {"page": 0, "exchange": None, "symbol": None}
        
        text, keyboard = format_positions_table(combined_data, page=0)
        
        load_more_row = []
        if current_index < len(exchange_list):
            load_more_row.append(InlineKeyboardButton(
                f"📥 Load 2 More ({loaded_count} loaded, {current_index}/{len(exchange_list)} checked)",
                callback_data="positions_exchange:load_more"
            ))
        else:
            load_more_row.append(InlineKeyboardButton(
                f"✅ All Loaded ({loaded_count} exchanges with data)",
                callback_data="positions_exchange:all_loaded"
            ))
        keyboard.append(load_more_row)
        
        back_button = [InlineKeyboardButton("⬅ Back to Exchanges", callback_data="positions_exchange:back")]
        keyboard.append(back_button)
        
        header_text = f"📊 <b>All Exchanges</b>\nLoaded: {loaded_count} exchanges with data ({current_index}/{len(exchange_list)} checked)\n\n"
        if len(text) > 3800:
            text = text[:3800] + "\n\n... (truncated)"
        
        await query.edit_message_text(
            header_text + text,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
        )
    except Exception as e:
        logger.error(f"Error showing all exchanges: {e}", exc_info=True)
        await query.edit_message_text(f"⚠ Error loading exchanges: {str(e)}")


async def positions_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle pagination callbacks."""
    query = update.callback_query
    await query.answer()
    
    tg_id = query.from_user.id
    data = POSITION_DATA_CACHE.get(tg_id)
    filters_data = POSITION_FILTERS_CACHE.get(tg_id, {})
    
    if not data:
        return
    
    page = filters_data.get("page", 0)
    _, direction = query.data.split(":", 1)
    
    if direction == "next":
        page += 1
    elif direction == "prev":
        page -= 1
    
    filters_data["page"] = page
    POSITION_FILTERS_CACHE[tg_id] = filters_data
    
    text, keyboard = format_positions_table(data, page=page)
    
    if filters_data.get("exchange") is None and ALL_EXCHANGE_PAGINATION_CACHE.get(tg_id):
        exchange_list = EXCHANGE_LIST_CACHE.get(tg_id, [])
        pagination_data = ALL_EXCHANGE_PAGINATION_CACHE.get(tg_id, {"loaded_count": 0})
        loaded_count = pagination_data.get("loaded_count", 0)
        
        load_more_row = []
        if loaded_count < len(exchange_list):
            load_more_row.append(InlineKeyboardButton(
                f"📥 Load 2 More ({loaded_count}/{len(exchange_list)})",
                callback_data="positions_exchange:load_more"
            ))
        else:
            load_more_row.append(InlineKeyboardButton(
                "✅ All Loaded",
                callback_data="positions_exchange:all_loaded"
            ))
        keyboard.append(load_more_row)
    
    if filters_data.get("exchange") is not None or ALL_EXCHANGE_PAGINATION_CACHE.get(tg_id):
        back_button = [InlineKeyboardButton("⬅ Back to Exchanges", callback_data="positions_exchange:back")]
        keyboard.append(back_button)
    
    await query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
    )


async def positions_filter_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle filter callbacks."""
    query = update.callback_query
    await query.answer()
    
    tg_id = query.from_user.id
    _, filter_type = query.data.split(":", 1)
    
    filters_data = POSITION_FILTERS_CACHE.get(tg_id, {})
    
    if filter_type == "clear":
        filters_data.pop("exchange", None)
        filters_data.pop("symbol", None)
        filters_data["page"] = 0
        POSITION_FILTERS_CACHE[tg_id] = filters_data
        
        token, user = get_logged_in(update, context)
        if token and user:
            user_ids = get_all_user_ids_under_user(user)
            aggregated_data = aggregate_positions(user_ids)
            POSITION_DATA_CACHE[tg_id] = aggregated_data
            
            text, keyboard = format_positions_table(aggregated_data, page=0)
            await query.edit_message_text(
                text,
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
            )
        return
    
    filter_key = filter_type
    context.user_data[f"positions_waiting_{filter_key}"] = True
    
    await query.edit_message_text(
        f"🔍 Please send the {filter_key} name to filter by (or /cancel to cancel):",
        parse_mode="HTML",
    )


async def handle_filter_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle filter input from user."""
    if not update.message or not update.message.text:
        return
    
    tg_id = update.effective_user.id
    text = update.message.text.strip()
    
    if text.startswith("/"):
        return
    
    waiting_exchange = context.user_data.pop("positions_waiting_exchange", False)
    waiting_symbol = context.user_data.pop("positions_waiting_symbol", False)
    
    if not (waiting_exchange or waiting_symbol):
        return
    
    filters_data = POSITION_FILTERS_CACHE.get(tg_id, {})
    
    if waiting_exchange:
        filters_data["exchange"] = text if text else None
        filters_data["page"] = 0
        POSITION_FILTERS_CACHE[tg_id] = filters_data
        
        token, user = get_logged_in(update, context)
        if token and user:
            user_ids = get_all_user_ids_under_user(user)
            exchange_filter = filters_data.get("exchange")
            symbol_filter = filters_data.get("symbol")
            
            aggregated_data = aggregate_positions(
            user_ids, 
            exchange_filter=exchange_filter, 
            symbol_filter=symbol_filter
        )
            POSITION_DATA_CACHE[tg_id] = aggregated_data
            
            msg_text, keyboard = format_positions_table(aggregated_data, page=0)
            msg = await update.message.reply_text(
                msg_text,
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
            )
            remember_bot_message_from_message(update, msg)
    
    elif waiting_symbol:
        filters_data["symbol"] = text if text else None
        filters_data["page"] = 0
        POSITION_FILTERS_CACHE[tg_id] = filters_data
        
        token, user = get_logged_in(update, context)
        if token and user:
            user_ids = get_all_user_ids_under_user(user)
            exchange_filter = filters_data.get("exchange")
            symbol_filter = filters_data.get("symbol")
            
            aggregated_data = aggregate_positions(
            user_ids, 
            exchange_filter=exchange_filter, 
            symbol_filter=symbol_filter
        )
            POSITION_DATA_CACHE[tg_id] = aggregated_data
            
            msg_text, keyboard = format_positions_table(aggregated_data, page=0)
            msg = await update.message.reply_text(
                msg_text,
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
            )
            remember_bot_message_from_message(update, msg)


def get_script_wise_pnl_data(user_ids: List[ObjectId]) -> List[Dict[str, Any]]:
    """
    Get script-wise PnL data aggregated by exchange and symbol.
    Uses open_positions collection and symbols collection for LTP.
    """
    if not user_ids:
        return []
    
    try:
        all_positions = list(open_positions.find({"userId": {"$in": user_ids}}))
        logger.info(f"get_script_wise_pnl_data: Found {len(all_positions)} open positions")
        
        if not all_positions:
            return []
        
        symbol_ids = set()
        exchange_ids = set()
        for pos in all_positions:
            symbol_id = pos.get("symbolId")
            exchange_id = pos.get("exchangeId")
            if symbol_id:
                symbol_ids.add(symbol_id)
            if exchange_id:
                exchange_ids.add(exchange_id)
        
        symbol_data_map = {}
        for symbol_id in symbol_ids:
            try:
                symbol_oid = ObjectId(symbol_id) if not isinstance(symbol_id, ObjectId) else symbol_id
                symbol_doc = symbols.find_one({"_id": symbol_oid})
                if symbol_doc:
                    symbol_data_map[symbol_id] = {
                        "symbolname": symbol_doc.get("name") or symbol_doc.get("title") or symbol_doc.get("symbolname") or symbol_doc.get("symbolName") or "",
                        "ltp": float(symbol_doc.get("ltp") or symbol_doc.get("lastPrice") or 0),
                    }
                else:
                    symbol_data_map[symbol_id] = {"symbolname": "", "ltp": 0}
            except Exception as e:
                logger.warning(f"Error fetching symbol {symbol_id}: {e}")
                symbol_data_map[symbol_id] = {"symbolname": "", "ltp": 0}
        
        exchange_name_map = {}
        for ex_id in exchange_ids:
            exchange_name_map[ex_id] = resolve_exchange_name(ex_id)
        
        grouped: Dict[Tuple[str, Any], Dict[str, Any]] = defaultdict(lambda: {
            "exchange": "",
            "symbol_id": None,
            "symbol": "",
            "buy_qty": 0.0,
            "buy_price_qty_sum": 0.0,
            "sell_qty": 0.0,
            "sell_price_qty_sum": 0.0,
            "ltp": 0.0,
        })
        
        for pos in all_positions:
            exchange_id = pos.get("exchangeId")
            symbol_id = pos.get("symbolId")
            if not exchange_id or not symbol_id:
                continue
            
            exchange_name = exchange_name_map.get(exchange_id, "UNKNOWN")
            key = (exchange_name, symbol_id)
            group = grouped[key]
            group["exchange"] = exchange_name
            group["symbol_id"] = symbol_id
            
            symbol_info = symbol_data_map.get(symbol_id, {"symbolname": "", "ltp": 0})
            if not group["symbol"]:
                group["symbol"] = symbol_info["symbolname"]
            if symbol_info["ltp"] > 0:
                group["ltp"] = symbol_info["ltp"]
            
            trade_type = str(pos.get("tradeType") or pos.get("orderType") or "").lower()
            buy_price = float(pos.get("buyPrice") or pos.get("price") or 0)
            buy_total_qty = float(pos.get("buyTotalQuantity") or pos.get("totalQuantity") or pos.get("quantity") or 0)
            
            if trade_type in ["buy", "b"]:
                group["buy_qty"] += buy_total_qty
                group["buy_price_qty_sum"] += buy_price * buy_total_qty
            elif trade_type in ["sell", "s"]:
                group["sell_qty"] += buy_total_qty
                group["sell_price_qty_sum"] += buy_price * buy_total_qty
        
        results: List[Dict[str, Any]] = []
        
        for (ex_name, symbol_id), group in grouped.items():
            buy_qty = group["buy_qty"]
            sell_qty = group["sell_qty"]
            net_qty = buy_qty - sell_qty
            
            avg_buy = group["buy_price_qty_sum"] / buy_qty if buy_qty > 0 else 0.0
            avg_sell = group["sell_price_qty_sum"] / sell_qty if sell_qty > 0 else 0.0
            
            net_avg_price = 0.0
            if abs(net_qty) > 0.01:
                net_avg_price = ((buy_qty * avg_buy) - (sell_qty * avg_sell)) / net_qty
            
            ltp = group["ltp"] if group["ltp"] > 0 else (avg_buy if avg_buy > 0 else avg_sell)
            
            pnl = 0.0
            if abs(net_qty) > 0.01 and ltp > 0 and net_avg_price > 0:
                pnl = (ltp - net_avg_price) * net_qty
            
            results.append({
                "exchange": ex_name or "—",
                "symbol": group["symbol"] or "—",
                "buy_qty": round(buy_qty, 2),
                "avg_buy": round(avg_buy, 2),
                "sell_qty": round(sell_qty, 2),
                "avg_sell": round(avg_sell, 2),
                "net_qty": round(net_qty, 2),
                "net_avg_price": round(net_avg_price, 2),
                "ltp": round(ltp, 2),
                "pnl": round(pnl, 2),
            })
        
        exchange_sequence_map = {}
        try:
            all_exchanges = list(exchange.find({}, {"name": 1, "masterName": 1, "sequence": 1}))
            for ex in all_exchanges:
                name = ex.get("name") or ex.get("masterName")
                if name:
                    exchange_sequence_map[name] = ex.get("sequence", 999)
                    if ex.get("masterName") and ex.get("masterName") != name:
                        exchange_sequence_map[ex.get("masterName")] = ex.get("sequence", 999)
        except Exception as e:
            logger.warning(f"Error building exchange sequence map in get_script_wise_pnl_data: {e}")
        
        return sorted(results, key=lambda x: (
            exchange_sequence_map.get(x["exchange"], 999),
            x["exchange"],
            x["symbol"]
        ))
    except Exception as e:
        logger.error(f"Error getting script-wise PnL data: {e}", exc_info=True)
        return []


def format_script_pnl_table(data: List[Dict[str, Any]], page: int = 0, page_size: int = 10) -> Tuple[str, List[List[InlineKeyboardButton]]]:
    """Format script-wise PnL data as a table."""
    if not data:
        return "📊 No script-wise PnL data found.", []
    
    total = len(data)
    max_page = (total - 1) // page_size
    if page < 0:
        page = 0
    if page > max_page:
        page = max_page
    
    start = page * page_size
    end = start + page_size
    chunk = data[start:end]
    
    header = (
        "📊 <b>Script Wise PnL</b>\n"
        f"Page {page + 1} / {max_page + 1} | Total: {total}\n"
    )
    
    table_rows = []
    header_line = (
        f"{'EX':<8} | {'SYMBOL':<12} | {'PNL':>14} | {'BUY QTY':>10} | {'AVG BUY':>9} | "
        f"{'SELL QTY':>10} | {'AVG SELL':>9} | {'NET QTY':>10} | {'NET AVG':>10} | {'LTP':>9}"
    )
    table_rows.append(f"<pre>{header_line}</pre>")
    separator = "-" * len(header_line)
    table_rows.append(f"<pre>{separator}</pre>")
    
    for row in chunk:
        net_qty = row['net_qty']
        net_qty_str = f"{net_qty:,.2f}"
        pnl = row['pnl']
        
        if pnl >= 0:
            pnl_str = f"+{pnl:,.2f}"
        else:
            pnl_str = f"{pnl:,.2f}"
        
        ex_name = (row['exchange'][:7] if row['exchange'] else "—")[:7]
        sym_name = (row['symbol'][:11] if row['symbol'] else "—")[:11]
        
        buy_qty_str = f"{row['buy_qty']:,.2f}"
        sell_qty_str = f"{row['sell_qty']:,.2f}"
        avg_buy_str = f"{row['avg_buy']:,.2f}"
        avg_sell_str = f"{row['avg_sell']:,.2f}"
        net_avg_str = f"{row['net_avg_price']:,.2f}"
        ltp_str = f"{row['ltp']:,.2f}"
        
        data_line = (
            f"{ex_name:<8} | {sym_name:<12} | {pnl_str:>14} | {buy_qty_str:>10} | {avg_buy_str:>9} | "
            f"{sell_qty_str:>10} | {avg_sell_str:>9} | {net_qty_str:>10} | {net_avg_str:>10} | {ltp_str:>9}"
        )
        table_rows.append(f"<pre>{data_line}</pre>")
    
    text = header + "\n".join(table_rows)
    
    keyboard: List[List[InlineKeyboardButton]] = []
    
    nav_row: List[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅ Prev", callback_data="script_pnl_page:prev"))
    if end < total:
        nav_row.append(InlineKeyboardButton("Next ➡", callback_data="script_pnl_page:next"))
    if nav_row:
        keyboard.append(nav_row)
    
    back_row: List[InlineKeyboardButton] = []
    back_row.append(InlineKeyboardButton("⬅ Back to Positions", callback_data="script_pnl_page:back"))
    keyboard.append(back_row)
    
    return text, keyboard


SCRIPT_PNL_CACHE: Dict[int, Dict[str, Any]] = {}


async def script_pnl_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle script-wise PnL button callback."""
    query = update.callback_query
    await query.answer()
    
    token, user = get_logged_in(update, context)
    if not token or not user:
        return await require_login_from_query(query, context)
    
    tg_id = query.from_user.id
    
    if query.data == "positions_script_pnl:show":
        user_ids = get_all_user_ids_under_user(user)
        if not user_ids:
            await query.edit_message_text("📊 No users found under you.")
            return
        
        try:
            script_pnl_data = get_script_wise_pnl_data(user_ids)
            logger.info(f"script_pnl_callback: Found {len(script_pnl_data)} script-wise PnL entries")
            
            SCRIPT_PNL_CACHE[tg_id] = {"data": script_pnl_data, "page": 0}
            
            text, keyboard = format_script_pnl_table(script_pnl_data, page=0)
            await query.edit_message_text(
                text,
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
            )
        except Exception as e:
            logger.error(f"Error showing script-wise PnL: {e}", exc_info=True)
            await query.edit_message_text(f"⚠ Error loading script-wise PnL: {str(e)}")


async def script_pnl_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle script-wise PnL pagination callbacks."""
    query = update.callback_query
    await query.answer()
    
    tg_id = query.from_user.id
    cache = SCRIPT_PNL_CACHE.get(tg_id)
    
    if not cache:
        return
    
    _, action = query.data.split(":", 1)
    
    if action == "back":
        token, user = get_logged_in(update, context)
        if not token or not user:
            return
        
        user_ids = get_all_user_ids_under_user(user)
        if not user_ids:
            await query.edit_message_text("📊 No users found under you.")
            return
        
        filters_data = POSITION_FILTERS_CACHE.get(tg_id, {})
        exchange_filter = filters_data.get("exchange")
        symbol_filter = filters_data.get("symbol")
        
        try:
            aggregated_data = aggregate_positions(
                user_ids, 
                exchange_filter=exchange_filter, 
                symbol_filter=symbol_filter
            )
            POSITION_DATA_CACHE[tg_id] = aggregated_data
            
            text, keyboard = format_positions_table(aggregated_data, page=0)
            await query.edit_message_text(
                text,
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
            )
        except Exception as e:
            logger.error(f"Error returning to positions: {e}", exc_info=True)
            await query.edit_message_text(f"⚠ Error: {str(e)}")
        return
    
    page = cache.get("page", 0)
    data = cache.get("data", [])
    
    if action == "next":
        page += 1
    elif action == "prev":
        page -= 1
    
    cache["page"] = page
    SCRIPT_PNL_CACHE[tg_id] = cache
    
    text, keyboard = format_script_pnl_table(data, page=page)
    
    await query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
    )


def register_position_handlers(app):
    app.add_handler(CommandHandler(["positions", "position"], positions_cmd))
    app.add_handler(CallbackQueryHandler(positions_exchange_callback, pattern=r"^positions_exchange:"))
    app.add_handler(CallbackQueryHandler(positions_page_callback, pattern=r"^positions_page:"))
    app.add_handler(CallbackQueryHandler(positions_filter_callback, pattern=r"^positions_filter:"))
    app.add_handler(CallbackQueryHandler(script_pnl_callback, pattern=r"^positions_script_pnl:"))
    app.add_handler(CallbackQueryHandler(script_pnl_page_callback, pattern=r"^script_pnl_page:"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_filter_input), group=98)
