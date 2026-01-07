# src/telegram/positions.py

from typing import Dict, Any, List, Tuple, Optional
import html
from datetime import datetime
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

from src.config import trade_market, exchange
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
    
    query: Dict[str, Any] = {
        "userId": {"$in": user_ids},
        "squareOff": False,
    }
    
    if date_from or date_to:
        date_range: Dict[str, Any] = {}
        if date_from:
            date_range["$gte"] = date_from
        if date_to:
            date_range["$lt"] = date_to
        query["createdAt"] = date_range
    
    if exchange_filter:
        ex_doc = exchange.find_one(
            {"$or": [{"name": exchange_filter}, {"masterName": exchange_filter}]},
            {"_id": 1}
        )
        if ex_doc:
            ex_id = ex_doc["_id"]
            query["exchangeId"] = {"$in": [ex_id, str(ex_id)]}
    
    if symbol_filter:
        query["symbolName"] = {"$regex": symbol_filter, "$options": "i"}
    
    try:
        logger.info(f"aggregate_positions: Starting query for {len(user_ids)} users...")
        positions_data = list(trade_market.find(query).limit(10000))
        logger.info(f"aggregate_positions: Found {len(positions_data)} position records")
        if not positions_data:
            logger.warning(f"aggregate_positions: No positions found with query: {query}")
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
    
    sorted_results = sorted(results, key=lambda x: (x["exchange"], x["symbol"]))
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
        f"Page {page + 1} / {max_page + 1} | Total: {total}\n\n"
    )
    
    table_rows = []
    table_rows.append(
        "<pre>"
        f"{'EXCHANGE':<10} {'SYMBOL':<20} {'BUY QTY':>10} {'BUY AVG':>9} "
        f"{'SELL QTY':>10} {'SELL AVG':>9} {'NET QTY':>10} {'NET AVG':>10} "
        f"{'LTP':>9} {'PNL':>12}"
        "</pre>"
    )
    
    for row in chunk:
        net_qty = row['net_qty']
        net_qty_str = f"{net_qty:.2f}"
        pnl = row['pnl']
        
        if pnl >= 0:
            pnl_str = f"+{pnl:,.2f}"
        else:
            pnl_str = f"{pnl:,.2f}"
        
        ex_name = (row['exchange'][:9] if row['exchange'] else "—")[:9]
        sym_name = (row['symbol'][:19] if row['symbol'] else "—")[:19]
        
        table_rows.append(
            f"<pre>"
            f"{ex_name:<10} {sym_name:<20} "
            f"{row['buy_qty']:>10,.0f} {row['buy_avg']:>9,.2f} "
            f"{row['sell_qty']:>10,.0f} {row['sell_avg']:>9,.2f} "
            f"{net_qty_str:>10} {row['net_avg_price']:>10,.2f} "
            f"{row['ltp']:>9,.2f} {pnl_str:>12}"
            "</pre>"
        )
    
    text = header + "\n".join(table_rows)
    
    keyboard: List[List[InlineKeyboardButton]] = []
    
    nav_row: List[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅ Prev", callback_data="positions_page:prev"))
    if end < total:
        nav_row.append(InlineKeyboardButton("Next ➡", callback_data="positions_page:next"))
    if nav_row:
        keyboard.append(nav_row)
    
    filter_row: List[InlineKeyboardButton] = []
    filter_row.append(InlineKeyboardButton("🔍 Filter Exchange", callback_data="positions_filter:exchange"))
    filter_row.append(InlineKeyboardButton("🔍 Filter Symbol", callback_data="positions_filter:symbol"))
    filter_row.append(InlineKeyboardButton("🔄 Clear Filters", callback_data="positions_filter:clear"))
    keyboard.append(filter_row)
    
    return text, keyboard


async def positions_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /positions command."""
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
    
    filters_data = POSITION_FILTERS_CACHE.get(tg_id, {})
    exchange_filter = filters_data.get("exchange")
    symbol_filter = filters_data.get("symbol")
    
    try:
        aggregated_data = aggregate_positions(
            user_ids, 
            exchange_filter=exchange_filter, 
            symbol_filter=symbol_filter
        )
        logger.info(f"positions_cmd: Aggregated {len(aggregated_data)} position groups")
    except Exception as e:
        logger.error(f"positions_cmd: Error aggregating positions: {e}", exc_info=True)
        msg = await update.message.reply_text(f"⚠ Error while loading positions: {str(e)}")
        remember_bot_message_from_message(update, msg)
        return
    
    POSITION_DATA_CACHE[tg_id] = aggregated_data
    POSITION_FILTERS_CACHE[tg_id] = {"page": 0, "exchange": exchange_filter, "symbol": symbol_filter}
    
    if not aggregated_data:
        msg = await update.message.reply_text(
            "📊 No positions found.\n\n"
            f"Queried {len(user_ids)} users.\n"
            "Make sure you have open positions in the system.",
            parse_mode="HTML",
        )
        remember_bot_message_from_message(update, msg)
        return
    
    text, keyboard = format_positions_table(aggregated_data, page=0)
    
    try:
        if len(text) > 4096:
            text = text[:4000] + "\n\n... (truncated)"
            logger.warning(f"positions_cmd: Message too long, truncated to 4000 chars")
        
        msg = await update.message.reply_text(
            text,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
        )
        remember_bot_message_from_message(update, msg)
        logger.info(f"positions_cmd: Successfully sent message with {len(aggregated_data)} positions")
    except Exception as e:
        logger.error(f"Error sending positions message: {e}", exc_info=True)
        error_msg = f"⚠ Error displaying positions.\nFound: {len(aggregated_data)} groups\nError: {str(e)[:200]}"
        msg = await update.message.reply_text(error_msg)
        remember_bot_message_from_message(update, msg)


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


def register_position_handlers(app):
    app.add_handler(CommandHandler(["positions", "position"], positions_cmd))
    app.add_handler(CallbackQueryHandler(positions_page_callback, pattern=r"^positions_page:"))
    app.add_handler(CallbackQueryHandler(positions_filter_callback, pattern=r"^positions_filter:"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_filter_input), group=98)
