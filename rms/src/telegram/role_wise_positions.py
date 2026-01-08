# src/telegram/role_wise_positions.py

from typing import Dict, Any, List, Tuple, Optional
import html
import logging
from datetime import datetime, timedelta, timezone
from bson import ObjectId
from collections import defaultdict

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

from src.config import trade_market, open_positions, exchange
from src.helpers.hierarchy_service import (
    get_admins_for_superadmin,
    get_masters_for_superadmin,
    get_masters_for_admin,
    get_users_for_superadmin,
    get_users_for_admin,
    get_users_for_master,
    get_user_full_by_id,
)

from .main import (
    get_logged_in,
    require_login,
    require_login_from_query,
    safe_delete_message,
    role_name_from_user,
    display_name,
    build_all_accessible_users,
    today_utc_range,
)

from .session_store import remember_bot_message_from_message

logger = logging.getLogger(__name__)

# ---------- caches ----------
ENTITY_LIST_CACHE: Dict[int, Dict[str, Any]] = {}
ENTITY_PAGE_SIZE = 5

RWP_POS_CACHE: Dict[int, Dict[str, Any]] = {}
RWP_POS_PAGE_SIZE = 5

RWP_SEARCH_CACHE: Dict[int, Dict[str, Any]] = {}
RWP_SEARCH_PAGE_SIZE = 10


# ---------- helpers ----------
def resolve_user_display(user_id: ObjectId) -> str:
    """
    Convert a stored userId(ObjectId) into a readable name/userName/username/phone.
    Falls back to the raw ObjectId string if lookup fails.
    """
    try:
        user = get_user_full_by_id(user_id)
        if not user:
            return str(user_id)

        return (
            user.get("name")
            or user.get("userName")
            or user.get("username")
            or user.get("phone")
            or str(user_id)
        )
    except Exception:
        return str(user_id)

def format_entity_header(entity_type: str, entity_id: str) -> str:
    """
    Example: 'Master: John'
    """
    try:
        name = resolve_user_display(ObjectId(entity_id))
    except Exception:
        name = entity_id
    return f"{entity_type}: {html.escape(str(name))}"

def _oid_from_user_doc(u: Dict[str, Any]) -> Optional[ObjectId]:
    raw = u.get("id") or u.get("_id")
    if not raw:
        return None
    try:
        return ObjectId(str(raw))
    except Exception:
        return None


def _role_title(rn: str) -> str:
    if rn == "superadmin":
        return "Role-wise Positions (Superadmin)"
    if rn == "admin":
        return "Role-wise Positions (Admin)"
    if rn == "master":
        return "Role-wise Positions (Master)"
    return "Role-wise Positions"


def _load_entities_for_category(current_user: Dict[str, Any], category: str) -> Tuple[str, List[Dict[str, Any]]]:
    rn = role_name_from_user(current_user)
    me_oid = _oid_from_user_doc(current_user)
    if not me_oid:
        return "❌ Invalid user id.", []

    if category == "admin":
        if rn != "superadmin":
            return "❌ Only superadmin can view Admins.", []
        title = "👑 Admins under you:"
        data = get_admins_for_superadmin(me_oid)
        return title, data

    if category == "master":
        if rn == "superadmin":
            title = "🧩 Masters (all):"
            data = get_masters_for_superadmin(me_oid)
            return title, data
        if rn == "admin":
            title = "🧩 Masters under you:"
            data = get_masters_for_admin(me_oid)
            return title, data
        return "❌ Only superadmin or admin can view Masters.", []

    if category == "client":
        if rn == "superadmin":
            title = "👤 Clients (all):"
            data = get_users_for_superadmin(me_oid)
            return title, data
        if rn == "admin":
            title = "👤 Clients under you:"
            data = get_users_for_admin(me_oid)
            return title, data
        if rn == "master":
            title = "👤 Clients under you:"
            data = get_users_for_master(me_oid)
            return title, data
        return "❌ Only superadmin/admin/master can view Clients.", []

    return "❌ Unknown category.", []


def _load_clients_under_entity(entity_category: str, entity_oid: ObjectId) -> List[Dict[str, Any]]:
    if entity_category == "admin":
        return get_users_for_admin(entity_oid)
    if entity_category == "master":
        return get_users_for_master(entity_oid)
    return []


def get_current_week_range() -> Tuple[datetime, datetime]:
    """Get Monday to Sunday of the current week in UTC."""
    now = datetime.now(timezone.utc)
    days_since_monday = now.weekday()
    monday = now - timedelta(days=days_since_monday)
    monday_start = datetime(monday.year, monday.month, monday.day, 0, 0, 0, tzinfo=timezone.utc)
    sunday_end = monday_start + timedelta(days=7)
    return monday_start, sunday_end


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


def aggregate_positions_for_role_wise(user_ids: List[ObjectId]) -> List[Dict[str, Any]]:
    """
    Aggregate positions by exchange and symbol for role-wise positions.
    Uses same logic as positions.py: open_positions + trade_market (current week, excluding linked trades).
    No exchange filter applied - shows all exchanges.
    """
    if not user_ids:
        return []
    
    try:
        logger.info(f"aggregate_positions_for_role_wise: Starting query for {len(user_ids)} users...")
        
        positions_data = []
        
        open_positions_list = list(open_positions.find({"userId": {"$in": user_ids}}))
        open_positions_count = len(open_positions_list)
        logger.info(f"📊 [1] Fetched from open_positions: {open_positions_count} documents")
        
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
        logger.info(f"aggregate_positions_for_role_wise: Current week range: {week_start} to {week_end}")
        
        query_without_exclude: Dict[str, Any] = {
            "userId": {"$in": user_ids},
            "createdAt": {
                "$gte": week_start,
                "$lt": week_end
            }
        }
        
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
            logger.warning(f"aggregate_positions_for_role_wise: No positions found")
            return []
        
        unique_exchange_ids = set()
        for pos in positions_data:
            ex_id = pos.get("exchangeId")
            if ex_id:
                unique_exchange_ids.add(ex_id)
        
        logger.info(f"aggregate_positions_for_role_wise: Pre-loading {len(unique_exchange_ids)} unique exchange IDs into cache...")
        for ex_id in unique_exchange_ids:
            resolve_exchange_name(ex_id)
        logger.info(f"aggregate_positions_for_role_wise: Exchange cache pre-loaded, starting aggregation...")
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
                logger.info(f"aggregate_positions_for_role_wise: Processed {processed_count}/{len(positions_data)} positions...")
        except Exception as e:
            error_count += 1
            if error_count < 5:
                logger.error(f"Error processing position {pos.get('_id')}: {e}", exc_info=True)
            continue
    
    logger.info(f"aggregate_positions_for_role_wise: Processed {processed_count} positions - Buy: {buy_count}, Sell: {sell_count}, Unknown: {unknown_type_count}, Errors: {error_count}")
    logger.info(f"aggregate_positions_for_role_wise: Created {len(grouped)} groups")
    
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
    logger.info(f"aggregate_positions_for_role_wise: Final sorted results count: {len(sorted_results)}")
    return sorted_results


def format_positions_table_for_role_wise(data: List[Dict[str, Any]], page: int = 0, page_size: int = 10, header_title: str = "") -> Tuple[str, List[List[InlineKeyboardButton]]]:
    """Format positions data as a table (same format as positions.py)."""
    if not data:
        return f"{header_title}\n\n📊 No positions found.", []
    
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
        f"{header_title}\n"
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
        nav_row.append(InlineKeyboardButton("⬅ Prev", callback_data="rwp_pos_page:prev"))
    if end < total:
        nav_row.append(InlineKeyboardButton("Next ➡", callback_data="rwp_pos_page:next"))
    if nav_row:
        keyboard.append(nav_row)
    
    return text, keyboard


def _build_entity_list_page(tg_id: int) -> Tuple[str, List[List[InlineKeyboardButton]]]:
    cache = ENTITY_LIST_CACHE.get(tg_id)
    if not cache:
        return "No list cached.", []

    title: str = cache["title"]
    entities: List[Dict[str, Any]] = cache["entities"]
    page: int = cache["page"]
    category: str = cache["category"]

    if not entities:
        return f"{html.escape(title)}\n\nNo records found.", []

    total = len(entities)
    max_page = (total - 1) // ENTITY_PAGE_SIZE
    page = max(0, min(page, max_page))
    cache["page"] = page

    start_i = page * ENTITY_PAGE_SIZE
    end_i = start_i + ENTITY_PAGE_SIZE
    chunk = entities[start_i:end_i]

    text = (
        f"{html.escape(title)}\n"
        f"Page {page + 1} / {max_page + 1}\n\n"
        "Select one:"
    )

    keyboard: List[List[InlineKeyboardButton]] = []
    for u in chunk:
        uid = str(u.get("id") or u.get("_id"))
        label = display_name(u)
        keyboard.append([InlineKeyboardButton(label, callback_data=f"rwp_entity:{category}:{uid}")])

    nav_row: List[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅ Prev", callback_data="rwp_entity_page:prev"))
    if end_i < total:
        nav_row.append(InlineKeyboardButton("Next ➡", callback_data="rwp_entity_page:next"))
    if nav_row:
        keyboard.append(nav_row)

    return text, keyboard


# ---------- /role_wise_position main ----------
async def role_wise_position_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    token, user = get_logged_in(update, context)
    if not token or not user:
        return await require_login(update, context)

    rn = role_name_from_user(user)
    title = _role_title(rn)

    buttons: List[List[InlineKeyboardButton]] = []
    if rn == "superadmin":
        buttons = [
            [InlineKeyboardButton("👑 Admins", callback_data="rwp_menu:admin")],
            [InlineKeyboardButton("🧩 Masters", callback_data="rwp_menu:master")],
            [InlineKeyboardButton("👤 Clients", callback_data="rwp_menu:client")],
            [InlineKeyboardButton("🔍 Search User", callback_data="rwp_search:start")],
        ]
    elif rn == "admin":
        buttons = [
            [InlineKeyboardButton("🧩 Masters", callback_data="rwp_menu:master")],
            [InlineKeyboardButton("👤 Clients", callback_data="rwp_menu:client")],
        ]
    elif rn == "master":
        buttons = [
            [InlineKeyboardButton("👤 Clients", callback_data="rwp_menu:client")],
        ]
    else:
        msg = await update.message.reply_text(
            "❌ This command is only for <b>superadmin</b>, <b>admin</b> or <b>master</b>.",
            parse_mode="HTML",
        )
        remember_bot_message_from_message(update, msg)
        return

    text = (
        f"📌 <b>{html.escape(title)}</b>\n\n"
        "Choose a category or search:\n"
        "Admins / Masters / Clients / Search Client\n"
        "Tap a button below."
    )

    msg = await update.message.reply_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(buttons),
    )
    remember_bot_message_from_message(update, msg)


# ---------- menu → list entities ----------
async def rwp_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    token, user = get_logged_in(update, context)
    if not token or not user:
        return await require_login_from_query(query, context)

    tg_id = query.from_user.id
    _, cat = query.data.split(":", 1)

    title, entities = _load_entities_for_category(user, cat)
    if not entities:
        return await query.edit_message_text(title, parse_mode="HTML")

    ENTITY_LIST_CACHE[tg_id] = {
        "category": cat,
        "title": title,
        "entities": entities,
        "page": 0,
    }

    text, keyboard = _build_entity_list_page(tg_id)
    await query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
    )


async def rwp_entity_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    tg_id = query.from_user.id
    cache = ENTITY_LIST_CACHE.get(tg_id)
    if not cache:
        return

    _, direction = query.data.split(":", 1)
    if direction == "next":
        cache["page"] += 1
    elif direction == "prev":
        cache["page"] -= 1

    text, keyboard = _build_entity_list_page(tg_id)
    await query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
    )


# ---------- entity selected → load positions ----------
async def rwp_entity_select_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    token, user = get_logged_in(update, context)
    if not token or not user:
        return await require_login_from_query(query, context)

    tg_id = query.from_user.id
    _, entity_category, entity_id = query.data.split(":", 2)

    try:
        entity_oid = ObjectId(entity_id)
    except Exception:
        msg = await query.message.reply_text("❌ Invalid selection.")
        remember_bot_message_from_message(update, msg)
        return

    # If entity is client: positions only for that client
    if entity_category == "client":
        client_ids = [entity_oid]
        header_title = "📊 <b>Positions Summary</b>\n" + format_entity_header("Client", entity_id)
    else:
        try:
            clients = _load_clients_under_entity(entity_category, entity_oid)
        except Exception as e:
            logger.error(f"_load_clients_under_entity error: {e}")
            msg = await query.message.reply_text("⚠ Error while loading clients under selection.")
            remember_bot_message_from_message(update, msg)
            return

        client_ids = []
        for c in clients:
            cid = c.get("id") or c.get("_id")
            if cid:
                try:
                    client_ids.append(ObjectId(str(cid)))
                except Exception:
                    pass

        header_title = "📊 <b>Positions Summary</b>\n" + format_entity_header(entity_category.title(), entity_id)

    if not client_ids:
        msg = await query.message.reply_text("📊 No clients found under that selection.")
        remember_bot_message_from_message(update, msg)
        return

    try:
        aggregated_data = aggregate_positions_for_role_wise(client_ids)
    except Exception as e:
        logger.error(f"positions query error: {e}", exc_info=True)
        msg = await query.message.reply_text("⚠ Error while loading positions.")
        remember_bot_message_from_message(update, msg)
        return

    RWP_POS_CACHE[tg_id] = {
        "items": aggregated_data,
        "page": 0,
        "header_title": header_title,
    }

    text, keyboard = format_positions_table_for_role_wise(aggregated_data, page=0, header_title=header_title)
    
    back_button = [InlineKeyboardButton("⬅ Back", callback_data="rwp_pos_page:back")]
    keyboard.append(back_button)
    
    if len(text) > 4096:
        text = text[:4000] + "\n\n... (truncated)"
    
    msg = await query.message.reply_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
    )
    remember_bot_message_from_message(update, msg)


async def rwp_pos_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    tg_id = query.from_user.id
    cache = RWP_POS_CACHE.get(tg_id)
    if not cache:
        return

    _, direction = query.data.split(":", 1)
    
    if direction == "back":
        token, user = get_logged_in(update, context)
        if not token or not user:
            return
        
        rn = role_name_from_user(user)
        title = _role_title(rn)

        buttons: List[List[InlineKeyboardButton]] = []
        if rn == "superadmin":
            buttons = [
                [InlineKeyboardButton("👑 Admins", callback_data="rwp_menu:admin")],
                [InlineKeyboardButton("🧩 Masters", callback_data="rwp_menu:master")],
                [InlineKeyboardButton("👤 Clients", callback_data="rwp_menu:client")],
                [InlineKeyboardButton("🔍 Search User", callback_data="rwp_search:start")],
            ]
        elif rn == "admin":
            buttons = [
                [InlineKeyboardButton("🧩 Masters", callback_data="rwp_menu:master")],
                [InlineKeyboardButton("👤 Clients", callback_data="rwp_menu:client")],
            ]
        elif rn == "master":
            buttons = [
                [InlineKeyboardButton("👤 Clients", callback_data="rwp_menu:client")],
            ]
        
        text = (
            f"📌 <b>{html.escape(title)}</b>\n\n"
            "Choose a category or search:\n"
            "Admins / Masters / Clients / Search User\n"
            "Tap a button below."
        )
        
        await query.edit_message_text(
            text,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(buttons),
        )
        return
    
    if direction == "next":
        cache["page"] += 1
    elif direction == "prev":
        cache["page"] -= 1

    header_title = cache.get("header_title", "📊 <b>Positions Summary</b>")
    text, keyboard = format_positions_table_for_role_wise(cache["items"], page=cache["page"], header_title=header_title)
    
    back_button = [InlineKeyboardButton("⬅ Back", callback_data="rwp_pos_page:back")]
    keyboard.append(back_button)
    
    await query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
    )


async def rwp_pos_detail_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    token, _ = get_logged_in(update, context)
    if not token:
        return await require_login_from_query(query, context)

    _, pid = query.data.split(":", 1)

    try:
        doc = trade_market.find_one({"_id": ObjectId(pid)})
    except Exception as e:
        logger.error(f"rwp_pos_detail find_one error: {e}")
        msg = await query.message.reply_text("❌ Could not load position details.")
        remember_bot_message_from_message(update, msg)
        return

    if not doc:
        msg = await query.message.reply_text("❌ Position not found.")
        remember_bot_message_from_message(update, msg)
        return

    # ✅ UPDATED: show readable User, not ObjectId
    field_order = [
        ("Symbol", "symbolName"),
        ("Quantity", "quantity"),
        ("Price", "price"),
        ("Lot Size", "lotSize"),
        ("Total Quantity", "totalQuantity"),
        ("Total", "total"),
        ("Product Type", "productType"),
        ("Trade Type", "tradeType"),
        ("Trade Margin", "tradeMargin"),
        ("Trade Margin Price", "tradeMarginPrice"),
        ("Trade Margin Total", "tradeMarginTotal"),
        ("Created At", "createdAt"),
        ("Updated At", "updatedAt"),
    ]

    rows: List[str] = []
    label_width = max(len(label) for label, _ in field_order)
    label_width = max(label_width, len("User"))

    user_display = "-"
    uid = doc.get("userId")
    if isinstance(uid, ObjectId):
        user_display = resolve_user_display(uid)

    rows.append(f"{'User'.ljust(label_width)} : {user_display}")

    for label, key in field_order:
        raw_val = doc.get(key, "-")
        if isinstance(raw_val, ObjectId):
            raw_val = str(raw_val)
        if isinstance(raw_val, datetime):
            raw_val = raw_val.isoformat()
        text_val = "-" if raw_val is None else str(raw_val)
        rows.append(f"{label.ljust(label_width)} : {text_val}")

    table_text = html.escape("\n".join(rows))
    header = "📊 <b>Position Summary</b>\n\n<pre>" + table_text + "</pre>"

    msg = await query.message.reply_text(header, parse_mode="HTML")
    remember_bot_message_from_message(update, msg)


# ---------- search flow (clients only) ----------
async def rwp_search_start_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    token, user = get_logged_in(update, context)
    if not token or not user:
        return await require_login_from_query(query, context)

    context.user_data["rwp_search_mode"] = True

    msg = await query.message.reply_text(
        "🔍 <b>Search client</b>\nType <code>phone</code>, <code>userName</code> or <code>name</code>.",
        parse_mode="HTML",
    )
    remember_bot_message_from_message(update, msg)
    context.user_data["rwp_search_prompt_msg_id"] = msg.message_id


def _build_search_page(tg_id: int) -> Tuple[str, List[List[InlineKeyboardButton]]]:
    cache = RWP_SEARCH_CACHE.get(tg_id)
    if not cache:
        return "No active search.", []

    results: List[Dict[str, Any]] = cache["results"]
    page: int = cache["page"]
    query_str: str = cache["query"]

    if not results:
        return f"🔍 No clients found for \"{html.escape(query_str)}\".", []

    total = len(results)
    max_page = (total - 1) // RWP_SEARCH_PAGE_SIZE
    page = max(0, min(page, max_page))
    cache["page"] = page

    start_i = page * RWP_SEARCH_PAGE_SIZE
    end_i = start_i + RWP_SEARCH_PAGE_SIZE
    chunk = results[start_i:end_i]

    text = (
        f"🔍 Client results for \"{html.escape(query_str)}\"\n"
        f"Page {page + 1} / {max_page + 1}\n\n"
        "Select a client:"
    )

    keyboard: List[List[InlineKeyboardButton]] = []
    for u in chunk:
        uid = str(u.get("id") or u.get("_id"))
        label = display_name(u)
        keyboard.append([InlineKeyboardButton(label, callback_data=f"rwp_entity:client:{uid}")])

    nav_row: List[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅ Prev", callback_data="rwp_search_page:prev"))
    if end_i < total:
        nav_row.append(InlineKeyboardButton("Next ➡", callback_data="rwp_search_page:next"))
    if nav_row:
        keyboard.append(nav_row)

    return text, keyboard


async def rwp_search_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("rwp_search_mode"):
        return

    context.user_data["rwp_search_mode"] = False

    term = (update.message.text or "").strip()
    chat_id = update.effective_chat.id
    tg_id = update.effective_user.id

    try:
        await update.message.delete()
    except Exception:
        pass

    prompt_id = context.user_data.pop("rwp_search_prompt_msg_id", None)
    if prompt_id is not None:
        await safe_delete_message(context.bot, chat_id, prompt_id)

    if not term:
        msg = await update.effective_chat.send_message(
            "⚠ Empty search. Use /role_wise_position and tap <b>Search Client</b> again.",
            parse_mode="HTML",
        )
        remember_bot_message_from_message(update, msg)
        return

    token, user = get_logged_in(update, context)
    if not token or not user:
        msg = await update.effective_chat.send_message(
            "⚠ Session expired. Please /start and login again.",
            parse_mode="HTML",
        )
        remember_bot_message_from_message(update, msg)
        return

    all_users = build_all_accessible_users(user)
    term_l = term.lower()

    results: List[Dict[str, Any]] = []
    for u in all_users:
        rn = role_name_from_user(u)
        if rn in ("superadmin", "admin", "master"):
            continue

        for field in ("phone", "userName", "username", "name"):
            val = u.get(field)
            if val and term_l in str(val).lower():
                results.append(u)
                break

    RWP_SEARCH_CACHE[tg_id] = {
        "query": term,
        "results": results,
        "page": 0,
    }

    text, keyboard = _build_search_page(tg_id)
    msg = await update.effective_chat.send_message(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
    )
    remember_bot_message_from_message(update, msg)


async def rwp_search_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    tg_id = query.from_user.id
    cache = RWP_SEARCH_CACHE.get(tg_id)
    if not cache:
        return

    _, direction = query.data.split(":", 1)
    if direction == "next":
        cache["page"] += 1
    elif direction == "prev":
        cache["page"] -= 1

    text, keyboard = _build_search_page(tg_id)
    await query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
    )


# ---------- register ----------
def register_role_wise_position_handlers(app):
    app.add_handler(CommandHandler(["role_wise_position", "role_wise_positions"], role_wise_position_cmd))

    app.add_handler(CallbackQueryHandler(rwp_menu_callback, pattern=r"^rwp_menu:"))
    app.add_handler(CallbackQueryHandler(rwp_entity_page_callback, pattern=r"^rwp_entity_page:"))
    app.add_handler(CallbackQueryHandler(rwp_entity_select_callback, pattern=r"^rwp_entity:"))

    app.add_handler(CallbackQueryHandler(rwp_pos_page_callback, pattern=r"^rwp_pos_page:"))
    app.add_handler(CallbackQueryHandler(rwp_pos_detail_callback, pattern=r"^rwp_pos_detail:"))

    app.add_handler(CallbackQueryHandler(rwp_search_start_callback, pattern=r"^rwp_search:start$"))
    app.add_handler(CallbackQueryHandler(rwp_search_page_callback, pattern=r"^rwp_search_page:"))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, rwp_search_text_handler))
