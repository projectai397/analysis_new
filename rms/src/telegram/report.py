# src/telegram/report.py

from typing import Dict, Any, List, Tuple
import logging
from datetime import datetime
from bson import ObjectId
from io import BytesIO
import asyncio

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)

from src.config import (
    users,
    open_positions,
    symbols,
    exchange,
    SUPERADMIN_ROLE_ID,
    ADMIN_ROLE_ID,
    MASTER_ROLE_ID,
)
from src.helpers.hierarchy_service import (
    get_users_for_superadmin,
    get_users_for_admin,
    get_users_for_master,
)
from .main import (
    get_logged_in,
    require_login,
    require_login_from_query,
    role_name_from_user,
)
from .session_store import remember_bot_message_from_message

try:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import letter, A4
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch
    HAS_REPORTLAB = True
except ImportError:
    HAS_REPORTLAB = False
    logging.warning("reportlab not installed. PDF generation will not work.")

logger = logging.getLogger(__name__)


def get_users_under_role(user: dict) -> List[Dict[str, Any]]:
    """
    Get all users under the logged-in user based on their role.
    Returns list of user dictionaries with full user data.
    """
    role = role_name_from_user(user)
    user_list: List[Dict[str, Any]] = []
    
    try:
        user_oid = ObjectId(user.get("id") or user.get("_id"))
    except Exception:
        logger.error("get_users_under_role: Invalid user ID")
        return []
    
    try:
        if role == "superadmin":
            user_list = get_users_for_superadmin(user_oid)
        elif role == "admin":
            user_list = get_users_for_admin(user_oid)
        elif role == "master":
            user_list = get_users_for_master(user_oid)
        else:
            logger.warning(f"get_users_under_role: Unknown role '{role}'")
            return []
        
        user_ids = [ObjectId(u.get("id") or u.get("_id")) for u in user_list if u.get("id") or u.get("_id")]
        
        full_users = []
        for uid in user_ids:
            user_doc = users.find_one({"_id": uid})
            if user_doc:
                full_users.append(user_doc)
        
        return full_users
    except Exception as e:
        logger.error(f"get_users_under_role: Error fetching users: {e}", exc_info=True)
        return []


def get_parent_username(user_doc: dict) -> str:
    """Get parent username from user document."""
    parent_id = user_doc.get("parentId") or user_doc.get("addedBy")
    if not parent_id:
        return "-"
    
    try:
        parent_oid = ObjectId(parent_id) if not isinstance(parent_id, ObjectId) else parent_id
        parent_doc = users.find_one({"_id": parent_oid})
        if parent_doc:
            return (
                parent_doc.get("userName")
                or parent_doc.get("name")
                or parent_doc.get("username")
                or parent_doc.get("phone")
                or "-"
            )
    except Exception:
        pass
    
    return "-"


def calculate_user_pnl(user_id: ObjectId) -> float:
    """
    Calculate total PnL for a user from all open positions.
    Returns total PnL as float.
    """
    try:
        user_positions = list(open_positions.find({"userId": user_id}))
        
        if not user_positions:
            return 0.0
        
        total_pnl = 0.0
        
        symbol_ids = set()
        for pos in user_positions:
            symbol_id = pos.get("symbolId")
            if symbol_id:
                symbol_ids.add(symbol_id)
        
        symbol_data = {}
        for symbol_id in symbol_ids:
            try:
                symbol_oid = ObjectId(symbol_id) if not isinstance(symbol_id, ObjectId) else symbol_id
                symbol_doc = symbols.find_one({"_id": symbol_oid})
                if symbol_doc:
                    symbol_data[symbol_id] = {
                        "symbolname": symbol_doc.get("name") or symbol_doc.get("title") or symbol_doc.get("symbolname") or symbol_doc.get("symbolName") or "",
                        "ask": float(symbol_doc.get("ask") or 0),
                        "bid": float(symbol_doc.get("bid") or 0),
                        "ltp": float(symbol_doc.get("ltp") or symbol_doc.get("lastPrice") or 0),
                    }
            except Exception as e:
                logger.warning(f"Error fetching symbol {symbol_id}: {e}")
                symbol_data[symbol_id] = {"symbolname": "", "ask": 0, "bid": 0, "ltp": 0}
        
        for pos in user_positions:
            try:
                symbol_id = pos.get("symbolId")
                if not symbol_id:
                    continue
                
                symbol_info = symbol_data.get(symbol_id, {"ask": 0, "bid": 0, "ltp": 0})
                # Use 'price' as entry price (same as positions.py) — buyPrice may be wrong for sell trades
                entry_price = float(pos.get("price") or pos.get("buyPrice") or pos.get("open_price") or 0)
                quantity = float(pos.get("quantity") or pos.get("totalQuantity") or 0)
                lot_size = float(pos.get("lotSize") or 1)
                total_qty = quantity * lot_size
                trade_type = str(pos.get("tradeType") or pos.get("orderType") or "").lower()
                ltp = symbol_info["ltp"]

                # Use LTP as current price (matching site / positions.py logic)
                # BUY: bought at entry_price, current value at LTP → PnL = (ltp - entry) * qty
                # SELL: sold at entry_price, current cost at LTP → PnL = (entry - ltp) * qty
                if trade_type in ["buy", "b"]:
                    pnl = (ltp - entry_price) * total_qty
                elif trade_type in ["sell", "s"]:
                    pnl = (entry_price - ltp) * total_qty
                else:
                    pnl = 0.0
                
                total_pnl += pnl
            except Exception as e:
                logger.warning(f"Error calculating PnL for position: {e}")
                continue
        
        return total_pnl
    except Exception as e:
        logger.error(f"Error calculating user PnL for {user_id}: {e}", exc_info=True)
        return 0.0


def calculate_use_margin(user_id: ObjectId) -> float:
    """Calculate total use margin (sum of tradeMarginTotal) for a user."""
    try:
        user_positions = list(open_positions.find({"userId": user_id}))
        total_margin = sum(float(pos.get("tradeMarginTotal") or 0) for pos in user_positions)
        return total_margin
    except Exception as e:
        logger.error(f"Error calculating use margin for {user_id}: {e}")
        return 0.0


def calculate_office_pnl_value(user_doc: dict, user_pnl: float) -> float:
    """
    Calculate office PnL value (not percentage).
    Office percentage is from inOfficeOf[0].pnl (as percentage)
    Office value = user_pnl * (office_percentage / 100)
    Example: if user_pnl is 5000 and office_percentage is 50, then office_value = 2500
    """
    try:
        in_office_of = user_doc.get("inOfficeOf")
        if not in_office_of or not isinstance(in_office_of, list) or len(in_office_of) == 0:
            return 0.0
        
        office_percentage = float(in_office_of[0].get("pnl", 0))
        if office_percentage == 0:
            return 0.0
        
        office_value = user_pnl * (office_percentage / 100)
        return office_value
    except Exception as e:
        logger.warning(f"Error calculating office PnL value: {e}")
        return 0.0


def generate_m2m_pdf(user_data_list: List[Dict[str, Any]]) -> BytesIO:
    """
    Generate M2M PDF report with all user data.
    Returns BytesIO buffer with PDF content.
    """
    if not HAS_REPORTLAB:
        raise ImportError("reportlab is not installed. Please install it to generate PDFs.")
    
    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        leftMargin=0.2*inch,
        rightMargin=0.2*inch,
        topMargin=0.3*inch,
        bottomMargin=0.3*inch
    )
    story = []
    
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=16,
        textColor=colors.HexColor('#000000'),
        spaceAfter=30,
        alignment=1,
    )
    
    story.append(Paragraph("Pnl Report", title_style))
    date_style = ParagraphStyle(
        'DateStyle',
        parent=styles['Normal'],
        fontSize=10,
        textColor=colors.HexColor('#666666'),
        alignment=1,
    )
    story.append(Paragraph(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", date_style))
    story.append(Spacer(1, 0.3*inch))
    
    # Columns: Username | Parent | Balance | PnL | Asset Value | Credit
    data = [["Username", "Parent", "Balance", "PnL", "Asset Value", "Credit"]]
    
    total_balance = 0.0
    total_credit = 0.0
    total_pnl = 0.0
    total_asset_value = 0.0
    
    for user_doc in user_data_list:
        username = (
            user_doc.get("userName")
            or user_doc.get("name")
            or user_doc.get("username")
            or user_doc.get("phone")
            or str(user_doc.get("_id", ""))
        )
        parent_username = get_parent_username(user_doc)
        balance = float(user_doc.get("balance") or 0)
        credit = float(user_doc.get("credit") or 0)
        user_id = user_doc.get("_id")
        
        pnl = calculate_user_pnl(user_id)
        asset_value = balance + pnl
        
        total_balance += balance
        total_credit += credit
        total_pnl += pnl
        total_asset_value += asset_value
        
        username_display = username[:7] + "..." if len(username) > 7 else username
        parent_display = (parent_username[:7] + "..." if len(parent_username) > 7 else parent_username) if parent_username != "-" else "-"
        
        # Display PnL with explicit + / - prefix
        if pnl > 0:
            pnl_display = f"+{pnl:,.2f}"
        elif pnl < 0:
            pnl_display = f"-{abs(pnl):,.2f}"
        else:
            pnl_display = f"{pnl:,.2f}"
        
        row = [
            username_display,
            parent_display,
            f"{balance:,.2f}",
            pnl_display,
            f"{asset_value:,.2f}",
            f"{credit:,.2f}",
        ]
        data.append(row)
    
    total_row_idx = len(data)
    data.append([
        "TOTAL",
        "-",
        f"{total_balance:,.2f}",
        # Total PnL with explicit + / - prefix
        (
            f"+{total_pnl:,.2f}"
            if total_pnl > 0
            else f"-{abs(total_pnl):,.2f}"
            if total_pnl < 0
            else f"{total_pnl:,.2f}"
        ),
        f"{total_asset_value:,.2f}",
        f"{total_credit:,.2f}",
    ])
    
    # A4 width 8.27" minus margins 0.2" each = 7.87"; wider Username/Parent so names don't overlap
    table = Table(
        data,
        colWidths=[
            1.45 * inch,  # Username (wider for 7 chars + ...)
            1.45 * inch,  # Parent (wider for 7 chars + ...)
            1.25 * inch,  # Balance
            1.25 * inch,  # PnL
            1.25 * inch,  # Asset Value
            1.25 * inch,  # Credit
        ],
    )
    
    table_style = TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#4A4A4A')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
        ('ALIGN', (0, 1), (1, -1), 'LEFT'),
        ('ALIGN', (2, 1), (-1, -1), 'RIGHT'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 11),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
        ('TOPPADDING', (0, 0), (-1, 0), 8),
        ('TEXTCOLOR', (0, 1), (-1, -2), colors.black),
        ('FONTNAME', (0, 1), (-1, -2), 'Helvetica'),
        ('FONTSIZE', (0, 1), (-1, -2), 10),
        ('TOPPADDING', (0, 1), (-1, -2), 6),
        ('BOTTOMPADDING', (0, 1), (-1, -2), 6),
        ('LEFTPADDING', (0, 1), (-1, -2), 10),
        ('RIGHTPADDING', (0, 1), (-1, -2), 10),
        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#CCCCCC')),
        ('BACKGROUND', (0, -1), (-1, -1), colors.HexColor('#E8E8E8')),
        ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, -1), (-1, -1), 11),
        ('TOPPADDING', (0, -1), (-1, -1), 7),
        ('BOTTOMPADDING', (0, -1), (-1, -1), 7),
        ('LEFTPADDING', (0, -1), (-1, -1), 10),
        ('RIGHTPADDING', (0, -1), (-1, -1), 10),
        ('ALIGN', (2, -1), (-1, -1), 'RIGHT'),
        ('TEXTCOLOR', (0, -1), (-1, -1), colors.black),
    ])
    
    # Lighter green/red for row background by asset value; cell text color for Balance, PnL, Asset Value, Credit
    LIGHT_GREEN = colors.HexColor('#e8f5e9')   # lighter green
    LIGHT_RED = colors.HexColor('#ffebee')    # lighter red
    GREEN = colors.HexColor('#28a745')
    RED = colors.HexColor('#dc3545')

    for i in range(1, len(data) - 1):
        user_doc = user_data_list[i - 1]
        user_id = user_doc.get("_id")
        balance = float(user_doc.get("balance") or 0)
        credit = float(user_doc.get("credit") or 0)
        pnl = calculate_user_pnl(user_id)
        asset_value = balance + pnl

        # Row background by Asset Value: + light green, - light red, 0 white
        if asset_value > 0:
            row_bg = LIGHT_GREEN
        elif asset_value < 0:
            row_bg = LIGHT_RED
        else:
            row_bg = colors.white
        table_style.add('BACKGROUND', (0, i), (-1, i), row_bg)

        # Username and Parent: always black
        table_style.add('TEXTCOLOR', (0, i), (1, i), colors.black)
        # Balance: + or 0 green, - red
        table_style.add('TEXTCOLOR', (2, i), (2, i), GREEN if balance >= 0 else RED)
        # PnL: + or 0 green, - red
        table_style.add('TEXTCOLOR', (3, i), (3, i), GREEN if pnl >= 0 else RED)
        # Asset Value: + or 0 green, - red
        table_style.add('TEXTCOLOR', (4, i), (4, i), GREEN if asset_value >= 0 else RED)
        # Credit: + or 0 green, - red
        table_style.add('TEXTCOLOR', (5, i), (5, i), GREEN if credit >= 0 else RED)
    
    total_balance_color = colors.HexColor('#28a745') if total_balance >= 0 else colors.HexColor('#dc3545')
    if total_pnl > 0:
        total_pnl_color = colors.HexColor('#28a745')
    elif total_pnl < 0:
        total_pnl_color = colors.HexColor('#dc3545')
    else:
        total_pnl_color = colors.black
    total_asset_color = colors.HexColor('#28a745') if total_asset_value > 0 else (colors.HexColor('#dc3545') if total_asset_value < 0 else colors.black)
    total_credit_color = colors.HexColor('#28a745') if total_credit >= 0 else colors.HexColor('#dc3545')
    
    table_style.add('TEXTCOLOR', (2, total_row_idx), (2, total_row_idx), total_balance_color)
    table_style.add('TEXTCOLOR', (3, total_row_idx), (3, total_row_idx), total_pnl_color)
    table_style.add('TEXTCOLOR', (4, total_row_idx), (4, total_row_idx), total_asset_color)
    table_style.add('TEXTCOLOR', (5, total_row_idx), (5, total_row_idx), total_credit_color)
    
    table.setStyle(table_style)
    story.append(table)
    
    doc.build(story)
    buffer.seek(0)
    return buffer


async def report_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /report command - show report options."""
    token, user = get_logged_in(update, context)
    if not token or not user:
        return await require_login(update, context)
    
    keyboard = [
        [InlineKeyboardButton("Pnl Report", callback_data="report_m2m")],
        [InlineKeyboardButton("Script Wise PnL", callback_data="report_script_pnl")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    msg = await update.message.reply_text(
        "📊 <b>Report Options</b>\n\nSelect a report type:",
        reply_markup=reply_markup,
        parse_mode="HTML",
    )
    remember_bot_message_from_message(update, msg)


async def report_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle report button callbacks."""
    query = update.callback_query
    await query.answer()
    
    token, user = get_logged_in(update, context)
    if not token or not user:
        return await require_login_from_query(query, context)
    
    if query.data == "report_m2m":
        await handle_m2m_report(query, user)
    elif query.data == "report_script_pnl":
        await handle_script_pnl_report(query, user)


async def handle_m2m_report(query, user: dict):
    """Handle M2M report generation and sending."""
    msg = None
    try:
        msg = await query.message.reply_text("⏳ Generating Pnl report...")
        
        user_list = get_users_under_role(user)
        
        if not user_list:
            try:
                await msg.edit_text("❌ No users found under your role.")
            except:
                await query.message.reply_text("❌ No users found under your role.")
            return
        
        if not HAS_REPORTLAB:
            try:
                await msg.edit_text("❌ PDF generation library (reportlab) is not installed.")
            except:
                await query.message.reply_text("❌ PDF generation library (reportlab) is not installed.")
            return
        
        try:
            await msg.edit_text(f"⏳ Processing {len(user_list)} users... This may take a moment.")
        except Exception:
            pass
        
        pdf_buffer = await asyncio.to_thread(generate_m2m_pdf, user_list)
        
        try:
            await msg.edit_text("📄 Sending PDF report...")
        except Exception:
            pass
        
        pdf_buffer.seek(0)
        await query.message.reply_document(
            document=pdf_buffer,
            filename=f"Pnl{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf",
            caption=f"📊 Pnl Report\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\nUsers: {len(user_list)}",
        )
        
        try:
            await msg.delete()
        except Exception:
            pass
        
    except Exception as e:
        logger.error(f"Error generating Pnl report: {e}", exc_info=True)
        error_msg = f"❌ Error generating report: {str(e)}"
        try:
            if msg:
                await msg.edit_text(error_msg)
            else:
                await query.message.reply_text(error_msg)
        except Exception:
            try:
                await query.message.reply_text(error_msg)
            except Exception as e2:
                logger.error(f"Failed to send error message: {e2}")


def resolve_exchange_name(exchange_id: Any) -> str:
    """Resolve exchange ID to exchange name."""
    if not exchange_id:
        return "—"
    
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
        
        if ex_doc:
            return ex_doc.get("name") or ex_doc.get("masterName") or str(exchange_id)
        return str(exchange_id)
    except Exception:
        return str(exchange_id)


def get_script_wise_pnl_data(user_data_list: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Get script-wise PnL data grouped by exchange.
    Returns dict: {exchange_name: [symbol_data, ...]}
    """
    if not user_data_list:
        return {}
    
    user_ids = [ObjectId(u.get("_id")) for u in user_data_list if u.get("_id")]
    
    all_positions = list(open_positions.find({"userId": {"$in": user_ids}}))
    
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
        except Exception as e:
            logger.warning(f"Error fetching symbol {symbol_id}: {e}")
            symbol_data_map[symbol_id] = {"symbolname": "", "ltp": 0}
    
    exchange_name_map = {}
    for ex_id in exchange_ids:
        exchange_name_map[ex_id] = resolve_exchange_name(ex_id)
    
    grouped_by_exchange_symbol: Dict[tuple, Dict[str, Any]] = {}
    
    for pos in all_positions:
        exchange_id = pos.get("exchangeId")
        symbol_id = pos.get("symbolId")
        if not exchange_id or not symbol_id:
            continue
        
        exchange_name = exchange_name_map.get(exchange_id, "UNKNOWN")
        key = (exchange_name, symbol_id)
        
        if key not in grouped_by_exchange_symbol:
            symbol_info = symbol_data_map.get(symbol_id, {"symbolname": "", "ltp": 0})
            grouped_by_exchange_symbol[key] = {
                "exchange": exchange_name,
                "symbol": symbol_info["symbolname"],
                "symbol_id": symbol_id,
                "buy_positions": [],
                "sell_positions": [],
                "total_buy_qty": 0.0,
                "total_sell_qty": 0.0,
                "buy_price_qty_sum": 0.0,
                "ltp": symbol_info["ltp"],
            }
        
        trade_type = str(pos.get("tradeType") or pos.get("orderType") or "").lower()
        buy_price = float(pos.get("buyPrice") or pos.get("price") or pos.get("open_price") or 0)
        buy_total_qty = float(pos.get("buyTotalQuantity") or pos.get("totalQuantity") or pos.get("quantity") or 0)
        
        grouped_by_exchange_symbol[key]["total_buy_qty"] += buy_total_qty
        grouped_by_exchange_symbol[key]["buy_price_qty_sum"] += buy_price * buy_total_qty
        
        if trade_type == "buy":
            grouped_by_exchange_symbol[key]["buy_positions"].append({
                "buyPrice": buy_price,
                "buyTotalQuantity": buy_total_qty,
            })
        elif trade_type == "sell":
            grouped_by_exchange_symbol[key]["sell_positions"].append({
                "buyPrice": buy_price,
                "buyTotalQuantity": buy_total_qty,
            })
    
    result: Dict[str, List[Dict[str, Any]]] = {}
    
    for (ex_name, symbol_id), data in grouped_by_exchange_symbol.items():
        if ex_name not in result:
            result[ex_name] = []
        
        buy_count = len(data["buy_positions"])
        sell_count = len(data["sell_positions"])
        trade_type = "BUY" if buy_count >= sell_count else "SELL"
        
        total_qty = data["total_buy_qty"] + data["total_sell_qty"]
        if total_qty == 0:
            continue
        
        avg_price = data["buy_price_qty_sum"] / total_qty if total_qty > 0 else 0.0
        current_price = data["ltp"]
        pnl = (current_price - avg_price) * total_qty
        
        result[ex_name].append({
            "symbol": data["symbol"],
            "type": trade_type,
            "total_quantity": total_qty,
            "avg_price": avg_price,
            "current_price": current_price,
            "pnl": pnl,
        })
    
    for ex_name in result:
        result[ex_name].sort(key=lambda x: x["symbol"])
    
    return result


def generate_script_pnl_pdf(user_data_list: List[Dict[str, Any]]) -> BytesIO:
    """
    Generate Script Wise PnL PDF report with exchange-wise tables.
    Returns BytesIO buffer with PDF content.
    """
    if not HAS_REPORTLAB:
        raise ImportError("reportlab is not installed. Please install it to generate PDFs.")
    
    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer, 
        pagesize=A4,
        leftMargin=0.5*inch,
        rightMargin=0.5*inch,
        topMargin=0.5*inch,
        bottomMargin=0.5*inch
    )
    story = []
    
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=16,
        textColor=colors.HexColor('#000000'),
        spaceAfter=30,
        alignment=1,
    )
    
    story.append(Paragraph("Script Wise PnL Report", title_style))
    date_style = ParagraphStyle(
        'DateStyle',
        parent=styles['Normal'],
        fontSize=10,
        textColor=colors.HexColor('#666666'),
        alignment=1,
    )
    story.append(Paragraph(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", date_style))
    story.append(Spacer(1, 0.3*inch))
    
    script_data = get_script_wise_pnl_data(user_data_list)
    
    if not script_data:
        story.append(Paragraph("No positions found.", styles['Normal']))
        doc.build(story)
        buffer.seek(0)
        return buffer
    
    for exchange_name, symbols_list in sorted(script_data.items()):
        exchange_header = ParagraphStyle(
            'ExchangeHeader',
            parent=styles['Heading2'],
            fontSize=12,
            textColor=colors.HexColor('#000000'),
            spaceAfter=10,
            spaceBefore=15,
            alignment=1,  # Center alignment
        )
        story.append(Paragraph(exchange_name.upper(), exchange_header))
        story.append(Spacer(1, 0.1*inch))
        
        data = [["Symbol", "Type", "Total Quantity", "Avg. Price", "Cur. Price", "PnL"]]
        
        for symbol_data in symbols_list:
            row = [
                symbol_data["symbol"],
                symbol_data["type"],
                f"{symbol_data['total_quantity']:,.2f}",
                f"{symbol_data['avg_price']:,.2f}",
                f"{symbol_data['current_price']:,.2f}",
                f"{symbol_data['pnl']:,.2f}",
            ]
            data.append(row)
        
        total_pnl = sum(s["pnl"] for s in symbols_list)
        data.append([
            "TOTAL",
            "-",
            f"{sum(s['total_quantity'] for s in symbols_list):,.2f}",
            "-",
            "-",
            f"{total_pnl:,.2f}",
        ])
        
        table = Table(data, colWidths=[1.8*inch, 0.7*inch, 1.0*inch, 1.0*inch, 1.0*inch, 1.0*inch])
        
        table_style = TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#4A4A4A')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
            ('ALIGN', (0, 1), (0, -1), 'LEFT'),
            ('ALIGN', (1, 1), (-1, -1), 'RIGHT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 9),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
            ('TOPPADDING', (0, 0), (-1, 0), 6),
            ('BACKGROUND', (0, 1), (-1, -2), colors.white),
            ('TEXTCOLOR', (0, 1), (-1, -2), colors.black),
            ('FONTNAME', (0, 1), (-1, -2), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -2), 8),
            ('TOPPADDING', (0, 1), (-1, -2), 4),
            ('BOTTOMPADDING', (0, 1), (-1, -2), 4),
            ('LEFTPADDING', (0, 1), (-1, -2), 8),
            ('RIGHTPADDING', (0, 1), (-1, -2), 8),
            ('ROWBACKGROUNDS', (0, 1), (-1, -2), [colors.white, colors.HexColor('#F5F5F5')]),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#CCCCCC')),
            ('BACKGROUND', (0, -1), (-1, -1), colors.HexColor('#E8E8E8')),
            ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, -1), (-1, -1), 9),
            ('TOPPADDING', (0, -1), (-1, -1), 5),
            ('BOTTOMPADDING', (0, -1), (-1, -1), 5),
            ('ALIGN', (1, -1), (-1, -1), 'RIGHT'),
            ('TEXTCOLOR', (0, -1), (-1, -1), colors.black),
        ])
        
        for i in range(1, len(data) - 1):
            symbol_data = symbols_list[i - 1]
            is_buy = symbol_data["type"] == "BUY"
            row_color = colors.HexColor('#28a745') if is_buy else colors.HexColor('#dc3545')
            table_style.add('TEXTCOLOR', (0, i), (-1, i), row_color)
        
        total_color = colors.HexColor('#28a745') if total_pnl >= 0 else colors.HexColor('#dc3545')
        table_style.add('TEXTCOLOR', (5, -1), (5, -1), total_color)
        
        table.setStyle(table_style)
        story.append(table)
        story.append(Spacer(1, 0.2*inch))
    
    doc.build(story)
    buffer.seek(0)
    return buffer


async def handle_script_pnl_report(query, user: dict):
    """Handle Script Wise PnL report generation and sending."""
    msg = None
    try:
        msg = await query.message.reply_text("⏳ Generating Script Wise PnL report...")
        
        user_list = get_users_under_role(user)
        
        if not user_list:
            try:
                await msg.edit_text("❌ No users found under your role.")
            except:
                await query.message.reply_text("❌ No users found under your role.")
            return
        
        if not HAS_REPORTLAB:
            try:
                await msg.edit_text("❌ PDF generation library (reportlab) is not installed.")
            except:
                await query.message.reply_text("❌ PDF generation library (reportlab) is not installed.")
            return
        
        try:
            await msg.edit_text(f"⏳ Processing {len(user_list)} users... This may take a moment.")
        except Exception:
            pass
        
        pdf_buffer = await asyncio.to_thread(generate_script_pnl_pdf, user_list)
        
        try:
            await msg.edit_text("📄 Sending PDF report...")
        except Exception:
            pass
        
        pdf_buffer.seek(0)
        await query.message.reply_document(
            document=pdf_buffer,
            filename=f"Script_Wise_PnL_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf",
            caption=f"📊 Script Wise PnL Report\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\nUsers: {len(user_list)}",
        )
        
        try:
            await msg.delete()
        except Exception:
            pass
        
    except Exception as e:
        logger.error(f"Error generating Script Wise PnL report: {e}", exc_info=True)
        error_msg = f"❌ Error generating report: {str(e)}"
        try:
            if msg:
                await msg.edit_text(error_msg)
            else:
                await query.message.reply_text(error_msg)
        except Exception:
            try:
                await query.message.reply_text(error_msg)
            except Exception as e2:
                logger.error(f"Failed to send error message: {e2}")


def register_report_handlers(app):
    """Register report command handlers."""
    app.add_handler(CommandHandler("report", report_cmd))
    app.add_handler(CallbackQueryHandler(report_callback, pattern="^report_"))

