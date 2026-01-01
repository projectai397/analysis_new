# # src/telegram/notification.py

# import os
# import asyncio 
# import logging
# from dotenv import load_dotenv
# from pymongo import MongoClient
# from typing import List, Any, Optional
# from bson import ObjectId
# import html
# from telegram import Bot # Import Bot class for typing
# from src.config import users, notification, config
# logger = logging.getLogger(__name__)

# # --- Configuration & MongoDB Connection ---
# load_dotenv()

# MONGO_URI = os.getenv("ANALYSIS_MONGO_URI")
# DB_NAME = os.getenv("SUPPORT_DB")
# NOTIFICATION_COLLECTION = os.getenv("NOTIFICATION") # Renamed for clarity
# USERS_COLLECTION_NAME = 'users' # Assuming your users are in a collection named 'users'

# # Connect to MongoDB
# try:
#     client = MongoClient(MONGO_URI)
#     db = client[DB_NAME]
#     notification_collection = db[NOTIFICATION_COLLECTION]
#     users_collection = db[USERS_COLLECTION_NAME] # Required for hierarchy lookup
#     logger.info("MongoDB connection established for notification services.")
# except Exception as e:
#     logger.error(f"Failed to connect to MongoDB: {e}")

# def get_role_string_from_id(role_id_obj):
#     if role_id_obj == config.SUPERADMIN_ROLE_ID:
#         return "superadmin"
#     elif role_id_obj == config.ADMIN_ROLE_ID:
#         return "admin"
#     elif role_id_obj == config.MASTER_ROLE_ID:
#         return "master"
#     else:
#         return None # Or handle 'user' role

# async def subscribe_user(user_id: str, role: str, chat_id: int):
#     """
#     Adds a user's chat_id to the notification document, keyed by the unique user_id.
#     :param user_id: The unique MongoDB _id of the Master/Admin/Superadmin.
#     :param role: The role of the user being subscribed ('master', 'admin', 'superadmin').
#     :param chat_id: The Telegram chat ID to be added.
#     """
#     try:
#         # Use $addToSet to add the chat_id only if it doesn't already exist
#         result = notification_collection.update_one(
#             {"user_id": user_id},
#             {
#                 "$set": {"role": role.lower()}, # Ensure role is set/updated
#                 "$addToSet": {"chat_ids": chat_id}
#             }, 
#             upsert=True # Creates the document if the user_id doesn't exist
#         )
#         if result.upserted_id or result.modified_count > 0:
#             logger.info(f"User {user_id} ({role}) subscribed with new chat_id {chat_id}.")
#             return True
#         return False
#     except Exception as e:
#         logger.error(f"Error subscribing user {user_id}: {e}")
#         return False
    
# async def unsubscribe_user(user_id: str, chat_id: int):
#     """
#     Removes a user's chat_id from the notification document.
#     :param user_id: The unique MongoDB _id of the Master/Admin/Superadmin.
#     :param chat_id: The Telegram chat ID to be removed.
#     """
#     try:
#         # $pull removes an element from an array
#         result = notification_collection.update_one(
#             {"user_id": user_id},
#             {"$pull": {"chat_ids": chat_id}} 
#         )
#         if result.modified_count > 0:
#             logger.info(f"User {user_id} unsubscribed chat_id {chat_id}.")
#             return True
#         return False
#     except Exception as e:
#         logger.error(f"Error unsubscribing user {user_id}: {e}")
#         return False
# # ============================================================
# # SUBSCRIPTION DATABASE HELPERS (Now keyed by user_id)
# # ============================================================
# async def get_subscribed_parents(trade_user_id: str) -> set:
#     """
#     Traverses up the user hierarchy (Client -> Master -> Admin -> SA) 
#     and collects the Telegram chat IDs of all subscribed supervisors.
    
#     Args:
#         trade_user_id: The MongoDB _id of the user who executed the trade.
        
#     Returns:
#         A set of unique Telegram chat_ids (integers) that need to receive the notification.
#     """
    
#     # 1. Initialize: Convert input ID to ObjectId
#     try:
#         current_user_id = ObjectId(trade_user_id)
#     except Exception:
#         logger.error(f"Invalid ObjectId provided: {trade_user_id}")
#         return set()

#     subscribed_chat_ids = set()
#     MAX_DEPTH = 10 # Safety break for infinite loops

#     for _ in range(MAX_DEPTH):
#         # A. Find the current user's document
#         user_doc = users.find_one({"_id": current_user_id})

#         if not user_doc:
#             logger.warning(f"User document not found for ID: {current_user_id}")
#             break

#         # B. Check for subscription
#         user_role_obj = user_doc.get('role') # This is an ObjectId
        
#         if user_role_obj:
#             user_role_string = get_role_string_from_id(user_role_obj)
#             mongo_user_id_str = str(user_doc['_id'])

#             if user_role_string:
#                 # Query the notification collection
#                 subscription_doc = notification.find_one({
#                     "user_id": mongo_user_id_str,
#                     "role": user_role_string 
#                 })

#                 if subscription_doc and subscription_doc.get('chat_ids'):
#                     # Collect the subscribed chat IDs
#                     subscribed_chat_ids.update(subscription_doc['chat_ids'])

#         # C. Move Up the Hierarchy
#         # Check both parentId and addedBy fields, or just parentId if that's standard.
#         next_parent_id = user_doc.get('parentId') or user_doc.get('addedBy')

#         if not next_parent_id or next_parent_id == current_user_id:
#             # Reached the top of the hierarchy (parentId is null or points to self)
#             break
        
#         # Move to the next level up
#         current_user_id = next_parent_id
        
#     return subscribed_chat_ids

# async def get_chat_ids_for_user_id(user_id: str) -> List[int]:
#     # This function remains the same, it now fetches the new structure correctly
#     try:
#         user_doc = notification_collection.find_one({"user_id": user_id}) 
#         if user_doc:
#             return [int(cid) for cid in user_doc.get("chat_ids", [])]
#     except Exception as e:
#         logger.error(f"Error fetching chat IDs for user {user_id}: {e}")
#     return []

# # NOTE: subscribe_user_to_role and unsubscribe_user_from_role logic will need to be 
# # updated in your main bot file to correctly use the 'user_id' field instead of 'role' 
# # when a user subscribes. The old functions won't work with the new schema.

# # ============================================================
# # 🔹 HIERARCHY TRAVERSAL LOGIC 🔹
# # ============================================================

# def get_hierarchy_user_ids(client_user_id: str) -> List[dict]:
#     """
#     Traverses the user hierarchy from the client up to the Superadmin
#     and returns the user_id, role, and chat_ids for each level that is subscribed.
    
#     :param client_user_id: The userId (ObjectId string) of the user who made the trade.
#     :return: A list of dicts: [{'user_id': str, 'role': str, 'chat_ids': List[int]}, ...]
#     """
#     user_ids_to_notify = []
#     current_id = client_user_id
    
#     # We will assume a maximum of 3 levels above the client (Master, Admin, Superadmin)
#     for _ in range(4): 
#         try:
#             # Look up the current user's document
#             user_doc = users_collection.find_one({"_id": ObjectId(current_id)})
#             if not user_doc:
#                 break # Stop if user not found or reached the top
            
#             # The client's own document will have their parentId, which is the Master/Admin/Superadmin
#             parent_id = str(user_doc.get("parentId")) if user_doc.get("parentId") else None
#             current_role = user_doc.get("role", "client").lower()

#             # For the current user (if they are Master/Admin/Superadmin), get their subscriptions
#             # We skip the client's own ID unless they are also a high-level user
#             if current_role in ['master', 'admin', 'superadmin']:
#                 # The user ID to search in the notification collection is the current user's _id
#                 chat_ids = asyncio.run(get_chat_ids_for_user_id(current_id))
                
#                 if chat_ids:
#                     # Collect the subscribed hierarchy member
#                     user_ids_to_notify.append({
#                         "user_id": current_id,
#                         "role": current_role,
#                         "chat_ids": chat_ids
#                     })

#             # Move up the hierarchy
#             if parent_id and parent_id != current_id:
#                 current_id = parent_id
#             else:
#                 break # Reached the top parent or no parentId found
                
#         except Exception as e:
#             logger.error(f"Error traversing hierarchy for ID {current_id}: {e}")
#             break
            
#     # The current logic will notify the highest level first (Superadmin) then down to Master
#     # We only care about Master, Admin, and Superadmin roles being notified.
#     # The client's own ID will be skipped unless they have a role (which they shouldn't if they are a trade maker client).
#     return user_ids_to_notify

# # ============================================================
# # TRADE NOTIFICATION CORE
# # ============================================================

# async def send_message_to_chats(message: str, chat_ids: List[int], bot):
#     """
#     Sends the trade notification to a list of specific chat IDs, dynamically
#     prepending the receiver's subscribed role for context.
#     """
#     if not chat_ids:
#         return
        
#     for chat_id in chat_ids:
#         # 1. Find the role associated with this specific chat_id
#         subscription_doc = notification.find_one({"chat_ids": chat_id})
        
#         # 2. Determine the role and create a role prefix
#         if subscription_doc and subscription_doc.get('role'):
#             receiver_role = subscription_doc['role'].upper()
#         else:
#             receiver_role = "UNKNOWN SUBSCRIBER"
            
#         # 3. Prepend the role to the original message (using BOLD for emphasis)
#         role_prefix = f"<b>[Sent to {html.escape(receiver_role)}]</b>\n"
#         final_message = role_prefix + message
        
#         try:
#             await bot.send_message(
#                 chat_id=chat_id, 
#                 text=final_message, 
#                 parse_mode="HTML"
#             )
#             logger.info(f"Trade notification sent to chat_id {chat_id} as {receiver_role}.")
#         except Exception as e:
#             logger.error(f"Error sending trade notification to {chat_id}: {e}")

# async def handle_trade_update(change: dict, bot_instance: Bot):
#     """
#     Handles updates from the position collection by traversing the user hierarchy 
#     and sending notifications to the specific subscribed Master, Admin, and Superadmin.
#     """
#     if change.get('operationType') not in ['insert', 'update']:
#         return

#     trade_data = change.get('fullDocument')
#     if not trade_data:
#         logger.warning("Trade update skipped: Missing fullDocument.")
#         return

#     # 1. Extract necessary trade maker info
#     trade_maker_role = trade_data.get('role', 'client') 
#     client_user_id = str(trade_data.get('userId')) 
    
#     if not client_user_id or client_user_id == 'None':
#         logger.warning("Trade update skipped: Missing client userId in trade data.")
#         return
        
#     # 2. Build the HTML trade message
#     trade_message = (
#         f"🚨 <b>Trade Alert: {trade_maker_role.upper()}</b>\n"
#         f"Client ID: <code>{html.escape(client_user_id)}</code>\n\n"
#         f"Symbol: {html.escape(trade_data.get('symbolName', 'Unknown'))}\n"
#         f"Quantity: {trade_data.get('totalQuantity', 'N/A')}\n"
#         f"Price: {trade_data.get('price', 'N/A')}\n"
#         f"Order Type: {html.escape(trade_data.get('orderType', 'N/A'))}\n"
#         f"Trade Type: {html.escape(trade_data.get('tradeType', 'N/A'))}"
#     )

#     # 3. Determine recipients based on hierarchy
#     # We need to call the synchronous function from an async context
#     hierarchy_members = get_hierarchy_user_ids(client_user_id)

#     if not hierarchy_members:
#         logger.info(f"No subscribed hierarchy members found for client {client_user_id}.")
#         return

#     # 4. Send notification to each subscribed hierarchy member
#     tasks = []
#     for member in hierarchy_members:
#         # The 'member' dict contains the user_id, role, and the list of chat_ids
#         tasks.append(
#             send_message_to_chats(
#                 trade_message, 
#                 member['chat_ids'], 
#                 bot_instance,
#                 member['role'] # for logging
#             )
#         )
    
#     if tasks:
#         await asyncio.gather(*tasks)


# src/telegram/notification.py

import os
import asyncio 
import logging
from dotenv import load_dotenv
from pymongo import MongoClient
from typing import List, Any, Optional
from bson import ObjectId
import html
from telegram import Bot # Import Bot class for typing
from src.config import users, notification, config
logger = logging.getLogger(__name__)

# --- Configuration & MongoDB Connection ---
load_dotenv()

MONGO_URI = os.getenv("ANALYSIS_MONGO_URI")
DB_NAME = os.getenv("SUPPORT_DB")
NOTIFICATION_COLLECTION = os.getenv("NOTIFICATION") # Renamed for clarity
USERS_COLLECTION_NAME = 'users' # Assuming your users are in a collection named 'users'

# Connect to MongoDB
try:
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    notification_collection = db[NOTIFICATION_COLLECTION]
    users_collection = db[USERS_COLLECTION_NAME] # Required for hierarchy lookup
    logger.info("MongoDB connection established for notification services.")
except Exception as e:
    logger.error(f"Failed to connect to MongoDB: {e}")

def get_role_string_from_id(role_id_obj):
    if role_id_obj == config.SUPERADMIN_ROLE_ID:
        return "superadmin"
    elif role_id_obj == config.ADMIN_ROLE_ID:
        return "admin"
    elif role_id_obj == config.MASTER_ROLE_ID:
        return "master"
    else:
        return None # Or handle 'user' role

async def subscribe_user(user_id: str, role: str, chat_id: int):
    """
    Adds a user's chat_id to the notification document, keyed by the unique user_id.
    :param user_id: The unique MongoDB _id of the Master/Admin/Superadmin.
    :param role: The role of the user being subscribed ('master', 'admin', 'superadmin').
    :param chat_id: The Telegram chat ID to be added.
    """
    try:
        # Use $addToSet to add the chat_id only if it doesn't already exist
        result = notification_collection.update_one(
            {"user_id": user_id},
            {
                "$set": {"role": role.lower()}, # Ensure role is set/updated
                "$addToSet": {"chat_ids": chat_id}
            }, 
            upsert=True # Creates the document if the user_id doesn't exist
        )
        if result.upserted_id or result.modified_count > 0:
            logger.info(f"User {user_id} ({role}) subscribed with new chat_id {chat_id}.")
            return True
        return False
    except Exception as e:
        logger.error(f"Error subscribing user {user_id}: {e}")
        return False
    
async def unsubscribe_user(user_id: str, chat_id: int):
    """
    Removes a user's chat_id from the notification document.
    :param user_id: The unique MongoDB _id of the Master/Admin/Superadmin.
    :param chat_id: The Telegram chat ID to be removed.
    """
    try:
        # $pull removes an element from an array
        result = notification_collection.update_one(
            {"user_id": user_id},
            {"$pull": {"chat_ids": chat_id}} 
        )
        if result.modified_count > 0:
            logger.info(f"User {user_id} unsubscribed chat_id {chat_id}.")
            return True
        return False
    except Exception as e:
        logger.error(f"Error unsubscribing user {user_id}: {e}")
        return False
# ============================================================
# SUBSCRIPTION DATABASE HELPERS (Now keyed by user_id)
# ============================================================
async def get_subscribed_parents(trade_user_id: str) -> set:
    """
    Traverses up the user hierarchy (Client -> Master -> Admin -> SA) 
    and collects the Telegram chat IDs of all subscribed supervisors.
    
    Args:
        trade_user_id: The MongoDB _id of the user who executed the trade.
        
    Returns:
        A set of unique Telegram chat_ids (integers) that need to receive the notification.
    """
    
    # 1. Initialize: Convert input ID to ObjectId
    try:
        current_user_id = ObjectId(trade_user_id)
    except Exception:
        logger.error(f"Invalid ObjectId provided: {trade_user_id}")
        return set()

    subscribed_chat_ids = set()
    MAX_DEPTH = 10 # Safety break for infinite loops

    for _ in range(MAX_DEPTH):
        # A. Find the current user's document
        user_doc = users.find_one({"_id": current_user_id})

        if not user_doc:
            logger.warning(f"User document not found for ID: {current_user_id}")
            break

        # B. Check for subscription
        user_role_obj = user_doc.get('role') # This is an ObjectId
        
        if user_role_obj:
            user_role_string = get_role_string_from_id(user_role_obj)
            mongo_user_id_str = str(user_doc['_id'])

            if user_role_string:
                # Query the notification collection
                subscription_doc = notification.find_one({
                    "user_id": mongo_user_id_str,
                    "role": user_role_string 
                })

                if subscription_doc and subscription_doc.get('chat_ids'):
                    # Collect the subscribed chat IDs
                    subscribed_chat_ids.update(subscription_doc['chat_ids'])

        # C. Move Up the Hierarchy
        # Check both parentId and addedBy fields, or just parentId if that's standard.
        next_parent_id = user_doc.get('parentId') or user_doc.get('addedBy')

        if not next_parent_id or next_parent_id == current_user_id:
            # Reached the top of the hierarchy (parentId is null or points to self)
            break
        
        # Move to the next level up
        current_user_id = next_parent_id
        
    return subscribed_chat_ids

async def get_chat_ids_for_user_id(user_id: str) -> List[int]:
    # This function remains the same, it now fetches the new structure correctly
    try:
        user_doc = notification_collection.find_one({"user_id": user_id}) 
        if user_doc:
            return [int(cid) for cid in user_doc.get("chat_ids", [])]
    except Exception as e:
        logger.error(f"Error fetching chat IDs for user {user_id}: {e}")
    return []

# NOTE: subscribe_user_to_role and unsubscribe_user_from_role logic will need to be 
# updated in your main bot file to correctly use the 'user_id' field instead of 'role' 
# when a user subscribes. The old functions won't work with the new schema.

# ============================================================
# 🔹 HIERARCHY TRAVERSAL LOGIC 🔹
# ============================================================

def get_hierarchy_user_ids(client_user_id: str) -> List[dict]:
    """
    Traverses the user hierarchy from the client up to the Superadmin
    and returns the user_id, role, and chat_ids for each level that is subscribed.
    
    :param client_user_id: The userId (ObjectId string) of the user who made the trade.
    :return: A list of dicts: [{'user_id': str, 'role': str, 'chat_ids': List[int]}, ...]
    """
    user_ids_to_notify = []
    current_id = client_user_id
    
    # We will assume a maximum of 3 levels above the client (Master, Admin, Superadmin)
    for _ in range(4): 
        try:
            # Look up the current user's document
            user_doc = users_collection.find_one({"_id": ObjectId(current_id)})
            if not user_doc:
                break # Stop if user not found or reached the top
            
            # The client's own document will have their parentId, which is the Master/Admin/Superadmin
            parent_id = str(user_doc.get("parentId")) if user_doc.get("parentId") else None
            current_role = user_doc.get("role", "client").lower()

            # For the current user (if they are Master/Admin/Superadmin), get their subscriptions
            # We skip the client's own ID unless they are also a high-level user
            if current_role in ['master', 'admin', 'superadmin']:
                # The user ID to search in the notification collection is the current user's _id
                chat_ids = asyncio.run(get_chat_ids_for_user_id(current_id))
                
                if chat_ids:
                    # Collect the subscribed hierarchy member
                    user_ids_to_notify.append({
                        "user_id": current_id,
                        "role": current_role,
                        "chat_ids": chat_ids
                    })

            # Move up the hierarchy
            if parent_id and parent_id != current_id:
                current_id = parent_id
            else:
                break # Reached the top parent or no parentId found
                
        except Exception as e:
            logger.error(f"Error traversing hierarchy for ID {current_id}: {e}")
            break
            
    # The current logic will notify the highest level first (Superadmin) then down to Master
    # We only care about Master, Admin, and Superadmin roles being notified.
    # The client's own ID will be skipped unless they have a role (which they shouldn't if they are a trade maker client).
    return user_ids_to_notify

# ============================================================
# TRADE NOTIFICATION CORE
# ============================================================

async def send_message_to_chats(message: str, chat_id: int, bot, prefix_role: str = None):
    """
    Sends the final message with the bold ROLE header.
    """
    header = f"<b>To: {prefix_role}</b>\n" if prefix_role else ""
    try:
        await bot.send_message(
            chat_id=chat_id,
            text=f"{header}{message}",
            parse_mode="HTML"
        )
        logger.info(f"✅ Notification sent to {prefix_role}")
    except Exception as e:
        logger.error(f"❌ Telegram Send Error for {chat_id}: {e}")

async def handle_trade_update(change: dict, bot_instance):
    """
    Formats notification to match pretty style and adds Quantity field.
    """
    doc = change.get("fullDocument")
    if not doc:
        return

    # 1. Extract Fields from Document
    user_id_str = str(doc.get("userId", ""))
    symbol = doc.get("symbolName", "N/A")
    quantity = doc.get("quantity", 0)  # 🆕 Added Quantity
    price = doc.get("price", 0)
    order_type = doc.get("orderType", "N/A")
    trade_type = doc.get("tradeType", "N/A")
    
    # Determine the status for the bold title
    status_raw = doc.get("status", "Executed")
    title = f"Trade {status_raw.capitalize()}"

    # 2. Get subscribed parents (IDs and Roles)
    parent_subscriptions = await get_subscribed_parents(user_id_str)
    if not parent_subscriptions:
        logger.info(f"No subscribers for client {user_id_str}")
        return

    # 3. Look up Client Name
    client_name = user_id_str
    try:
        client_doc = await asyncio.to_thread(users.find_one, {"_id": ObjectId(user_id_str)})
        if client_doc:
            client_name = client_doc.get("userName") or client_doc.get("name") or user_id_str
    except Exception as e:
        logger.error(f"⚠️ Client name lookup failed: {e}")

    # 4. Construct the Body (Matching image_026ae3.png)
    # Note: Labels and values are NOT bolded for the 'pretty' look.
    body = (
        f"🔔 <b>{html.escape(str(title))}</b>\n"
        f"Client Name: {html.escape(str(client_name))}\n"
        f"Symbol: {html.escape(str(symbol))}\n"
        f"Quantity: {quantity}\n"
        f"Price: {price}\n"
        f"Order Type: {html.escape(str(order_type))}\n"
        f"Trade Type: {html.escape(str(trade_type))}"
    )

    # 5. Add Footer Message (Upper Case, with blank line spacing)
    footer_text = doc.get("comment") or doc.get("message")
    if footer_text:
        body += f"\n\n{html.escape(str(footer_text)).upper()}"

    # 6. Send to each parent with dynamic "To: ROLE" bold header
    for item in parent_subscriptions:
        if isinstance(item, (tuple, list)):
            chat_id, role = item[0], item[1]
        else:
            chat_id = item
            sub_doc = await asyncio.to_thread(notification.find_one, {"chat_ids": chat_id})
            role = sub_doc.get("role", "ADMIN").upper() if sub_doc else "ADMIN"

        await send_message_to_chats(body, chat_id, bot_instance, role)