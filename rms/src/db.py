# src/db.py
import os
from urllib.parse import urlparse
from bson import ObjectId
from pymongo import MongoClient
import mongoengine as me

def _infer_db_name(uri: str):
    if not uri:
        return None
    try:
        p = urlparse(uri)
        return p.path.lstrip("/") or None
    except Exception:
        return None

PROMONGO_URI     = os.getenv("SOURCE_MONGO_URI")
PRO_DB_NAME      = os.getenv("SOURCE_DB_NAME") or _infer_db_name(PROMONGO_URI)

SUPPORTMONGO_URI = os.getenv("ANALYSIS_MONGO_URI")
SUPPORT_DB_NAME  = os.getenv("SUPPORT_DB") or _infer_db_name(SUPPORTMONGO_URI)

# --- PyMongo clients/handles ---
_pro_client    = MongoClient(PROMONGO_URI)
PRO_DB_MONGO   = _pro_client[PRO_DB_NAME]     # Database: market
PRO_USER_COLL  = PRO_DB_MONGO["user"]         # Collection: user (singular)

_support_client    = MongoClient(SUPPORTMONGO_URI)
SUPPORT_DB_MONGO   = _support_client[SUPPORT_DB_NAME]
support_db         = SUPPORT_DB_MONGO
faqs_coll          = support_db[ "FAQs"]
notifications_coll = support_db["notification"]
chaturl_coll       = support_db["chat_url"]
support_users_coll = support_db["users"]
demo_users_coll    = support_db["demo_users"]
demo_chatrooms_coll = support_db["demo_chatroom"]
demo_messages_coll = support_db["demo_messages"]

# --- MongoEngine connections (unchanged) ---
me.register_connection(alias="pro",     host=PROMONGO_URI,     db=PRO_DB_NAME,     uuidRepresentation="standard")
me.register_connection(alias="support", host=SUPPORTMONGO_URI, db=SUPPORT_DB_NAME, uuidRepresentation="standard")

# --- Role ObjectIds (strip quotes/spaces) ---
def _to_oid(env_key: str):
    v = os.getenv(env_key)
    if not v:
        return None
    try:
        return ObjectId(v.strip().strip('"').strip("'"))
    except Exception:
        return None

SUPERADMIN_ROLE_ID = _to_oid("SUPERADMIN_ROLE_ID")
ADMIN_ROLE_ID      = _to_oid("ADMIN_ROLE_ID")
MASTER_ROLE_ID     = _to_oid("MASTER_ROLE_ID")
USER_ROLE_ID       = _to_oid("USER_ROLE_ID")
BOT_ROLE_ID        = _to_oid("BOT_ROLE_ID")

# Optional: keep the names for printing banners
PRO_DB = PRO_DB_NAME
SUPPORT_DB = SUPPORT_DB_NAME
