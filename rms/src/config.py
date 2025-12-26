import os
from zoneinfo import ZoneInfo
from bson import ObjectId
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()


class Settings:
    SOURCE_MONGO_URI = os.getenv("SOURCE_MONGO_URI")
    SOURCE_DB_NAME = os.getenv("SOURCE_DB_NAME", "market")
    USERS_COLL = os.getenv("USERS_COLL", "user")
    TRADE_COLL = os.getenv("TRADE_COLL", "trade")
    POSITIONS_COLL = os.getenv("POSITIONS_COLL", "trade")
    POSITIONS_COLL_ACTUAL = os.getenv("POSITION_COLL", "position")
    WALLETS_COLL = os.getenv("WALLETS_COLL", "user")
    TRANSACTIONS_COLL = os.getenv("TRANSACTIONS_COLL", "transaction")
    ALERTS_COLL = os.getenv("ALERTS_COLL", "alerts")
    ANALYSIS_MONGO_URI = os.getenv("ANALYSIS_MONGO_URI")
    ANALYSIS_DB_NAME = os.getenv("ANALYSIS_DB_NAME", "pro_analysis")
    ANALYSIS_COLL = os.getenv("ANALYSIS_COLL", "analysis")
    ANALYSIS_USERS_COLL = os.getenv("ANALYSIS_USERS_COLL", "Users")
    DATA_COLL = os.getenv("DATA_COLL", "setting")
    TRADE_COLL_ANALYSIS = os.getenv("TRADE_COLL_ANALYSIS", "trade")
    EXCHANGE_COLL = os.getenv("EXCHANGE_COLL", "exchange")
    NOTIFICATION = os.getenv("NOTIFICATION", "notification_list")
    TELE_NOTIFICATION_COLL =os.getenv("TELE_NOTIFICATION_COLL","notification")
    NOTIFICATION_TELEGRAM =os.getenv("NOTIFICATION_TELEGRAM")
    LOGIN_HISTORY_COLL = os.getenv("LOGIN_HISTORY_COLL", "loginHistory")
    DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "10"))
    CREATED_AT_IS_UTC = os.getenv("CREATED_AT_IS_UTC", "1") == "1"
    APP_TZ = ZoneInfo(os.getenv("APP_TZ", "Asia/Kolkata"))
    JWT_SECRET = os.getenv("JWT_SECRET", "JWT_SECRET")
    JWT_ALG = os.getenv("JWT_ALG", "JWT_ALG")
    PORT = int(os.getenv("PORT", "8001"))
    SOCKET_PORT = int(os.getenv("SOCKET_PORT", 5001))
    RATE_WINDOW_SEC = int(os.getenv("RATE_WINDOW_SEC", "60"))
    RATE_LIMIT_HITS = int(os.getenv("RATE_LIMIT_HITS", "30"))
    BLOCK_DURATION_SEC = int(os.getenv("BLOCK_DURATION_SEC", str(600)))
    DEVELOPMENT = int(os.getenv("DEVELOPEMNT", 1))
    SUPERADMIN_ROLE_ID = ObjectId(os.getenv("SUPERADMIN_ROLE_ID"))
    ADMIN_ROLE_ID = ObjectId(os.getenv("ADMIN_ROLE_ID"))
    MASTER_ROLE_ID = ObjectId(os.getenv("MASTER_ROLE_ID"))
    USER_ROLE_ID = ObjectId(os.getenv("USER_ROLE_ID"))
    TTL = 10800

    # ==== Telegram / RMS Bot settings ====
    # Bot token from BotFather
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    TELEGRAM_BOT_TOKEN_1 = os.getenv("TELEGRAM_BOT_TOKEN_1")
    # Base URL of your RMS HTTP API that the bot will call for login etc.
    # Example in .env: RMS_API_BASE_URL="http://127.0.0.1:8001"
    RMS_API_BASE_URL = os.getenv("RMS_API_BASE_URL", "http://127.0.0.1:8001")

    # Optional: comma-separated list of Telegram user IDs who are admins of the bot
    # Example: TELEGRAM_ADMIN_IDS="12345678,98765432"
    TELEGRAM_ADMIN_IDS = [
        int(x) for x in os.getenv("TELEGRAM_ADMIN_IDS", "").split(",") if x.strip()
    ]
    FAQ_SHEET_CSV_URL = os.getenv("FAQ_SHEET_CSV_URL", "")
    FAQ_MATCH_THRESHOLD = int(os.getenv("FAQ_MATCH_THRESHOLD", "80"))
    OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434")
    OLLAMA_PHI_MODEL = os.getenv("OLLAMA_PHI_MODEL", "phi:2.7b")

config = Settings()

SUPERADMIN_ROLE_ID = config.SUPERADMIN_ROLE_ID
ADMIN_ROLE_ID = config.ADMIN_ROLE_ID
MASTER_ROLE_ID = config.MASTER_ROLE_ID
USER_ROLE_ID = config.USER_ROLE_ID

src_client = MongoClient(config.SOURCE_MONGO_URI)
src_db = src_client[config.SOURCE_DB_NAME]
users = src_db[config.USERS_COLL]
orders = src_db[config.TRADE_COLL]
positions = src_db[config.POSITIONS_COLL]
trade_market = src_db[config.POSITIONS_COLL_ACTUAL]
alerts = src_db[config.ALERTS_COLL]
wallets = src_db[config.WALLETS_COLL]
transactions = src_db[config.TRANSACTIONS_COLL]
exchange = src_db[config.EXCHANGE_COLL]
login_history = src_db[config.LOGIN_HISTORY_COLL]
tele_notification =src_db[config.TELE_NOTIFICATION_COLL]

dst_client = MongoClient(config.ANALYSIS_MONGO_URI)
dst_db = dst_client[config.ANALYSIS_DB_NAME]
analysis = dst_db[config.ANALYSIS_COLL]
analysis_users = dst_db[config.ANALYSIS_USERS_COLL]
data = dst_db[config.DATA_COLL]
trade = dst_db[config.TRADE_COLL_ANALYSIS]
notification = dst_db[config.NOTIFICATION]
