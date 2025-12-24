# src/models.py
from datetime import datetime, timezone
import mongoengine as me

class ProUser(me.Document):
    meta = {"db_alias": "pro", "collection": "user", "indexes": ["role"], "index_background": True}
    role = me.StringField(required=True)
    parent_id = me.ObjectIdField(null=True)   # parent doc’s _id in pro_v2.users

class SCUser(me.Document):
    meta = {
        "db_alias": "support",
        "collection": "users",
        "indexes": [
            {"fields": ["user_id"], "sparse": True},
            {"fields": ["role"]},
            {"fields": ["is_bot"]},
        ]
    }

    # For humans: user_id is the pro.users._id; for bot: user_id is None
    user_id   = me.ObjectIdField(null=True)
    role      = me.ObjectIdField(null=True)
    parent_id = me.ObjectIdField(null=True)

    is_bot    = me.BooleanField(default=False)

    # New identity fields (from pro.users)
    name      = me.StringField(null=True)
    user_name = me.StringField(null=True)
    phone     = me.StringField(null=True)

    created_time = me.DateTimeField(default=lambda: datetime.now(timezone.utc))
    updated_time = me.DateTimeField(default=lambda: datetime.now(timezone.utc))
    
class Bot(me.Document):
    meta = {"db_alias": "support", "collection": "bot", "indexes": [{"fields":["name"], "unique":True}]}
    name = me.StringField(required=True, unique=True)
    created_time = me.DateTimeField(default=lambda: datetime.now(timezone.utc))
    updated_time = me.DateTimeField(default=lambda: datetime.now(timezone.utc))

class Chatroom(me.Document):
    meta = {
        "db_alias": "support",
        "collection": "chatroom",
        "indexes": [
            "user_id",
            "super_admin_id",                          # legacy
            "admin_id",                                # NEW INDEX (optional but recommended)
            "status",

            # 🔹 fast listing by owner
            {"fields": ["owner_id", "status", "-updated_time"]},

            # 🔹 keep exactly one OPEN room per (user, owner)
            {"fields": ["user_id", "owner_id", "status"], "unique": True, "sparse": True},
        ],
    }

    # Core identifiers
    user_id        = me.ObjectIdField(required=True)   # pro.users._id (the CLIENT)
    owner_id       = me.ObjectIdField(required=True)       # true/top superadmin's pro.users._id
    super_admin_id = me.ObjectIdField(required=True)       # legacy
    admin_id       = me.ObjectIdField(required=True)       # 🔹 NEW: admin managing the chatroom

    status = me.StringField(choices=("open", "closed"), default="open")

    # Presence flags
    is_user_active        = me.BooleanField(default=False)
    is_superadmin_active  = me.BooleanField(default=False)

    # NEW presence flags
    is_owner_active       = me.BooleanField(default=False)   # 🔹 NEW
    is_admin_active       = me.BooleanField(default=False)   # 🔹 NEW

    created_time = me.DateTimeField(default=lambda: datetime.now(timezone.utc))
    updated_time = me.DateTimeField(default=lambda: datetime.now(timezone.utc))

    def clean(self):
        # auto-touch updated_time
        self.updated_time = datetime.now(timezone.utc)
        
class Message(me.Document):
    meta = {"db_alias": "support", "collection": "messages", "indexes": [("chatroom_id","created_time")]}
    chatroom_id = me.ObjectIdField(required=True)
    message_by  = me.ObjectIdField(required=True)         # SCUser._id (human) OR Bot/SCUser(bot)._id
    message     = me.StringField(null=True)
    is_file     = me.BooleanField(default=False)
    path        = me.StringField(null=True)
    is_bot      = me.BooleanField(default=False)
    created_time = me.DateTimeField(default=lambda: datetime.now(timezone.utc))
    updated_time = me.DateTimeField(default=lambda: datetime.now(timezone.utc))
