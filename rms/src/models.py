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
            # basic filters
            "user_id",
            "room_type",
            "super_admin_id",   # legacy
            "admin_id",
            "owner_id",
            "status",

            # fast listing by owner (support rooms)
            {"fields": ["owner_id", "status", "-updated_time"]},

            # 🔹 ensure exactly one OPEN support room per (user, owner)
            # (keep your old behavior)
            {"fields": ["user_id", "owner_id", "status"], "unique": True, "sparse": True},

            # 🔹 ensure exactly one OPEN staff bot room per (user_id)
            # user_id here will be the superadmin's pro.users._id
            {"fields": ["user_id", "room_type", "status"], "unique": True, "sparse": True},
        ],
    }

    # 🔹 NEW: room discriminator
    room_type = me.StringField(choices=("support", "staff_bot"), default="support")

    # Core identifiers
    # For support rooms: user_id = client id
    # For staff_bot rooms: user_id = superadmin id
    user_id = me.ObjectIdField(required=True)

    # These are REQUIRED for support rooms, but should be empty for staff_bot room
    owner_id       = me.ObjectIdField(required=False, null=True)
    super_admin_id = me.ObjectIdField(required=False, null=True)  # legacy
    admin_id       = me.ObjectIdField(required=False, null=True)

    status = me.StringField(choices=("open", "closed"), default="open")
    title = me.StringField(default="")
    # Presence flags
    is_user_active        = me.BooleanField(default=False)
    is_superadmin_active  = me.BooleanField(default=False)
    is_owner_active       = me.BooleanField(default=False)
    is_admin_active       = me.BooleanField(default=False)

    created_time = me.DateTimeField(default=lambda: datetime.now(timezone.utc))
    updated_time = me.DateTimeField(default=lambda: datetime.now(timezone.utc))

    def clean(self):
        # auto-touch updated_time
        self.updated_time = datetime.now(timezone.utc)

        # Enforce field requirements based on room_type
        if self.room_type == "support":
            # These must exist for normal client support rooms
            if not self.owner_id:
                raise me.ValidationError("owner_id is required for support chatrooms")
            if not self.super_admin_id:
                raise me.ValidationError("super_admin_id is required for support chatrooms")
            if not self.admin_id:
                raise me.ValidationError("admin_id is required for support chatrooms")
        else:
            # staff_bot room should not carry routing/ownership fields
            self.owner_id = None
            self.super_admin_id = None
            self.admin_id = None
        
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
