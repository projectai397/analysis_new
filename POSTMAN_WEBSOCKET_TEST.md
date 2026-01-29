# Postman WebSocket Testing Guide for Call Signaling

## Prerequisites
- Postman version 10.0+ (supports WebSocket)
- Server running (default: `http://localhost:5000` or your configured port)

---

## Step 1: Get JWT Token (Login)

### For USER (to initiate call):
**Request:**
```
POST http://localhost:5000/api/login
Content-Type: application/json

{
  "phone": "USER_PHONE_NUMBER",
  "password": "USER_PASSWORD"
}
```

**Response:**
```json
{
  "ok": true,
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "user": {
    "id": "USER_ID",
    "role": "ROLE_ID"
  }
}
```

**Copy the `access_token` value!**

### For MASTER (to receive call):
**Request:**
```
POST http://localhost:5000/api/login
Content-Type: application/json

{
  "phone": "MASTER_PHONE_NUMBER",
  "password": "MASTER_PASSWORD"
}
```

**Copy the `access_token` value!**

---

## Step 2: Connect to WebSocket

### For USER:
1. In Postman, create a **New Request**
2. Change method to **WebSocket**
3. Enter URL:
   ```
   ws://localhost:5000/ws?token=YOUR_USER_TOKEN_HERE
   ```
   Replace `YOUR_USER_TOKEN_HERE` with the token from Step 1

4. Click **Connect**
5. You should receive:
   ```json
   {"type": "joined", "chat_id": "CHAT_ID", "role": "user"}
   ```

### For MASTER/ADMIN/SUPERADMIN:
1. Create another **New Request** (or use a different Postman window)
2. Change method to **WebSocket**
3. Enter URL:
   ```
   ws://localhost:5000/ws?token=YOUR_MASTER_TOKEN_HERE
   ```
   Replace `YOUR_MASTER_TOKEN_HERE` with the master/admin/superadmin token from Step 1

4. Click **Connect**
5. You should receive:
   ```json
   {
     "type": "joined",
     "role": "master",
     "needs_selection": true,
     "chatrooms": [...],
     "pagination": {
       "total_count": 250,
       "total_pages": 5,
       "current_page": 1,
       "limit": 50
     }
   }
   ```
   **Note:** Only first 50 chatrooms are sent initially. Use `list_chatrooms` for pagination.

---

## Step 3: List Chatrooms with Pagination and Search (MASTER/ADMIN/SUPERADMIN)

After connecting, you can request paginated chatroom lists and search:

### Get Next Page:
**Send from MASTER/ADMIN/SUPERADMIN WebSocket:**
```json
{
  "type": "list_chatrooms",
  "page": 2,
  "limit": 50
}
```

**Response:**
```json
{
  "type": "chatrooms_list",
  "chatrooms": [...],
  "pagination": {
    "total_count": 250,
    "total_pages": 5,
    "current_page": 2,
    "limit": 50,
    "search": null
  }
}
```

### Search Across All Chatrooms:
**Send from MASTER/ADMIN/SUPERADMIN WebSocket:**
```json
{
  "type": "list_chatrooms",
  "page": 1,
  "search": "john",
  "limit": 50
}
```

**Response:**
```json
{
  "type": "chatrooms_list",
  "chatrooms": [...],
  "pagination": {
    "total_count": 15,
    "total_pages": 1,
    "current_page": 1,
    "limit": 50,
    "search": "john"
  }
}
```

**Note:** Search searches across **ALL** chatrooms for that role (e.g., all 250), not just the current page. It searches in:
- User name
- User username (userName/user_name)
- User phone number

### Search Parameters:
- `page` (optional): Page number, defaults to 1
- `search` (optional): Search term (searches name, username, phone)
- `limit` (optional): Items per page (1-100), defaults to 50

---

## Step 4: Hierarchical Selection (SUPERADMIN/ADMIN)

### For SUPERADMIN: Select Admin

When superadmin connects, they receive:
```json
{
  "type": "joined",
  "role": "superadmin",
  "needs_selection": true,
  "hierarchy": {
    "type": "superadmin",
    "admins": [
      {
        "id": "ADMIN_ID_1",
        "name": "Admin Name",
        "userName": "admin_username",
        "phone": "1234567890"
      }
    ]
  },
  "chatrooms": [
    {
      "chat_id": "SUPERADMIN_STAFF_BOT_CHAT_ID",
      "room_type": "staff_bot",
      ...
    }
  ]
}
```

**To select an admin, send:**
```json
{
  "type": "select_admin",
  "admin_id": "ADMIN_ID_1"
}
```

**Response:**
```json
{
  "type": "admin_selected",
  "admin_id": "ADMIN_ID_1",
  "chatrooms": [
    {
      "chat_id": "ADMIN_STAFF_BOT_CHAT_ID",
      "room_type": "staff_bot",
      ...
    }
  ],
  "masters": [
    {
      "id": "MASTER_ID_1",
      "name": "Master Name",
      "userName": "master_username",
      "phone": "0987654321"
    }
  ],
  "pagination": {
    "total_count": 1,
    "total_pages": 1,
    "current_page": 1,
    "limit": 50
  }
}
```

### For SUPERADMIN: Select Master (after selecting admin)

**Send:**
```json
{
  "type": "select_master",
  "master_id": "MASTER_ID_1",
  "admin_id": "ADMIN_ID_1"
}
```

**Response:**
```json
{
  "type": "master_selected",
  "master_id": "MASTER_ID_1",
  "chatrooms": [
    {
      "chat_id": "MASTER_STAFF_BOT_CHAT_ID",
      "room_type": "staff_bot",
      ...
    },
    {
      "chat_id": "USER_CHATROOM_ID",
      "room_type": "support",
      "user": {...}
    }
  ],
  "pagination": {...}
}
```

### For ADMIN: Select Master

When admin connects, they receive:
```json
{
  "type": "joined",
  "role": "admin",
  "needs_selection": true,
  "hierarchy": {
    "type": "admin",
    "masters": [...]
  },
  "chatrooms": [
    {
      "chat_id": "ADMIN_STAFF_BOT_CHAT_ID",
      "room_type": "staff_bot",
      ...
    }
  ]
}
```

**To select a master, send:**
```json
{
  "type": "select_master",
  "master_id": "MASTER_ID_1"
}
```

**Response:**
```json
{
  "type": "master_selected",
  "master_id": "MASTER_ID_1",
  "chatrooms": [...],
  "pagination": {...}
}
```

---

## Step 5: Master Selects Chatroom (Optional)

If master received `needs_selection: true`, they need to select a chatroom:

**Send from MASTER WebSocket:**
```json
{
  "type": "select_chatroom",
  "chat_id": "CHAT_ID_FROM_USER_JOINED_MESSAGE"
}
```

**Response:**
```json
{"type": "selected", "chat_id": "CHAT_ID", "role": "master"}
```

**Note:** For testing call signaling when master is NOT in chatroom, you can skip this step or select a different chatroom!

---

## Step 6: Test Call Signaling

### USER Initiates Call:

**Send from USER WebSocket:**
```json
{
  "type": "call.start"
}
```

**Expected Responses:**

**USER receives:**
```json
{"type": "call.ringing", "call_id": "abc123...", "chat_id": "CHAT_ID"}
```

**MASTER receives (even if not in that chatroom!):**
```json
{
  "type": "call.incoming",
  "call_id": "abc123...",
  "chat_id": "CHAT_ID",
  "from_user_id": "USER_ID",
  "from_role": "user",
  "to_role": "master"
}
```

---

## Step 7: Master Accepts Call

**Send from MASTER WebSocket:**
```json
{
  "type": "call.accept",
  "call_id": "abc123..."
}
```

**Expected Responses:**

**MASTER receives:**
```json
{"type": "call.accepted_ack", "call_id": "abc123..."}
```

**USER receives:**
```json
{"type": "call.accepted", "call_id": "abc123...", "chat_id": "CHAT_ID"}
```

---

## Step 8: WebRTC Signaling (if implementing)

### USER sends offer:
```json
{
  "type": "call.offer",
  "call_id": "abc123...",
  "sdp": "v=0\r\no=- 1234567890 1234567890 IN IP4..."
}
```

### MASTER sends answer:
```json
{
  "type": "call.answer",
  "call_id": "abc123...",
  "sdp": "v=0\r\no=- 9876543210 9876543210 IN IP4..."
}
```

### ICE Candidates:
```json
{
  "type": "call.ice",
  "call_id": "abc123...",
  "candidate": "candidate:1 1 UDP 2130706431..."
}
```

---

## Step 9: End Call

**Send from either USER or MASTER:**
```json
{
  "type": "call.end",
  "call_id": "abc123..."
}
```

**Both receive:**
```json
{"type": "call.ended", "call_id": "abc123...", "chat_id": "CHAT_ID"}
```

---

## Step 10: Master Rejects Call (Alternative)

**Send from MASTER WebSocket:**
```json
{
  "type": "call.reject",
  "call_id": "abc123..."
}
```

**Expected Responses:**

**MASTER receives:**
```json
{"type": "call.rejected_ack", "call_id": "abc123..."}
```

**USER receives:**
```json
{"type": "call.rejected", "call_id": "abc123...", "chat_id": "CHAT_ID"}
```

---

## Testing Scenarios

### ✅ Test 1: Master NOT in chatroom (Your Requirement)
1. USER connects and joins their chatroom
2. MASTER connects but **DOES NOT** select the user's chatroom (or selects a different one)
3. USER sends `call.start`
4. **Expected:** MASTER should still receive `call.incoming` message!

### ✅ Test 2: Master in different chatroom
1. USER connects to chatroom A
2. MASTER connects and selects chatroom B
3. USER sends `call.start` for chatroom A
4. **Expected:** MASTER should receive `call.incoming` for chatroom A

### ✅ Test 3: Master offline
1. USER connects
2. MASTER does NOT connect
3. USER sends `call.start`
4. **Expected:** USER receives `{"type": "call.error", "error": "target_offline"}`

---

## Keep-Alive (Ping)

Send periodically to keep connection alive:
```json
{"type": "ping"}
```

Response:
```json
{"type": "pong"}
```

---

## Troubleshooting

### Connection Issues:
- Check server is running: `http://localhost:5000/api/health` (if exists)
- Verify token is valid (not expired)
- Check WebSocket URL uses `ws://` not `http://`

### Call Not Received:
- Verify master's `pro_id` matches `chat.super_admin_id` in database
- Check master is registered in `MASTER_SOCKETS` (check server logs)
- Verify token has correct role (master/superadmin)

### Error Messages:
- `"error": "no_chat_selected"` - User needs to be in a chatroom
- `"error": "no_target_assigned"` - Chatroom doesn't have a master assigned
- `"error": "target_offline"` - Master is not connected
- `"error": "forbidden_chatroom"` - User doesn't have access to that chatroom

---

## Quick Test Commands Summary

### Login:
**1. Login (USER):**
```bash
curl -X POST http://localhost:5000/api/login \
  -H "Content-Type: application/json" \
  -d '{"phone":"USER_PHONE","password":"USER_PASS"}'
```

**2. Login (MASTER/ADMIN/SUPERADMIN):**
```bash
curl -X POST http://localhost:5000/api/login \
  -H "Content-Type: application/json" \
  -d '{"phone":"MASTER_PHONE","password":"MASTER_PASS"}'
```

### WebSocket URLs:
- User: `ws://localhost:5000/ws?token=USER_TOKEN`
- Master/Admin/Superadmin: `ws://localhost:5000/ws?token=MASTER_TOKEN`

### Chatroom List & Search (from MASTER/ADMIN/SUPERADMIN):
**List page 2:**
```json
{"type": "list_chatrooms", "page": 2, "limit": 50}
```

**Search all chatrooms:**
```json
{"type": "list_chatrooms", "page": 1, "search": "john", "limit": 50}
```

**Search with pagination:**
```json
{"type": "list_chatrooms", "page": 2, "search": "john", "limit": 50}
```

### Call Signaling:
**Call Start (from USER):**
```json
{"type": "call.start"}
```

**Call Accept (from MASTER):**
```json
{"type": "call.accept", "call_id": "CALL_ID_FROM_INCOMING"}
```
