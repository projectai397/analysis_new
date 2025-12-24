import time
from threading import Lock
from collections import deque
from flask import request, jsonify
from src.config import config
from flask_caching import Cache
cache = Cache()

_ip_hits = {}
_ip_blocked_until = {}
_ip_lock = Lock()

def _client_ip():
    xff = request.headers.get("X-Forwarded-For", "")
    if xff: return xff.split(",")[0].strip()
    return request.remote_addr or "0.0.0.0"

def ratelimit_guard():
    if request.method in ("OPTIONS","HEAD"): return
    now = time.time(); ip = _client_ip()
    with _ip_lock:
        blocked_until = _ip_blocked_until.get(ip, 0.0)
        if blocked_until > now:
            retry_after = int(blocked_until - now)
            return jsonify({"ok": False, "error": "Too many requests. Temporarily blocked.", "retry_after_sec": retry_after}), 429, {"Retry-After": str(retry_after)}
        q = _ip_hits.get(ip) or deque(); _ip_hits.setdefault(ip, q)
        window_start = now - config.RATE_WINDOW_SEC
        while q and q[0] < window_start: q.popleft()
        q.append(now)
        if len(q) >= config.RATE_LIMIT_HITS:
            _ip_blocked_until[ip] = now + config.BLOCK_DURATION_SEC
            return jsonify({"ok": False, "error": "Rate limit exceeded. IP blocked for 10 minutes.", "retry_after_sec": config.BLOCK_DURATION_SEC}), 429, {"Retry-After": str(config.BLOCK_DURATION_SEC)}
