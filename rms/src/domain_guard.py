# src/domain_guard.py
import re
from typing import Tuple
from typing import Tuple, Dict, List
# Tune with your exact product vocabulary
WHITELIST: Dict[str, int] = {
    # brand / product
    r"\bprotrader5(\.io)?\b": 5,
    r"\bpt5\b": 4,
    r"\b(trading\s*platform|trading\s*app)\b": 3,

    # core intents (PHRASINGS users actually type)
    r"\b(buy|purchase|go long|place buy|enter long)\b": 6,
    r"\b(sell|exit|go short|place sell|close position)\b": 6,
    r"\b(how to\s+)?(buy|sell)\s+(stocks?|shares?|crypto|btc|eth|symbol|ticker)\b": 7,

    # account / auth / recovery
    r"\baccount\b": 3,
    r"\b(log(?:in|out))\b": 3,
    r"\b(sign\s*in|sign\s*out)\b": 3,
    r"\busername\b": 2,
    r"\b(password|passcode|pass word|pwd)s?\b": 4,
    r"\b(forgot|forget|reset|change)\s*password\b": 7,
    r"\botp\b": 3,
    r"\bkyc\b": 3,

    # modules / features
    r"\bwatchlists?\b": 2,
    r"\border(s| id| status)?\b": 3,
    r"\bposition(s)?\b": 3,
    r"\bportfolio\b": 2,
    r"\b(script(\s*manager|\s*policy)?|policy( settings)?)\b": 3,
    r"\bwebsocket(s)?\b": 2,
    r"\buser\s*tree\b": 2,

    # wallet / funds  (FIXED GROUPING)
    r"\b(wallet|balance|credit|margin)\b": 3,
    r"\b(deposit|withdraw(?:al)?)\b": 3,

    # orders / risk
    r"\b(limit|market|stop(?:\s*loss)?|take\s*profit|t[ps]|s[ls])\b": 3,
    r"\b(leverage|required\s*margin|holding\s*margin|funding\s*rate|liquidat(?:e|ion))\b": 3,
    r"\bspread\b": 2,

    # instruments / exchanges
    r"\b(nse|bse|mcx|forex|options?|futures?|banknifty|finnifty|nifty|sensex)\b": 3,
    r"\b(script\s*name|symbol|ticker|lot size|expiry|strike)\b": 2,

    # app / site
    r"\b(website|app|mobile|dashboard|tabs?|button|page)\b": 2,
    r"\b(error\s*\d{3}|maintenance|downtime|status)\b": 2,
}

SAFE_SMALLTALK = [
    r"\bhi\b", r"\bhello\b", r"\bhey\b", r"\bthanks?\b", r"\bthank you\b", r"\bok(ay)?\b", r"\bhelp\b", r"\bsupport\b",
]

# clear off-topic (keep very small to avoid false negatives)
DENYLIST = [
    r"\b(symptom|diagnos(?:is|e)|treatment|medicine|calorie|diet)\b",
    r"\b(recipe|cook|bake|ingredients)\b",
    r"\b(relationship|dating|sex|porn)\b",
    r"\b(homework|essay|solve (?:math|equation)|thesis)\b",
    r"\b(astrology|horoscope|celebrity gossip|movie review)\b",
]

OOD_MESSAGE = (
   "Hi! 😊 What brings you here today? I can help you with anything — just ask!"
)

ALLOW_SCORE = 1.0
DENY_SCORE = 1.2
IN_DOMAIN_STRICT = 1.5
IN_DOMAIN_WEAK   = 1.0
OUT_DOMAIN_STRICT = 1.5

# ───────────────── Engine ─────────────────

def _score_allow(text: str) -> Tuple[float, List[str]]:
    t = (text or "").lower()
    s = 0.0
    hits = []
    for pat, _w in WHITELIST.items():
        if re.search(pat, t):
            s += ALLOW_SCORE
            hits.append(pat)
    # weight by your own weights (optional but harmless)
    # you can uncomment next 3 lines if you want to use the configured weights
    # s = 0.0; hits=[]
    # for pat, w in WHITELIST.items():
    #     (re.search(pat, t) and (s:=s+w) is None) or None
    return s, hits

def _score_deny(text: str) -> Tuple[float, List[str]]:
    t = (text or "").lower()
    s = 0.0
    hits = []
    for pat in DENYLIST:
        if re.search(pat, t):
            s += DENY_SCORE
            hits.append(pat)
    return s, hits

def _is_smalltalk(text: str) -> bool:
    t = (text or "").lower()
    return any(re.search(p, t) for p in SAFE_SMALLTALK)

def classify(text: str) -> Dict:
    if _is_smalltalk(text):
        return {"decision": "in_domain", "confidence": "high", "allow": 1.0, "deny": 0.0, "allow_hits": ["smalltalk"], "deny_hits": []}

    allow, allow_hits = _score_allow(text)
    deny, deny_hits = _score_deny(text)

    if allow >= IN_DOMAIN_STRICT:
        return {"decision": "in_domain", "confidence": "high", "allow": allow, "deny": deny, "allow_hits": allow_hits, "deny_hits": deny_hits}
    if allow >= IN_DOMAIN_WEAK:
        return {"decision": "in_domain", "confidence": "medium", "allow": allow, "deny": deny, "allow_hits": allow_hits, "deny_hits": deny_hits}
    if deny >= OUT_DOMAIN_STRICT and allow == 0:
        return {"decision": "out_of_domain", "confidence": "high", "allow": allow, "deny": deny, "allow_hits": allow_hits, "deny_hits": deny_hits}
    return {"decision": "ambiguous", "confidence": "low", "allow": allow, "deny": deny, "allow_hits": allow_hits, "deny_hits": deny_hits}

def is_in_domain(text: str, *, threshold: int = 3) -> Tuple[bool, str]:
    """Backward compatible: True if classify() says in_domain."""
    c = classify(text)
    return (c["decision"] == "in_domain", f"allow={c['allow']}, deny={c['deny']}, conf={c['confidence']}")

def guard_action(text: str) -> Dict:
    """
    - 'answer'  → proceed (includes weak/medium confidence to avoid false 'out of scope')
    - 'clarify' → nudge toward app scope if ambiguous
    - 'refuse'  → only when clearly off-topic
    """
    c = classify(text)
    if c["decision"] == "in_domain":
        return {"action": "answer", "meta": c}
    if c["decision"] == "ambiguous":
        return {
            "action": "clarify",
            "prompt": "Do you mean within our app—for example placing a buy/sell order, resetting your password, or deposits/withdrawals?",
            "meta": c,
        }
    return {"action": "refuse", "message": OOD_MESSAGE, "meta": c}