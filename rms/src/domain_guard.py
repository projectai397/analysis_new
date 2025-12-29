# src/domain_guard.py
import re
from typing import Tuple, Dict, List

# ─────────────────────────────────────────────────────────────
# Strong Domain Guard for a Trading Support Chatbot
# - in_domain: trading + account access + KYC + payments + app/site usage
# - out_of_domain: politics, general knowledge, entertainment, etc.
# - ambiguous: very short / unclear messages -> ask one clean question
# ─────────────────────────────────────────────────────────────

# Clean user-facing messages (no emojis, no brand names)
OOD_MESSAGE = (
    "I can help only with trading-related questions (orders, account access, or payments). "
    "Please ask something related to that."
)

CLARIFY_MESSAGE = (
    "I can help with trading, account access, and payments. What do you need help with?"
)

# Smalltalk that should NOT trigger clarify/refuse
SAFE_SMALLTALK = [
    r"^(hi|hii|hello|hey|heyy)\b",
    r"^good\s+(morning|afternoon|evening)\b",
    r"^(thanks?|thank\s+you)\b",
    r"^(ok|okay|cool|great|nice)\b",
    r"^(help|support)\b",

    # ✅ added: common short acknowledgements
    r"^(yes|no|yep|nope|sure|please)\b",
    r"^(ok\s+bro|ok\s+sir|okay\s+bro|okay\s+sir)\b",

    # ✅ added: Hindi greetings / acknowledgements (optional but useful)
    r"^(namaste|hlo|hlw)\b",
    r"^(धन्यवाद|शुक्रिया|ठीक|अच्छा)\b",
]

# Strong allowlist: core product scope + common user phrasing
WHITELIST: Dict[str, int] = {
    # Trading actions
    r"\b(buy|sell)\b": 4,
    r"\b(place|open|enter|execute)\s+(a\s+)?(buy|sell)\b": 5,
    r"\b(close|exit)\s+(a\s+)?(position|trade)\b": 5,
    r"\b(order|orders)\b": 4,
    r"\b(market|limit|stop)\s+order\b": 5,

    # ✅ added: broader trading verbs users type
    r"\b(trade|trading|invest|investment)\b": 3,
    r"\b(go\s+long|go\s+short|long|short)\b": 4,
    r"\b(entry|exit)\s+price\b": 3,

    # Risk controls / order params
    r"\b(stop\s*loss|sl)\b": 5,
    r"\b(take\s*profit|tp)\b": 5,
    r"\b(trigger\s*price|entry\s*price|target|qty|quantity)\b": 3,

    # ✅ added: bracket order / GTT / advanced
    r"\b(bracket\s*order|bo|gtt|oco)\b": 3,

    # Positions & PNL
    r"\b(position|positions)\b": 4,
    r"\b(pnl|profit|loss)\b": 4,

    # ✅ added: charges and fills
    r"\b(filled|rejected|pending|executed|average\s*price|avg\s*price)\b": 3,
    r"\b(charges?|fees?|brokerage)\b": 2,

    # Margin / leverage / liquidation
    r"\b(margin|leverage|liquidat(?:e|ion)|maintenance\s+margin|required\s+margin)\b": 5,

    # Instruments / symbols
    r"\b(symbol|ticker|instrument|script|pair)\b": 3,
    r"\b(lot\s*size|expiry|strike)\b": 2,

    # ✅ added: common markets (your earlier version had these too; adding here helps)
    r"\b(nse|bse|mcx|forex|options?|futures?|nifty|sensex|banknifty|finnifty)\b": 3,

    # Deposits / withdrawals / funds
    r"\b(wallet|balance|funds|available\s+balance)\b": 4,
    r"\b(deposit|add\s+money|top\s*up)\b": 5,
    r"\b(withdraw|withdrawal|payout)\b": 5,

    # ✅ added: UPI/Bank common terms (generic)
    r"\b(upi|bank|card|netbanking|refund)\b": 3,

    # KYC
    r"\b(kyc|verification|verify\s+account|documents?|id\s+proof)\b": 5,

    # ✅ added: Hindi KYC/auth/payment keywords (light, generic)
    r"\b(केवाईसी|वेरिफिकेशन|पासवर्ड|लॉगिन|ओटीपी|निकासी|जमा)\b": 4,

    # Account access / security
    r"\b(login|log\s*in|sign\s*in)\b": 5,
    r"\b(log\s*out|sign\s*out)\b": 3,
    r"\b(username|user\s*name)\b": 3,
    r"\b(password|passcode|pwd)\b": 5,
    r"\b(forgot|forget|reset|change)\s*password\b": 6,
    r"\b(otp|2fa|authenticator)\b": 5,

    # ✅ added: account/profile
    r"\b(account|profile)\b": 3,

    # App/site usage
    r"\b(app|website|dashboard|profile|settings|button|page|tab)\b": 3,
    r"\b(error|issue|problem|not\s+working|failed|unable)\b": 2,

    # ✅ added: navigation words users type
    r"\b(where|how|why)\b": 1,
    r"\b(screen|menu|option|step|steps)\b": 2,
}
PREDICTION_BLOCK = [
    r"\bhow\s+much\s+profit\b",
    r"\bprofit\s+i\s+got\b",
    r"\bhow\s+much\s+will\s+i\s+earn\b",
    r"\bcan\s+i\s+make\s+profit\b",
    r"\bguarantee(d)?\s+profit\b",
    r"\bprofit\s+guarantee\b",
    r"\bshould\s+i\s+buy\b",          # optional: keep if you want no advice
    r"\bis\s+it\s+good\s+to\s+buy\b", # optional
    r"\bprice\s+will\s+go\b",         # optional
    r"\btarget\s+price\b",            # optional
]
# Strong denylist: common off-topic categories users ask about
DENYLIST: List[str] = [
    # Politics / current affairs / general knowledge prompts
    r"\b(pm|prime\s+minister|president|politics|government|election|parliament|congress|senate)\b",
    r"\b(capital\s+of|who\s+is|what\s+is\s+the\s+capital|when\s+did)\b",

    # ✅ added: generic Q/A patterns that are clearly not support
    r"\b(define|meaning\s+of|full\s+form\s+of)\b",

    # Health / medical
    r"\b(symptom|diagnos(?:is|e)|treatment|medicine|calorie|diet|hospital|doctor)\b",

    # Cooking / recipes
    r"\b(recipe|cook|bake|ingredients)\b",

    # Relationships / adult
    r"\b(relationship|dating|sex|porn)\b",

    # School / homework
    r"\b(homework|essay|solve\s+(?:math|equation)|assignment|thesis)\b",

    # Entertainment / gossip
    r"\b(astrology|horoscope|celebrity|gossip|movie|song|lyrics)\b",

    # ✅ added: jokes/chitchat
    r"\b(joke|meme|roast|funny)\b",

    # Tech support unrelated to trading platform
    r"\b(windows|linux|iphone|android\s+update|printer|router)\b",
]

# Scoring thresholds
ALLOW_HARD = 6       # confidently in-domain
ALLOW_SOFT = 3       # probably in-domain
DENY_HARD = 3        # confidently out-of-domain

# Messages that should be treated as ambiguous if too short/unclear
_MIN_LEN_AMBIG = 6   # characters
_MIN_TOK_AMBIG = 2   # tokens

# ✅ added: ultra-short tokens that should trigger clarify (not refuse)
_AMBIG_SHORT_WORDS = {"hi", "hii", "hello", "hey", "help", "support", "ok", "okay"}

# ✅ added: detect Hindi script
_HI_RE = re.compile(r"[\u0900-\u097F]")


# ─────────────────────────────────────────────────────────────
# Engine
# ─────────────────────────────────────────────────────────────

def _norm(text: str) -> str:
    t = (text or "").lower().strip()
    # normalize punctuation a bit (including Hindi danda)
    t = re.sub(r"[।\?\!\.\,]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def _is_smalltalk(text: str) -> bool:
    t = _norm(text)
    return any(re.search(p, t) for p in SAFE_SMALLTALK)

def _score_allow(text: str) -> Tuple[int, List[str]]:
    t = _norm(text)
    s = 0
    hits: List[str] = []
    for pat, w in WHITELIST.items():
        if re.search(pat, t):
            s += int(w)
            hits.append(pat)
    return s, hits

def _score_deny(text: str) -> Tuple[int, List[str]]:
    t = _norm(text)
    s = 0
    hits: List[str] = []
    for pat in DENYLIST:
        if re.search(pat, t):
            s += 1
            hits.append(pat)
    return s, hits

def _is_prediction_request(text: str) -> bool:
    t = _norm(text)
    return any(re.search(p, t) for p in PREDICTION_BLOCK)

def _is_ambiguous(text: str) -> bool:
    t = _norm(text)
    if not t:
        return True

    # If it's smalltalk, not ambiguous
    if _is_smalltalk(t):
        return False

    # very short messages that are not smalltalk
    if len(t) < _MIN_LEN_AMBIG:
        return True

    toks = t.split()
    if len(toks) < _MIN_TOK_AMBIG:
        # allow a few short explicit cues to clarify instead of refusing
        if toks and toks[0] in _AMBIG_SHORT_WORDS:
            return True
        # if it's just punctuation/garbage
        if not any(ch.isalnum() for ch in t):
            return True

    # Hindi single-word questions can be ambiguous (e.g., "पासवर्ड")
    if _HI_RE.search(t) and len(toks) <= 1:
        return True

    return False

def classify(text: str) -> Dict:
    """
    Returns:
      decision ∈ {'in_domain','out_of_domain','ambiguous'}
      confidence ∈ {'high','medium','low'}
      allow/deny scores + pattern hits (for debugging/logging)
    """
    if _is_smalltalk(text):
        return {
            "decision": "in_domain",
            "confidence": "high",
            "allow": ALLOW_HARD,
            "deny": 0,
            "allow_hits": ["smalltalk"],
            "deny_hits": [],
        }

    if _is_ambiguous(text):
        return {
            "decision": "ambiguous",
            "confidence": "low",
            "allow": 0,
            "deny": 0,
            "allow_hits": [],
            "deny_hits": [],
        }

    allow, allow_hits = _score_allow(text)
    deny, deny_hits = _score_deny(text)

    # Hard deny wins unless allow is clearly strong
    if deny >= DENY_HARD and allow < ALLOW_HARD:
        return {
            "decision": "out_of_domain",
            "confidence": "high",
            "allow": allow,
            "deny": deny,
            "allow_hits": allow_hits,
            "deny_hits": deny_hits,
        }

    if allow >= ALLOW_HARD:
        return {
            "decision": "in_domain",
            "confidence": "high",
            "allow": allow,
            "deny": deny,
            "allow_hits": allow_hits,
            "deny_hits": deny_hits,
        }

    if allow >= ALLOW_SOFT:
        return {
            "decision": "in_domain",
            "confidence": "medium",
            "allow": allow,
            "deny": deny,
            "allow_hits": allow_hits,
            "deny_hits": deny_hits,
        }

    # ✅ IMPORTANT CHANGE:
    # Previously you returned out_of_domain by default.
    # Keep that (clean support behavior), but keep confidence low/medium for logging.
    return {
        "decision": "out_of_domain",
        "confidence": "medium",
        "allow": allow,
        "deny": deny,
        "allow_hits": allow_hits,
        "deny_hits": deny_hits,
    }

def is_in_domain(text: str, *, threshold: int = 3) -> Tuple[bool, str]:
    c = classify(text)
    return (
        c["decision"] == "in_domain",
        f"allow={c['allow']}, deny={c['deny']}, conf={c['confidence']}",
    )

def guard_action(text: str) -> Dict:
    c = classify(text)

    # ✅ Block profit prediction / investment advice questions
    if _is_prediction_request(text):
        return {
            "action": "refuse",
            "message": (
                "I can’t predict profits or price moves. "
                "If you tell me what you want to do in the app (buy/sell, SL/TP, or check P&L), I can guide you."
            ),
            "meta": c,
        }

    if c["decision"] == "in_domain":
        return {"action": "answer", "meta": c}
    if c["decision"] == "ambiguous":
        return {"action": "clarify", "prompt": CLARIFY_MESSAGE, "meta": c}
    return {"action": "refuse", "message": OOD_MESSAGE, "meta": c}
