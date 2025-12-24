from __future__ import annotations
import re
from typing import Dict, Any, List, Optional, Tuple

from pymongo import MongoClient
from rapidfuzz import process, fuzz

# Optional semantic layer
try:
    from sentence_transformers import SentenceTransformer
    import numpy as np
    _SEM = True
except Exception:
    _SEM = False

_FAQS: List[Dict[str, Any]] = []
_SEM_MODEL = None
_SEM_EMBS = None  # per-faq embedding of all keywords joined

def _norm(t: str) -> str:
    t = (t or "").lower()
    t = re.sub(r"[^a-z0-9\s']", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    # light canonicalization
    t = t.replace("forgot pass", "reset password")
    t = t.replace("forget pass", "reset password")
    t = t.replace("change pass", "reset password")
    return t

def load_faqs(uri: str, db: str, coll: str) -> None:
    """Call once on startup."""
    global _FAQS, _SEM_MODEL, _SEM_EMBS
    cli = MongoClient(uri)
    docs = list(cli[db][coll].find({}, {"keywords": 1, "reply": 1, "rating": 1}))
    _FAQS = []
    for d in docs:
        kws = [k for k in (d.get("keywords") or []) if isinstance(k, str)]
        rep = [r for r in (d.get("reply") or []) if isinstance(r, str)]
        _FAQS.append({
            "_id": str(d.get("_id")),
            "keywords": kws,
            "reply": "\n".join(rep).strip(),
            "rating": int(d.get("rating") or 0),
        })
    # semantic index (one embedding per FAQ item, concatenated keywords)
    if _SEM and _FAQS:
        _SEM_MODEL = SentenceTransformer("all-MiniLM-L6-v2")
        texts = ["; ".join(x["keywords"]) for x in _FAQS]
        _SEM_EMBS = _SEM_MODEL.encode(texts, normalize_embeddings=True)

def answer_from_faq(user_msg: str,
                    fuzzy_threshold: int = 86,
                    sem_threshold: float = 0.58) -> Optional[str]:
    """Return FAQ reply if the question is 'same meaning' as any keyword."""
    if not _FAQS:
        return None
    q = _norm(user_msg)

    # 1) FUZZY over all individual keywords
    corpus = []
    for idx, item in enumerate(_FAQS):
        for kw in item["keywords"]:
            corpus.append((idx, kw))
    choices = [kw for _, kw in corpus]
    hit = process.extractOne(q, choices, scorer=fuzz.WRatio)
    if hit:
        _, score, pos = hit
        if score >= fuzzy_threshold:
            idx = corpus[pos][0]
            return _FAQS[idx]["reply"]

    # 2) SEMANTIC over concatenated keywords per FAQ
    if _SEM and _SEM_MODEL is not None and _SEM_EMBS is not None:
        qv = _SEM_MODEL.encode([q], normalize_embeddings=True)
        sims = (qv @ _SEM_EMBS.T).ravel()
        i = int(sims.argmax())
        if float(sims[i]) >= sem_threshold:
            return _FAQS[i]["reply"]

    return None
