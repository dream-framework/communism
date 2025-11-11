#!/usr/bin/env python3
# askbot.py — replies to Masto mentions AND any public posts with #ask
import os, json, time, re, html, datetime
from typing import Dict, Any, List, Optional
from urllib.parse import urljoin

import requests

# ========= ENV =========
MASTODON_INSTANCE = os.getenv("MASTODON_INSTANCE", "https://mastodon.social").rstrip("/")
MASTODON_TOKEN    = os.getenv("MASTODON_TOKEN", "")
ASK_STATE_PATH    = os.getenv("ASK_STATE_PATH", "data/ask_state.json")

# Hashtag trigger (public tag timeline). Provide without '#', we accept both here.
TRIGGER_TAG       = os.getenv("TRIGGER_TAG", "ask").lstrip("#").lower()

# Whether to also react to mentions that include the tag/keyword
ENABLE_MENTIONS   = os.getenv("ENABLE_MENTIONS", "1") == "1"
MENTION_MAX_AGE_MIN = int(os.getenv("MENTION_MAX_AGE_MIN", "180"))  # ignore very old mentions

# Rate limits / batching
MAX_REPLIES_PER_RUN = int(os.getenv("MAX_REPLIES_PER_RUN", "6"))
PUBLIC_TAG_LIMIT    = int(os.getenv("PUBLIC_TAG_LIMIT", "60"))   # how many recent #ask to examine per run

# Visibility for replies (default to original visibility if present)
REPLIES_VIS        = os.getenv("REPLIES_VISIBILITY", "unlisted")  # fallback
FOLLOWERS_ONLY     = os.getenv("FOLLOWERS_ONLY", "0") == "1"      # only reply to people who follow you (safe mode)

# Optional allow/deny lists
ALLOW_DOMAINS   = [x.strip().lower() for x in os.getenv("ALLOW_DOMAINS", "").split(",") if x.strip()]
BLOCK_DOMAINS   = [x.strip().lower() for x in os.getenv("BLOCK_DOMAINS", "").split(",") if x.strip()]

# Meta text (optional context you want injected into answers)
META_PATH         = os.getenv("META_PATH", "meta/primer.txt")

# Groq
GROQ_API_KEY      = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL        = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")
GROQ_MAX_COMPLETION_TOKENS = int(os.getenv("GROQ_MAX_OUTPUT_TOKENS", "320"))
GROQ_SYSTEM_PROMPT = os.getenv("GROQ_SYSTEM_PROMPT",
    "Ты — лаконичный фактологичный помощник-аналитик. Отвечай по существу, 3–6 предложений. "
    "Используй предоставленный метатекст как фоновые рамки, но не цитируй его дословно. "
    "Избегай домыслов и ссылок; если чего-то не хватает, ясно обозначь ограничения."
)

DEBUG = os.getenv("ASKBOT_DEBUG", "1") == "1"

# ======== HTTP session ========
session = requests.Session()
if MASTODON_TOKEN:
    session.headers.update({"Authorization": f"Bearer {MASTODON_TOKEN}"})
session.headers.update({"User-Agent": "askbot/1.2 (+bot)"})

# ========= UTIL =========
def _now_utc() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)

def _iso_to_dt_utc(s: str) -> datetime.datetime:
    s2 = s.strip()
    if s2.endswith("Z"):
        s2 = s2[:-1] + "+00:00"
    dt = datetime.datetime.fromisoformat(s2)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt.astimezone(datetime.timezone.utc)

def _strip_html(s: str) -> str:
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.I)
    s = re.sub(r"</p\s*>", "\n\n", s, flags=re.I)
    s = re.sub(r"<[^>]+>", "", s)
    return html.unescape(s).strip()

def _load_state() -> Dict[str, Any]:
    try:
        with open(ASK_STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}

def _save_state(st: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(ASK_STATE_PATH), exist_ok=True)
    with open(ASK_STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False, indent=2)

def _mastodon_get(path: str, params: Dict[str, Any] = None) -> Any:
    url = urljoin(MASTODON_INSTANCE + "/", path.lstrip("/"))
    r = session.get(url, params=params or {}, timeout=30)
    if DEBUG:
        print(f"[GET] {r.url} -> {r.status_code}")
    r.raise_for_status()
    return r.json()

def _mastodon_post(path: str, data: Dict[str, Any], idem_key: str = "") -> Any:
    url = urljoin(MASTODON_INSTANCE + "/", path.lstrip("/"))
    headers = {}
    if idem_key:
        headers["Idempotency-Key"] = idem_key
    r = session.post(url, data=data, headers=headers, timeout=45)
    if DEBUG:
        print(f"[POST] {url} -> {r.status_code}")
        if r.status_code >= 400:
            print(r.text[:600])
    r.raise_for_status()
    return r.json()

def _verify_credentials() -> Dict[str, Any]:
    return _mastodon_get("/api/v1/accounts/verify_credentials")

def _token_scopes() -> List[str]:
    try:
        info = _mastodon_get("/oauth/token/info")
        scope = info.get("scope", "") or ""
        scopes = sorted(set([s.strip() for s in scope.split() if s.strip()]))
        if DEBUG:
            print("[scopes]", scopes)
        return scopes
    except Exception:
        return []

def _relationships(ids: List[str]) -> Dict[str, Any]:
    out = {}
    if not ids:
        return out
    url = urljoin(MASTODON_INSTANCE + "/", "/api/v1/accounts/relationships")
    # chunk to stay safe
    for i in range(0, len(ids), 80):
        q = [("id[]", _id) for _id in ids[i:i+80]]
        r = session.get(url, params=q, timeout=30)
        r.raise_for_status()
        for row in r.json():
            out[str(row["id"])] = row
    return out

def _read_meta() -> str:
    try:
        with open(META_PATH, "r", encoding="utf-8") as f:
            return f.read().strip()
    except Exception:
        return ""

# ========= GROQ =========
def _call_groq(prompt_user: str, meta_text: str) -> str:
    if not GROQ_API_KEY:
        # Fallback (fast) — never fail silently
        txt = prompt_user.strip()
        return txt[:450] if len(txt) > 450 else txt
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    messages = [
        {"role": "system", "content": GROQ_SYSTEM_PROMPT},
        {"role": "user",   "content": f"Метатекст (фон, можно игнорировать если нерелевантно):\n{meta_text}\n\nЗапрос:\n{prompt_user}"}
    ]
    payload = {
        "model": GROQ_MODEL,
        "messages": messages,
        "temperature": 0.2,
        "n": 1,
        "max_completion_tokens": GROQ_MAX_COMPLETION_TOKENS,
    }
    for attempt in range(3):
        r = requests.post(url, headers=headers, json=payload, timeout=45)
        if r.status_code == 429:
            retry = int(r.headers.get("retry-after", "2"))
            time.sleep(min(5, max(1, retry))); continue
        try:
            r.raise_for_status()
            j = r.json()
            txt = (j.get("choices", [{}])[0].get("message", {}) or {}).get("content", "") or ""
            return txt.strip() or "Извините, не удалось сформировать ответ."
        except Exception:
            time.sleep(0.5)
    return "Извините, сервис перегружен. Попробуйте ещё раз."

# ========= Trigger checks =========
def _text_has_trigger(text: str) -> bool:
    # accept '#ask' anywhere (case-insensitive), including '#Ask' etc.
    pat = r"(?:^|\s)#" + re.escape(TRIGGER_TAG) + r"(?:\b|$)"
    return re.search(pat, text, re.I) is not None

def _allowed_account(acct_obj: Dict[str, Any], self_id: str, relmap: Dict[str, Any]) -> bool:
    if str(acct_obj.get("id")) == str(self_id):
        return False  # skip ourselves
    domain = ""
    acct = acct_obj.get("acct", "")
    if "@" in acct:
        domain = acct.split("@", 1)[1].lower()
    if BLOCK_DOMAINS and domain and domain in BLOCK_DOMAINS:
        return False
    if ALLOW_DOMAINS and domain and domain not in ALLOW_DOMAINS:
        return False
    if FOLLOWERS_ONLY:
        rel = relmap.get(str(acct_obj.get("id")), {})
        if not rel.get("followed_by", False):
            return False
    return True

def _reply_text(handle_acct: str, answer: str) -> str:
    return f"@{handle_acct}\n\n{answer}"

# ========= Builders =========
def _build_prompt_from_status(st: Dict[str, Any], include_context: bool = False) -> str:
    base = _strip_html(st.get("content", ""))
    who  = st.get("account", {}).get("acct", "?")
    prompt = [f"Пользователь @{who} спросил:\n{base}"]
    # Keep it fast — skip heavy thread context by default
    return "\n".join(prompt).strip()

# ========= Fetchers =========
def _fetch_mentions(since_id: Optional[str]) -> List[Dict[str, Any]]:
    params = {"types[]": "mention"}
    if since_id:
        params["since_id"] = since_id
    notifs = _mastodon_get("/api/v1/notifications", params=params)
    notifs = [n for n in notifs if n.get("type") == "mention" and n.get("status")]
    notifs.sort(key=lambda n: int(n["id"]))  # oldest -> newest
    return notifs

def _fetch_tag_timeline(tag: str, since_id: Optional[str], limit: int) -> List[Dict[str, Any]]:
    tag = tag.lstrip("#")
    params = {"limit": max(1, min(80, limit))}
    if since_id:
        params["since_id"] = since_id
    # NOTE: this is the instance's view of the hashtag timeline (federated as available).
    items = _mastodon_get(f"/api/v1/timelines/tag/{tag}", params=params)
    # Oldest -> newest
    items.sort(key=lambda s: int(s["id"]))
    return items

# ========= MAIN =========
def main():
    print(f"[askbot] start {_now_utc().isoformat()}")
    if not MASTODON_TOKEN:
        print("[askbot] No MASTODON_TOKEN; exit 0")
        return

    # Scopes check (need read:statuses to scan public tags)
    scopes = _token_scopes()
    if ("read:statuses" not in scopes) and ("read" not in scopes):
        print("[warn] token missing read:statuses — public hashtag timeline may fail.")

    me = _verify_credentials()
    self_id   = str(me.get("id"))
    self_acct = me.get("acct")
    if DEBUG:
        print(f"[askbot] acting as @{self_acct} id={self_id}")

    state = _load_state()
    since_mention_id = state.get("since_mention_id")
    since_tag_id     = state.get("since_tag_id")  # public hashtag since_id
    handled_ids      = set(state.get("handled_ids", []))  # idempotency across runs

    replies = 0
    meta_text = _read_meta()

    # Followers-only safety: prefetch relationships if needed (for mentions only)
    relmap = {}
    if FOLLOWERS_ONLY and ENABLE_MENTIONS:
        try:
            m_notifs = _fetch_mentions(since_mention_id)
            relmap = _relationships([str(n["account"]["id"]) for n in m_notifs])
        except Exception:
            relmap = {}

    # -------- 1) Mentions (optional) --------
    if ENABLE_MENTIONS:
        try:
            notifs = _fetch_mentions(since_mention_id)
        except Exception as e:
            print("[askbot] mention fetch failed:", e)
            notifs = []

        for n in notifs:
            if replies >= MAX_REPLIES_PER_RUN:
                break
            notif_id = str(n["id"])
            st = n["status"]
            st_id = str(st["id"])
            if st_id in handled_ids:
                continue

            created_at = _iso_to_dt_utc(st.get("created_at"))
            if (_now_utc() - created_at).total_seconds() > MENTION_MAX_AGE_MIN * 60:
                since_mention_id = notif_id  # advance but skip
                continue

            acct_obj = n.get("account", {})
            if not _allowed_account(acct_obj, self_id, relmap):
                since_mention_id = notif_id
                continue

            body_txt = _strip_html(st.get("content", ""))
            if not _text_has_trigger(body_txt):
                # Mentions require the tag too, to avoid generic chatter
                since_mention_id = notif_id
                continue

            prompt = _build_prompt_from_status(st, include_context=False)
            answer = _call_groq(prompt, meta_text)
            vis = st.get("visibility") or REPLIES_VIS
            idem = f"askbot:mention:{st_id}"
            reply = _reply_text(acct_obj.get("acct", ""), answer)
            data = {"status": reply, "in_reply_to_id": st_id, "visibility": vis}

            try:
                _mastodon_post("/api/v1/statuses", data, idem_key=idem)
                replies += 1
                handled_ids.add(st_id)
                if DEBUG: print(f"[askbot] replied mention st={st_id}")
            except Exception as e:
                print(f"[askbot] post failed (mention) st={st_id}: {e}")

            since_mention_id = notif_id

    # -------- 2) Public hashtag timeline (#ask) --------
    try:
        tag_items = _fetch_tag_timeline(TRIGGER_TAG, since_tag_id, PUBLIC_TAG_LIMIT)
    except Exception as e:
        print("[askbot] tag fetch failed:", e)
        tag_items = []

    # Build minimal relationship map if followers_only is set for tag mode too
    if FOLLOWERS_ONLY and tag_items:
        relmap = _relationships([str(st["account"]["id"]) for st in tag_items])

    newest_tag_id = since_tag_id
    for st in tag_items:
        if replies >= MAX_REPLIES_PER_RUN:
            break
        st_id = str(st["id"])
        newest_tag_id = st_id  # tag timeline is sorted; advance as we go
        if st_id in handled_ids:
            continue
        if st.get("reblog"):
            continue  # skip boosts

        acct_obj = st.get("account", {})
        if not _allowed_account(acct_obj, self_id, relmap):
            continue

        text = _strip_html(st.get("content", ""))
        # The timeline already filtered by tag, but keep a guard:
        if not _text_has_trigger(text):
            continue

        prompt = _build_prompt_from_status(st, include_context=False)
        answer = _call_groq(prompt, meta_text)
        vis = st.get("visibility") or REPLIES_VIS
        idem = f"askbot:tag:{st_id}"
        reply = _reply_text(acct_obj.get("acct", ""), answer)
        data = {"status": reply, "in_reply_to_id": st_id, "visibility": vis}

        try:
            _mastodon_post("/api/v1/statuses", data, idem_key=idem)
            replies += 1
            handled_ids.add(st_id)
            if DEBUG: print(f"[askbot] replied tag st={st_id}")
        except Exception as e:
            print(f"[askbot] post failed (tag) st={st_id}: {e}")

    # -------- Save state --------
    state["since_mention_id"] = since_mention_id
    state["since_tag_id"]     = newest_tag_id or since_tag_id
    # Keep handled ids reasonably small
    kept = list(handled_ids)[-5000:]
    state["handled_ids"] = kept
    _save_state(state)

    print(f"[askbot] done replies={replies}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("[askbot] fatal:", e)
