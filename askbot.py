#!/usr/bin/env python3
import os, json, time, re, html, datetime
from typing import Dict, Any, List, Tuple
from urllib.parse import urljoin

import requests

# ========= ENV =========
MASTODON_INSTANCE = os.getenv("MASTODON_INSTANCE", "https://mastodon.social").rstrip("/")
MASTODON_TOKEN    = os.getenv("MASTODON_TOKEN", "")
ASK_STATE_PATH    = os.getenv("ASK_STATE_PATH", "data/ask_state.json")

# Optional meta text (used only if present & allowed)
META_PATH         = os.getenv("META_PATH", "meta/primer.txt")
USE_META          = os.getenv("USE_META", "auto").lower()   # "auto" | "1" | "0"
MAX_META_CHARS    = int(os.getenv("MAX_META_CHARS", "1200"))

# Trigger & policy
ASK_KEYWORD       = os.getenv("ASK_KEYWORD", "#ask")                # must be at start
ASK_START_ONLY    = os.getenv("ASK_START_ONLY", "1") == "1"         # require prefix
REPLIES_VIS       = os.getenv("REPLIES_VISIBILITY", "unlisted")     # or "public"
FOLLOWERS_ONLY    = os.getenv("FOLLOWERS_ONLY", "0") == "1"         # restrict to followers
ALLOW_ALL_DOMAINS = [x.strip().lower() for x in os.getenv("ALLOW_ALL_DOMAINS", "").split(",") if x.strip()]
MAX_REPLIES_PER_RUN = int(os.getenv("MAX_REPLIES_PER_RUN", "4"))
MENTION_MAX_AGE_MIN = int(os.getenv("MENTION_MAX_AGE_MIN", "180"))  # ignore very old mentions

# Speed / output shaping
ASK_CONTEXT_MODE  = os.getenv("ASK_CONTEXT_MODE", "none").lower()   # "none" (fast) | "full"
MAX_ANSWER_CHARS  = int(os.getenv("MAX_ANSWER_CHARS", "450"))       # keep replies concise for most instances
HTTP_TIMEOUT_S    = float(os.getenv("HTTP_TIMEOUT_S", "15"))

DEBUG = os.getenv("ASKBOT_DEBUG", "1") == "1"

# Groq (fast params)
GROQ_API_KEY      = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL        = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")
GROQ_MAX_COMPLETION_TOKENS = int(os.getenv("GROQ_MAX_OUTPUT_TOKENS", "220"))
GROQ_SYSTEM_PROMPT = os.getenv("GROQ_SYSTEM_PROMPT",
    "Ты — лаконичный фактологичный помощник-аналитик. Отвечай быстро и по существу, 2–5 коротких предложений. "
    "Если метатекст передан — используй его как фон (но не цитируй дословно). "
    "Без ссылок, без эмодзи. Если данных не хватает — кратко отметь это."
)

session = requests.Session()
if MASTODON_TOKEN:
    session.headers.update({"Authorization": f"Bearer {MASTODON_TOKEN}"})


# ========= UTIL =========
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

def _strip_html(s: str) -> str:
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.I)
    s = re.sub(r"</p\s*>", "\n\n", s, flags=re.I)
    s = re.sub(r"<[^>]+>", "", s)
    return html.unescape(s).strip()

def _iso_to_dt_utc(s: str) -> datetime.datetime:
    s2 = s.strip()
    if s2.endswith("Z"): s2 = s2[:-1] + "+00:00"
    dt = datetime.datetime.fromisoformat(s2)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt.astimezone(datetime.timezone.utc)

def _now_utc() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)

def _mastodon_get(path: str, params: Dict[str, Any] = None):
    url = urljoin(MASTODON_INSTANCE + "/", path.lstrip("/"))
    r = session.get(url, params=params or {}, timeout=HTTP_TIMEOUT_S)
    if DEBUG: print(f"[GET] {r.url} -> {r.status_code}")
    r.raise_for_status()
    return r.json()

def _mastodon_post(path: str, data: Dict[str, Any], idem_key: str = ""):
    url = urljoin(MASTODON_INSTANCE + "/", path.lstrip("/"))
    headers = {}
    if idem_key:
        headers["Idempotency-Key"] = idem_key
    r = session.post(url, data=data, headers=headers, timeout=HTTP_TIMEOUT_S)
    if DEBUG:
        print(f"[POST] {url} -> {r.status_code}")
        if r.status_code >= 400:
            print(r.text[:500])
    r.raise_for_status()
    return r.json()

def _verify_credentials_fast(state: Dict[str, Any]) -> Dict[str, Any]:
    # Cache self id/acct for speed; refresh if missing
    if state.get("self_id") and state.get("self_acct"):
        return {"id": state["self_id"], "acct": state["self_acct"]}
    me = _mastodon_get("/api/v1/accounts/verify_credentials")
    state["self_id"] = str(me.get("id"))
    state["self_acct"] = me.get("acct")
    _save_state(state)
    return {"id": state["self_id"], "acct": state["self_acct"]}

def _relationships(ids: List[str]) -> Dict[str, Any]:
    out = {}
    if not ids: return out
    url = urljoin(MASTODON_INSTANCE + "/", "/api/v1/accounts/relationships")
    chunk = 80
    for i in range(0, len(ids), chunk):
        q = [("id[]", _id) for _id in ids[i:i+chunk]]
        r = session.get(url, params=q, timeout=HTTP_TIMEOUT_S)
        r.raise_for_status()
        for row in r.json():
            out[str(row["id"])] = row
    return out

def _status_context(status_id: str) -> Dict[str, Any]:
    return _mastodon_get(f"/api/v1/statuses/{status_id}/context")

def _read_meta() -> str:
    if USE_META == "0":
        return ""
    try:
        with open(META_PATH, "r", encoding="utf-8") as f:
            txt = f.read().strip()
            return txt[:MAX_META_CHARS] if USE_META in ("1","auto") else ""
    except Exception:
        return ""

def _call_groq(prompt_user: str, meta_text: str) -> str:
    # Fast path: if no key — return trimmed echo (never silently fail)
    if not GROQ_API_KEY:
        return (prompt_user.strip()[:MAX_ANSWER_CHARS] or "Нет текста запроса.")

    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}

    # Use the smallest message set possible for speed
    if meta_text:
        user_content = f"Метатекст (фон):\n{meta_text}\n\nЗапрос:\n{prompt_user}"
    else:
        user_content = prompt_user

    payload = {
        "model": GROQ_MODEL,
        "messages": [
            {"role":"system","content": GROQ_SYSTEM_PROMPT},
            {"role":"user","content": user_content}
        ],
        "temperature": 0.2,
        "n": 1,
        "max_completion_tokens": GROQ_MAX_COMPLETION_TOKENS,
    }

    for attempt in range(2):  # keep it snappy
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=min(HTTP_TIMEOUT_S, 12))
            if r.status_code == 429:
                retry = int(r.headers.get("retry-after", "2"))
                time.sleep(max(1, min(3, retry))); continue
            r.raise_for_status()
            j = r.json()
            txt = (j.get("choices",[{}])[0].get("message",{}) or {}).get("content","") or ""
            txt = txt.strip()
            if len(txt) > MAX_ANSWER_CHARS:
                txt = txt[:MAX_ANSWER_CHARS - 1] + "…"
            return txt or "Извините, не удалось сформировать ответ."
        except Exception as e:
            if DEBUG: print("[groq] error:", e)
            time.sleep(0.8)
    return "Извините, сервис перегружен. Попробуйте ещё раз."

def _trim_self_mentions(text: str, self_acct: str) -> str:
    # Remove leading self-mentions (@you or @you@instance)
    pat = r"^\s*@"+re.escape(self_acct)+r"(?:\s|:|,)+"
    return re.sub(pat, "", text, flags=re.I).strip()

def _extract_ask_payload(text: str, keyword: str, start_only: bool) -> Tuple[bool, str]:
    """
    Returns (is_triggered, payload).
    Trigger only when the STATUS CONTENT starts with '#ask' (or env keyword),
    allowing optional punctuation right after (#ask: …).
    """
    t = text.strip()
    kw = keyword.strip()
    if not kw:
        return True, t

    # ^\s*#ask(\b|[:\-—])\s*(.*)
    pat = r"^\s*" + re.escape(kw) + r"(?:\b|[:\-—])\s*(.*)$"
    m = re.search(pat, t, flags=re.I | re.S)
    if m:
        return True, (m.group(1) or "").strip()

    if start_only:
        return False, ""
    # fallback (not used by default): keyword anywhere
    anywhere = re.search(re.escape(kw), t, flags=re.I)
    return (anywhere is not None, t) if anywhere else (False, "")

def _allowed_account(n: Dict[str, Any], self_id: str, relmap: Dict[str, Any]) -> bool:
    acct = n.get("account",{})
    if str(acct.get("id")) == str(self_id):
        return False  # ignore ourselves
    domain = (acct.get("acct","").split("@",1)[1].lower() if "@" in acct.get("acct","") else "")
    if ALLOW_ALL_DOMAINS and domain and domain not in ALLOW_ALL_DOMAINS:
        return False
    if FOLLOWERS_ONLY:
        rel = relmap.get(str(acct.get("id")), {})
        if not rel.get("followed_by", False):
            return False
    return True

def _reply_text(handle_acct: str, answer: str) -> str:
    at = "@" + handle_acct
    return f"{at}\n\n{answer}"


# ========= MAIN =========
def main():
    print(f"[askbot] start {_now_utc().isoformat()}")
    if not MASTODON_TOKEN:
        print("[askbot] No MASTODON_TOKEN; exit 0")
        return

    state = _load_state()
    since_id = state.get("since_id", None)

    me_fast = _verify_credentials_fast(state)
    self_id = str(me_fast.get("id"))
    self_acct = me_fast.get("acct")
    if DEBUG:
        print(f"[askbot] acting as @{self_acct} id={self_id} since_id={since_id}")

    params = {"types[]": "mention"}
    if since_id:
        params["since_id"] = since_id
    notifs = _mastodon_get("/api/v1/notifications", params=params)

    # filter & sort
    notifs = [n for n in notifs if n.get("type")=="mention" and n.get("status")]
    notifs.sort(key=lambda n: int(n["id"]))

    if not notifs:
        print("[askbot] no new mentions")
        return

    # relationships (followers_only support)
    if FOLLOWERS_ONLY:
        relmap = _relationships([str(n["account"]["id"]) for n in notifs])
    else:
        relmap = {}

    # Only read meta if possibly needed (speed)
    meta_text_maybe = None  # lazy-load

    replies = 0
    newest_id = since_id

    for n in notifs:
        notif_id = str(n["id"])
        st = n["status"]
        created_at = _iso_to_dt_utc(st.get("created_at"))
        if (_now_utc() - created_at).total_seconds() > MENTION_MAX_AGE_MIN*60:
            if DEBUG: print(f"[askbot] skip old mention id={notif_id}")
            newest_id = max(notif_id, newest_id or notif_id, key=int); continue

        if not _allowed_account(n, self_id, relmap):
            if DEBUG: print(f"[askbot] not allowed account id={notif_id}")
            newest_id = max(notif_id, newest_id or notif_id, key=int); continue

        body_txt = _strip_html(st.get("content",""))
        body_txt = _trim_self_mentions(body_txt, self_acct)

        triggered, payload = _extract_ask_payload(body_txt, ASK_KEYWORD, ASK_START_ONLY)
        if not triggered or not payload:
            if DEBUG: print(f"[askbot] no trigger/payload in id={notif_id}")
            newest_id = max(notif_id, newest_id or notif_id, key=int); continue

        # OPTIONAL: thread context (slower). Default is "none" for speed.
        prompt_user = payload
        if ASK_CONTEXT_MODE == "full":
            try:
                ctx = _status_context(str(st["id"]))
                ancestors = (ctx.get("ancestors") or [])[-2:]
                if ancestors:
                    ctx_lines = []
                    for a in ancestors:
                        who = a.get("account",{}).get("acct","?")
                        txt = _strip_html(a.get("content",""))
                        if txt: ctx_lines.append(f"- @{who}: {txt}")
                    if ctx_lines:
                        prompt_user = f"{payload}\n\nКонтекст:\n" + "\n".join(ctx_lines)
            except Exception as e:
                if DEBUG: print("[askbot] context fetch failed:", e)

        if meta_text_maybe is None:
            meta_text_maybe = _read_meta()

        # GROQ
        answer = _call_groq(prompt_user, meta_text_maybe)

        # Reply (inherit original visibility for better UX; else fallback)
        vis = st.get("visibility") or REPLIES_VIS
        idem = f"askbot:{notif_id}"
        reply = _reply_text(n["account"]["acct"], answer)
        data = {
            "status": reply[: MAX_ANSWER_CHARS + 50],  # headroom for mention
            "in_reply_to_id": str(st["id"]),
            "visibility": vis,
        }
        try:
            _mastodon_post("/api/v1/statuses", data, idem_key=idem)
            replies += 1
            print(f"[askbot] replied to notif={notif_id}")
        except Exception as e:
            print(f"[askbot] post failed for notif={notif_id}: {e}")

        newest_id = max(notif_id, newest_id or notif_id, key=int)
        if replies >= MAX_REPLIES_PER_RUN:
            if DEBUG: print(f"[askbot] hit MAX_REPLIES_PER_RUN={MAX_REPLIES_PER_RUN}")
            break

    if newest_id:
        state["since_id"] = newest_id
        _save_state(state)
    print(f"[askbot] done replies={replies}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("[askbot] fatal:", e)