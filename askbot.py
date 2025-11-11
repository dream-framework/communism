#!/usr/bin/env python3
import os, json, time, re, html, datetime
from typing import Dict, Any, List
from urllib.parse import urljoin

import requests

# ========= ENV =========
MASTODON_INSTANCE = os.getenv("MASTODON_INSTANCE", "https://mastodon.social").rstrip("/")
MASTODON_TOKEN    = os.getenv("MASTODON_TOKEN", "")
ASK_STATE_PATH    = os.getenv("ASK_STATE_PATH", "data/ask_state.json")
META_PATH         = os.getenv("META_PATH", "meta/primer.txt")     # optional meta/primer
USE_META          = os.getenv("USE_META", "auto")                 # "auto" | "1" | "0"
ASK_KEYWORD       = os.getenv("ASK_KEYWORD", "#ask")
ASK_START_ONLY    = os.getenv("ASK_START_ONLY", "1") == "1"       # require keyword at start
REPLIES_VIS       = os.getenv("REPLIES_VISIBILITY", "unlisted")   # or "public"
FOLLOWERS_ONLY    = os.getenv("FOLLOWERS_ONLY", "0") == "1"
ALLOW_ALL_DOMAINS = [x.strip().lower() for x in os.getenv("ALLOW_ALL_DOMAINS", "").split(",") if x.strip()]
MAX_REPLIES_PER_RUN = int(os.getenv("MAX_REPLIES_PER_RUN", "4"))
MENTION_MAX_AGE_MIN = int(os.getenv("MENTION_MAX_AGE_MIN", "180"))
ASK_CONTEXT_MODE  = os.getenv("ASK_CONTEXT_MODE", "none")         # "none" (fast) or "full"
MAX_ANSWER_CHARS  = int(os.getenv("MAX_ANSWER_CHARS", "450"))
HTTP_TIMEOUT_S    = float(os.getenv("HTTP_TIMEOUT_S", "15"))
INCLUDE_THREAD_MENTIONS = os.getenv("INCLUDE_THREAD_MENTIONS", "1") == "1"

DEBUG = os.getenv("ASKBOT_DEBUG", "1") == "1"

# Groq
GROQ_API_KEY      = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL        = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")
GROQ_MAX_COMPLETION_TOKENS = int(os.getenv("GROQ_MAX_OUTPUT_TOKENS", "320"))
GROQ_SYSTEM_PROMPT = os.getenv("GROQ_SYSTEM_PROMPT",
    "Ты — лаконичный фактологичный помощник-аналитик. Отвечай по существу, 3–6 предложений. "
    "Используй предоставленный метатекст как фоновые рамки, но не цитируй его дословно. "
    "Избегай домыслов и ссылок; если чего-то не хватает, ясно обозначь ограничения."
)

session = requests.Session()
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

def _url(path: str) -> str:
    return urljoin(MASTODON_INSTANCE + "/", path.lstrip("/"))

def _mastodon_get(path: str, params: Dict[str, Any] = None):
    url = _url(path)
    r = session.get(url, params=params or {}, timeout=HTTP_TIMEOUT_S)
    if DEBUG:
        p = "&".join([f"{k}={v}" for k,v in (params or {}).items()])
        print(f"[GET] {url}{('?' + p) if p else ''} -> {r.status_code}")
    r.raise_for_status()
    return r.json()

def _mastodon_post(path: str, data: Dict[str, Any], idem_key: str = ""):
    url = _url(path)
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

def _verify_credentials() -> Dict[str, Any]:
    return _mastodon_get("/api/v1/accounts/verify_credentials")

def _token_scopes() -> List[str]:
    # Not all servers expose this; handle errors gracefully.
    try:
        j = _mastodon_get("/oauth/token/info")
        scopes = j.get("scopes") or j.get("scope") or ""
        if isinstance(scopes, str):
            scopes = scopes.split()
        return [s.strip() for s in scopes if s.strip()]
    except Exception as e:
        if DEBUG: print("[scopes] token info not available:", e)
        return []

def _relationships(ids: List[str]) -> Dict[str, Any]:
    out = {}
    chunk = 80
    for i in range(0, len(ids), chunk):
        q = [("id[]", _id) for _id in ids[i:i+chunk]]
        url = _url("/api/v1/accounts/relationships")
        r = session.get(url, params=q, timeout=HTTP_TIMEOUT_S)
        r.raise_for_status()
        for row in r.json():
            out[str(row["id"])] = row
    return out

def _status_context(status_id: str) -> Dict[str, Any]:
    return _mastodon_get(f"/api/v1/statuses/{status_id}/context")

def _read_meta() -> str:
    try:
        with open(META_PATH, "r", encoding="utf-8") as f:
            return f.read().strip()
    except Exception:
        return ""

def _call_groq(prompt_user: str, meta_text: str) -> str:
    if not GROQ_API_KEY:
        text = prompt_user.strip()
        return text[:MAX_ANSWER_CHARS] if len(text) > MAX_ANSWER_CHARS else text
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    use_meta = False
    if USE_META == "1":
        use_meta = True
    elif USE_META == "auto":
        use_meta = bool(meta_text.strip())
    # Keep prompt tiny for speed:
    if use_meta:
        user_content = f"Метатекст (фон):\n{meta_text}\n\nЗапрос:\n{prompt_user}"
    else:
        user_content = f"Запрос:\n{prompt_user}"

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
    for attempt in range(2):  # keep it fast
        r = requests.post(url, headers=headers, json=payload, timeout=HTTP_TIMEOUT_S)
        if r.status_code == 429:
            retry = int(r.headers.get("retry-after","2"))
            time.sleep(min(3, max(1, retry))); continue
        try:
            r.raise_for_status()
            j = r.json()
            txt = (j.get("choices",[{}])[0].get("message",{}) or {}).get("content","") or ""
            ans = txt.strip() or "Извините, не удалось сформировать ответ."
            return ans[:MAX_ANSWER_CHARS]
        except Exception:
            time.sleep(0.5)
    return "Извините, сервис перегружен. Попробуйте ещё раз."

def _build_prompt_from_thread(mention_status: Dict[str, Any], ctx: Dict[str, Any]) -> str:
    base = _strip_html(mention_status.get("content",""))
    parts = [f"Вопрос пользователя:\n{base}"]
    if ASK_CONTEXT_MODE == "full":
        ancestors = (ctx.get("ancestors") or [])[-2:]  # last two only (fast)
        if ancestors:
            parts.append("Контекст (фрагменты нити):")
            for st in ancestors:
                who = st.get("account",{}).get("acct","?")
                txt = _strip_html(st.get("content",""))
                parts.append(f"- @{who}: {txt}")
    return "\n".join(parts).strip()

def _looks_valid_trigger(text: str) -> bool:
    if not ASK_KEYWORD:
        return True
    if ASK_START_ONLY:
        return re.search(rf'^\s*{re.escape(ASK_KEYWORD)}\b', text, re.I) is not None
    return re.search(re.escape(ASK_KEYWORD), text, re.I) is not None

def _allowed_account(n: Dict[str, Any], self_id: str, relmap: Dict[str, Any]) -> bool:
    acct = n.get("account",{})
    if str(acct.get("id")) == str(self_id):
        return False
    domain = (acct.get("acct","").split("@",1)[1].lower() if "@" in acct.get("acct","") else "")
    if ALLOW_ALL_DOMAINS and domain and domain not in ALLOW_ALL_DOMAINS:
        return False
    if FOLLOWERS_ONLY:
        rel = relmap.get(str(acct.get("id")), {})
        if not rel.get("followed_by", False):
            return False
    return True

def _reply_text(status: Dict[str,Any], handle_acct: str, answer: str) -> str:
    # Mention the asker and (optionally) others in the same status to keep the thread audience.
    mentions = [handle_acct]
    if INCLUDE_THREAD_MENTIONS:
        for m in status.get("mentions", []):
            acct = m.get("acct","").strip()
            if acct and acct.lower() != handle_acct.lower():
                mentions.append(acct)
    mentions = list(dict.fromkeys(mentions))  # dedupe, preserve order
    at_line = " ".join("@" + a for a in mentions)
    return f"{at_line}\n\n{answer}"


# ========= NOTIFICATIONS (v1 with v2 fallback) =========
def _get_mentions(since_id: str | None) -> List[Dict[str,Any]]:
    params = {"types[]": "mention"}
    if since_id:
        params["since_id"] = since_id
    # Try v1 first
    url_v1 = "/api/v1/notifications"
    try:
        notifs = _mastodon_get(url_v1, params=params)
        return [n for n in notifs if n.get("type")=="mention" and n.get("status")]
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 403:
            # Try v2
            if DEBUG: print("[notif] v1 403; trying v2")
            try:
                notifs = _mastodon_get("/api/v2/notifications", params=params)
                return [n for n in notifs if n.get("type")=="mention" and n.get("status")]
            except Exception as e2:
                raise e  # bubble original 403
        raise

# ========= MAIN =========
def main():
    print(f"[askbot] start {_now_utc().isoformat()}")
    if not MASTODON_TOKEN:
        print("[askbot] No MASTODON_TOKEN; exit 0")
        return

    # Scope sanity check (best-effort)
    scopes = _token_scopes()
    if scopes:
        if DEBUG: print("[scopes]", scopes)
        need = {"read:notifications", "write:statuses"}
        missing = [s for s in need if s not in set(scopes)]
        if missing:
            print(f"[askbot] ERROR: token missing scopes {missing}. "
                  f"Recreate token with read:notifications, write:statuses (and read:statuses).")
            return

    state = _load_state()
    since_id = state.get("since_id", None)

    me = _verify_credentials()
    self_id = str(me.get("id"))
    self_acct = me.get("acct")
    if DEBUG:
        print(f"[askbot] acting as @{self_acct} id={self_id} since_id={since_id}")

    try:
        notifs = _get_mentions(since_id)
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 403:
            print("[askbot] fatal: 403 on notifications — token likely lacks read:notifications "
                  f"or server policy blocks it on {MASTODON_INSTANCE}. Re-issue token on this instance.")
            return
        raise

    # sort oldest->newest
    notifs.sort(key=lambda n: int(n["id"]))
    if not notifs:
        print("[askbot] no new mentions")
        return

    relmap = _relationships([str(n["account"]["id"]) for n in notifs]) if FOLLOWERS_ONLY else {}

    meta_text = _read_meta()
    replies = 0
    newest_id = since_id

    for n in notifs:
        notif_id = str(n["id"])
        st = n["status"]
        created_at = _iso_to_dt_utc(st.get("created_at"))
        if (_now_utc() - created_at).total_seconds() > MENTION_MAX_AGE_MIN*60:
            if DEBUG: print(f"[askbot] skip old mention id={notif_id}")
            newest_id = max(notif_id, newest_id or notif_id, key=int)
            continue

        body_txt = _strip_html(st.get("content",""))
        if not _looks_valid_trigger(body_txt):
            if DEBUG: print(f"[askbot] no trigger keyword in id={notif_id}")
            newest_id = max(notif_id, newest_id or notif_id, key=int)
            continue

        if not _allowed_account(n, self_id, relmap):
            if DEBUG: print(f"[askbot] not allowed account id={notif_id}")
            newest_id = max(notif_id, newest_id or notif_id, key=int)
            continue

        # Minimal context for speed; optionally include last 2 ancestors
        ctx = {"ancestors": []}
        if ASK_CONTEXT_MODE == "full":
            ctx = _status_context(str(st["id"]))
        prompt_user = _build_prompt_from_thread(st, ctx)

        answer = _call_groq(prompt_user, meta_text)

        vis = st.get("visibility") or REPLIES_VIS
        idem = f"askbot:{notif_id}"
        reply = _reply_text(st, n["account"]["acct"], answer)
        data = {
            "status": reply,
            "in_reply_to_id": str(st["id"]),  # stays in same thread
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
