import os, json, time, re, html, datetime
from typing import Dict, Any, List
from urllib.parse import urljoin

import requests

# ========= ENV =========
MASTODON_INSTANCE = os.getenv("MASTODON_INSTANCE", "https://defcon.social").rstrip("/")
MASTODON_TOKEN    = os.getenv("MASTODON_TOKEN", "")

ASK_STATE_PATH    = os.getenv("ASK_STATE_PATH", "data/ask_state.json")
META_PATH         = os.getenv("META_PATH", "meta/primer.txt")
ASK_KEYWORD       = os.getenv("ASK_KEYWORD", "#ask")
REPLIES_VIS       = os.getenv("REPLIES_VISIBILITY", "unlisted")

FOLLOWERS_ONLY    = os.getenv("FOLLOWERS_ONLY", "0") == "1"
ALLOW_ALL_DOMAINS = [x.strip().lower() for x in os.getenv("ALLOW_ALL_DOMAINS", "").split(",") if x.strip()]

MAX_REPLIES_PER_RUN   = int(os.getenv("MAX_REPLIES_PER_RUN", "6"))
MENTION_MAX_AGE_MIN   = int(os.getenv("MENTION_MAX_AGE_MIN", "240"))
ASK_TAG_ENABLE        = os.getenv("ASK_TAG_ENABLE", "1") == "1"
ASK_TAG_MAX_AGE_MIN   = int(os.getenv("ASK_TAG_MAX_AGE_MIN", "240"))
ASK_TAG_LIMIT         = int(os.getenv("ASK_TAG_LIMIT", "60"))
ASK_TAG_BOOTSTRAP     = os.getenv("ASK_TAG_BOOTSTRAP_SKIP_OLD", "1") == "1"
REPLY_TO_ROOT         = os.getenv("REPLY_TO_ROOT", "1") == "1"
REPLY_SLEEP_SEC       = float(os.getenv("REPLY_SLEEP_SEC", "0.8"))
SCOPE_WARN            = os.getenv("ASK_SCOPE_WARN", "1") == "1"
DEBUG                 = os.getenv("ASK_DEBUG", "1") == "1"

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
if MASTODON_TOKEN:
    session.headers.update({"Authorization": f"Bearer {MASTODON_TOKEN}"})


# ========= UTIL =========
def _now_utc() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)

def _iso_to_dt_utc(s: str) -> datetime.datetime:
    s2 = s.strip()
    if s2.endswith("Z"): s2 = s2[:-1] + "+00:00"
    dt = datetime.datetime.fromisoformat(s2)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt.astimezone(datetime.timezone.utc)

def _strip_html(s: str) -> str:
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.I)
    s = re.sub(r"</p\s*>", "\n\n", s, flags=re.I)
    s = re.sub(r"<[^>]+>", "", s)
    return html.unescape(s).strip()

def _looks_valid_trigger(text: str) -> bool:
    if not ASK_KEYWORD:
        return True
    return re.search(re.escape(ASK_KEYWORD), text, re.I) is not None

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

def _read_meta() -> str:
    try:
        with open(META_PATH, "r", encoding="utf-8") as f:
            return f.read().strip()
    except Exception:
        return ""

def _mastodon_get(path: str, params: Dict[str, Any] = None):
    url = urljoin(MASTODON_INSTANCE + "/", path.lstrip("/"))
    r = session.get(url, params=params or {}, timeout=30)
    if DEBUG:
        try:
            print(f"[GET] {r.url} -> {r.status_code}")
        except Exception:
            print(f"[GET] {path} -> {r.status_code}")
    r.raise_for_status()
    return r.json()

def _mastodon_post(path: str, data: Dict[str, Any], idem_key: str = ""):
    url = urljoin(MASTODON_INSTANCE + "/", path.lstrip("/"))
    headers = {}
    if idem_key:
        headers["Idempotency-Key"] = idem_key
    r = session.post(url, data=data, headers=headers, timeout=45)
    if DEBUG:
        print(f"[POST] {url} -> {r.status_code}")
        if r.status_code >= 400:
            print(r.text[:500])
    r.raise_for_status()
    return r.json()

def _verify_credentials() -> Dict[str, Any]:
    return _mastodon_get("/api/v1/accounts/verify_credentials")

def _relationships(ids: List[str]) -> Dict[str, Any]:
    out = {}
    if not ids: return out
    chunk = 80
    for i in range(0, len(ids), chunk):
        q = [("id[]", _id) for _id in ids[i:i+chunk]]
        url = urljoin(MASTODON_INSTANCE + "/", "/api/v1/accounts/relationships")
        r = session.get(url, params=q, timeout=30)
        r.raise_for_status()
        for row in r.json():
            out[str(row["id"])] = row
    return out

def _status_context(status_id: str) -> Dict[str, Any]:
    return _mastodon_get(f"/api/v1/statuses/{status_id}/context")

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

def _reply_text(handle_acct: str, answer: str) -> str:
    return f"@{handle_acct}\n\n{answer}".strip()

def _call_groq(prompt_user: str, meta_text: str) -> str:
    if not GROQ_API_KEY:
        txt = prompt_user.strip()
        return txt[:450] if len(txt) > 450 else txt
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    messages = [
        {"role":"system","content": GROQ_SYSTEM_PROMPT},
        {"role":"user","content": f"Метатекст (фон):\n{meta_text}\n\nЗапрос:\n{prompt_user}"}
    ]
    payload = {
        "model": GROQ_MODEL,
        "messages": messages,
        "temperature": 0.2,
        "n": 1,
        "max_completion_tokens": GROQ_MAX_COMPLETION_TOKENS,
    }
    for attempt in range(3):
        r = requests.post(url, headers=headers, json=payload, timeout=60)
        if r.status_code == 429:
            retry = int(r.headers.get("retry-after","2"))
            time.sleep(min(5, max(1, retry))); continue
        try:
            r.raise_for_status()
            j = r.json()
            txt = (j.get("choices",[{}])[0].get("message",{}) or {}).get("content","") or ""
            return txt.strip() or "Извините, не удалось сформировать ответ."
        except Exception:
            time.sleep(1)
    return "Извините, сервис перегружен. Попробуйте ещё раз."

def _build_prompt_from_thread(mention_status: Dict[str, Any], ctx: Dict[str, Any]) -> str:
    base = _strip_html(mention_status.get("content",""))
    parts = [f"Вопрос пользователя:\n{base}"]
    # include two most recent ancestors for context
    ancestors = (ctx.get("ancestors") or [])[-2:]
    if ancestors:
        parts.append("Контекст (фрагменты нити):")
        for st in ancestors:
            who = st.get("account",{}).get("acct","?")
            txt = _strip_html(st.get("content",""))
            parts.append(f"- @{who}: {txt}")
    return "\n".join(parts).strip()

def _root_with_ask(status: Dict[str, Any], ctx: Dict[str, Any], ask_kw: str) -> str:
    """Return the ID of the topmost ancestor that contains #ask (or the status itself)."""
    kw = re.escape(ask_kw)
    for anc in (ctx.get("ancestors") or []):
        if re.search(kw, _strip_html(anc.get("content","")), re.I):
            return str(anc.get("id"))
    if re.search(kw, _strip_html(status.get("content","")), re.I):
        return str(status.get("id"))
    return str(status.get("id"))

def _scopes() -> List[str]:
    try:
        j = _mastodon_get("/oauth/token/info")
        scopes = (j.get("scopes") or "").split(" ")
        return [s.strip() for s in scopes if s.strip()]
    except Exception:
        return []

# ========= PROCESSORS =========
def handle_mentions(state: Dict[str, Any], me: Dict[str, Any], meta_text: str) -> int:
    self_id   = str(me.get("id"))
    self_acct = me.get("acct")
    since_id  = state.get("since_id_mentions", None)

    params = {"types[]": "mention"}
    if since_id: params["since_id"] = since_id
    notifs = _mastodon_get("/api/v1/notifications", params=params)
    notifs = [n for n in notifs if n.get("type")=="mention" and n.get("status")]
    # oldest → newest
    notifs.sort(key=lambda n: int(n["id"]))

    if FOLLOWERS_ONLY:
        relmap = _relationships([str(n["account"]["id"]) for n in notifs])
    else:
        relmap = {}

    replies = 0
    newest_id = since_id
    replied_map = state.setdefault("replied", {})  # status_id -> ts

    for n in notifs:
        notif_id = str(n["id"])
        st = n["status"]
        st_id = str(st.get("id"))
        created_at = _iso_to_dt_utc(st.get("created_at"))
        if (_now_utc() - created_at).total_seconds() > MENTION_MAX_AGE_MIN * 60:
            if DEBUG: print(f"[mentions] skip old id={notif_id}")
            newest_id = max(notif_id, newest_id or notif_id, key=int); continue

        # already handled?
        if st_id in replied_map:
            if DEBUG: print(f"[mentions] already replied st={st_id}")
            newest_id = max(notif_id, newest_id or notif_id, key=int); continue

        body_txt = _strip_html(st.get("content",""))
        if not _looks_valid_trigger(body_txt):
            if DEBUG: print(f"[mentions] no trigger in id={notif_id}")
            newest_id = max(notif_id, newest_id or notif_id, key=int); continue

        if not _allowed_account(n, self_id, relmap):
            if DEBUG: print(f"[mentions] not allowed account id={notif_id}")
            newest_id = max(notif_id, newest_id or notif_id, key=int); continue

        # thread context
        try:
            ctx = _status_context(st_id)
        except Exception:
            ctx = {"ancestors":[]}

        prompt_user = _build_prompt_from_thread(st, ctx)
        answer = _call_groq(prompt_user, meta_text)

        target_id = st_id
        if REPLY_TO_ROOT:
            target_id = _root_with_ask(st, ctx, ASK_KEYWORD)

        data = {
            "status": _reply_text(n["account"]["acct"], answer),
            "in_reply_to_id": target_id,
            "visibility": st.get("visibility") or REPLIES_VIS,
        }
        idem = f"askbot:mention:{notif_id}"
        try:
            _mastodon_post("/api/v1/statuses", data, idem_key=idem)
            replies += 1
            replied_map[st_id] = _now_utc().isoformat()
            print(f"[mentions] replied notif={notif_id} st={st_id} → thread under {target_id}")
        except Exception as e:
            print(f"[mentions] post failed notif={notif_id}: {e}")

        newest_id = max(notif_id, newest_id or notif_id, key=int)
        if replies >= MAX_REPLIES_PER_RUN:
            if DEBUG: print(f"[mentions] hit MAX_REPLIES_PER_RUN={MAX_REPLIES_PER_RUN}")
            break

        time.sleep(REPLY_SLEEP_SEC)

    if newest_id:
        state["since_id_mentions"] = newest_id
    return replies

def handle_tag_ask(state: Dict[str, Any], meta_text: str) -> int:
    if not ASK_TAG_ENABLE:
        if DEBUG: print("[tag] ASK_TAG_ENABLE=0 — skipped")
        return 0

    # scope hints
    if SCOPE_WARN:
        scopes = _scopes()
        if scopes and ("read:statuses" not in scopes):
            print("[warn] token missing read:statuses — public hashtag timeline might be limited.")

    since_id = state.get("since_id_tag", None)

    # bootstrap: on first run, optionally skip the backlog by recording top id and exiting
    if since_id is None and ASK_TAG_BOOTSTRAP:
        try:
            j = _mastodon_get(f"/api/v1/timelines/tag/{ASK_KEYWORD.lstrip('#')}", params={"limit": 1})
            if j:
                top_id = str(j[0].get("id"))
                state["since_id_tag"] = top_id
                print(f"[tag] bootstrap set since_id_tag={top_id} (skipping backlog)")
                return 0
        except Exception as e:
            print(f"[tag] bootstrap failed: {e}")
        # if bootstrap fails, we just proceed normally

    params = {"limit": max(1, min(80, ASK_TAG_LIMIT))}
    if since_id:
        params["since_id"] = since_id

    try:
        timeline = _mastodon_get(f"/api/v1/timelines/tag/{ASK_KEYWORD.lstrip('#')}", params=params)
    except Exception as e:
        print(f"[tag] fetch failed: {e}")
        return 0

    if not timeline:
        if DEBUG: print("[tag] no new #ask posts")
        return 0

    # oldest → newest
    timeline.sort(key=lambda st: int(st["id"]))
    replies = 0
    newest_seen = since_id
    replied_map = state.setdefault("replied", {})  # status_id -> ts

    for st in timeline:
        # If this is a boost/reblog, reply to the original
        if st.get("reblog"):
            st = st["reblog"]

        st_id = str(st.get("id"))
        if st_id in replied_map:
            if DEBUG: print(f"[tag] already replied st={st_id}")
            newest_seen = max(st_id, newest_seen or st_id, key=int); continue

        created_at = _iso_to_dt_utc(st.get("created_at"))
        if (_now_utc() - created_at).total_seconds() > ASK_TAG_MAX_AGE_MIN * 60:
            if DEBUG: print(f"[tag] skip old st={st_id}")
            newest_seen = max(st_id, newest_seen or st_id, key=int); continue

        acct = st.get("account", {}).get("acct", "")
        body_txt = _strip_html(st.get("content",""))

        # extra safety (though tag feed already filtered by #ask)
        if not _looks_valid_trigger(body_txt):
            if DEBUG: print(f"[tag] missing trigger st={st_id}")
            newest_seen = max(st_id, newest_seen or st_id, key=int); continue

        # Build prompt using thread context if available
        try:
            ctx = _status_context(st_id)
        except Exception:
            ctx = {"ancestors":[]}

        prompt_user = _build_prompt_from_thread(st, ctx)
        answer = _call_groq(prompt_user, meta_text)

        target_id = st_id
        if REPLY_TO_ROOT:
            target_id = _root_with_ask(st, ctx, ASK_KEYWORD)

        data = {
            "status": _reply_text(acct, answer),
            "in_reply_to_id": target_id,
            "visibility": REPLIES_VIS,
        }
        idem = f"askbot:tag:{st_id}"
        try:
            _mastodon_post("/api/v1/statuses", data, idem_key=idem)
            replies += 1
            replied_map[st_id] = _now_utc().isoformat()
            print(f"[tag] replied st={st_id} → thread under {target_id}")
        except Exception as e:
            print(f"[tag] post failed st={st_id}: {e}")

        newest_seen = max(st_id, newest_seen or st_id, key=int)
        if replies >= MAX_REPLIES_PER_RUN:
            if DEBUG: print(f"[tag] hit MAX_REPLIES_PER_RUN={MAX_REPLIES_PER_RUN}")
            break

        time.sleep(REPLY_SLEEP_SEC)

    if newest_seen:
        state["since_id_tag"] = newest_seen
    return replies


# ========= MAIN =========
def main():
    print(f"[askbot] start {_now_utc().isoformat()}")
    if not MASTODON_TOKEN:
        print("[askbot] No MASTODON_TOKEN; exit 0")
        return

    state = _load_state()
    meta_text = _read_meta()

    # Optional scope warning
    if SCOPE_WARN:
        try:
            scopes = _scopes()
            if scopes:
                print(f"[scopes] {scopes}")
        except Exception:
            pass

    me = _verify_credentials()
    print(f"[askbot] acting as @{me.get('acct')} id={me.get('id')}")

    total = 0
    total += handle_mentions(state, me, meta_text)
    total += handle_tag_ask(state, meta_text)

    _save_state(state)
    print(f"[askbot] done replies={total}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("[askbot] fatal:", e)
