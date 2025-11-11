#!/usr/bin/env python3
import os, json, time, re, html, datetime
from typing import Dict, Any, List
from urllib.parse import urljoin

import requests

# ========= ENV =========
MASTODON_INSTANCE = os.getenv("MASTODON_INSTANCE", "https://mastodon.social").rstrip("/")
MASTODON_TOKEN    = os.getenv("MASTODON_TOKEN", "")
ASK_STATE_PATH    = os.getenv("ASK_STATE_PATH", "data/ask_state.json")

# Trigger & behavior
ASK_KEYWORD       = os.getenv("ASK_KEYWORD", "#ask")
ASK_START_ONLY    = os.getenv("ASK_START_ONLY", "0") == "1"     # match only at start
REPLIES_VIS       = os.getenv("REPLIES_VISIBILITY", "unlisted") # or "public"
MAX_REPLIES_PER_RUN = int(os.getenv("MAX_REPLIES_PER_RUN", "4"))
MENTION_MAX_AGE_MIN = int(os.getenv("MENTION_MAX_AGE_MIN", "180"))

# Public hashtag mode
ASK_TAG_ENABLE            = os.getenv("ASK_TAG_ENABLE", "1") == "1"
ASK_TAG                   = os.getenv("ASK_TAG", "ask").lstrip("#")
ASK_TAG_REQUIRE_MENTION   = os.getenv("ASK_TAG_REQUIRE_MENTION", "0") == "1"
REPLY_TO_ROOT             = os.getenv("REPLY_TO_ROOT", "1") == "1"

# Access policy
FOLLOWERS_ONLY   = os.getenv("FOLLOWERS_ONLY", "0") == "1"
ALLOW_SELF_ASK   = os.getenv("ALLOW_SELF_ASK", "1") == "1"
# Allowlist (either name is accepted)
_allow_from1 = [x.strip().lower() for x in os.getenv("ALLOW_DOMAINS", "").split(",") if x.strip()]
_allow_from2 = [x.strip().lower() for x in os.getenv("ALLOW_ALL_DOMAINS", "").split(",") if x.strip()]
ALLOW_DOMAINS = list(dict.fromkeys(_allow_from1 + _allow_from2))  # dedup, preserve order

# Meta text (optional)
META_PATH   = os.getenv("META_PATH", "meta/primer.txt")
USE_META    = (os.getenv("USE_META", "auto") or "auto").lower()   # "auto"|"1"|"0"

# Speed & formatting
ASK_CONTEXT_MODE = (os.getenv("ASK_CONTEXT_MODE", "none") or "none").lower()  # "none"|"full"
MAX_ANSWER_CHARS = int(os.getenv("MAX_ANSWER_CHARS", "450"))
HTTP_TIMEOUT_S   = float(os.getenv("HTTP_TIMEOUT_S", "15"))

# Groq
GROQ_API_KEY      = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL        = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")
GROQ_MAX_COMPLETION_TOKENS = int(os.getenv("GROQ_MAX_OUTPUT_TOKENS", "220"))
GROQ_SYSTEM_PROMPT = os.getenv("GROQ_SYSTEM_PROMPT",
    "Ты — лаконичный фактологичный помощник-аналитик. "
    "Отвечай по существу, 3–6 предложений. "
    "Используй метатекст как фон (если дан), но не цитируй его. "
    "Без ссылок и домыслов; обозначай ограничения."
)

DEBUG = os.getenv("ASKBOT_DEBUG", "1") == "1"

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
    if DEBUG:
        print(f"[GET] {r.url} -> {r.status_code}")
    r.raise_for_status()
    # /oauth/token/info sometimes returns plain text; guard
    try:
        return r.json()
    except Exception:
        return {}

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

def _verify_credentials() -> Dict[str, Any]:
    return _mastodon_get("/api/v1/accounts/verify_credentials")

def _relationships(ids: List[str]) -> Dict[str, Any]:
    if not ids: return {}
    out = {}
    chunk = 80
    for i in range(0, len(ids), chunk):
        q = [("id[]", _id) for _id in ids[i:i+chunk]]
        url = urljoin(MASTODON_INSTANCE + "/", "/api/v1/accounts/relationships")
        r = session.get(url, params=q, timeout=HTTP_TIMEOUT_S)
        r.raise_for_status()
        for row in r.json():
            out[str(row["id"])] = row
    return out

def _status_context(status_id: str) -> Dict[str, Any]:
    try:
        return _mastodon_get(f"/api/v1/statuses/{status_id}/context")
    except Exception:
        return {"ancestors": [], "descendants": []}

def _read_meta() -> str:
    try:
        with open(META_PATH, "r", encoding="utf-8") as f:
            return f.read().strip()
    except Exception:
        return ""

def _call_groq(prompt_user: str, meta_text: str) -> str:
    if not GROQ_API_KEY:
        text = prompt_user.strip()
        return text[:MAX_ANSWER_CHARS]
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    use_meta = (USE_META == "1") or (USE_META == "auto")
    meta_block = f"Метатекст (фон):\n{meta_text}\n\n" if (use_meta and meta_text) else ""
    messages = [
        {"role":"system","content": GROQ_SYSTEM_PROMPT},
        {"role":"user","content": f"{meta_block}Запрос:\n{prompt_user}"}
    ]
    payload = {
        "model": GROQ_MODEL,
        "messages": messages,
        "temperature": 0.2,
        "n": 1,
        "max_completion_tokens": GROQ_MAX_COMPLETION_TOKENS,
    }
    for _ in range(3):
        r = requests.post(url, headers=headers, json=payload, timeout=HTTP_TIMEOUT_S)
        if r.status_code == 429:
            retry = int(r.headers.get("retry-after", "2"))
            time.sleep(min(5, max(1, retry))); continue
        try:
            r.raise_for_status()
            j = r.json()
            txt = (j.get("choices",[{}])[0].get("message",{}) or {}).get("content","") or ""
            ans = txt.strip() or "Извините, не удалось сформировать ответ."
            return ans[:MAX_ANSWER_CHARS]
        except Exception:
            time.sleep(0.8)
    return "Извините, сервис перегружен. Попробуйте ещё раз."

def _text_has_trigger(text: str) -> bool:
    if not ASK_KEYWORD:
        return True
    pat = re.escape(ASK_KEYWORD)
    if ASK_START_ONLY:
        # start with (#ask …)
        return re.search(rf"^\s*{pat}\b", text, re.I) is not None
    return re.search(pat, text, re.I) is not None

def _build_prompt_from_thread(root_or_curr: Dict[str, Any], ctx: Dict[str, Any]) -> str:
    # Fast path: just the post body
    base = _strip_html(root_or_curr.get("content", ""))
    if ASK_CONTEXT_MODE == "none":
        return f"Вопрос пользователя:\n{base}".strip()

    # Context mode: include up to 2 recent ancestors
    parts = [f"Вопрос пользователя:\n{base}"]
    ancestors = (ctx.get("ancestors") or [])[-2:]
    if ancestors:
        parts.append("Контекст (фрагменты нити):")
        for st in ancestors:
            who = st.get("account",{}).get("acct","?")
            txt = _strip_html(st.get("content",""))
            parts.append(f"- @{who}: {txt}")
    return "\n".join(parts).strip()

def _reply_text(handle_acct: str, answer: str) -> str:
    at = "@" + handle_acct
    return f"{at}\n\n{answer}"

def _acct_domain(acct_str: str) -> str:
    return acct_str.split("@",1)[1].lower() if "@" in acct_str else ""

def _allowed_by_policy(acct: Dict[str, Any], self_id: str, relmap: Dict[str, Any]) -> bool:
    # self-ask
    if str(acct.get("id")) == str(self_id):
        return ALLOW_SELF_ASK
    # domain allowlist
    if ALLOW_DOMAINS:
        dom = _acct_domain(acct.get("acct",""))
        # If local account (no domain), allow; otherwise require allowlist membership
        if dom and dom not in ALLOW_DOMAINS:
            return False
    # followers-only
    if FOLLOWERS_ONLY:
        rel = relmap.get(str(acct.get("id")), {})
        if not rel.get("followed_by", False):
            return False
    return True

# ========= MAIN =========
def main():
    print(f"[askbot] start {_now_utc().isoformat()}")
    if not MASTODON_TOKEN:
        print("[askbot] No MASTODON_TOKEN; exit 0")
        return

    # scopes (best-effort)
    try:
        tok = _mastodon_get("/oauth/token/info")
        scopes = tok.get("scopes", [])
        if DEBUG:
            print(f"[scopes] {scopes}")
        if "read:statuses" not in scopes:
            print("[warn] token missing read:statuses — public hashtag timeline may fail.")
    except Exception:
        pass

    state = _load_state()
    since_id = state.get("since_id")  # mentions cursor
    since_id_tag = state.get("since_id_tag")  # tag cursor

    me = _verify_credentials()
    self_id = str(me.get("id"))
    self_acct = me.get("acct")
    if DEBUG:
        print(f"[askbot] acting as @{self_acct} id={self_id}")

    replies = 0

    # --- mention mode (fallback) ---
    try:
        notifs = _mastodon_get("/api/v1/notifications", params={"types[]": "mention"})
        notifs = [n for n in (notifs or []) if n.get("type")=="mention" and n.get("status")]
        notifs.sort(key=lambda n: int(n["id"]))
    except Exception:
        notifs = []

    # build relationships map (for policy)
    if FOLLOWERS_ONLY and notifs:
        relmap = _relationships([str(n["account"]["id"]) for n in notifs])
    else:
        relmap = {}

    newest_id = since_id
    meta_text = _read_meta()

    for n in notifs:
        if replies >= MAX_REPLIES_PER_RUN: break
        st = n["status"]; notif_id = str(n["id"])
        created_at = _iso_to_dt_utc(st.get("created_at"))
        if (_now_utc() - created_at).total_seconds() > MENTION_MAX_AGE_MIN*60:
            newest_id = max(notif_id, newest_id or notif_id, key=int); continue

        body_txt = _strip_html(st.get("content",""))
        if not _text_has_trigger(body_txt):
            newest_id = max(notif_id, newest_id or notif_id, key=int); continue

        if not _allowed_by_policy(n.get("account",{}), self_id, relmap):
            newest_id = max(notif_id, newest_id or notif_id, key=int); continue

        ctx = _status_context(str(st["id"]))
        prompt_user = _build_prompt_from_thread(st, ctx)
        answer = _call_groq(prompt_user, meta_text)

        vis = st.get("visibility") or REPLIES_VIS
        idem = f"askbot:mention:{notif_id}"
        reply = _reply_text(n["account"]["acct"], answer)
        data = {"status": reply, "in_reply_to_id": str(st["id"]), "visibility": vis}
        try:
            _mastodon_post("/api/v1/statuses", data, idem_key=idem)
            replies += 1
            print(f"[askbot] replied mention id={notif_id}")
        except Exception as e:
            print(f"[askbot] post failed mention id={notif_id}: {e}")
        newest_id = max(notif_id, newest_id or notif_id, key=int)

    if newest_id:
        state["since_id"] = newest_id
        _save_state(state)

    # --- public hashtag mode ---
    if ASK_TAG_ENABLE and replies < MAX_REPLIES_PER_RUN:
        params = {"limit": 60}
        if since_id_tag:
            params["since_id"] = since_id_tag

        # First-run bootstrap: set cursor to newest and skip backlog
        bootstrap = False
        if since_id_tag is None:
            bootstrap = True
            # get 1 newest to set cursor
            newest_list = _mastodon_get(f"/api/v1/timelines/tag/{ASK_TAG}", params={"limit": 1}) or []
            if newest_list:
                state["since_id_tag"] = str(newest_list[0]["id"])
                _save_state(state)
                print(f"[tag] bootstrap set since_id_tag={state['since_id_tag']} (skipping backlog)")
            else:
                print("[tag] bootstrap: no posts yet")

        # Pull new items if not bootstrapping
        if not bootstrap:
            try:
                items = _mastodon_get(f"/api/v1/timelines/tag/{ASK_TAG}", params=params) or []
                if DEBUG:
                    print(f"[tag] got {len(items)} after since_id_tag={since_id_tag}")
            except Exception as e:
                print(f"[askbot] tag fetch failed: {e}")
                items = []

            # relationships only if needed
            if FOLLOWERS_ONLY and items:
                relmap2 = _relationships([str(s["account"]["id"]) for s in items])
            else:
                relmap2 = {}

            # process oldest->newest
            items.sort(key=lambda s: int(s["id"]))
            newest_tag_id = since_id_tag

            for st in items:
                if replies >= MAX_REPLIES_PER_RUN: break
                st_id = str(st["id"])
                acct = st.get("account", {})
                acct_acct = acct.get("acct","")
                created_at = _iso_to_dt_utc(st.get("created_at"))
                if (_now_utc() - created_at).total_seconds() > MENTION_MAX_AGE_MIN*60:
                    newest_tag_id = max(st_id, newest_tag_id or st_id, key=int); continue

                # Require @mention only if configured
                if ASK_TAG_REQUIRE_MENTION:
                    mentions = st.get("mentions") or []
                    mentioned = any(m.get("acct") == self_acct or str(m.get("id")) == self_id for m in mentions)
                    if not mentioned:
                        newest_tag_id = max(st_id, newest_tag_id or st_id, key=int); continue

                if not _allowed_by_policy(acct, self_id, relmap2):
                    if DEBUG: print(f"[tag] not allowed acct st={st_id}")
                    newest_tag_id = max(st_id, newest_tag_id or st_id, key=int); continue

                body_txt = _strip_html(st.get("content",""))
                if not _text_has_trigger(body_txt):
                    newest_tag_id = max(st_id, newest_tag_id or st_id, key=int); continue

                # Decide reply target (root vs this status)
                reply_to_id = st_id
                root_status = st
                if REPLY_TO_ROOT:
                    ctx = _status_context(st_id)
                    ancestors = ctx.get("ancestors") or []
                    if ancestors:
                        root_status = ancestors[0]
                        reply_to_id = str(root_status["id"])

                # Build prompt from chosen root/current
                ctx_for_prompt = _status_context(reply_to_id) if ASK_CONTEXT_MODE != "none" else {"ancestors": []}
                prompt_user = _build_prompt_from_thread(root_status, ctx_for_prompt)
                answer = _call_groq(prompt_user, meta_text)

                vis = (root_status.get("visibility") or st.get("visibility")) or REPLIES_VIS
                reply = _reply_text(acct_acct, answer)
                idem = f"askbot:tag:{st_id}"
                data = {"status": reply, "in_reply_to_id": reply_to_id, "visibility": vis}

                try:
                    _mastodon_post("/api/v1/statuses", data, idem_key=idem)
                    replies += 1
                    print(f"[askbot] replied tag st={st_id} → root={reply_to_id}")
                except Exception as e:
                    print(f"[askbot] post failed tag st={st_id}: {e}")

                newest_tag_id = max(st_id, newest_tag_id or st_id, key=int)

            if newest_tag_id:
                state["since_id_tag"] = newest_tag_id
                _save_state(state)

    print(f"[askbot] done replies={replies}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("[askbot] fatal:", e)
