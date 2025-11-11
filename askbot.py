#!/usr/bin/env python3
import os, json, time, re, html, datetime
from typing import Dict, Any, List, Optional
from urllib.parse import urljoin

import requests

# ========= ENV =========
MASTODON_INSTANCE = os.getenv("MASTODON_INSTANCE", "https://mastodon.social").rstrip("/")
MASTODON_TOKEN    = os.getenv("MASTODON_TOKEN", "")
ASK_STATE_PATH    = os.getenv("ASK_STATE_PATH", "data/ask_state.json")
META_PATH         = os.getenv("META_PATH", "meta/primer.txt")         # optional meta/TOE text file

ASK_KEYWORD       = os.getenv("ASK_KEYWORD", "#ask")                  # trigger token
REPLIES_VIS       = os.getenv("REPLIES_VISIBILITY", "unlisted")       # reply visibility
FOLLOWERS_ONLY    = os.getenv("FOLLOWERS_ONLY", "0") == "1"           # restrict to followers? default: no
ALLOW_DOMAINS     = [x.strip().lower() for x in os.getenv("ALLOW_DOMAINS", "").split(",") if x.strip()]

MAX_REPLIES_PER_RUN   = int(os.getenv("MAX_REPLIES_PER_RUN", "4"))
MENTION_MAX_AGE_MIN   = int(os.getenv("MENTION_MAX_AGE_MIN", "180"))

# ---- Hashtag timeline scanning (public #ask) ----
ASK_TAG_ENABLE            = os.getenv("ASK_TAG_ENABLE", "1") == "1"
ASK_TAG                   = os.getenv("ASK_TAG", "ask").lstrip("#")
ASK_TAG_REQUIRE_MENTION   = os.getenv("ASK_TAG_REQUIRE_MENTION", "0") == "1"  # if True, must @mention us too
ASK_TAG_MAX_AGE_MIN       = int(os.getenv("ASK_TAG_MAX_AGE_MIN", "240"))
ASK_TAG_BOOTSTRAP_SKIP_OLD= os.getenv("ASK_TAG_BOOTSTRAP_SKIP_OLD", "0") == "1"  # default now OFF
ASK_TAG_FETCH_LIMIT       = int(os.getenv("ASK_TAG_FETCH_LIMIT", "60"))     # 40–80 is typical
REPLY_TO_ROOT             = os.getenv("REPLY_TO_ROOT", "1") == "1"          # reply to thread root

# Debug
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
        print(f"[GET] {r.url} -> {r.status_code}")
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

def _root_id(status_id: str) -> str:
    """Return thread root id (or the same status id if already root)."""
    try:
        ctx = _status_context(status_id)
        anc = ctx.get("ancestors") or []
        if anc:
            return str(anc[0]["id"])
    except Exception:
        pass
    return status_id

def _looks_valid_trigger(text: str) -> bool:
    if not ASK_KEYWORD:
        return True
    return re.search(re.escape(ASK_KEYWORD), text, re.I) is not None

def _allowed_account(acct: Dict[str, Any], self_id: str, relmap: Dict[str, Any]) -> bool:
    if str(acct.get("id")) == str(self_id):
        return False  # ignore ourselves
    domain = (acct.get("acct","").split("@",1)[1].lower() if "@" in acct.get("acct","") else "")
    if ALLOW_DOMAINS and domain and domain not in ALLOW_DOMAINS:
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
        t = prompt_user.strip()
        return t[:450] if len(t) > 450 else t
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

def _build_prompt_from_status(st: Dict[str, Any]) -> str:
    base = _strip_html(st.get("content",""))
    parts = [f"Вопрос пользователя:\n{base}"]
    try:
        ctx = _status_context(str(st["id"]))
        ancestors = (ctx.get("ancestors") or [])[-2:]
        if ancestors:
            parts.append("Контекст (фрагменты нити):")
            for a in ancestors:
                who = a.get("account",{}).get("acct","?")
                txt = _strip_html(a.get("content",""))
                parts.append(f"- @{who}: {txt}")
    except Exception:
        pass
    return "\n".join(parts).strip()

# ========= FETCHERS =========
def _fetch_mentions(since_id: Optional[str]) -> List[Dict[str, Any]]:
    params = {"types[]": "mention"}
    if since_id: params["since_id"] = since_id
    notifs = _mastodon_get("/api/v1/notifications", params=params)
    notifs = [n for n in notifs if n.get("type")=="mention" and n.get("status")]
    notifs.sort(key=lambda n: int(n["id"]))
    return notifs

def _fetch_tag_since(tag: str, since_id_tag: Optional[str]) -> List[Dict[str, Any]]:
    params = {"limit": min(ASK_TAG_FETCH_LIMIT, 80)}
    if since_id_tag:
        params["since_id"] = since_id_tag
    res = _mastodon_get(f"/api/v1/timelines/tag/{tag}", params=params)
    # API returns newest-first; we want oldest-first for stable processing
    res = list(res)[::-1]
    if DEBUG:
        sid = since_id_tag or "-"
        print(f"[tag] got {len(res)} after since_id_tag={sid}")
    return res

# ========= MAIN =========
def main():
    print(f"[askbot] start {_now_utc().isoformat()}")
    if not MASTODON_TOKEN:
        print("[askbot] No MASTODON_TOKEN; exit 0"); return

    # sanity: list scopes
    try:
        tok = _mastodon_get("/oauth/token/info")
        scopes = (tok.get("scopes") or [])
        if DEBUG: print("[scopes]", scopes)
        if "read:statuses" not in scopes:
            print("[warn] token missing read:statuses — public hashtag timeline may fail.")
    except Exception as e:
        if DEBUG: print("[warn] token info failed:", e)

    state = _load_state()
    since_id_mention = state.get("since_id_mention")
    since_id_tag     = state.get("since_id_tag")

    me = _verify_credentials()
    self_id = str(me.get("id"))
    self_acct = me.get("acct")
    print(f"[askbot] acting as @{self_acct} id={self_id}")

    # ----- Mentions pass (still supported) -----
    notifs = _fetch_mentions(since_id_mention)
    # relationships map (only needed if FOLLOWERS_ONLY)
    relmap = _relationships([str(n["account"]["id"]) for n in notifs]) if FOLLOWERS_ONLY else {}

    processed = 0
    newest_mention = since_id_mention
    meta_text = _read_meta()

    for n in notifs:
        notif_id = str(n["id"])
        st = n["status"]
        created_at = _iso_to_dt_utc(st.get("created_at"))
        if (_now_utc() - created_at).total_seconds() > MENTION_MAX_AGE_MIN*60:
            if DEBUG: print(f"[mention] skip old id={notif_id}")
            newest_mention = max(notif_id, newest_mention or notif_id, key=int)
            continue

        body_txt = _strip_html(st.get("content",""))
        if not _looks_valid_trigger(body_txt):
            if DEBUG: print(f"[mention] no keyword in id={notif_id}")
            newest_mention = max(notif_id, newest_mention or notif_id, key=int)
            continue

        if not _allowed_account(n.get("account",{}), self_id, relmap):
            if DEBUG: print(f"[mention] not allowed account id={notif_id}")
            newest_mention = max(notif_id, newest_mention or notif_id, key=int)
            continue

        prompt_user = _build_prompt_from_status(st)
        answer = _call_groq(prompt_user, meta_text)

        # reply to root if configured
        reply_to = _root_id(str(st["id"])) if REPLY_TO_ROOT else str(st["id"])
        vis = st.get("visibility") or REPLIES_VIS
        idem = f"askbot:mention:{notif_id}"
        data = {"status": _reply_text(n["account"]["acct"], answer),
                "in_reply_to_id": reply_to, "visibility": vis}
        try:
            _mastodon_post("/api/v1/statuses", data, idem_key=idem)
            processed += 1
            print(f"[mention] replied notif={notif_id} → in_reply_to={reply_to}")
        except Exception as e:
            print(f"[mention] post failed for notif={notif_id}: {e}")

        newest_mention = max(notif_id, newest_mention or notif_id, key=int)
        if processed >= MAX_REPLIES_PER_RUN:
            print(f"[askbot] hit MAX_REPLIES_PER_RUN={MAX_REPLIES_PER_RUN}"); break

    # ----- Tag pass (public #ask) -----
    total_tag_processed = 0
    newest_tag = since_id_tag
    if ASK_TAG_ENABLE and processed < MAX_REPLIES_PER_RUN:
        try:
            # Bootstrap behavior: if we have no watermark yet
            if since_id_tag is None:
                posts = _fetch_tag_since(ASK_TAG, None)
                if ASK_TAG_BOOTSTRAP_SKIP_OLD and posts:
                    newest_tag = str(posts[-1]["id"])
                    print(f"[tag] bootstrap set since_id_tag={newest_tag} (skipping backlog)")
                    posts = []
            else:
                posts = _fetch_tag_since(ASK_TAG, since_id_tag)

            # Optionally restrict to followers
            relmap_tag = {}
            if FOLLOWERS_ONLY and posts:
                relmap_tag = _relationships([str(p["account"]["id"]) for p in posts])

            for p in posts:
                st_id = str(p["id"])
                acct  = p.get("account",{})
                txt   = _strip_html(p.get("content",""))
                created_at = _iso_to_dt_utc(p.get("created_at"))

                # Skip too old
                if (_now_utc() - created_at).total_seconds() > ASK_TAG_MAX_AGE_MIN*60:
                    if DEBUG: print(f"[tag] skip old st={st_id}")
                    newest_tag = max(st_id, newest_tag or st_id, key=int); continue

                # Must contain keyword
                if not _looks_valid_trigger(txt):
                    if DEBUG: print(f"[tag] no keyword st={st_id}")
                    newest_tag = max(st_id, newest_tag or st_id, key=int); continue

                # Optionally require mention of us
                if ASK_TAG_REQUIRE_MENTION and f"@{self_acct}" not in txt:
                    if DEBUG: print(f"[tag] require mention; missing in st={st_id}")
                    newest_tag = max(st_id, newest_tag or st_id, key=int); continue

                # Domain/followers filter
                if not _allowed_account(acct, self_id, relmap_tag):
                    if DEBUG: print(f"[tag] not allowed acct st={st_id}")
                    newest_tag = max(st_id, newest_tag or st_id, key=int); continue

                # Build answer
                prompt_user = _build_prompt_from_status(p)
                answer = _call_groq(prompt_user, meta_text)

                # Reply to root (the #ask post itself is usually the root)
                reply_to = _root_id(st_id) if REPLY_TO_ROOT else st_id
                vis = p.get("visibility") or REPLIES_VIS
                idem = f"askbot:tag:{st_id}"
                data = {"status": _reply_text(acct.get("acct",""), answer),
                        "in_reply_to_id": reply_to, "visibility": vis}
                try:
                    _mastodon_post("/api/v1/statuses", data, idem_key=idem)
                    total_tag_processed += 1
                    print(f"[tag] replied st={st_id} → in_reply_to={reply_to}")
                except Exception as e:
                    print(f"[tag] post failed st={st_id}: {e}")

                newest_tag = max(st_id, newest_tag or st_id, key=int)
                if processed + total_tag_processed >= MAX_REPLIES_PER_RUN:
                    print(f"[askbot] hit MAX_REPLIES_PER_RUN={MAX_REPLIES_PER_RUN}")
                    break

        except requests.HTTPError as e:
            print(f"[tag] fetch failed: {e}")

    # Persist watermarks
    if newest_mention: state["since_id_mention"] = newest_mention
    if newest_tag:     state["since_id_tag"]     = newest_tag
    _save_state(state)

    print(f"[askbot] done replies={processed + total_tag_processed}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("[askbot] fatal:", e)
