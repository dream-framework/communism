import os, json, datetime, pytz, hashlib, time, pytz
from typing import List, Dict, Any
import requests, feedparser

# =========================
# CONFIG (via env vars)
# =========================
MASTODON_INSTANCE = os.getenv("MASTODON_INSTANCE", "https://mastodon.social").rstrip("/")
MASTODON_TOKEN = os.environ["MASTODON_TOKEN"]  # REQUIRED
VISIBILITY = os.getenv("MASTODON_VISIBILITY", "unlisted")  # public/unlisted/private
POST_TZ = os.getenv("POST_TZ", "Europe/Moscow")
STATE_PATH = os.getenv("STATE_PATH", "data/state.json")

# Groq (optional; if GROQ_API_KEY is unset, we use a free local summarizer)
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")
GROQ_MAX_OUTPUT_TOKENS = int(os.getenv("GROQ_MAX_OUTPUT_TOKENS", "320"))

# Feeds & limits
NEWS_MAX_ITEMS = 50
FEEDS = {
    "geopolitics": [
        "https://thediplomat.com/feed/",
        "https://rss.nytimes.com/services/xml/rss/nyt/World.xml",
    ],
    "markets": [
        "https://www.reuters.com/markets/rss",
        "https://feeds.marketwatch.com/marketwatch/topstories/",
    ],
}

# Identify this bot to feed servers (recommended by feedparser)
feedparser.USER_AGENT = "news-bot/1.0 (+https://github.com/your/repo)"

# =========================
# UTILITIES
# =========================
def sha(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]

def load_state() -> dict:
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"feeds": {}, "seen": {}}

def save_state(state: dict) -> None:
    dirpath = os.path.dirname(STATE_PATH)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def get_instance_max_chars(instance: str) -> int:
    url = f"{instance}/api/v2/instance"
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        return int(r.json()["configuration"]["statuses"]["max_characters"])
    except Exception:
        # Fallback for most instances (mastodon.social = 500)
        return 500

# =========================
# FEED FETCH + DEDUPE
# =========================
def fetch_feed_items(theme_urls: List[str], state: dict, max_items: int) -> List[Dict[str, str]]:
    items: List[Dict[str, str]] = []
    for url in theme_urls:
        cache = state["feeds"].get(url, {})
        try:
            d = feedparser.parse(url, etag=cache.get("etag"), modified=cache.get("modified"))
            # 304 - not modified, skip
            if getattr(d, "status", None) == 304:
                continue
            # persist etag/modified for future runs
            state["feeds"][url] = {
                "etag": getattr(d, "etag", None),
                "modified": getattr(d, "modified", None),
            }
            for entry in d.entries[:max_items]:
                title = (entry.get("title") or "").strip()
                link = (entry.get("link") or "").strip()
                summary = (entry.get("summary") or "").strip()
                if not (title or summary or link):
                    continue
                items.append({"title": title, "link": link, "summary": summary})
        except Exception as e:
            print(f"[warn] feed error {url}: {e}")
    return items

def dedupe_items(items: List[Dict[str, str]], state: dict) -> List[Dict[str, str]]:
    seen = state.get("seen", {})
    out: List[Dict[str, str]] = []
    for it in items:
        key = it.get("link") or sha((it.get("title") or "")[:140])
        if key in seen:
            continue
        seen[key] = datetime.datetime.utcnow().isoformat()
        out.append(it)
    state["seen"] = seen
    return out

def build_items_text(items: List[Dict[str, str]], max_chars: int = 16000) -> str:
    """Concatenate title + summary + link lines for LLM input, capped to avoid big prompts."""
    txt = ""
    for it in items:
        snippet = f"{it['title']} — {it['summary']} {it['link']}\n"
        if len(txt) + len(snippet) > max_chars:
            break
        txt += snippet
    return txt

# =========================
# SUMMARIZERS
# =========================
def summarize_locally(items: List[Dict[str, Any]]) -> str:
    """
    Heuristic 3–4 sentence digest (ru-RU), zero cost.
    """
    if not items:
        return "Нет новых новостей за этот период."
    # Use top N for compactness & diversity
    top = items[:8]
    # Build 3 short factual sentences from titles
    bullets = [it["title"].rstrip(".") + "." for it in top if it.get("title")]
    core = " ".join(bullets[:3]) if bullets else "Нет новых новостей за этот период."
    # Add up to 3 links
    links = []
    for it in top:
        l = it.get("link")
        if l and l not in links:
            links.append(l)
        if len(links) == 3:
            break
    more = (" Читать подробнее: " + " ".join(links)) if links else ""
    return (core + more).strip()

def groq_generate_digest(items: List[Dict[str, Any]]) -> str:
    if not items:
        return "Нет новых новостей за этот период."

    prepared_text = build_items_text(items, max_chars=16000)
    system_msg = (
        "Ты — лаконичный фактологичный помощник-аналитик. "
        "Составь сводку новостей на русском языке в виде 3–4 предложений, "
        "строго по фактам, без домыслов. "
        "Если есть ссылки, добавь их в конце после 'Читать подробнее:' (не более 3)."
    )
    user_msg = f"Сделай краткую русскоязычную сводку (3–4 предложения) из этих материалов:\n{prepared_text}"

    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "model": GROQ_MODEL,
        "messages": [
            {"role": "system", "content": system_msg},
            {"role": "user", "content": user_msg},
        ],
        "temperature": 0.0,
        # Many OpenAI-compatible endpoints accept max_tokens; Groq may also accept max_completion_tokens.
        "max_tokens": GROQ_MAX_OUTPUT_TOKENS,
    }

    for attempt in range(3):
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=60)
            if resp.status_code == 429:
                retry = int(resp.headers.get("retry-after", "2"))
                time.sleep(min(5, max(1, retry)))
                continue
            if resp.status_code >= 400:
                print("Groq error:", resp.status_code, resp.text)
                break  # fall back to local
            resp.raise_for_status()
            j = resp.json()
            text = j.get("choices", [{}])[0].get("message", {}).get("content", "")
            return text.strip() or "Нет доступных новостей."
        except Exception as e:
            print("Groq request failed:", e)
            time.sleep(1)

    # fallback
    return summarize_locally(items)

# =========================
# MASTODON
# =========================
def post_to_mastodon(status: str, visibility: str = "unlisted") -> dict:
    url = f"{MASTODON_INSTANCE}/api/v1/statuses"
    headers = {
        "Authorization": f"Bearer {MASTODON_TOKEN}",
        "Idempotency-Key": sha(status),  # avoid duplicate posts on retries
    }
    data = {"status": status, "visibility": visibility}
    r = requests.post(url, headers=headers, data=data, timeout=30)
    r.raise_for_status()
    return r.json()

# =========================
# MAIN
# =========================
def main():
    state = load_state()

    # Collect + dedupe
    all_items: List[Dict[str, str]] = []
    for theme, urls in FEEDS.items():
        all_items.extend(fetch_feed_items(urls, state, NEWS_MAX_ITEMS))
    all_items = dedupe_items(all_items, state)

    # Summarize (Groq if key provided; else local)
    if GROQ_API_KEY:
        digest = groq_generate_digest(all_items)
    else:
        digest = summarize_locally(all_items)

    # Timestamp (MSK by default)
    tz = pytz.timezone(POST_TZ)
    now = datetime.datetime.now(tz)
    header = f"📰 Часовая сводка новостей — {now.strftime('%d.%m.%Y %H:%M %Z')}\n\n"
    footer = f"\n\n#новости #дайджест — {now.strftime('%d.%m.%Y %H:%M %Z')}"

    content = header + digest + footer

    # Enforce instance character limit
    max_chars = get_instance_max_chars(MASTODON_INSTANCE)  # typically 500
    if len(content) > max_chars:
        allowed = max_chars - len(header) - len(footer) - 1
        core = (digest[:allowed] + "…") if allowed > 0 else ""
        content = header + core + footer

    try:
        resp = post_to_mastodon(content, visibility=VISIBILITY)
        print("Posted:", resp.get("url"))
    except Exception as e:
        print("Error posting to Mastodon:", e)

    save_state(state)

if __name__ == "__main__":
    main()
