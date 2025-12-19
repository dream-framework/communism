import os
import json
import time
import re
import datetime
from datetime import timezone
from typing import List, Dict, Any
from urllib.parse import urlparse

import requests
from html import unescape

# =========================
# CONFIG
# =========================

MASTODON_INSTANCE = os.getenv("MASTODON_INSTANCE", "").strip()
MASTODON_TOKEN = os.getenv("MASTODON_TOKEN", "").strip()

# Какой хэштег сводить (по умолчанию #sum)
SUM_TAG = os.getenv("SUM_TAG", "sum").lstrip("#")

STATE_PATH = os.getenv("STATE_PATH", "data/state.json")

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "").strip()
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")
GROQ_MAX_COMPLETION_TOKENS = int(os.getenv("GROQ_MAX_OUTPUT_TOKENS", "320"))

VISIBILITY = os.getenv("MASTODON_VISIBILITY", "unlisted")

# Сколько новых постов максимум сводим за раз
MAX_POSTS_PER_SUMMARY = int(os.getenv("MAX_POSTS_PER_SUMMARY", "12"))
MIN_POSTS_TO_SUMMARIZE = int(os.getenv("MIN_POSTS_TO_SUMMARIZE", "1"))

USER_AGENT = (
    "SumBot/1.0 (+https://github.com/)"
)

# =========================
# SYSTEM PROMPT ДЛЯ GROQ
# =========================

GROQ_SYSTEM_PROMPT = os.getenv(
    "GROQ_SYSTEM_PROMPT",
    (
        "Ты — профессиональный аналитик и редактор научно-популярных и политических текстов. "
        "Ты делаешь краткие, логично выстроенные сводки по подборке сообщений из соцсетей.\n\n"
        "Требования к языку и стилю:\n"
        "• Пиши только на грамотном литературном русском языке.\n"
        "• Не используй английские слова, фразы и транслитерацию. "
        "Исключение: общеизвестные аббревиатуры: ООН, ЕС, НАТО, ВТО, БРИКС, МВФ и т.п.\n"
        "• Подбирай нормальные русские термины, а не кальки с английского.\n"
        "• Не используй разговорные выражения, сленг и канцелярит.\n\n"
        "Требования к содержанию:\n"
        "• Опирайся только на факты и формулировки из исходных постов; не добавляй домыслов.\n"
        "• Не повторяй одну и ту же мысль разными словами.\n"
        "• Не обращайся к читателю и не давай советов.\n"
        "• Не используй эмодзи, хэштеги, списки и Markdown-разметку.\n\n"
        "Формат ответа:\n"
        "• 3–6 коротких, но содержательных предложений.\n"
        "• Первое предложение — чёткая формулировка общей темы подборки.\n"
        "• Остальные предложения — ключевые факты, аргументы и выводы.\n"
        "• Последнее предложение при необходимости аккуратно фиксирует общий вывод."
    )
)

# =========================
# УТИЛИТЫ
# =========================


def normalize_instance_url(raw: str) -> str:
    """
    Приводит значение MASTODON_INSTANCE к полному URL.
    Примеры входа:
      - mastodon.social  -> https://mastodon.social
      - https://mastodon.social/ -> https://mastodon.social
    """
    s = (raw or "").strip()
    if not s:
        raise RuntimeError("MASTODON_INSTANCE env var is not set")

    if not s.startswith("http://") and not s.startswith("https://"):
        s = "https://" + s

    parsed = urlparse(s)
    if not parsed.scheme or not parsed.netloc:
        raise RuntimeError(f"MASTODON_INSTANCE looks invalid: {raw!r}")

    return s.rstrip("/")


def load_state() -> dict:
    """
    Загружает состояние из файла. Если файл отсутствует, пустой или повреждён —
    аккуратно инициализирует новое состояние.
    """
    state: dict = {}

    if not os.path.exists(STATE_PATH):
        print(f"[state] no existing state at {STATE_PATH}, starting fresh")
        return {"last_seen_id": None}

    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            raw = f.read().strip()
            if not raw:
                raise ValueError("empty state file")
            state = json.loads(raw)
    except Exception as e:
        print(f"[state] WARNING: invalid or corrupted state file ({e}); reinitializing")
        state = {}

    if not isinstance(state, dict):
        state = {}

    if "last_seen_id" not in state:
        state["last_seen_id"] = None

    return state


def save_state(state: dict) -> None:
    """
    Безопасная запись состояния: сначала во временный файл, потом atomic rename.
    Это снижает риск частично записанного JSON при обрыве.
    """
    os.makedirs(os.path.dirname(STATE_PATH) or ".", exist_ok=True)
    tmp_path = STATE_PATH + ".tmp"

    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())

    os.replace(tmp_path, STATE_PATH)
    print(f"[state] saved to {STATE_PATH}")


def html_to_text(html: str) -> str:
    """Грубое, но работающее превращение Mastodon HTML в обычный текст."""
    if not html:
        return ""
    txt = re.sub(r"<br\s*/?>", "\n", html, flags=re.I)
    txt = re.sub(r"</p\s*>", "\n", txt, flags=re.I)
    txt = re.sub(r"<.*?>", "", txt)
    txt = unescape(txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


def _cleanup_russian_summary(text: str, max_sentences: int = 6) -> str:
    """
    Нормализует ответ модели:
    - убирает лишние переводы строк и пробелы
    - убирает повторяющиеся предложения
    - ограничивает количеством предложений
    - следит, чтобы текст заканчивался на .!?…
    """
    if not text:
        return ""

    t = re.sub(r"\s+", " ", text).strip()

    # Разбиваем на предложения по .!?…
    parts = re.split(r"(?<=[\.\!\?…])\s+", t)
    sentences = []
    seen = set()

    for s in parts:
        s = s.strip()
        if not s:
            continue
        s = s.lstrip("•*-— ").strip()
        norm = s.lower()
        if norm in seen:
            continue
        seen.add(norm)
        sentences.append(s)
        if len(sentences) >= max_sentences:
            break

    if not sentences:
        return ""

    out = " ".join(sentences).strip()
    if out and out[-1] not in ".!?…":
        out += "."

    return out


def safe_truncate(text: str, max_len: int) -> str:
    """
    Аккуратно обрезает текст по границе предложения или слова.
    """
    if len(text) <= max_len:
        return text

    if max_len <= 10:
        return text[:max_len]

    truncated = text[: max_len - 1]

    end_idx = -1
    for ch in ".!?…":
        idx = truncated.rfind(ch)
        if idx > end_idx:
            end_idx = idx

    if end_idx >= 40:
        return truncated[: end_idx + 1]

    space_idx = truncated.rfind(" ")
    if space_idx > 0:
        return truncated[:space_idx] + "…"

    return truncated + "…"


def get_instance_max_chars(base_url: str) -> int:
    """
    Определяет лимит символов статуса на инстансе Mastodon.
    """
    url = f"{base_url}/api/v2/instance"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        return int(data["configuration"]["statuses"]["max_characters"])
    except Exception as e:
        print("[mastodon] cannot get max_characters, fallback to 500:", e)
        return 500


# =========================
# MASTODON API
# =========================


def get_posts(base_url: str, state: dict) -> List[Dict[str, Any]]:
    """
    Получает новые посты по хэштегу SUM_TAG.
    Использует since_id из state, чтобы не брать то, что уже обрабатывали.
    """
    last_id = state.get("last_seen_id")
    params = {"limit": str(MAX_POSTS_PER_SUMMARY)}
    if last_id:
        params["since_id"] = str(last_id)

    url = f"{base_url}/api/v1/timelines/tag/{SUM_TAG}"

    headers = {"User-Agent": USER_AGENT}
    if MASTODON_TOKEN:
        headers["Authorization"] = f"Bearer {MASTODON_TOKEN}"

    r = requests.get(url, headers=headers, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()

    if not isinstance(data, list):
        print("[mastodon] unexpected response format (not list)")
        return []

    def _id_int(p: Dict[str, Any]) -> int:
        try:
            return int(p.get("id", "0"))
        except Exception:
            return 0

    # Сортируем по id по возрастанию (от старых к новым)
    data.sort(key=_id_int)
    return data


def post_to_mastodon(base_url: str, text: str, visibility: str = "unlisted") -> dict:
    if not MASTODON_TOKEN:
        print("[info] MASTODON_TOKEN is not set — skipping post")
        return {}

    url = f"{base_url}/api/v1/statuses"
    headers = {"Authorization": f"Bearer {MASTODON_TOKEN}"}
    payload = {"status": text, "visibility": visibility}

    r = requests.post(url, headers=headers, data=payload, timeout=15)
    r.raise_for_status()
    return r.json()


# =========================
# GROQ SUMMARIZER
# =========================


def groq_summarize_posts(posts: List[Dict[str, Any]]) -> str:
    """
    Делает сводку по списку постов Mastodon через Groq.
    """
    if not GROQ_API_KEY or not posts:
        return ""

    ctx_parts = []
    for i, st in enumerate(posts, 1):
        acc = st.get("account", {}) or {}
        author = acc.get("display_name") or acc.get("acct") or "неизвестный автор"
        created_at = st.get("created_at", "")
        url = st.get("url") or st.get("uri") or ""
        text = html_to_text(st.get("content") or "")
        text = text[:600]

        ctx_parts.append(
            f"Пост {i}:\n"
            f"Автор: {author}\n"
            f"Время: {created_at}\n"
            f"Текст: {text}\n"
            f"Ссылка: {url}\n"
        )

    ctx = "\n\n".join(ctx_parts)

    api_url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": GROQ_MODEL,
        "messages": [
            {"role": "system", "content": GROQ_SYSTEM_PROMPT},
            {
                "role": "user",
                "content": (
                    "Ниже приведена подборка постов с хэштегом. "
                    "Сделай краткую, логичную сводку по ним.\n\n"
                    "СТРОГО соблюдай требования:\n"
                    "• Ответ только на русском языке.\n"
                    "• Не используй английские слова, фразы и транслитерацию "
                    "(кроме общеизвестных аббревиатур вроде ООН, ЕС, НАТО, МВФ, ВТО, БРИКС).\n"
                    "• 3–6 предложений.\n"
                    "• Не повторяй одну и ту же мысль разными словами.\n"
                    "• Только факты из текста, без домыслов и оценок.\n"
                    "• Без списков, эмодзи, хэштегов и обращений к читателю.\n\n"
                    "Контекст постов:\n"
                    f"{ctx}"
                ),
            },
        ],
        "temperature": 0.0,
        "n": 1,
        "max_completion_tokens": GROQ_MAX_COMPLETION_TOKENS,
    }

    for _ in range(3):
        try:
            r = requests.post(api_url, headers=headers, json=payload, timeout=20)
            if r.status_code == 429:
                delay = min(5, max(1, int(r.headers.get("retry-after", "2"))))
                print(f"[groq] rate-limited, sleep {delay}s")
                time.sleep(delay)
                continue
            r.raise_for_status()
            j = r.json()
            raw = (j.get("choices", [{}])[0].get("message", {}) or {}).get("content", "") or ""
            cleaned = _cleanup_russian_summary(raw)
            if cleaned:
                return cleaned
        except Exception as e:
            print("[groq] error:", e)
            time.sleep(0.8)

    return ""


# =========================
# MAIN
# =========================


def main() -> None:
    print(f"[run] start {datetime.datetime.utcnow().isoformat()}Z")

    try:
        base_url = normalize_instance_url(MASTODON_INSTANCE)
    except RuntimeError as e:
        print("[config] ERROR:", e)
        return

    state = load_state()

    posts = get_posts(base_url, state)
    posts = [p for p in posts if "#sum" in p['content'] and p['account']['acct']="highercause"]
    if not posts:
        print(f"[info] no new posts for #{SUM_TAG}")
        save_state(state)
        return

    # берём только последние MAX_POSTS_PER_SUMMARY постов, если их слишком много
    if len(posts) > MAX_POSTS_PER_SUMMARY:
        posts = posts[-MAX_POSTS_PER_SUMMARY :]

    if len(posts) < MIN_POSTS_TO_SUMMARIZE:
        print(f"[info] not enough new posts for #{SUM_TAG}: {len(posts)} < {MIN_POSTS_TO_SUMMARIZE}")
        # всё равно обновим last_seen_id, чтобы не зацикливаться
        try:
            max_id = None
            for p in posts:
                pid = p.get("id")
                if pid is None:
                    continue
                if max_id is None or int(pid) > int(max_id):
                    max_id = pid
            if max_id is not None:
                state["last_seen_id"] = max_id
        except Exception:
            pass
        save_state(state)
        return

    summary = groq_summarize_posts(posts)
    summary = summary.strip()

    if not summary:
        print("[info] Groq summary is empty; skipping post")
        # но всё равно отметим, что мы эти посты видели
        try:
            max_id = None
            for p in posts:
                pid = p.get("id")
                if pid is None:
                    continue
                if max_id is None or int(pid) > int(max_id):
                    max_id = pid
            if max_id is not None:
                state["last_seen_id"] = max_id
        except Exception:
            pass
        save_state(state)
        return

    now = datetime.datetime.now(timezone.utc)
    header = f"🧾 Сводка по хэштегу #{SUM_TAG} — {now.strftime('%d.%m.%Y %H:%M UTC')}\n\n"

    # Собираем несколько уникальных ссылок на исходные посты (новые → сверху)
    links: List[str] = []
    for p in reversed(posts):
        url = p.get("url") or ""
        if url and url not in links:
            links.append(url)
        if len(links) >= 3:
            break

    links_block = ""
    if links:
        links_block = "\n\nИсточники:\n" + "\n".join(f"- {u}" for u in links)

    max_chars = get_instance_max_chars(base_url)
    allowed_for_summary = max_chars - len(header) - len(links_block) - 1
    if allowed_for_summary < 80:
        # если совсем мало места — выкинем ссылки
        links_block = ""
        allowed_for_summary = max_chars - len(header) - 1

    summary = safe_truncate(summary, allowed_for_summary)
    status_text = header + summary + links_block

    try:
        resp = post_to_mastodon(base_url, status_text, VISIBILITY)
        print("[post] summary posted:", resp.get("url", "(no url)"))
    except Exception as e:
        print("[post] ERROR:", e)

    # Обновляем last_seen_id только после попытки поста
    try:
        max_id = None
        for p in posts:
            pid = p.get("id")
            if pid is None:
                continue
            if max_id is None or int(pid) > int(max_id):
                max_id = pid
        if max_id is not None:
            state["last_seen_id"] = max_id
    except Exception:
        pass

    save_state(state)
    print("[run] done")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("[fatal] unhandled exception:", e)
