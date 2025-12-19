import os
import json
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta



STATE_PATH = os.getenv("STATE_PATH", "data/state.json")

MASTODON = os.environ["MASTODON_INSTANCE"]
TOKEN = os.environ["MASTODON_TOKEN"]
GROQ = os.environ["GROQ_API_KEY"]
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
STATE_FILE = "state.json"

def load_state() -> dict:
    """
    Загружает состояние из файла. Если файл отсутствует, пустой или повреждён —
    аккуратно инициализирует новое состояние.
    """
    state: dict = {}

    if not os.path.exists(STATE_PATH):
        print(f"[state] no existing state at {STATE_PATH}, starting fresh")
        return {"feeds": {}, "seen": {}}

    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            raw = f.read().strip()
            if not raw:
                raise ValueError("empty state file")
            state = json.loads(raw)
    except Exception as e:
        # Любая ошибка парсинга → не падаем, а создаём новое состояние
        print(f"[state] WARNING: invalid or corrupted state file ({e}); reinitializing")
        state = {}

    if not isinstance(state, dict):
        state = {}

    # Гарантируем необходимые ключи
    if not isinstance(state.get("feeds"), dict):
        state["feeds"] = {}
    if not isinstance(state.get("seen"), dict):
        state["seen"] = {}

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


def get_posts():
   r = requests.get(
       f"{MASTODON}/api/v1/timelines/tag/sum",
       headers=HEADERS,
       params={"limit": 5}
   )
   r.raise_for_status()
   return r.json()
def extract_url(text):
   m = re.search(r"https?://\S+", text)
   return m.group(0) if m else None
def fetch_article(url):
   r = requests.get(url, timeout=10)
   soup = BeautifulSoup(r.text, "html.parser")
   for tag in soup(["script", "style", "noscript"]):
       tag.decompose()
   text = " ".join(soup.stripped_strings)
   return text[:8000]  # cap input
def groq_summarize(text):
   payload = {
       "model": "llama-3.1-8b-instant",
       "messages": [
           {
               "role": "system",
               "content": (
                   "Ты новостной редактор. "
                   "Суммируй текст на русском языке. "
                   "3–4 предложения. Факты только. Без мнений."
               )
           },
           {
               "role": "user",
               "content": text
           }
       ],
       "temperature": 0.2
   }
   r = requests.post(
       "https://api.groq.com/openai/v1/chat/completions",
       headers={
           "Authorization": f"Bearer {GROQ}",
           "Content-Type": "application/json"
       },
       json=payload,
       timeout=30
   )
   r.raise_for_status()
   return r.json()["choices"][0]["message"]["content"]
def post_to_mastodon(text, reply_to=None):
   data = {"status": text}
   if reply_to:
       data["in_reply_to_id"] = reply_to
   requests.post(
       f"{MASTODON}/api/v1/statuses",
       headers=HEADERS,
       data=data
   ).raise_for_status()
def moscow_time():
   return (datetime.now(timezone.utc) + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M MSK")
def main():
   state = load_state()
   posts = get_posts()
   for post in reversed(posts):
       if post["id"] <= state["last_id"]:
           continue
       text = BeautifulSoup(post["content"], "html.parser").get_text()
       url = extract_url(text)
       if not url:
           continue
       article = fetch_article(url)
       summary = groq_summarize(article)
       final = f"{summary}\n\n🕒 {moscow_time()}"
       post_to_mastodon(final, reply_to=post["id"])
       state["last_id"] = post["id"]
   save_state(state)
if __name__ == "__main__":
   main()
