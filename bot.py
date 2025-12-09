import os
import re
import json
import base64
import requests
from uuid import uuid4
from datetime import datetime, timezone
from mastodon import Mastodon, StreamListener
# --- Mastodon config ---
MASTODON_INSTANCE = os.getenv("MASTODON_INSTANCE")  # e.g. https://mastodon.social
MASTODON_TOKEN = os.getenv("MASTODON_TOKEN")        # Bot account token
# --- GitHub config ---
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")           # Personal Access Token
REPO_OWNER = os.getenv("REPO_OWNER")
REPO_NAME = os.getenv("REPO_NAME")
REPO_BRANCH = os.getenv("REPO_BRANCH", "main")
mastodon = Mastodon(
   access_token=MASTODON_TOKEN,
   api_base_url=MASTODON_INSTANCE
)
def github_headers():
   return {
       "Authorization": f"token {GITHUB_TOKEN}",
       "Accept": "application/vnd.github.v3+json"
   }
def push_request_to_github(obj):
   """Push a JSON request to GitHub repo to trigger Actions workflow"""
   path = f"jobs/requests/{obj['id']}.json"
   content = base64.b64encode(json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")).decode("utf-8")
   r = requests.put(
       f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{path}",
       headers=github_headers(),
       json={"message": f"Add summarize request {obj['id']}", "content": content, "branch": REPO_BRANCH},
       timeout=30
   )
   r.raise_for_status()
   return r.json()
class SummarizeListener(StreamListener):
   """Listens for #SUMMARIZE posts and pushes jobs to GitHub"""
   def on_update(self, status):
       text = status.content
       if "#SUMMARIZE" in text.upper():
           urls = re.findall(r'https?://\S+', text)
           if not urls:
               return
           url = urls[0]
           rid = f"req-{datetime.now(timezone.utc).isoformat().replace(':','').replace('-','')}-{uuid4().hex[:8]}"
           obj = {
               "id": rid,
               "url": url,
               "intent": "summarize",
               "tag": "mastodon",
               "created_at": datetime.now(timezone.utc).isoformat(),
               "status": "pending",
               "reply_to": status.id
           }
           try:
               push_request_to_github(obj)
               print(f"[{datetime.now()}] Triggered GitHub Action for {url}")
           except Exception as e:
               print(f"[{datetime.now()}] Failed to push request: {e}")
if __name__ == "__main__":
   print("Starting Mastodon Summarizer Bot...")
   while True:
       try:
           mastodon.stream_user(SummarizeListener())
       except Exception as e:
           print(f"[{datetime.now()}] Stream disconnected, reconnecting in 5s: {e}")
           import time
           time.sleep(5)