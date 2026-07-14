# Tech Communism — Static Site

A 100% static, GitHub-Pages-hostable site about tech communism, NPA (Net Present Attention), and the cognitive extraction critique of digital capitalism. Bilingual (RU/EN).

## Repository layout

```
.
├── index.html              ← redirects to ru/manifesto.html
├── ru/                     ← Russian pages (manifesto, npa-manifesto, death, math, overview, altruists, faq)
├── en/                     ← English pages (same set)
├── css/global.css          ← shared dark-theme stylesheet
├── js/bot.js               ← Groq chat widget (calls Render backend, see below)
├── assets/                 ← chart PNGs (chart1/2 have _en and _ru variants; chart3 too)
├── npa-calculator.html     ← standalone NPR calculator toy
├── server/                 ← tiny Express proxy for the chat (deploy to Render, see below)
└── render.yaml             ← Render blueprint (auto-fills settings on first deploy)
```

## Deploy the static site (GitHub Pages)

1. Push this folder to a GitHub repo.
2. Settings → Pages → Source: `GitHub Actions` (or `Deploy from branch: main / root`).
3. Open `https://<username>.github.io/<repo>/` — the site loads immediately.
4. No build step needed; everything is already static HTML/CSS/JS.

## Deploy the chat backend (Render, free)

The chat bubble calls a tiny Express proxy that holds the `GROQ_API_KEY` as an env var. The static site itself never sees the key. The bubble stays hidden until you wire up the backend.

1. On Render, **New + → Web Service → connect this repo**.
   - Render reads `render.yaml` and pre-fills everything (free tier, Node, `rootDir: server`).
2. After the first deploy, go to **Environment → Add Environment Variable**:
   - Key: `GROQ_API_KEY`
   - Value: your Groq key (free at <https://console.groq.com/keys>)
3. Wait for the service to deploy. Visit `https://<your-service>.onrender.com/health` — you should see `{"ok":true,"groq_configured":true}`.
4. Open `js/bot.js` in this repo. Set:
   ```js
   const BACKEND_URL = 'https://<your-service>.onrender.com';
   ```
5. Push the change. The chat bubble appears on every page within a minute.

**Total cost: $0/month** — GitHub Pages is free for public repos, Render free tier covers 750h/month, Groq free tier covers 14k req/day. Rate-limited to 5 requests/minute per IP.

## Rebuilding the charts

If the underlying data or labels change, regenerate the chart PNGs (both languages, both output directories) with:

```bash
python3 /home/z/my-project/scripts/build_npa_charts.py
```

This produces `npa_chart1_d_extraction_{en,ru}.png` and `npa_chart2_d_vs_penetration_{en,ru}.png` in both `tc_static/assets/` and `download/`. Chart 3 (timeline) is left untouched — it already exists in both languages.

## Rebuilding the PDFs

```bash
python3 /home/z/my-project/scripts/build_npa_manifesto_ru.py      # full Russian PDF
python3 /home/z/my-project/scripts/build_npa_short_ru.py          # short Russian PDF
python3 /home/z/my-project/scripts/build_npa_manifesto.py         # English PDF (no charts)
```

PDFs are written to `/home/z/my-project/download/`.
