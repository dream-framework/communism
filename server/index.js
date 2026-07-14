// ============================================================================
// Tech Communism — Groq chat proxy (Render-hosted)
// Holds GROQ_API_KEY as an env var (set on Render, never in the repo).
// The static GitHub-Pages site calls /groq-chat; we proxy to api.groq.com.
// ============================================================================

const express = require('express');

const app = express();
const PORT = process.env.PORT || 3000;
const GROQ_API_URL = 'https://api.groq.com/openai/v1/chat/completions';
const DEFAULT_MODEL = 'llama-3.3-70b-versatile';

// ── Per-IP rate limit (in-memory, resets on redeploy) ─────────────────────
// Groq free tier = 30 req/min global, 14k/day. 5/min/IP is generous + safe.
const RATE_LIMIT = { windowMs: 60_000, max: 5 };
const ipHits = new Map();
function rateLimited(ip) {
  const now = Date.now();
  const hits = (ipHits.get(ip) || []).filter(h => now - h.ts < RATE_LIMIT.windowMs);
  if (hits.length >= RATE_LIMIT.max) return true;
  hits.push({ ts: now });
  ipHits.set(ip, hits);
  return false;
}

// ── Middleware ────────────────────────────────────────────────────────────
app.use(express.json({ limit: '1mb' }));

// CORS for ALL routes — set headers on every response, including errors.
app.use((req, res, next) => {
  res.set('Access-Control-Allow-Origin', '*');
  res.set('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.set('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(204).end();
  next();
});

// ── Health check ──────────────────────────────────────────────────────────
app.get('/health', (req, res) => {
  res.json({
    ok: true,
    service: 'tech-communism-groq-proxy',
    groq_configured: !!process.env.GROQ_API_KEY,
  });
});

// ── Main endpoint ─────────────────────────────────────────────────────────
app.post('/groq-chat', async (req, res) => {
  const ip = req.headers['x-forwarded-for']?.split(',')[0] || req.ip || 'unknown';
  if (rateLimited(ip)) {
    return res.status(429).json({ error: 'Rate limit: 5 requests per minute. Try again shortly.' });
  }

  if (!process.env.GROQ_API_KEY) {
    return res.status(500).json({ error: 'GROQ_API_KEY not set on server. Set it in Render → Environment.' });
  }

  try {
    const { message, lang, model } = req.body || {};
    const userMsg = (message || '').trim();
    if (!userMsg) {
      return res.status(400).json({ error: 'Provide "message" field.' });
    }

    const sysEn = `You are the Tech Communism site guide. Answer concisely (3-5 sentences) about the manifesto, math, governance, NPA (Net Present Attention), or anything else on the site. Cite formulas when relevant. Stay in plain English. If a question is off-topic, redirect politely to site themes: tech-communism, NPA, retention law, cognitive extraction.`;
    const sysRu = `Ты — гид по сайту «Технокоммунизм». Отвечай кратко (3-5 предложений) про манифест, математику, управление, НПС (чистая приведённая вовлечённость) и другие темы сайта. Приводи формулы, если уместно. Пиши на чистом русском. Если вопрос не по теме, вежливо вернись к темам сайта: технокоммунизм, НПС, закон ретенции, когнитивная экстракция.`;
    const systemPrompt = lang === 'ru' ? sysRu : sysEn;

    const messages = [
      { role: 'system', content: systemPrompt },
      { role: 'user', content: userMsg },
    ];

    const groqResp = await fetch(GROQ_API_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${process.env.GROQ_API_KEY}`,
      },
      body: JSON.stringify({
        model: model || DEFAULT_MODEL,
        messages,
        temperature: 0.3,
        max_tokens: 500,
      }),
    });

    if (!groqResp.ok) {
      const errText = await groqResp.text();
      let msg = `Groq API HTTP ${groqResp.status}`;
      try {
        const j = JSON.parse(errText);
        msg += `: ${j.error?.message || errText.slice(0, 200)}`;
      } catch {
        msg += `: ${errText.slice(0, 200)}`;
      }
      return res.status(502).json({ error: msg });
    }

    const groqJson = await groqResp.json();
    const reply = groqJson.choices?.[0]?.message?.content?.trim() || '(no reply)';
    res.json({ ok: true, reply });
  } catch (err) {
    console.error('[/groq-chat] error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ── Start ─────────────────────────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`Tech Communism Groq proxy on :${PORT}`);
  console.log(`  Groq: ${process.env.GROQ_API_KEY ? 'configured' : 'NOT configured (set GROQ_API_KEY on Render)'}`);
});
