// ============================================================================
// Tech Communism — Groq chat bot (static-site version)
// Calls a tiny Render-hosted Express proxy that holds GROQ_API_KEY as an env
// var. Visitors don't need their own key. Set BACKEND_URL below to your
// deployed Render URL — the chat bubble stays hidden until then.
// ============================================================================

(function () {
  // ─────────────────────────────────────────────────────────────────────────
  // SET THIS to your deployed Render backend URL.
  // Example: 'https://tech-communism-groq.onrender.com'
  // Leave empty ('') to keep the chat hidden until the backend is deployed.
  // ─────────────────────────────────────────────────────────────────────────
  const BACKEND_URL = '';

  const lang = document.body.getAttribute('data-lang') === 'ru' ? 'ru' : 'en';

  const T = {
    en: {
      toggle: 'Chat · Groq',
      title: 'Groq Assistant',
      placeholder: 'Ask about the manifesto, math, governance…',
      sending: 'Sending…',
      hello: 'Hi! Ask me about the manifesto, math, or governance.',
      error: 'Server error',
    },
    ru: {
      toggle: 'Чат · Groq',
      title: 'Ассистент Groq',
      placeholder: 'Спросите про манифест, математику, управление…',
      sending: 'Отправка…',
      hello: 'Привет! Спрашивайте про манифест, математику или устройство.',
      error: 'Ошибка сервера',
    }
  }[lang];

  // Inject chat panel HTML into the page
  const panelHTML = `
    <button id="botToggle" class="btn primary" style="display:none">${T.toggle}</button>
    <div id="botPanel" style="display:none">
      <header style="display:flex;gap:8px;align-items:center;">
        <strong id="botTitle">${T.title}</strong>
        <span id="botStatus" class="pill" style="display:none"></span>
        <button id="botClose" style="margin-left:auto;background:transparent;border:none;color:inherit;font-size:20px;cursor:pointer;line-height:1">×</button>
      </header>
      <div class="log" id="botLog"></div>
      <div class="entry">
        <input id="botInput" class="form-control" autocomplete="off" placeholder="${T.placeholder}">
        <button id="botSend" class="btn">➤</button>
      </div>
    </div>
  `;
  document.body.insertAdjacentHTML('beforeend', panelHTML);

  const $ = (id) => document.getElementById(id);
  const elToggle = $('botToggle');
  const elPanel  = $('botPanel');
  const elStatus = $('botStatus');
  const elClose  = $('botClose');
  const elLog    = $('botLog');
  const elInput  = $('botInput');
  const elSend   = $('botSend');

  function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, c => (
      { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#039;' }[c]
    ));
  }

  function addLine(text, who) {
    const div = document.createElement('div');
    div.className = `msg ${who}`;
    div.innerHTML = escapeHtml(text).replace(/\n/g, '<br>');
    elLog.appendChild(div);
    elLog.scrollTop = elLog.scrollHeight;
  }

  function setBusy(b) {
    elSend.disabled = b;
    elInput.disabled = b;
    elStatus.style.display = b ? 'inline-block' : 'none';
    elStatus.textContent = b ? T.sending : '';
  }

  function openPanel() {
    elPanel.style.display = 'block';
    elInput.focus();
    if (!elPanel._greeted) {
      addLine(T.hello, 'bot');
      elPanel._greeted = true;
    }
  }
  function closePanel() { elPanel.style.display = 'none'; }

  elToggle.addEventListener('click', () => {
    if (elPanel.style.display === 'none' || !elPanel.style.display) openPanel();
    else closePanel();
  });
  elClose.addEventListener('click', closePanel);
  elInput.addEventListener('keydown', (e) => { if (e.key === 'Enter') send(); });
  elSend.addEventListener('click', send);

  // If backend isn't configured, leave the toggle hidden and bail
  if (!BACKEND_URL) return;

  // Health check — only show toggle if backend is up AND has GROQ_API_KEY
  fetch(BACKEND_URL + '/health')
    .then(r => r.json())
    .then(j => {
      if (j && j.ok && j.groq_configured) {
        elToggle.style.display = 'inline-block';
      }
    })
    .catch(() => { /* keep hidden on failure */ });

  async function send() {
    const msg = (elInput.value || '').trim();
    if (!msg) return;
    addLine(msg, 'user');
    elInput.value = '';
    setBusy(true);
    try {
      const res = await fetch(BACKEND_URL + '/groq-chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: msg, lang })
      });
      const j = await res.json().catch(() => ({ ok: false, reply: T.error }));
      if (j && (j.ok === true || j.reply)) {
        addLine(j.reply || T.error, 'bot');
      } else {
        addLine((j && j.error) || T.error, 'bot');
      }
    } catch (e) {
      addLine(T.error, 'bot');
    } finally {
      setBusy(false);
    }
  }
})();
