(() => {
  const lang = document.body.getAttribute('data-lang') === 'ru' ? 'ru' : 'en';

  // i18n strings
  const T = {
    en: {
      toggle: 'Chat · Assistant',
      title: 'Groq Assistant',
      placeholder: 'Ask about the manifesto, math, governance…',
      sending: 'Sending…',
      offline: 'Assistant unavailable',
      hello: 'Hi! Ask me about the manifesto, math, or governance.',
      error: 'Server error',
    },
    ru: {
      toggle: 'Чат · Ассистент',
      title: 'Ассистент Groq',
      placeholder: 'Спросите про манифест, математику, управление…',
      sending: 'Отправка…',
      offline: 'Ассистент недоступен',
      hello: 'Привет! Спрашивайте про манифест, математику или устройство.',
      error: 'Ошибка сервера',
    }
  }[lang];

  const $ = (id) => document.getElementById(id);
  const elToggle = $('botToggle');
  const elPanel  = $('botPanel');
  const elTitle  = $('botTitle');
  const elStatus = $('botStatus');
  const elClose  = $('botClose');
  const elLog    = $('botLog');
  const elInput  = $('botInput');
  const elSend   = $('botSend');

  // Apply language
  elToggle.textContent = T.toggle;
  elTitle.textContent  = T.title;
  elInput.placeholder  = T.placeholder;

  // Helpers
  function addLine(text, who) {
    const div = document.createElement('div');
    div.className = who === 'user' ? 'line user' : 'line bot';
    div.style.margin = '6px 0';
    div.innerHTML = (who==='user' ? '🧑 ' : '🤖 ') + escapeHtml(text);
    elLog.appendChild(div);
    elLog.scrollTop = elLog.scrollHeight;
  }
  function escapeHtml(s){ return String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#039;'}[c])); }
  function setBusy(b){
    elSend.disabled = b;
    elInput.disabled = b;
    elStatus.style.display = b ? 'inline-block' : 'none';
    elStatus.textContent = b ? T.sending : '';
  }

  // Show panel
  function openPanel(){
    elPanel.style.display = 'block';
    elInput.focus();
    if(!elPanel._greeted){
      addLine(T.hello, 'bot');
      elPanel._greeted = true;
    }
  }
  function closePanel(){ elPanel.style.display = 'none'; }

  // Wire UI
  elToggle.addEventListener('click', () => {
    if(elPanel.style.display === 'none') openPanel(); else closePanel();
  });
  elClose.addEventListener('click', closePanel);
  elInput.addEventListener('keydown', (e) => { if(e.key === 'Enter') send(); });
  elSend.addEventListener('click', send);

  // Health check — if key is present, show toggle; if not, hide chat entirely
  fetch('/health/env').then(r => r.json()).then(j => {
    if(j && j.ok && j.groq_present){
      elToggle.style.display = 'inline-block';
    } else {
      // Don’t show an “add key” nag; simply keep the chat hidden
      elToggle.style.display = 'none';
      elPanel.style.display  = 'none';
    }
  }).catch(() => {
    // If health endpoint is missing, still show toggle; backend may still work
    elToggle.style.display = 'inline-block';
  });

  async function send(){
    const msg = (elInput.value || '').trim();
    if(!msg) return;
    addLine(msg, 'user');
    elInput.value = '';
    setBusy(true);
    try{
      const res = await fetch('/api/groq_chat', {
        method: 'POST',
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify({ message: msg })
      });
      const j = await res.json().catch(()=>({ ok:false, reply:T.error }));
      if(j && (j.ok === true || j.reply)){
        addLine(j.reply || T.error, 'bot');
      }else{
        addLine(T.error, 'bot');
      }
    }catch(e){
      addLine(T.error, 'bot');
    }finally{
      setBusy(false);
    }
  }
})();