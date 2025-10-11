
import os, requests
from flask import Flask, render_template, redirect, url_for, request, jsonify, Blueprint
from functools import wraps


MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")  # ← same as physics

app = Flask(__name__)
app.url_map.strict_slashes = False  # /api/groq_chat and /api/groq_chat/ both work
pages = Blueprint('pages', __name__)

def with_lang(fn):
    @wraps(fn)
    def _inner(lang='en', *a, **k):
        if lang not in ('en','ru'):
            return redirect(url_for(f'pages.{fn.__name__}', lang='en', **k))
        return fn(lang, *a, **k)
    return _inner

@app.route('/')
def home(): return redirect(url_for('pages.manifesto', lang='en'))

@pages.route('/<lang>/manifesto')
@with_lang
def manifesto(lang): return render_template(f'{lang}/manifesto.html', active='manifesto', lang=lang)

@pages.route('/<lang>/death')
@with_lang
def death(lang): return render_template(f'{lang}/death.html', active='death', lang=lang)

@pages.route('/<lang>/math')
@with_lang
def math(lang): return render_template(f'{lang}/math.html', active='math', lang=lang)

@pages.route('/<lang>/overview')
@with_lang
def overview(lang): return render_template(f'{lang}/overview.html', active='overview', lang=lang)

@pages.route('/<lang>/altruists')
@with_lang
def altruists(lang): return render_template(f'{lang}/altruists.html', active='altruists', lang=lang)

@pages.route('/<lang>/faq')
@with_lang
def faq(lang): return render_template(f'{lang}/faq.html', active='faq', lang=lang)

@pages.route('/<lang>/appendix')
@with_lang
def appendix(lang): return render_template(f'{lang}/appendix.html', active='appendix', lang=lang)

app.register_blueprint(pages)

# ---- Diagnostics endpoints (optional but very helpful) ----
@app.get('/health/env')
def health_env():
    present = bool(os.getenv('GROQ_API_KEY'))
    return jsonify(ok=True, groq_present=present)

@app.route('/api/groq_chat', methods=['POST','GET'])
def groq_chat():
    key = os.getenv('GROQ_API_KEY')
    if not key:
        return jsonify({'ok': False, 'reply': '[GROQ_API_KEY not set on server]'}), 500

    msg = (request.get_json(silent=True) or {}).get('message') if request.method=='POST' else request.args.get('message')
    msg = (msg or '').strip()
    if not msg:
        return jsonify({'ok': False, 'reply': 'Provide "message"'}), 400

    url = 'https://api.groq.com/openai/v1/chat/completions'
    headers = {'Authorization': f'Bearer {key}', 'Content-Type': 'application/json', 'Accept': 'application/json'}
    payload = {
        'model': MODEL,  # ← llama-3.3-70b-versatile
        'messages': [
            {'role':'system','content':'Act as a precise, urgent site guide; cite math clearly.'},
            {'role':'user','content': msg}
        ],
        'temperature': 0.3,
        'max_tokens': 500,
    }
    r = requests.post(url, json=payload, headers=headers, timeout=45)
    try:
        j = r.json()
    except Exception:
        j = {'error': {'message': r.text}}
    if r.status_code != 200:
        err = (j.get('error') or {}).get('message') or r.text
        return jsonify({'ok': False, 'reply': f'[Groq {r.status_code}] {err}'}), 500

    txt = (j.get('choices') or [{}])[0].get('message', {}).get('content', '').strip() or '[empty]'
    return jsonify({'ok': True, 'reply': txt})    
    
if __name__ == '__main__':
    # Turn off the reloader when running inside Spyder/Jupyter
    app.run(host='127.0.0.1', port=5000, debug=False, use_reloader=False)
