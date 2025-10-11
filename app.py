
import os, requests
from flask import Flask, render_template, redirect, url_for, request, jsonify, Blueprint
from functools import wraps

app = Flask(__name__)
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

@app.post('/api/groq_chat')
def groq_chat():
    key = os.getenv('GROQ_API_KEY', '')
    data = request.get_json(silent=True) or {}
    msg = (data.get('message') or '').strip()
    if not key: return jsonify({'reply':'[Set GROQ_API_KEY to enable the assistant]'})
    if not msg: return jsonify({'reply':'Ask about the manifesto, math, or governance.'})
    try:
        r = requests.post('https://api.groq.com/openai/v1/chat/completions',
            headers={'Authorization':f'Bearer {key}','Content-Type':'application/json'},
            json={'model':'llama-3.1-70b-versatile',
                  'messages':[
                    {'role':'system','content':'Act as a precise, urgent site guide; cite math clearly.'},
                    {'role':'user','content': msg}
                  ],
                  'temperature':0.4,'max_tokens':500},
            timeout=25)
        j = r.json()
        txt = (j.get('choices') or [{}])[0].get('message',{}).get('content','[no content]')
        return jsonify({'reply':txt})
    except Exception as e:
        return jsonify({'reply':f'[error: {e}]'}), 500

if __name__ == '__main__':
    # Turn off the reloader when running inside Spyder/Jupyter
    app.run(host='127.0.0.1', port=5000, debug=False, use_reloader=False)
