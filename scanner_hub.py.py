"""
AutoScanner Pro — Hub v3  (scanner_hub)
------------------------------------------
ARCHITECTURE CHANGE: Hub now calculates ALL technicals server-side.
Browser receives ready-to-render data — zero Yahoo calls from browser.

Requirements:  pip install flask flask-cors requests
Deploy:        Render / Railway / Fly.io (see deploy steps below)
Start locally: python scanner_hub.py
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests, datetime, time, random, threading, json

app = Flask(__name__)
CORS(app)

# ── Default criteria (matches HTML defaults) ──────────────────────────────────
DEFAULT_CFG = {
    'rvol': 2.5, 'atr': 3.0, 'rvolsw': 1.5,
    'ema': 3, 'gap': 3.0, 'rmin': 50, 'rmax': 72,
}

PDF = {
    'ORCL':'GAP','CRM':'GAP','DDOG':'GAP','NOW':'GAP','WDAY':'GAP',
    'FTNT':'GAP','HPE':'GAP','INTU':'GAP','GDDY':'STD','CRDO':'STD',
    'MSFT':'STD','AMD':'STD','PANW':'STD','QCOM':'STD','AMZN':'STD',
    'META':'STD','GOOGL':'STD','NFLX':'STD','NVDA':'HB','AVGO':'HB',
    'PLTR':'HB','SMCI':'HB','SOXL':'HB','TQQQ':'HB','COIN':'HB',
}

FALLBACK_POOL = [
    'NVDA','AMD','TSLA','MRVL','SMCI','PLTR','COIN','AVGO','QCOM','ORCL',
    'CRM','META','AMZN','MSFT','GOOGL','AAPL','DDOG','NOW','WDAY','FTNT',
]

SCREENER_URLS = {
    'most_active':      'https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?scrIds=most_actives&count=50',
    'day_gainers':      'https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?scrIds=day_gainers&count=50',
    'day_losers':       'https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?scrIds=day_losers&count=50',
    'trending':         'https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?scrIds=most_actives&count=50',
    'week52_high':      'https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?scrIds=day_gainers&count=50',
    'small_cap':        'https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?scrIds=small_cap_gainers&count=50',
    'large_cap':        'https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?scrIds=large_cap_gainers&count=50',
    'highest_dividend': 'https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?scrIds=high_yield_bond&count=50',
    'unusual_volume':   'https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?scrIds=most_actives&count=50',
    'highest_beta':     'https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?scrIds=most_actives&count=50',
    'most_expensive':   'https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?scrIds=most_actives&count=50',
    'pink_sheet':       'https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?scrIds=most_actives&count=50',
}

# ── Thread-local session ──────────────────────────────────────────────────────
_local = threading.local()

def get_session():
    if not hasattr(_local, 'session'):
        s = requests.Session()
        s.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9',
        })
        retry = Retry(total=2, backoff_factor=0.4, status_forcelist=[500,502,503,504], allowed_methods=['GET'])
        adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=20)
        s.mount('https://', adapter)
        s.mount('http://', adapter)
        _local.session = s
    return _local.session

# ══ MATH ═════════════════════════════════════════════════════════════════════

def ema_array(closes, period):
    if len(closes) < period:
        return [None] * len(closes)
    k = 2 / (period + 1)
    out = [None] * len(closes)
    out[period-1] = sum(closes[:period]) / period
    for i in range(period, len(closes)):
        out[i] = closes[i] * k + out[i-1] * (1 - k)
    return out

def ema_last(closes, period):
    arr = ema_array(closes, period)
    return next((v for v in reversed(arr) if v is not None), None)

def rsi(closes, p=14):
    if len(closes) < p+1: return None
    g = l = 0.0
    for i in range(len(closes)-p, len(closes)):
        d = closes[i] - closes[i-1]
        if d > 0: g += d
        else:     l -= d
    ag, al = g/p, l/p
    return 100.0 if al == 0 else 100 - 100/(1 + ag/al)

def vwap(closes, highs, lows, vols, n=20):
    if not closes: return None
    n = min(len(closes), n)
    tv = tpv = 0.0
    for i in range(len(closes)-n, len(closes)):
        tp = (highs[i]+lows[i]+closes[i])/3
        v  = vols[i] or 1
        tpv += tp*v; tv += v
    return tpv/tv if tv else None

def atr(highs, lows, closes, p=14):
    if len(highs) < p+1: return None
    trs = [max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
           for i in range(len(highs)-p, len(highs))]
    return sum(trs)/len(trs)

def macd(closes):
    if len(closes) < 35: return None
    e12 = ema_array(closes, 12); e26 = ema_array(closes, 26)
    ml  = [e12[i]-e26[i] for i in range(len(closes)) if e12[i] is not None and e26[i] is not None]
    if len(ml) < 9: return None
    line = ml[-1]
    sa   = ema_array(ml, 9)
    sig  = next((v for v in reversed(sa) if v is not None), None)
    hist = line - sig if sig is not None else None
    return {'line': round(line,6), 'sig': round(sig,6) if sig else None,
            'hist': round(hist,6) if hist is not None else None,
            'bull': hist > 0 if hist is not None else None}

def detect_cross(fa, sa, lookback=15):
    fv, sv = [], []
    for i in range(len(fa)-1, -1, -1):
        if fa[i] is not None and sa[i] is not None:
            fv.insert(0, fa[i]); sv.insert(0, sa[i])
            if len(fv) >= lookback: break
    if len(fv) < 2: return None
    for i in range(len(fv)-1, 0, -1):
        d = len(fv)-1-i
        if fv[i-1] <= sv[i-1] and fv[i] > sv[i]: return {'bull':True,  'daysAgo':d, 'fresh':d<=2}
        if fv[i-1] >= sv[i-1] and fv[i] < sv[i]: return {'bull':False, 'daysAgo':d, 'fresh':d<=2}
    return None

def trend_score(d):
    return sum([
        bool(d.get('price') and d.get('e9')   and d['price'] > d['e9']),
        bool(d.get('e9')    and d.get('e20')  and d['e9']    > d['e20']),
        bool(d.get('e20')   and d.get('e200') and d['e20']   > d['e200']),
        bool(d.get('price') and d.get('e200') and d['price'] > d['e200']),
    ])

def calc_bias(d):
    sc = 0
    if d.get('e200'):
        p = (d['price']-d['e200'])/d['e200']*100
        sc += 2 if p>2 else 1 if p>0 else -1 if p>-2 else -2
    if d.get('e9') and d.get('e20'):
        s = (d['e9']-d['e20'])/d['price']*100
        sc += 2 if s>0.5 else 1 if s>0 else -1 if s>-0.5 else -2
    if d.get('macd'):
        m = d['macd']
        s = abs(m.get('hist',0) or 0)/d['price']*100 if d['price'] else 0
        if m.get('bull') is True:  sc += 2 if s>0.5 else 1
        elif m.get('bull') is False: sc -= 2 if s>0.5 else 1
    r = d.get('rsi') or 0
    sc += 2 if r>=60 else 1 if r>=50 else -1 if r>=40 else -2
    if d.get('vwap'): sc += 1 if (d['price']-d['vwap'])/d['vwap']*100 > 0 else -1
    n = max(-10, min(10, sc))
    if   n>=6:  lb,cls = '↑↑ Bull','sb'
    elif n>=2:  lb,cls = '↑ Bull', 'b'
    elif n>=-1: lb,cls = 'Neutral','n'
    elif n>=-5: lb,cls = '↓ Bear', 'br'
    else:       lb,cls = '↓↓ Bear','sbr'
    return {'score':n, 'label':lb, 'cls':cls}

def conf_score(d, reg, cfg):
    sc = 0
    rv = d.get('rvol') or 0
    sc += 2 if rv>=cfg['rvol'] else 1 if rv>=cfg['rvol']*0.7 else 0
    ap = d.get('atrPct') or 0
    sc += 2 if ap>=cfg['atr'] else 1 if ap>=cfg['atr']*0.7 else 0
    ts = trend_score(d); sc += 2 if ts==4 else 1 if ts>=2 else 0
    if d.get('rsi') and cfg['rmin']<=d['rsi']<=cfg['rmax']: sc+=1
    if d.get('e9') and d.get('e20') and d['e9']>d['e20']:   sc+=1
    if d.get('vwap') and d['price']>d['vwap']:               sc+=1
    if d.get('macd') and d['macd'].get('bull') is True:      sc+=1
    if reg=='GAP' and abs(d.get('gapPct',0))>=cfg['gap']:    sc+=1
    if d.get('atrAbs') and d['atrAbs']>=0.5:                 sc+=1
    if d.get('chgPct',0)>0:                                  sc+=1
    return round(sc/13*10, 1)

def classify_type(d, reg, cfg):
    is_scalp = (d.get('rvol',0)>=cfg['rvol'] and d.get('atrPct',0)>=cfg['atr'] and reg in ('GAP','HB'))
    is_swing = (trend_score(d)>=cfg['ema'] and cfg['rmin']<=(d.get('rsi') or 0)<=cfg['rmax'] and d.get('rvol',0)>=cfg['rvolsw'])
    if is_scalp and is_swing: return 'BOTH'
    if is_scalp: return 'SCALP'
    if is_swing: return 'SWING'
    ss = sum([d.get('rvol',0)>=cfg['rvol'], d.get('atrPct',0)>=cfg['atr'], reg in ('GAP','HB')])
    ws = (2 if trend_score(d)>=cfg['ema'] else 0) + (1 if cfg['rmin']<=(d.get('rsi') or 0)<=cfg['rmax'] else 0) + (1 if d.get('rvol',0)>=cfg['rvolsw'] else 0)
    return 'SCALP' if ss>=ws else 'SWING'

def calc_est(d):
    entry    = d.get('e9') or d['price']
    atr_stop = entry - 1.5*d['atrAbs'] if d.get('atrAbs') else entry*0.98
    vwap_s   = d['vwap'] if (d.get('vwap') and d['vwap']<d['price']) else None
    stop     = max(vwap_s, atr_stop) if vwap_s else atr_stop
    risk     = entry - stop
    target   = entry + risk*2 if risk>0 else entry*1.04
    rr       = round((target-entry)/risk, 1) if risk>0 else None
    return {'entry':round(entry,4), 'stop':round(stop,4), 'target':round(target,4), 'rr':rr}

def entry_status(d, bias):
    if not d.get('e9'): return {'status':'WATCH','pct':0}
    pct = (d['price']-d['e9'])/d['e9']*100
    is_bull = bias['cls'] in ('sb','b')
    is_bear = bias['cls'] in ('sbr','br')
    if is_bear:              return {'status':'BEAR',    'pct':round(pct,2)}
    if pct<=0.5 and is_bull: return {'status':'GO',      'pct':round(pct,2)}
    if pct<=1.5:             return {'status':'WATCH',   'pct':round(pct,2)}
    if pct<=3.0:             return {'status':'WAIT',    'pct':round(pct,2)}
    return                          {'status':'CHASING', 'pct':round(pct,2)}

def build_sigs(d):
    return [
        {'k':'9E',  'on': bool(d.get('price') and d.get('e9')   and d['price']>d['e9'])},
        {'k':'20E', 'on': bool(d.get('e9')    and d.get('e20')  and d['e9']   >d['e20'])},
        {'k':'50E', 'on': bool(d.get('price') and d.get('e50')  and d['price']>d['e50'])},
        {'k':'200', 'on': bool(d.get('price') and d.get('e200') and d['price']>d['e200'])},
        {'k':'VWP', 'on': bool(d.get('vwap')  and d['price']>d['vwap'])},
        {'k':'MAC', 'on': bool(d.get('macd')  and d['macd'].get('bull') is True)},
    ]

# ══ CORE FETCHER ══════════════════════════════════════════════════════════════

def fetch_and_analyse(symbol, cfg=None, retry=True):
    if cfg is None: cfg = DEFAULT_CFG
    session   = get_session()
    intra_url = f'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1m&range=1d&includePrePost=true'
    daily_url = f'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=12mo&includePrePost=true'
    short_url = f'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=3mo&includePrePost=true'

    state = regular = pre = post = last_tick = None

    # Step 1: intraday
    try:
        r = session.get(intra_url, timeout=8)
        if r.status_code == 429:
            time.sleep(random.uniform(1.5, 3.0)); r = session.get(intra_url, timeout=8)
        r.raise_for_status()
        res = r.json().get('chart',{}).get('result',[None])[0]
        if res:
            m = res.get('meta',{})
            state=m.get('marketState'); regular=m.get('regularMarketPrice')
            pre=m.get('preMarketPrice'); post=m.get('postMarketPrice')
            q1m = res.get('indicators',{}).get('quote',[{}])[0]
            c1m = [v for v in (q1m.get('close') or []) if v is not None]
            if c1m: last_tick = c1m[-1]
    except Exception as e:
        print(f'[Hub] {symbol} intraday: {e}')

    # Step 2: daily (try 12mo then 3mo)
    daily = None
    for url in [daily_url, short_url]:
        try:
            r2 = session.get(url, timeout=10)
            if r2.status_code == 429:
                time.sleep(random.uniform(1.5,3.0)); r2 = session.get(url, timeout=10)
            r2.raise_for_status()
            res2 = r2.json().get('chart',{}).get('result',[None])[0]
            if not res2: continue
            m2 = res2.get('meta',{}); q = res2.get('indicators',{}).get('quote',[{}])[0]
            closes = [v for v in (q.get('close') or [])  if v is not None]
            highs  = [v for v in (q.get('high')  or [])  if v is not None]
            lows   = [v for v in (q.get('low')   or [])  if v is not None]
            vols   = [v for v in (q.get('volume')or [])  if v is not None]
            opens  = [v for v in (q.get('open')  or [])  if v is not None]
            if len(closes) < 30: continue
            if not state:   state   = m2.get('marketState')
            if not regular: regular = m2.get('regularMarketPrice') or closes[-1]
            if not pre:     pre     = m2.get('preMarketPrice')
            if not post:    post    = m2.get('postMarketPrice')
            prev     = m2.get('chartPreviousClose') or m2.get('regularMarketPreviousClose')
            name_raw = m2.get('shortName') or m2.get('longName') or symbol
            earn_ts  = m2.get('earningsTimestamp')
            daily = {'closes':closes,'highs':highs,'lows':lows,'vols':vols,
                     'opens':opens,'prev':prev,'name':name_raw,'earn_ts':earn_ts}
            break
        except Exception as e:
            print(f'[Hub] {symbol} daily: {e}')

    if not regular and not pre and not post:
        if retry:
            time.sleep(random.uniform(0.8,1.5))
            return fetch_and_analyse(symbol, cfg, retry=False)
        return None
    if not daily: return None

    # Resolve price/mode
    s = (state or '').upper()
    if   s == 'REGULAR':              price,mode = last_tick or regular, 'LIVE'
    elif s in ('PRE','PREPRE'):        price = pre or last_tick or regular; mode = 'PRE-MKT' if (pre or last_tick) else 'CLOSE'
    elif s in ('POST','POSTPOST'):     price = post or last_tick or regular; mode = 'AFTER-HRS' if (post or last_tick) else 'CLOSE'
    elif s == 'CLOSED':
        if pre:                         price,mode = pre,'PRE-MKT'
        elif post:                      price,mode = post,'AFTER-HRS'
        elif last_tick and last_tick!=regular: price,mode = last_tick,'AFTER-HRS'
        else:                           price,mode = regular,'CLOSE'
    else:
        if pre:       price,mode = pre,'PRE-MKT'
        elif post:    price,mode = post,'AFTER-HRS'
        elif last_tick: price,mode = last_tick,'CLOSE'
        else:         price,mode = regular,'CLOSE'

    if not price: return None
    price = float(price)

    # Technicals
    closes=daily['closes']; highs=daily['highs']; lows=daily['lows']
    vols=daily['vols']; opens=daily['opens']; prev=daily['prev']

    e4a  = ema_array(closes,4);  e9a  = ema_array(closes,9)
    e20a = ema_array(closes,20); e50a = ema_array(closes,50); e200a= ema_array(closes,200)
    e4   = next((v for v in reversed(e4a)   if v),None)
    e9   = next((v for v in reversed(e9a)   if v),None)
    e20  = next((v for v in reversed(e20a)  if v),None)
    e50  = next((v for v in reversed(e50a)  if v),None)
    e200 = next((v for v in reversed(e200a) if v),None)

    rsi_v  = rsi(closes)
    vwap_v = vwap(closes,highs,lows,vols)
    atr_v  = atr(highs,lows,closes)
    atr_pct= (atr_v/price*100) if atr_v and price else None
    macd_v = macd(closes)
    avg_vol= sum(vols[-20:])/20 if len(vols)>=20 else (sum(vols)/len(vols) if vols else None)
    rvol   = (vols[-1]/avg_vol) if avg_vol and vols else None
    chg    = ((price-float(prev))/float(prev)*100) if prev else 0.0
    gap    = ((opens[-1]-float(prev))/float(prev)*100) if (prev and opens) else 0.0
    c9_20  = detect_cross(e9a,e20a)
    c4_9   = detect_cross(e4a,e9a)
    dte    = round((daily['earn_ts']*1000-time.time()*1000)/86400000) if daily.get('earn_ts') else None
    name   = daily['name']
    for sfx in [', Inc.','Corp.','Ltd.','LLC','Inc.']: name=name.replace(sfx,'').strip().rstrip(',')

    reg  = PDF.get(symbol, 'GAP' if abs(gap)>=cfg['gap'] else 'STD')
    base = {
        'ticker':symbol,'name':name,'price':round(price,4),'priceMode':mode,
        'marketState':s or 'CLOSED','chgPct':round(chg,2),'gapPct':round(gap,2),
        'e4':round(e4,4) if e4 else None,'e9':round(e9,4) if e9 else None,
        'e20':round(e20,4) if e20 else None,'e50':round(e50,4) if e50 else None,
        'e200':round(e200,4) if e200 else None,
        'rsi':round(rsi_v,1) if rsi_v else None,
        'vwap':round(vwap_v,4) if vwap_v else None,
        'atrAbs':round(atr_v,4) if atr_v else None,
        'atrPct':round(atr_pct,2) if atr_pct else None,
        'macd':macd_v,'rvol':round(rvol,2) if rvol else None,
        'avgVol':round(avg_vol) if avg_vol else None,
        'cross9_20':c9_20,'cross4_9':c4_9,'daysToEarn':dte,
        'prevClose':round(float(prev),4) if prev else None,
        'preMarket':round(float(pre),4) if pre else None,
        'postMarket':round(float(post),4) if post else None,
        'lastTick':round(float(last_tick),4) if last_tick else None,'reg':reg,
    }

    bias = calc_bias(base); est = calc_est(base); es = entry_status(base,bias)
    sc   = conf_score(base,reg,cfg); ttype = classify_type(base,reg,cfg)
    ts   = trend_score(base); tier = 1 if sc>=7 else 2 if sc>=5 else 3
    sigs = build_sigs(base); sig_cnt = sum(1 for s in sigs if s['on'])
    cs20 = (10 if c9_20.get('fresh') else max(0,10-c9_20['daysAgo'])) if c9_20 else 0
    cs49 = (10 if c4_9.get('fresh')  else max(0,10-c4_9['daysAgo']))  if c4_9  else 0

    return {**base,
            'bias':bias,'biasScore':bias['score'],
            'entry':est['entry'],'stop':est['stop'],'target':est['target'],'rr':est['rr'],
            'entryStatus':es['status'],'entryPct':es['pct'],
            'confScore':sc,'tradeType':ttype,'trendScore':ts,'tier':tier,
            'sigs':sigs,'sigCount':sig_cnt,'crossScore':cs20,'cross49Score':cs49,
            'macdBull':(1 if macd_v and macd_v.get('bull') is True else -1 if macd_v and macd_v.get('bull') is False else 0),
            'isExtended':es['status'] in ('WAIT','CHASING'),'extPct':es['pct']}

# ══ PARALLEL FETCH ════════════════════════════════════════════════════════════
MAX_WORKERS = 10

def fetch_all(pool, cfg=None):
    results = {}
    def go(sym):
        time.sleep(random.uniform(0, 0.25))
        return sym, fetch_and_analyse(sym, cfg)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for sym, data in [f.result() for f in as_completed({ex.submit(go,s):s for s in pool})]:
            if data: results[sym] = data
    return [results[s] for s in pool if s in results]

def resolve_screener_pool(screen_id, count):
    url = SCREENER_URLS.get(screen_id, SCREENER_URLS['most_active'])
    try:
        r = get_session().get(url, timeout=8); r.raise_for_status()
        quotes = r.json().get('finance',{}).get('result',[{}])[0].get('quotes',[])
        tickers = [q['symbol'] for q in quotes if q.get('symbol')][:count+8]
        if tickers: return tickers
    except Exception as e:
        print(f'[Hub] screener failed: {e}')
    return FALLBACK_POOL[:count+8]

# ══ ROUTES ════════════════════════════════════════════════════════════════════

@app.route('/api/scan')
def scan():
    tickers_param = request.args.get('tickers','')
    screen_id     = request.args.get('screen','')
    count         = int(request.args.get('count',20))
    cfg = {**DEFAULT_CFG}
    try:
        ov = json.loads(request.args.get('cfg','{}'))
        cfg.update({k:float(v) for k,v in ov.items() if k in cfg})
        cfg['ema'] = int(cfg['ema'])
    except: pass

    if tickers_param:
        pool = [t.strip().upper() for t in tickers_param.split(',') if t.strip()]
    elif screen_id:
        pool = resolve_screener_pool(screen_id, count)
    else:
        pool = FALLBACK_POOL[:count]

    t0 = time.time()
    results = fetch_all(pool, cfg)
    elapsed = round(time.time()-t0, 2)
    results.sort(key=lambda d: d.get('confScore',0), reverse=True)
    if not tickers_param: results = results[:count]

    ts = datetime.datetime.now().strftime('%H:%M:%S')
    modes = {}
    for r in results: modes[r['priceMode']] = modes.get(r['priceMode'],0)+1
    print(f'[Hub] {ts} — {len(results)}/{len(pool)} | {elapsed}s | {modes}')
    return jsonify({'results':results,'elapsed':elapsed,'pool':len(pool),'ts':ts})

@app.route('/api/quote/<symbol>')
def quote(symbol):
    data = fetch_and_analyse(symbol.upper())
    if not data: return jsonify({'error':'not found'}),404
    return jsonify(data)

@app.route('/health')
def health():
    return jsonify({'status':'ok','time':datetime.datetime.now().isoformat(),'workers':MAX_WORKERS})

if __name__ == '__main__':
    print('='*54)
    print('  AutoScanner Hub v3  —  http://localhost:8080')
    print(f'  Workers: {MAX_WORKERS}  |  All technicals server-side')
    print('  Press Ctrl+C to stop.')
    print('='*54)
    app.run(host='0.0.0.0', port=8080, debug=False, use_reloader=False)
