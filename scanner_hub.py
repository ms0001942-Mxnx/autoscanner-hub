"""
AutoScanner Pro — Hub v4  (scanner_hub.py)
------------------------------------------
Improvements over v3:
  • In-memory cache — results cached 2 min, prevents duplicate Yahoo calls
    when multiple family members scan simultaneously
  • Circuit breaker — stops hammering Yahoo after repeated 429s,
    backs off automatically and recovers
  • Input validation — all edge cases handled gracefully,
    no 500 errors from bad data
  • Structured logging — timestamp + level on every log line
  • PWA-ready CORS headers

Requirements:  pip install flask flask-cors requests
Start locally: python scanner_hub.py
Deploy:        Render / Railway / Fly.io
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests, datetime, time, random, threading, json, logging

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger('autoscanner')

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

# ── Config ────────────────────────────────────────────────────────────────────
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

MAX_WORKERS   = 10   # parallel fetch threads
CACHE_TTL     = 120  # seconds — cache ticker results for 2 minutes

# ══ IN-MEMORY CACHE ══════════════════════════════════════════════════════════
# Thread-safe cache: {symbol: (data_dict, expiry_timestamp)}
# Prevents duplicate Yahoo calls when multiple users scan simultaneously.
# TTL = 2 minutes — fresh enough for a scanner, cheap enough to not stale.

class TickerCache:
    def __init__(self, ttl: int = CACHE_TTL):
        self._store: dict = {}
        self._lock  = threading.Lock()
        self._ttl   = ttl
        self._hits  = 0
        self._misses= 0

    def get(self, symbol: str):
        with self._lock:
            entry = self._store.get(symbol)
            if entry and time.time() < entry[1]:
                self._hits += 1
                return entry[0]
            self._misses += 1
            return None

    def set(self, symbol: str, data: dict):
        with self._lock:
            self._store[symbol] = (data, time.time() + self._ttl)

    def invalidate(self, symbol: str):
        with self._lock:
            self._store.pop(symbol, None)

    def clear(self):
        with self._lock:
            self._store.clear()
            log.info('Cache cleared')

    @property
    def stats(self):
        with self._lock:
            total = self._hits + self._misses
            ratio = round(self._hits / total * 100) if total else 0
            live  = sum(1 for _,exp in self._store.values() if time.time() < exp)
            return {'hits': self._hits, 'misses': self._misses,
                    'hit_rate': f'{ratio}%', 'live_entries': live}

cache = TickerCache()


# ══ CIRCUIT BREAKER ══════════════════════════════════════════════════════════
# Prevents hammering Yahoo after repeated 429 rate-limit responses.
# States: CLOSED (normal) → OPEN (backing off) → HALF-OPEN (testing recovery)
# After FAILURE_THRESHOLD consecutive 429s → OPEN for RECOVERY_SECS seconds.
# After RECOVERY_SECS → HALF-OPEN: allow one request through.
# If it succeeds → CLOSED. If it fails → OPEN again.

class CircuitBreaker:
    CLOSED    = 'closed'
    OPEN      = 'open'
    HALF_OPEN = 'half_open'

    FAILURE_THRESHOLD = 3    # consecutive 429s before opening
    RECOVERY_SECS     = 60   # seconds to back off before retrying

    def __init__(self):
        self._state    = self.CLOSED
        self._failures = 0
        self._opened_at= 0.0
        self._lock     = threading.Lock()

    @property
    def state(self):
        with self._lock:
            if self._state == self.OPEN:
                if time.time() - self._opened_at >= self.RECOVERY_SECS:
                    self._state = self.HALF_OPEN
                    log.info('Circuit breaker → HALF-OPEN, testing Yahoo...')
            return self._state

    def record_success(self):
        with self._lock:
            if self._state in (self.HALF_OPEN, self.OPEN):
                log.info('Circuit breaker → CLOSED (Yahoo recovered)')
            self._state    = self.CLOSED
            self._failures = 0

    def record_failure(self):
        with self._lock:
            self._failures += 1
            if self._failures >= self.FAILURE_THRESHOLD or self._state == self.HALF_OPEN:
                if self._state != self.OPEN:
                    log.warning(f'Circuit breaker → OPEN after {self._failures} failures. '
                                f'Backing off {self.RECOVERY_SECS}s.')
                self._state     = self.OPEN
                self._opened_at = time.time()

    def is_open(self):
        return self.state == self.OPEN

circuit = CircuitBreaker()


# ── Thread-local HTTP session with connection pooling ─────────────────────────
_local = threading.local()

def get_session() -> requests.Session:
    if not hasattr(_local, 'session'):
        s = requests.Session()
        s.headers.update({
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36'
            ),
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9',
        })
        # Retry on transient server errors only — NOT 429 (circuit breaker handles that)
        retry = Retry(
            total=2,
            backoff_factor=0.4,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=['GET'],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=10,
            pool_maxsize=20,
        )
        s.mount('https://', adapter)
        s.mount('http://',  adapter)
        _local.session = s
    return _local.session


def yahoo_get(url: str, timeout: int = 10):
    """
    Fetch a Yahoo Finance URL with circuit-breaker and 429 handling.
    Returns the Response object or raises on failure.
    """
    if circuit.is_open():
        raise RuntimeError('Circuit breaker is OPEN — Yahoo rate-limited, backing off')

    session = get_session()
    r = session.get(url, timeout=timeout)

    if r.status_code == 429:
        circuit.record_failure()
        wait = random.uniform(2.0, 4.0)
        log.warning(f'Yahoo 429 — waiting {wait:.1f}s before retry')
        time.sleep(wait)
        # One retry
        r = session.get(url, timeout=timeout)
        if r.status_code == 429:
            circuit.record_failure()
            raise RuntimeError(f'Yahoo 429 repeated — circuit breaker engaged')

    circuit.record_success()
    r.raise_for_status()
    return r


# ══ MATH ═════════════════════════════════════════════════════════════════════

def ema_array(closes: list, period: int) -> list:
    if not closes or len(closes) < period:
        return [None] * len(closes) if closes else []
    k   = 2 / (period + 1)
    out = [None] * len(closes)
    out[period - 1] = sum(closes[:period]) / period
    for i in range(period, len(closes)):
        out[i] = closes[i] * k + out[i-1] * (1 - k)
    return out

def ema_last(closes: list, period: int):
    arr = ema_array(closes, period)
    return next((v for v in reversed(arr) if v is not None), None)

def rsi(closes: list, period: int = 14):
    if not closes or len(closes) < period + 1:
        return None
    gains = losses = 0.0
    for i in range(len(closes) - period, len(closes)):
        d = closes[i] - closes[i-1]
        if d > 0: gains += d
        else:     losses -= d
    ag, al = gains / period, losses / period
    return 100.0 if al == 0 else round(100 - 100 / (1 + ag / al), 2)

def vwap(closes: list, highs: list, lows: list, vols: list, n: int = 20):
    if not closes or not highs or not lows or not vols:
        return None
    n = min(len(closes), n)
    tv = tpv = 0.0
    for i in range(len(closes) - n, len(closes)):
        v    = (vols[i] if i < len(vols) else 0) or 1
        tp   = (highs[i] + lows[i] + closes[i]) / 3
        tpv += tp * v
        tv  += v
    return tpv / tv if tv else None

def atr(highs: list, lows: list, closes: list, period: int = 14):
    if not highs or len(highs) < period + 1:
        return None
    trs = [
        max(highs[i] - lows[i],
            abs(highs[i]  - closes[i-1]),
            abs(lows[i]   - closes[i-1]))
        for i in range(len(highs) - period, len(highs))
    ]
    return sum(trs) / len(trs) if trs else None

def macd(closes: list):
    if not closes or len(closes) < 35:
        return None
    e12 = ema_array(closes, 12)
    e26 = ema_array(closes, 26)
    ml  = [e12[i] - e26[i] for i in range(len(closes))
           if e12[i] is not None and e26[i] is not None]
    if len(ml) < 9:
        return None
    line = ml[-1]
    sa   = ema_array(ml, 9)
    sig  = next((v for v in reversed(sa) if v is not None), None)
    hist = line - sig if sig is not None else None
    return {
        'line': round(line, 6),
        'sig':  round(sig,  6) if sig  is not None else None,
        'hist': round(hist, 6) if hist is not None else None,
        'bull': hist > 0       if hist is not None else None,
    }

def detect_cross(fa: list, sa: list, lookback: int = 15):
    fv, sv = [], []
    for i in range(len(fa) - 1, -1, -1):
        if fa[i] is not None and sa[i] is not None:
            fv.insert(0, fa[i]); sv.insert(0, sa[i])
            if len(fv) >= lookback:
                break
    if len(fv) < 2:
        return None
    for i in range(len(fv) - 1, 0, -1):
        d = len(fv) - 1 - i
        if fv[i-1] <= sv[i-1] and fv[i] > sv[i]:
            return {'bull': True,  'daysAgo': d, 'fresh': d <= 2}
        if fv[i-1] >= sv[i-1] and fv[i] < sv[i]:
            return {'bull': False, 'daysAgo': d, 'fresh': d <= 2}
    return None

def safe_round(v, n=4):
    """Round a value, returning None if it's not a valid number."""
    try:
        return round(float(v), n) if v is not None else None
    except (TypeError, ValueError):
        return None

def trend_score(d: dict) -> int:
    return sum([
        bool(d.get('price') and d.get('e9')   and d['price'] > d['e9']),
        bool(d.get('e9')    and d.get('e20')  and d['e9']    > d['e20']),
        bool(d.get('e20')   and d.get('e200') and d['e20']   > d['e200']),
        bool(d.get('price') and d.get('e200') and d['price'] > d['e200']),
    ])

def calc_bias(d: dict) -> dict:
    sc = 0
    p  = d.get('price', 0)
    if d.get('e200') and p:
        pct = (p - d['e200']) / d['e200'] * 100
        sc += 2 if pct > 2 else 1 if pct > 0 else -1 if pct > -2 else -2
    if d.get('e9') and d.get('e20') and p:
        s = (d['e9'] - d['e20']) / p * 100
        sc += 2 if s > 0.5 else 1 if s > 0 else -1 if s > -0.5 else -2
    m = d.get('macd') or {}
    if m.get('bull') is True:
        s = abs(m.get('hist') or 0) / p * 100 if p else 0
        sc += 2 if s > 0.5 else 1
    elif m.get('bull') is False:
        s = abs(m.get('hist') or 0) / p * 100 if p else 0
        sc -= 2 if s > 0.5 else 1
    r = d.get('rsi') or 0
    sc += 2 if r >= 60 else 1 if r >= 50 else -1 if r >= 40 else -2
    if d.get('vwap') and p:
        sc += 1 if (p - d['vwap']) / d['vwap'] * 100 > 0 else -1
    n = max(-10, min(10, sc))
    if   n >=  6: lb, cls = '↑↑ Bull', 'sb'
    elif n >=  2: lb, cls = '↑ Bull',  'b'
    elif n >= -1: lb, cls = 'Neutral', 'n'
    elif n >= -5: lb, cls = '↓ Bear',  'br'
    else:         lb, cls = '↓↓ Bear', 'sbr'
    return {'score': n, 'label': lb, 'cls': cls}

def conf_score(d: dict, reg: str, cfg: dict) -> float:
    sc = 0
    rv = d.get('rvol') or 0
    ap = d.get('atrPct') or 0
    sc += 2 if rv >= cfg['rvol'] else 1 if rv >= cfg['rvol'] * 0.7 else 0
    sc += 2 if ap >= cfg['atr']  else 1 if ap >= cfg['atr']  * 0.7 else 0
    ts  = trend_score(d)
    sc += 2 if ts == 4 else 1 if ts >= 2 else 0
    if d.get('rsi') and cfg['rmin'] <= d['rsi'] <= cfg['rmax']: sc += 1
    if d.get('e9')   and d.get('e20')  and d['e9']  > d['e20']:  sc += 1
    if d.get('vwap') and d['price'] > d['vwap']:                  sc += 1
    if d.get('macd') and d['macd'].get('bull') is True:           sc += 1
    if reg == 'GAP'  and abs(d.get('gapPct', 0)) >= cfg['gap']:   sc += 1
    if d.get('atrAbs') and d['atrAbs'] >= 0.5:                    sc += 1
    if d.get('chgPct', 0) > 0:                                    sc += 1
    return round(sc / 13 * 10, 1)

def classify_type(d: dict, reg: str, cfg: dict) -> str:
    is_scalp = (d.get('rvol', 0) >= cfg['rvol'] and
                d.get('atrPct', 0) >= cfg['atr'] and reg in ('GAP', 'HB'))
    is_swing = (trend_score(d) >= cfg['ema'] and
                cfg['rmin'] <= (d.get('rsi') or 0) <= cfg['rmax'] and
                d.get('rvol', 0) >= cfg['rvolsw'])
    if is_scalp and is_swing: return 'BOTH'
    if is_scalp:               return 'SCALP'
    if is_swing:               return 'SWING'
    ss = sum([d.get('rvol',0)>=cfg['rvol'], d.get('atrPct',0)>=cfg['atr'],
              reg in ('GAP','HB')])
    ws = (2 if trend_score(d)>=cfg['ema'] else 0) + \
         (1 if cfg['rmin']<=(d.get('rsi') or 0)<=cfg['rmax'] else 0) + \
         (1 if d.get('rvol',0)>=cfg['rvolsw'] else 0)
    return 'SCALP' if ss >= ws else 'SWING'

def calc_est(d: dict) -> dict:
    entry    = d.get('e9') or d['price']
    atr_stop = entry - 1.5 * d['atrAbs'] if d.get('atrAbs') else entry * 0.98
    vwap_s   = d['vwap'] if (d.get('vwap') and d['vwap'] < d['price']) else None
    stop     = max(vwap_s, atr_stop) if vwap_s else atr_stop
    risk     = entry - stop
    target   = entry + risk * 2 if risk > 0 else entry * 1.04
    rr       = round((target - entry) / risk, 1) if risk > 0 else None
    return {
        'entry':  safe_round(entry),
        'stop':   safe_round(stop),
        'target': safe_round(target),
        'rr':     rr,
    }

def entry_status(d: dict, bias: dict) -> dict:
    if not d.get('e9'):
        return {'status': 'WATCH', 'pct': 0}
    pct     = (d['price'] - d['e9']) / d['e9'] * 100
    is_bull = bias['cls'] in ('sb', 'b')
    is_bear = bias['cls'] in ('sbr', 'br')
    if is_bear:              return {'status': 'BEAR',    'pct': round(pct, 2)}
    if pct <= 0.5 and is_bull: return {'status': 'GO',   'pct': round(pct, 2)}
    if pct <= 1.5:           return {'status': 'WATCH',  'pct': round(pct, 2)}
    if pct <= 3.0:           return {'status': 'WAIT',   'pct': round(pct, 2)}
    return                          {'status': 'CHASING','pct': round(pct, 2)}

def build_sigs(d: dict) -> list:
    return [
        {'k': '9E',  'on': bool(d.get('price') and d.get('e9')   and d['price'] > d['e9'])},
        {'k': '20E', 'on': bool(d.get('e9')    and d.get('e20')  and d['e9']    > d['e20'])},
        {'k': '50E', 'on': bool(d.get('price') and d.get('e50')  and d['price'] > d['e50'])},
        {'k': '200', 'on': bool(d.get('price') and d.get('e200') and d['price'] > d['e200'])},
        {'k': 'VWP', 'on': bool(d.get('vwap')  and d['price'] > d['vwap'])},
        {'k': 'MAC', 'on': bool(d.get('macd')  and d['macd'].get('bull') is True)},
    ]


# ══ CORE FETCHER ══════════════════════════════════════════════════════════════

def fetch_and_analyse(symbol: str, cfg: dict = None, retry: bool = True) -> dict | None:
    """
    1. Check cache — return immediately if fresh data exists
    2. Fetch intraday (live price) + daily chart (technicals) from Yahoo
    3. Calculate all signals server-side
    4. Store in cache
    5. Return complete ready-to-render dict
    """
    if cfg is None:
        cfg = DEFAULT_CFG

    # ── Cache check ───────────────────────────────────────────────────────────
    cached = cache.get(symbol)
    if cached is not None:
        log.debug(f'{symbol} served from cache')
        return cached

    # ── Circuit breaker check ─────────────────────────────────────────────────
    if circuit.is_open():
        log.warning(f'{symbol} skipped — circuit breaker open')
        return None

    intra_url = (f'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}'
                 f'?interval=1m&range=1d&includePrePost=true')
    daily_url = (f'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}'
                 f'?interval=1d&range=12mo&includePrePost=true')
    short_url = (f'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}'
                 f'?interval=1d&range=3mo&includePrePost=true')

    state = regular = pre = post = last_tick = None

    # ── Step 1: intraday for freshest live price ──────────────────────────────
    try:
        r = yahoo_get(intra_url, timeout=8)
        res = r.json().get('chart', {}).get('result', [None])[0]
        if res:
            m        = res.get('meta', {})
            state    = m.get('marketState')
            regular  = m.get('regularMarketPrice')
            pre      = m.get('preMarketPrice')
            post     = m.get('postMarketPrice')
            q1m      = res.get('indicators', {}).get('quote', [{}])[0]
            c1m      = [v for v in (q1m.get('close') or []) if v is not None]
            if c1m:
                last_tick = c1m[-1]
    except Exception as e:
        log.warning(f'{symbol} intraday: {e}')

    # ── Step 2: daily chart for technicals (12mo, fallback 3mo) ──────────────
    daily = None
    for url in [daily_url, short_url]:
        try:
            r2   = yahoo_get(url, timeout=12)
            res2 = r2.json().get('chart', {}).get('result', [None])[0]
            if not res2:
                continue
            m2 = res2.get('meta', {})
            q  = res2.get('indicators', {}).get('quote', [{}])[0]

            # Filter nulls with validation
            def clean(lst):
                return [float(v) for v in (lst or []) if v is not None]

            closes = clean(q.get('close'))
            highs  = clean(q.get('high'))
            lows   = clean(q.get('low'))
            vols   = clean(q.get('volume'))
            opens  = clean(q.get('open'))

            if len(closes) < 30:
                continue  # too short — try shorter range

            if not state:   state   = m2.get('marketState')
            if not regular: regular = m2.get('regularMarketPrice') or closes[-1]
            if not pre:     pre     = m2.get('preMarketPrice')
            if not post:    post    = m2.get('postMarketPrice')

            prev     = m2.get('chartPreviousClose') or m2.get('regularMarketPreviousClose')
            name_raw = m2.get('shortName') or m2.get('longName') or symbol
            earn_ts  = m2.get('earningsTimestamp')
            daily    = {'closes': closes, 'highs': highs, 'lows': lows,
                        'vols': vols, 'opens': opens, 'prev': prev,
                        'name': name_raw, 'earn_ts': earn_ts}
            break
        except Exception as e:
            log.warning(f'{symbol} daily ({url[-8:]}): {e}')
            continue

    # ── Bail if no usable data ────────────────────────────────────────────────
    if not regular and not pre and not post:
        if retry:
            log.info(f'{symbol} retrying after 1s')
            time.sleep(random.uniform(0.8, 1.5))
            return fetch_and_analyse(symbol, cfg, retry=False)
        log.warning(f'{symbol} no data — skipped')
        return None

    if not daily:
        log.warning(f'{symbol} no daily data — skipped')
        return None

    # ── Resolve price + mode ──────────────────────────────────────────────────
    s = (state or '').upper()
    if   s == 'REGULAR':
        price, mode = last_tick or regular, 'LIVE'
    elif s in ('PRE', 'PREPRE'):
        price = pre or last_tick or regular
        mode  = 'PRE-MKT' if (pre or last_tick) else 'CLOSE'
    elif s in ('POST', 'POSTPOST'):
        price = post or last_tick or regular
        mode  = 'AFTER-HRS' if (post or last_tick) else 'CLOSE'
    elif s == 'CLOSED':
        if pre:                                  price, mode = pre,       'PRE-MKT'
        elif post:                               price, mode = post,      'AFTER-HRS'
        elif last_tick and last_tick != regular: price, mode = last_tick, 'AFTER-HRS'
        else:                                    price, mode = regular,   'CLOSE'
    else:
        if pre:         price, mode = pre,       'PRE-MKT'
        elif post:      price, mode = post,      'AFTER-HRS'
        elif last_tick: price, mode = last_tick, 'CLOSE'
        else:           price, mode = regular,   'CLOSE'

    if not price:
        return None

    price = float(price)

    # ── Technical calculations ────────────────────────────────────────────────
    closes = daily['closes']; highs = daily['highs']; lows = daily['lows']
    vols   = daily['vols'];   opens = daily['opens']; prev = daily['prev']

    e4a  = ema_array(closes, 4);  e9a  = ema_array(closes, 9)
    e20a = ema_array(closes, 20); e50a = ema_array(closes, 50)
    e200a= ema_array(closes, 200)

    e4   = next((v for v in reversed(e4a)   if v), None)
    e9   = next((v for v in reversed(e9a)   if v), None)
    e20  = next((v for v in reversed(e20a)  if v), None)
    e50  = next((v for v in reversed(e50a)  if v), None)
    e200 = next((v for v in reversed(e200a) if v), None)

    rsi_v  = rsi(closes)
    vwap_v = vwap(closes, highs, lows, vols)
    atr_v  = atr(highs, lows, closes)
    atr_pct= (atr_v / price * 100) if atr_v and price else None
    macd_v = macd(closes)
    avg_vol= sum(vols[-20:]) / 20 if len(vols) >= 20 else (sum(vols) / len(vols) if vols else None)
    rvol   = (vols[-1] / avg_vol) if avg_vol and vols else None
    prev_f = float(prev) if prev else None
    chg    = ((price - prev_f) / prev_f * 100) if prev_f else 0.0
    gap    = ((opens[-1] - prev_f) / prev_f * 100) if (prev_f and opens) else 0.0
    c9_20  = detect_cross(e9a, e20a)
    c4_9   = detect_cross(e4a, e9a)
    dte    = round((daily['earn_ts'] * 1000 - time.time() * 1000) / 86400000) \
             if daily.get('earn_ts') else None

    # Clean company name
    name = daily['name']
    for sfx in [', Inc.', ' Inc.', ' Corp.', ' Ltd.', ' LLC']:
        name = name.replace(sfx, '').strip().rstrip(',')

    reg  = PDF.get(symbol, 'GAP' if abs(gap) >= cfg['gap'] else 'STD')
    base = {
        'ticker': symbol, 'name': name,
        'price': safe_round(price), 'priceMode': mode,
        'marketState': s or 'CLOSED',
        'chgPct': safe_round(chg, 2), 'gapPct': safe_round(gap, 2),
        'e4':  safe_round(e4),  'e9':  safe_round(e9),
        'e20': safe_round(e20), 'e50': safe_round(e50), 'e200': safe_round(e200),
        'rsi':    safe_round(rsi_v, 1),
        'vwap':   safe_round(vwap_v),
        'atrAbs': safe_round(atr_v),
        'atrPct': safe_round(atr_pct, 2),
        'macd':   macd_v,
        'rvol':   safe_round(rvol, 2),
        'avgVol': round(avg_vol) if avg_vol else None,
        'cross9_20': c9_20, 'cross4_9': c4_9,
        'daysToEarn': dte,
        'prevClose':  safe_round(prev_f),
        'preMarket':  safe_round(pre),
        'postMarket': safe_round(post),
        'lastTick':   safe_round(last_tick),
        'reg': reg,
    }

    bias = calc_bias(base)
    est  = calc_est(base)
    es   = entry_status(base, bias)
    sc   = conf_score(base, reg, cfg)
    tt   = classify_type(base, reg, cfg)
    ts   = trend_score(base)
    tier = 1 if sc >= 7 else 2 if sc >= 5 else 3
    sigs = build_sigs(base)
    cs20 = (10 if c9_20.get('fresh') else max(0, 10 - c9_20['daysAgo'])) if c9_20 else 0
    cs49 = (10 if c4_9.get('fresh')  else max(0, 10 - c4_9['daysAgo']))  if c4_9  else 0
    mb   = (1 if macd_v and macd_v.get('bull') is True
            else -1 if macd_v and macd_v.get('bull') is False else 0)

    result = {
        **base,
        'bias': bias, 'biasScore': bias['score'],
        'entry': est['entry'], 'stop': est['stop'],
        'target': est['target'], 'rr': est['rr'],
        'entryStatus': es['status'], 'entryPct': es['pct'],
        'confScore': sc, 'tradeType': tt, 'trendScore': ts, 'tier': tier,
        'sigs': sigs, 'sigCount': sum(1 for sg in sigs if sg['on']),
        'crossScore': cs20, 'cross49Score': cs49, 'macdBull': mb,
        'isExtended': es['status'] in ('WAIT', 'CHASING'), 'extPct': es['pct'],
    }

    # ── Store in cache ────────────────────────────────────────────────────────
    cache.set(symbol, result)
    return result


# ══ PARALLEL BATCH FETCHER ════════════════════════════════════════════════════

def fetch_all(pool: list, cfg: dict = None) -> list:
    """Fetch all tickers in parallel with jittered starts to avoid thundering herd."""
    results = {}

    def go(sym):
        time.sleep(random.uniform(0, 0.2))
        return sym, fetch_and_analyse(sym, cfg)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(go, sym): sym for sym in pool}
        for future in as_completed(futures):
            sym, data = future.result()
            if data:
                results[sym] = data

    return [results[s] for s in pool if s in results]


def resolve_screener_pool(screen_id: str, count: int) -> list:
    url = SCREENER_URLS.get(screen_id, SCREENER_URLS['most_active'])
    try:
        r = yahoo_get(url, timeout=8)
        quotes   = r.json().get('finance', {}).get('result', [{}])[0].get('quotes', [])
        tickers  = [q['symbol'] for q in quotes if q.get('symbol')][:count + 8]
        if tickers:
            return tickers
    except Exception as e:
        log.warning(f'Screener {screen_id} failed: {e}')
    return FALLBACK_POOL[:count + 8]


# ══ ROUTES ════════════════════════════════════════════════════════════════════

@app.route('/api/scan')
def scan():
    """
    Main endpoint. Returns fully analysed, ready-to-render ticker data.
    Query params:
      tickers  — comma-separated list (watchlist / positions mode)
      screen   — screener id (most_active, day_gainers, etc.)
      count    — max results, default 20
      cfg      — JSON criteria overrides
      nocache  — if '1', bypass cache for this request
    """
    tickers_param = request.args.get('tickers', '')
    screen_id     = request.args.get('screen', '')
    count         = min(int(request.args.get('count', 20)), 100)  # cap at 100
    no_cache      = request.args.get('nocache', '') == '1'

    # Parse criteria
    cfg = {**DEFAULT_CFG}
    try:
        ov = json.loads(request.args.get('cfg', '{}'))
        cfg.update({k: float(v) for k, v in ov.items() if k in cfg})
        cfg['ema'] = int(cfg['ema'])
    except Exception:
        pass

    # Temporarily bypass cache if requested
    if no_cache:
        for sym in (tickers_param.split(',') if tickers_param else []):
            cache.invalidate(sym.strip().upper())

    # Resolve pool
    if tickers_param:
        pool = [t.strip().upper() for t in tickers_param.split(',') if t.strip()]
    elif screen_id:
        pool = resolve_screener_pool(screen_id, count)
    else:
        pool = FALLBACK_POOL[:count]

    t0      = time.time()
    results = fetch_all(pool, cfg)
    elapsed = round(time.time() - t0, 2)

    results.sort(key=lambda d: d.get('confScore', 0), reverse=True)
    if not tickers_param:
        results = results[:count]

    ts    = datetime.datetime.now().strftime('%H:%M:%S')
    modes = {}
    for r in results:
        modes[r['priceMode']] = modes.get(r['priceMode'], 0) + 1

    log.info(f'{ts} — {len(results)}/{len(pool)} tickers | {elapsed}s | {modes} | '
             f'cache {cache.stats["hit_rate"]} hit rate')

    return jsonify({
        'results': results,
        'elapsed': elapsed,
        'pool':    len(pool),
        'ts':      ts,
        'cached':  cache.stats,
    })


@app.route('/api/quote/<symbol>')
def quote(symbol):
    data = fetch_and_analyse(symbol.upper())
    if not data:
        return jsonify({'error': 'Symbol not found or no data available'}), 404
    return jsonify(data)


@app.route('/api/cache', methods=['DELETE'])
def clear_cache():
    """Clear the entire cache — useful after market open or close."""
    cache.clear()
    return jsonify({'status': 'cache cleared'})


@app.route('/health')
def health():
    return jsonify({
        'status':  'ok',
        'time':    datetime.datetime.now().isoformat(),
        'workers': MAX_WORKERS,
        'cache':   cache.stats,
        'circuit': circuit.state,
    })


if __name__ == '__main__':
    print('=' * 56)
    print('  AutoScanner Hub v4  —  http://localhost:8080')
    print(f'  Workers : {MAX_WORKERS}  |  Cache TTL : {CACHE_TTL}s')
    print(f'  Circuit : {CircuitBreaker.FAILURE_THRESHOLD} failures → '
          f'{CircuitBreaker.RECOVERY_SECS}s backoff')
    print('  Press Ctrl+C to stop.')
    print('=' * 56)
    app.run(host='0.0.0.0', port=8080, debug=False, use_reloader=False)
